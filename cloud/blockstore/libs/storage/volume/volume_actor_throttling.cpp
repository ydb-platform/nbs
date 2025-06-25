#include "volume_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TThrottlingRequestInfo BuildThrottlingRequestInfo(
    ui32, const T&, const ui32)
{
    return {};
}

TThrottlingRequestInfo BuildThrottlingRequestInfo(
    const ui32 blockSize,
    const TEvService::TEvReadBlocksRequest& request,
    const ui32 policyVersion)
{
    return {
        IntegerCast<ui32>(CalculateBytesCount(request.Record, blockSize)),
        static_cast<ui32>(TVolumeThrottlingPolicy::EOpType::Read),
        policyVersion,
    };
}

TThrottlingRequestInfo BuildThrottlingRequestInfo(
    const ui32 blockSize,
    const TEvService::TEvWriteBlocksRequest& request,
    const ui32 policyVersion)
{
    return {
        IntegerCast<ui32>(CalculateBytesCount(request.Record, blockSize)),
        static_cast<ui32>(TVolumeThrottlingPolicy::EOpType::Write),
        policyVersion,
    };
}

TThrottlingRequestInfo BuildThrottlingRequestInfo(
    const ui32 blockSize,
    const TEvService::TEvZeroBlocksRequest& request,
    const ui32 policyVersion)
{
    return {
        IntegerCast<ui32>(CalculateBytesCount(request.Record, blockSize)),
        static_cast<ui32>(TVolumeThrottlingPolicy::EOpType::Zero),
        policyVersion,
    };
}

TThrottlingRequestInfo BuildThrottlingRequestInfo(
    const ui32 blockSize,
    const TEvService::TEvReadBlocksLocalRequest& request,
    const ui32 policyVersion)
{
    return {
        blockSize * request.Record.GetBlocksCount(),
        static_cast<ui32>(TVolumeThrottlingPolicy::EOpType::Read),
        policyVersion,
    };
}

TThrottlingRequestInfo BuildThrottlingRequestInfo(
    const ui32 blockSize,
    const TEvService::TEvWriteBlocksLocalRequest& request,
    const ui32 policyVersion)
{
    return {
        blockSize * request.Record.BlocksCount,
        static_cast<ui32>(TVolumeThrottlingPolicy::EOpType::Write),
        policyVersion,
    };
}

TThrottlingRequestInfo BuildThrottlingRequestInfo(
    const ui32 blockSize,
    const TEvVolume::TEvDescribeBlocksRequest& request,
    const ui32 policyVersion)
{
    return {
        blockSize * request.Record.GetBlocksCountToRead(),
        static_cast<ui32>(TVolumeThrottlingPolicy::EOpType::Describe),
        policyVersion,
    };
}

template <typename TMethod>
bool GetThrottlingEnabled(
    const TStorageConfig& config,
    const NProto::TPartitionConfig& partitionConfig)
{
    if constexpr (IsZeroMethod<TMethod>) {
        return GetThrottlingEnabledZeroBlocks(config, partitionConfig);
    }

    return GetThrottlingEnabled(config, partitionConfig);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::UpdateDelayCounter(
    TVolumeThrottlingPolicy::EOpType opType,
    TDuration time)
{
    if (!VolumeSelfCounters) {
        return;
    }
    switch (opType) {
        case TVolumeThrottlingPolicy::EOpType::Read:
            VolumeSelfCounters->ThrottlerDelayRequestCounters.ReadBlocks
                .Increment(time.MicroSeconds());
            return;
        case TVolumeThrottlingPolicy::EOpType::Write:
            VolumeSelfCounters->ThrottlerDelayRequestCounters.WriteBlocks
                .Increment(time.MicroSeconds());
            return;
        case TVolumeThrottlingPolicy::EOpType::Zero:
            VolumeSelfCounters->ThrottlerDelayRequestCounters.ZeroBlocks
                .Increment(time.MicroSeconds());
            return;
        case TVolumeThrottlingPolicy::EOpType::Describe:
            VolumeSelfCounters->ThrottlerDelayRequestCounters.DescribeBlocks
                .Increment(time.MicroSeconds());
            return;
        case TVolumeThrottlingPolicy::EOpType::Last:
        default:
            Y_DEBUG_ABORT_UNLESS(0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleBackpressureReport(
    const NPartition::TEvPartition::TEvBackpressureReport::TPtr& ev,
    const TActorContext& ctx)
{
    auto index = State->FindPartitionIndex(ev->Sender);
    if (!index) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Partition %s for disk %s backpressure report not found",
            ToString(ev->Sender).c_str(),
            State->GetDiskId().Quote().c_str());

        index = State->GetPartitions().size();
    }

    auto& policy = State->AccessThrottlingPolicy();
    policy.OnBackpressureReport(ctx.Now(), *ev->Get(), *index);
}

void TVolumeActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr&,
    const TActorContext& ctx)
{
    Throttler->StartFlushing(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
NProto::TError TVolumeActor::Throttle(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    bool throttlingDisabled)
{
    static const auto ok = MakeError(S_OK);
    static const auto err = MakeError(E_BS_THROTTLED, "Throttled");

    if (!RequiresThrottling<TMethod>
            || throttlingDisabled
            || !GetThrottlingEnabled<TMethod>(*Config, State->GetConfig()))
    {
        return ok;
    }

    auto* msg = ev->Get();

    const auto& tp = State->GetThrottlingPolicy();
    const auto requestInfo = BuildThrottlingRequestInfo(
        State->GetConfig().GetBlockSize(),
        *msg,
        tp.GetVersion()
    );

    if (static_cast<TVolumeThrottlingPolicy::EOpType>(requestInfo.OpType) ==
            TVolumeThrottlingPolicy::EOpType::Describe &&
        requestInfo.ByteCount == 0)
    {
        // DescribeBlocks with zero weight should not be affected by
        // throttling limits.
        return ok;
    }

    const auto status = Throttler->Throttle(
        ctx,
        msg->CallContext,
        requestInfo,
        [&ev]() { return NActors::IEventHandlePtr(ev.Release()); },
        TMethod::Name);

    switch (status) {
        case ETabletThrottlerStatus::POSTPONED:
            VolumeSelfCounters->Cumulative.ThrottlerPostponedRequests.Increment(1);
            break;
        case ETabletThrottlerStatus::ADVANCED:
            break;
        case ETabletThrottlerStatus::REJECTED:
            VolumeSelfCounters->Cumulative.ThrottlerRejectedRequests.Increment(1);
            return err;
        default:
            Y_DEBUG_ABORT_UNLESS(false);
    }

    return ok;
}

////////////////////////////////////////////////////////////////////////////////

#define GENERATE_IMPL(name, ns)                                                \
template NProto::TError TVolumeActor::Throttle<                                \
    ns::T##name##Method>(                                                      \
        const TActorContext& ctx,                                              \
        const ns::TEv##name##Request::TPtr& ev,                                \
        bool throttlingDisabled);                                              \
// GENERATE_IMPL

GENERATE_IMPL(ReadBlocks,            TEvService)
GENERATE_IMPL(WriteBlocks,           TEvService)
GENERATE_IMPL(ZeroBlocks,            TEvService)
GENERATE_IMPL(CreateCheckpoint,      TEvService)
GENERATE_IMPL(DeleteCheckpoint,      TEvService)
GENERATE_IMPL(GetChangedBlocks,      TEvService)
GENERATE_IMPL(GetCheckpointStatus,   TEvService)
GENERATE_IMPL(ReadBlocksLocal,       TEvService)
GENERATE_IMPL(WriteBlocksLocal,      TEvService)

GENERATE_IMPL(DescribeBlocks,           TEvVolume)
GENERATE_IMPL(GetUsedBlocks,            TEvVolume)
GENERATE_IMPL(GetPartitionInfo,         TEvVolume)
GENERATE_IMPL(CompactRange,             TEvVolume)
GENERATE_IMPL(GetCompactionStatus,      TEvVolume)
GENERATE_IMPL(DeleteCheckpointData,     TEvVolume)
GENERATE_IMPL(RebuildMetadata,          TEvVolume)
GENERATE_IMPL(GetRebuildMetadataStatus, TEvVolume)
GENERATE_IMPL(ScanDisk,                 TEvVolume)
GENERATE_IMPL(GetScanDiskStatus,        TEvVolume)
GENERATE_IMPL(CheckRange,               TEvVolume)

}   // namespace NCloud::NBlockStore::NStorage
