#include "part_nonrepl_actor.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TNonreplicatedPartitionActor::TNonreplicatedPartitionActor(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId statActorId)
    : Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , StatActorId(statActorId)
    , DeviceStats(PartConfig->GetDevices().size())
    , PartCounters(
        CreatePartitionDiskCounters(EPublishingPolicy::DiskRegistryBased))
{}

TNonreplicatedPartitionActor::~TNonreplicatedPartitionActor() = default;

TDuration TNonreplicatedPartitionActor::GetMinRequestTimeout() const
{
    const auto hddKind = NProto::STORAGE_MEDIA_HDD_NONREPLICATED;
    if (PartConfig->GetVolumeInfo().MediaKind == hddKind) {
        return Config->GetNonReplicatedMinRequestTimeoutHDD();
    }

    return Config->GetNonReplicatedMinRequestTimeoutSSD();
}

TDuration TNonreplicatedPartitionActor::GetMaxRequestTimeout() const
{
    const auto hddKind = NProto::STORAGE_MEDIA_HDD_NONREPLICATED;
    if (PartConfig->GetVolumeInfo().MediaKind == hddKind) {
        return Config->GetNonReplicatedMaxRequestTimeoutHDD();
    }

    return Config->GetNonReplicatedMaxRequestTimeoutSSD();
}

void TNonreplicatedPartitionActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    ScheduleCountersUpdate(ctx);
    ctx.Schedule(GetMinRequestTimeout(), new TEvents::TEvWakeup());
}

bool TNonreplicatedPartitionActor::CheckReadWriteBlockRange(const TBlockRange64& range) const
{
    return range.End >= range.Start && PartConfig->GetBlockCount() > range.End;
}

void TNonreplicatedPartitionActor::ScheduleCountersUpdate(const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvNonreplPartitionPrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }
}

bool TNonreplicatedPartitionActor::IsInflightLimitReached() const
{
    return RequestsInProgress.GetRequestCount() >=
           Config->GetNonReplicatedInflightLimit();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
bool TNonreplicatedPartitionActor::InitRequests(
    const typename TMethod::TRequest& msg,
    const NActors::TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequest* request)
{
    auto reply = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        NProto::TError error)
    {
        auto response = std::make_unique<typename TMethod::TResponse>(
            std::move(error));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            TMethod::Name,
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (IsInflightLimitReached()) {
        reply(
            ctx,
            requestInfo,
            PartConfig->MakeError(E_REJECTED, "Inflight limit reached"));
        return false;
    }

    if (!CheckReadWriteBlockRange(blockRange)) {
        reply(
            ctx,
            requestInfo,
            PartConfig->MakeError(E_ARGUMENT, TStringBuilder()
                << "invalid block range ["
                << "index: " << blockRange.Start
                << ", count: " << blockRange.Size()
                << "]"));
        return false;
    }

    if (!msg.Record.GetHeaders().GetIsBackgroundRequest()
            && RequiresReadWriteAccess<TMethod>)
    {
        TString errorMessage;
        bool cooldownPassed = true;
        if (PartConfig->IsReadOnly()) {
            errorMessage = "disk in error state";
        } else if (HasBrokenDevice) {
            errorMessage = "disk has broken device";
            cooldownPassed = IOErrorCooldownPassed(ctx.Now());
        }

        if (errorMessage){
            reply(
                ctx,
                requestInfo,
                PartConfig->MakeIOError(
                    std::move(errorMessage),
                    cooldownPassed));
            return false;
        }
    } else if (RequiresCheckpointSupport(msg.Record)) {
        reply(
            ctx,
            requestInfo,
            PartConfig->MakeError(E_ARGUMENT, "checkpoints not supported"));
        return false;
    }

    *deviceRequests = PartConfig->ToDeviceRequests(blockRange);

    if (deviceRequests->empty()) {
        // block range contains only dummy devices
        reply(ctx, requestInfo, NProto::TError());
        return false;
    }

    request->Ts = ctx.Now();
    request->Timeout = GetMinRequestTimeout();
    for (const auto& dr: *deviceRequests) {
        const auto& deviceStat = DeviceStats[dr.DeviceIdx];
        auto maxTimedOutDeviceStateDuration =
            PartConfig->GetMaxTimedOutDeviceStateDuration();
        if (!maxTimedOutDeviceStateDuration) {
            maxTimedOutDeviceStateDuration =
                Config->GetMaxTimedOutDeviceStateDuration();
        }

        TString errorMessage;
        bool cooldownPassed = true;
        if (!msg.Record.GetHeaders().GetIsBackgroundRequest()
            && deviceStat.TimedOutStateDuration > maxTimedOutDeviceStateDuration)
        {
            errorMessage = TStringBuilder() << "broken device requested: "
                << dr.Device.GetDeviceUUID();
            if (!BrokenTransitionTs) {
                BrokenTransitionTs = ctx.Now();
            }
            cooldownPassed = IOErrorCooldownPassed(ctx.Now());
        } else if (dr.Device.GetNodeId() == 0) {
            errorMessage = TStringBuilder() << "unavailable device requested: "
                << dr.Device.GetDeviceUUID();
        }

        if (errorMessage) {
            HasBrokenDevice = true;

            reply(
                ctx,
                requestInfo,
                PartConfig->MakeIOError(
                    std::move(errorMessage),
                    cooldownPassed));
            return false;
        }

        request->DeviceIndices.push_back(dr.DeviceIdx);
        if (deviceStat.CurrentTimeout > request->Timeout &&
            !deviceStat.DeviceIsUnavailable)
        {
            request->Timeout = deviceStat.CurrentTimeout;
        }
    }

    return true;
}

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TWriteBlocksMethod>(
    const TEvService::TWriteBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequest* request);

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TWriteBlocksLocalMethod>(
    const TEvService::TWriteBlocksLocalMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequest* request);

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TZeroBlocksMethod>(
    const TEvService::TZeroBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequest* request);

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TReadBlocksMethod>(
    const TEvService::TReadBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequest* request);

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TReadBlocksLocalMethod>(
    const TEvService::TReadBlocksLocalMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequest* request);

template bool TNonreplicatedPartitionActor::InitRequests<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
    const TEvNonreplPartitionPrivate::TChecksumBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequest* request);

void TNonreplicatedPartitionActor::OnResponse(
    ui32 deviceIndex,
    TDuration responseTime)
{
    auto& stat = DeviceStats[deviceIndex];
    stat.LastTimeoutTs = {};
    stat.TimedOutStateDuration = {};
    stat.CurrentTimeout = {};
    stat.ExpectedClientBackoff = {};
    auto& rt = stat.ResponseTimes;
    rt.PushBack(responseTime);
    for (ui32 i = rt.FirstIndex(); i < rt.TotalSize(); ++i) {
        stat.CurrentTimeout = Max(stat.CurrentTimeout, rt[i]);
    }
    stat.CurrentTimeout += GetMinRequestTimeout();
}

bool TNonreplicatedPartitionActor::IOErrorCooldownPassed(const TInstant now) const
{
    return (now - BrokenTransitionTs) > Config->GetNonReplicatedAgentMaxTimeout();
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleUpdateCounters(
    const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCountersScheduled = false;

    SendStats(ctx);
    ScheduleCountersUpdate(ctx);
}

void TNonreplicatedPartitionActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto now = ctx.Now();

    for (const auto& [actorId, requestInfo]: RequestsInProgress.AllRequests()) {
        const auto& request = requestInfo.Value;
        if (now - request.Ts < request.Timeout) {
            continue;
        }

        NCloud::Send<TEvNonreplPartitionPrivate::TEvCancelRequest>(
            ctx,
            actorId,
            0,  // cookie
            TEvNonreplPartitionPrivate::TCancelRequest::EReason::Timeouted);

        for (const auto i: request.DeviceIndices) {
            auto& deviceStat = DeviceStats[i];
            deviceStat.ExpectedClientBackoff +=
                Config->GetExpectedClientBackoffIncrement();
            deviceStat.TimedOutStateDuration += Min(
                request.Timeout + deviceStat.ExpectedClientBackoff,
                now - deviceStat.LastTimeoutTs
            );
            if (!deviceStat.CurrentTimeout.GetValue()) {
                deviceStat.CurrentTimeout = GetMinRequestTimeout();
            }
            deviceStat.CurrentTimeout = Min(
                GetMaxRequestTimeout(),
                deviceStat.CurrentTimeout + Min(
                    request.Timeout,
                    now - deviceStat.LastTimeoutTs
                )
            );
            deviceStat.LastTimeoutTs = now;

            Y_DEBUG_ABORT_UNLESS(PartConfig->GetDevices().size() <= i);
            LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                "Updated deviceStat: DeviceIndex=" << i
                << ", DeviceUUID=" << PartConfig->GetDevices()[i].GetDeviceUUID()
                << ", ExpectedClientBackoff=" << deviceStat.ExpectedClientBackoff
                << ", TimedOutStateDuration=" << deviceStat.TimedOutStateDuration
                << ", CurrentTimeout=" << deviceStat.CurrentTimeout
                << ", LastTimeoutTs=" << deviceStat.LastTimeoutTs);

            if (deviceStat.TimedOutStateDuration > TDuration::Seconds(3) &&
                !deviceStat.DeviceIsUnavailable)
            {
                auto request =
                    std::make_unique<TEvVolume::TEvDeviceTimeoutedRequest>(
                        i,
                        PartConfig->GetDevices()[i].GetDeviceUUID());
                NCloud::Send(
                    ctx,
                    PartConfig->GetParentActorId(),
                    std::move(request));
            }
        }
    }

    ctx.Schedule(GetMinRequestTimeout(), new TEvents::TEvWakeup());
}

void TNonreplicatedPartitionActor::HandleAgentIsUnavailable(
    const NPartition::TEvPartition::TEvAgentIsUnavailable::TPtr& ev,
    const TActorContext& ctx)
{

    Y_UNUSED(ctx);
    const auto* msg = ev->Get();

    for (int i = 0; i < PartConfig->GetDevices().size(); ++i) {
        if (PartConfig->GetDevices().at(i).GetAgentId() == msg->AgentId) {
            DeviceStats[i].DeviceIsUnavailable = true;
        }
    }

    for (const auto& [actorId, requestInfo]: RequestsInProgress.AllRequests()) {
        for (int deviceIndex : requestInfo.Value.DeviceIndices) {
            if (PartConfig->GetDevices()[deviceIndex].GetAgentId() == msg->AgentId) {

                // Reject the request.
                // TODO: implement fast reject (via error flags).
                NCloud::Send<TEvents::TEvWakeup>(ctx, actorId);
                break;
            }
        }
    }
}

void TNonreplicatedPartitionActor::HandleAgentIsBackOnline(
    const NPartition::TEvPartition::TEvAgentIsBackOnline::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();

    for (int i = 0; i < PartConfig->GetDevices().size(); ++i) {
        if (PartConfig->GetDevices().at(i).GetAgentId() == msg->AgentId) {
            DeviceStats[i].DeviceIsUnavailable = false;
        }
    }
}

void TNonreplicatedPartitionActor::ReplyAndDie(const NActors::TActorContext& ctx)
{
    NCloud::Reply(ctx, *Poisoner, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

void TNonreplicatedPartitionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Become(&TThis::StateZombie);

    Poisoner = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    if (!RequestsInProgress.Empty()) {
        return;
    }

    ReplyAndDie(ctx);
}

bool TNonreplicatedPartitionActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        // TODO

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(name, ns)                      \
    void TNonreplicatedPartitionActor::Handle##name(                           \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        RejectUnimplementedRequest<ns::T##name##Method>(ev, ctx);              \
    }                                                                          \
                                                                               \
// BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST

BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(DescribeBlocks,           TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(CompactRange,             TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetCompactionStatus,      TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(RebuildMetadata,          TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetRebuildMetadataStatus, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(ScanDisk,                 TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetScanDiskStatus,        TEvVolume);

////////////////////////////////////////////////////////////////////////////////

/*
STFUNC(TNonreplicatedPartitionActor::StateInit)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            break;
    }
}
*/

STFUNC(TNonreplicatedPartitionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, DrainActorCompanion.HandleDrain);

        HFunc(NPartition::TEvPartition::TEvAgentIsUnavailable, HandleAgentIsUnavailable);
        HFunc(NPartition::TEvPartition::TEvAgentIsBackOnline, HandleAgentIsBackOnline);

        HFunc(
            TEvService::TEvGetChangedBlocksRequest,
            GetChangedBlocksCompanion.HandleGetChangedBlocks);

        HFunc(TEvNonreplPartitionPrivate::TEvReadBlocksCompleted, HandleReadBlocksCompleted);
        HFunc(TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted, HandleWriteBlocksCompleted);
        HFunc(TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted, HandleZeroBlocksCompleted);

        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest, HandleChecksumBlocks);
        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted, HandleChecksumBlocksCompleted);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, HandleGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, HandleCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, HandleRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, HandleGetRebuildMetadataStatus);
        HFunc(TEvVolume::TEvScanDiskRequest, HandleScanDisk);
        HFunc(TEvVolume::TEvGetScanDiskStatusRequest, HandleGetScanDiskStatus);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvVolume::TEvDeviceTimeoutedResponse);
        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            }
            break;
    }
}

STFUNC(TNonreplicatedPartitionActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);

        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvService::TEvReadBlocksRequest, RejectReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, RejectWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, RejectZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, RejectReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, RejectWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, RejectDrain);

        HFunc(TEvNonreplPartitionPrivate::TEvReadBlocksCompleted, HandleReadBlocksCompleted);
        HFunc(TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted, HandleWriteBlocksCompleted);
        HFunc(TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted, HandleZeroBlocksCompleted);

        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest, RejectChecksumBlocks);
        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted, HandleChecksumBlocksCompleted);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, RejectDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, RejectGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, RejectCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, RejectRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, RejectGetRebuildMetadataStatus);
        HFunc(TEvVolume::TEvScanDiskRequest, RejectScanDisk);
        HFunc(TEvVolume::TEvGetScanDiskStatusRequest, RejectGetScanDiskStatus);

        IgnoreFunc(TEvents::TEvPoisonPill);
        IgnoreFunc(TEvVolume::TEvDeviceTimeoutedResponse);
        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            }
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
