#include "part_nonrepl_actor.h"

#include "part_nonrepl_common.h"

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

using EReason = TEvNonreplPartitionPrivate::TCancelRequest::EReason;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TNonreplicatedPartitionActor::TNonreplicatedPartitionActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId statActorId)
    : Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , PartConfig(std::move(partConfig))
    , StatActorId(statActorId)
    , DeviceStats(PartConfig->GetDevices().size())
    , PartCounters(CreatePartitionDiskCounters(
          EPublishingPolicy::DiskRegistryBased,
          DiagnosticsConfig->GetHistogramCounterOptions()))
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

TDuration
TNonreplicatedPartitionActor::GetMaxTimedOutDeviceStateDuration() const
{
    auto maxTimedOutDeviceStateDuration =
        PartConfig->GetMaxTimedOutDeviceStateDuration();

    if (!maxTimedOutDeviceStateDuration) {
        maxTimedOutDeviceStateDuration =
            Config->GetMaxTimedOutDeviceStateDuration();
    }
    return maxTimedOutDeviceStateDuration;
}

bool TNonreplicatedPartitionActor::CalculateHasBrokenDeviceCounterValue(
    const NActors::TActorContext& ctx,
    bool silent) const
{
    if (!silent && PartConfig->GetMuteIOErrors()) {
        return false;
    }

    return AnyOf(
        DeviceStats,
        [&](const TDeviceStat& stat)
        {
            return stat.DeviceStatus == EDeviceStatus::Broken ||
                   (stat.DeviceStatus == EDeviceStatus::SilentBroken &&
                    (silent || stat.CooldownPassed(
                                   ctx.Now(),
                                   Config->GetNonReplicatedAgentMaxTimeout())));
        });
}

TRequestTimeoutPolicy TNonreplicatedPartitionActor::MakeTimeoutPolicyForRequest(
    const TVector<TDeviceRequest>& deviceRequests,
    TInstant now,
    bool isBackground) const
{
    TDuration longestTimedOutStateDuration = {};
    TDuration worstRequestTime = {};
    EDeviceStatus worstDeviceStatus = EDeviceStatus::Ok;
    for (const auto& dr: deviceRequests) {
        const auto& deviceStats = DeviceStats[dr.DeviceIdx];

        longestTimedOutStateDuration =
            Max(longestTimedOutStateDuration,
                deviceStats.GetTimedOutStateDuration(now));
        worstRequestTime =
            Max(worstRequestTime, deviceStats.WorstRequestTime());

        if (deviceStats.DeviceStatus == EDeviceStatus::SilentBroken &&
            deviceStats.CooldownPassed(
                now,
                Config->GetNonReplicatedAgentMaxTimeout()))
        {
            worstDeviceStatus = EDeviceStatus::Broken;
        } else {
            worstDeviceStatus =
                Max(worstDeviceStatus, deviceStats.DeviceStatus);
        }
    }

    const bool hasTimeouts = worstDeviceStatus != EDeviceStatus::Ok ||
                             longestTimedOutStateDuration != TDuration();
    // When a device is unavailable, we shouldn't increase timeouts. There won't
    // be any huge requests, just small probings.
    if (!hasTimeouts || worstDeviceStatus == EDeviceStatus::Unavailable) {
        // If there were no timeouts, then we slightly increase the timeout for
        // the time of the longest response among the last ones.
        return TRequestTimeoutPolicy{
            .Timeout = GetMinRequestTimeout() + worstRequestTime,
            .ErrorCode = E_TIMEOUT,
            .OverrideMessage = {}};
    }

    // If there was a timeout, then add more time as long as we are in the
    // timed out state.
    TRequestTimeoutPolicy policy{
        .Timeout =
            Min(GetMaxRequestTimeout(),
                GetMinRequestTimeout() + longestTimedOutStateDuration),
        .ErrorCode = E_TIMEOUT,
        .OverrideMessage = {}};

    if (isBackground) {
        // For background requests always response with E_TIMEOUT.
        return policy;
    }

    // Setting up which error to respond to the request in case of a timeout for
    // user requests.
    auto makeMessage = [&]()
    {
        TString devices;
        for (const auto& dr: deviceRequests) {
            if (devices) {
                devices += ", ";
            }
            devices += dr.Device.GetDeviceUUID().Quote();
        }
        return "broken devices requested: [" + devices + "]";
    };

    switch (worstDeviceStatus) {
        case EDeviceStatus::Ok:
        case EDeviceStatus::Unavailable: {
            break;
        }
        case EDeviceStatus::SilentBroken: {
            policy.OverrideMessage = makeMessage();
            policy.ErrorCode = E_IO_SILENT;
            break;
        }
        case EDeviceStatus::Broken: {
            policy.OverrideMessage = makeMessage();
            policy.ErrorCode = E_IO;
            break;
        }
    }

    return policy;
}

void TNonreplicatedPartitionActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    ScheduleCountersUpdate(ctx);
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
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData)
{
    return InitRequests<
        typename TMethod::TRequest,
        typename TMethod::TResponse>(
        TMethod::Name,
        IsWriteMethod<TMethod>,
        msg,
        ctx,
        requestInfo,
        blockRange,
        deviceRequests,
        timeoutPolicy,
        requestData);
}

template <typename TRequest, typename TResponse>
bool TNonreplicatedPartitionActor::InitRequests(
    const char* methodName,
    const bool isWriteMethod,
    const TRequest& msg,
    const NActors::TActorContext& ctx,
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData)
{
    auto reply = [=](const TActorContext& ctx,
                     const TRequestInfo& requestInfo,
                     NProto::TError error)
    {
        auto response = std::make_unique<TResponse>(std::move(error));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            methodName,
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
            PartConfig->MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "invalid block range " << blockRange.Print()));
        return false;
    }

    if (RequiresCheckpointSupport(msg.Record)) {
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

    if (isWriteMethod && PartConfig->IsReadOnly() &&
        !msg.Record.GetHeaders().GetIsBackgroundRequest())
    {
        reply(
            ctx,
            requestInfo,
            PartConfig->MakeIOError("disk in error state"));
        return false;
    }

    for (const auto& dr: *deviceRequests) {
        if (PartConfig->GetOutdatedDeviceIds().contains(
                dr.Device.GetDeviceUUID()))
        {
            reply(
                ctx,
                requestInfo,
                PartConfig->MakeError(
                    E_REJECTED,
                    TStringBuilder() << "Device " << dr.Device.GetDeviceUUID()
                                     << " is lagging behind on data. All IO "
                                        "operations are prohibited."));
            return false;
        }

        requestData->DeviceIndices.push_back(dr.DeviceIdx);

        TDeviceStat& deviceStat = DeviceStats[dr.DeviceIdx];
        if (dr.Device.GetNodeId() == 0) {
            // Accessing a non-allocated device causes the disk to break.
            deviceStat.BrokenTransitionTs = ctx.Now();
            deviceStat.DeviceStatus = EDeviceStatus::Broken;

            reply(
                ctx,
                requestInfo,
                PartConfig->MakeIOError(
                    TStringBuilder() << "unavailable device requested: "
                                     << dr.Device.GetDeviceUUID()));
            return false;
        }

        if (PartConfig->GetLaggingDevicesAllowed() &&
            deviceStat.DeviceStatus == EDeviceStatus::Ok &&
            deviceStat.GetTimedOutStateDuration(ctx.Now()) >
                Config->GetLaggingDeviceTimeoutThreshold())
        {
            NCloud::Send(
                ctx,
                PartConfig->GetParentActorId(),
                std::make_unique<TEvVolumePrivate::TEvDeviceTimedOutRequest>(
                    dr.Device.GetDeviceUUID()));
        }
    }

    *timeoutPolicy = MakeTimeoutPolicyForRequest(
        *deviceRequests,
        ctx.Now(),
        msg.Record.GetHeaders().GetIsBackgroundRequest());

    return true;
}

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TWriteBlocksMethod>(
    const TEvService::TWriteBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData);

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TWriteBlocksLocalMethod>(
    const TEvService::TWriteBlocksLocalMethod::TRequest& msg,
    const TActorContext& ctx,
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData);

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TZeroBlocksMethod>(
    const TEvService::TZeroBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData);

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TReadBlocksMethod>(
    const TEvService::TReadBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData);

template bool TNonreplicatedPartitionActor::InitRequests<TEvService::TReadBlocksLocalMethod>(
    const TEvService::TReadBlocksLocalMethod::TRequest& msg,
    const TActorContext& ctx,
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData);

template bool TNonreplicatedPartitionActor::InitRequests<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
    const TEvNonreplPartitionPrivate::TChecksumBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData);

template bool TNonreplicatedPartitionActor::InitRequests<
    TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest,
    TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
    const char* methodName,
    const bool isWriteRequest,
    const TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest& msg,
    const TActorContext& ctx,
    const TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests,
    TRequestTimeoutPolicy* timeoutPolicy,
    TRequestData* requestData);

void TNonreplicatedPartitionActor::OnRequestCompleted(
    const TEvNonreplPartitionPrivate::TOperationCompleted& operation,
    TInstant now)
{
    using EStatus = TEvNonreplPartitionPrivate::TOperationCompleted::EStatus;
    switch (operation.Status) {
        case EStatus::Success: {
            for (const auto& [deviceIndex, _]: operation.RequestResults) {
                OnRequestSuccess(deviceIndex, operation.ExecutionTime, now);
            }
            break;
        }
        case EStatus::Fail: {
            break;
        }
        case EStatus::Timeout: {
            for (const auto& [deviceIndex, _]: operation.RequestResults) {
                OnRequestTimeout(deviceIndex, operation.ExecutionTime, now);
            }
            break;
        }
    }
}

void TNonreplicatedPartitionActor::OnRequestSuccess(
    ui32 deviceIndex,
    TDuration executionTime,
    TInstant now)
{
    auto& stat = DeviceStats[deviceIndex];
    stat.FirstTimedOutRequestStartTs = {};
    stat.LastSuccessfulRequestStartTs = now - executionTime;
    stat.ResponseTimes.PushBack(executionTime);
    stat.DeviceStatus = EDeviceStatus::Ok;
    stat.BrokenTransitionTs = {};
}

void TNonreplicatedPartitionActor::OnRequestTimeout(
    ui32 deviceIndex,
    TDuration executionTime,
    TInstant now)
{
    auto& stat = DeviceStats[deviceIndex];

    const TInstant requestStartTs = now - executionTime;
    // Request timeout can be delivered with delay. Ignore ones that were
    // started before the last successful request.
    if (requestStartTs < stat.LastSuccessfulRequestStartTs) {
        return;
    }

    if (!stat.FirstTimedOutRequestStartTs) {
        stat.FirstTimedOutRequestStartTs = requestStartTs;
    }

    switch (stat.DeviceStatus) {
        case EDeviceStatus::Ok:
        case EDeviceStatus::Unavailable: {
            if (stat.GetTimedOutStateDuration(now) >
                GetMaxTimedOutDeviceStateDuration())
            {
                stat.DeviceStatus = EDeviceStatus::SilentBroken;
                stat.BrokenTransitionTs = now;
            }
            break;
        }
        case EDeviceStatus::SilentBroken: {
            if (stat.CooldownPassed(
                    now,
                    Config->GetNonReplicatedAgentMaxTimeout()))
            {
                stat.DeviceStatus = EDeviceStatus::Broken;
            }
            break;
        }
        case EDeviceStatus::Broken: {
            break;
        }
    }
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

void TNonreplicatedPartitionActor::HandleDeviceTimedOutResponse(
    const TEvVolumePrivate::TEvDeviceTimedOutResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Attempted to deem device %s as lagging. Result: %s",
        PartConfig->GetName().c_str(),
        PartConfig->GetDevices()[ev->Cookie].GetDeviceUUID().c_str(),
        FormatError(msg->GetError()).c_str());
}

void TNonreplicatedPartitionActor::HandleAgentIsUnavailable(
    const TEvNonreplPartitionPrivate::TEvAgentIsUnavailable::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& laggingAgentId = msg->LaggingAgent.GetAgentId();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Agent %s has become unavailable",
        PartConfig->GetName().c_str(),
        laggingAgentId.Quote().c_str());

    if (!PartConfig->GetLaggingDevicesAllowed()) {
        return;
    }

    auto getAgentIdByRow = [&](int row) -> const TString&
    {
        return PartConfig->GetDevices()[row].GetAgentId();
    };

    TSet<ui32> laggingRows;
    for (const auto& laggingDevice: msg->LaggingAgent.GetDevices()) {
        Y_DEBUG_ABORT_UNLESS(DeviceStats.size() > laggingDevice.GetRowIndex());
        Y_DEBUG_ABORT_UNLESS(
            static_cast<ui32>(PartConfig->GetDevices().size()) >
            laggingDevice.GetRowIndex());

        laggingRows.insert(laggingDevice.GetRowIndex());

        if (getAgentIdByRow(laggingDevice.GetRowIndex()) == laggingAgentId) {
            DeviceStats[laggingDevice.GetRowIndex()].DeviceStatus =
                EDeviceStatus::Unavailable;
        }
    }

    // Cancel all write/zero requests that intersects with the rows of the lagging
    // agent. And read requests to the lagging replica.
    for (const auto& [actorId, requestData]: RequestsInProgress.AllRequests()) {
        for (int deviceIndex: requestData.Value.DeviceIndices) {
            const bool shouldCancelRequest =
                laggingRows.contains(deviceIndex) &&
                (requestData.IsWrite ||
                 getAgentIdByRow(deviceIndex) == laggingAgentId);

            if (shouldCancelRequest) {
                NCloud::Send<TEvNonreplPartitionPrivate::TEvCancelRequest>(
                    ctx,
                    actorId,
                    0,   // cookie
                    EReason::Canceled);
                break;
            }
        }
    }
}

void TNonreplicatedPartitionActor::HandleAgentIsBackOnline(
    const TEvNonreplPartitionPrivate::TEvAgentIsBackOnline::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Agent %s is back online",
        PartConfig->GetName().c_str(),
        msg->AgentId.Quote().c_str());

    for (int i = 0; i < PartConfig->GetDevices().size(); ++i) {
        const auto& device = PartConfig->GetDevices()[i];
        if (device.GetAgentId() == msg->AgentId &&
            DeviceStats[i].DeviceStatus <= EDeviceStatus::Unavailable)
        {
            DeviceStats[i].DeviceStatus = EDeviceStatus::Ok;
            DeviceStats[i].FirstTimedOutRequestStartTs = {};
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

STFUNC(TNonreplicatedPartitionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters, HandleUpdateCounters);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, DrainActorCompanion.HandleDrain);
        HFunc(NPartition::TEvPartition::TEvWaitForInFlightWritesRequest, DrainActorCompanion.HandleWaitForInFlightWrites);
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            GetDeviceForRangeCompanion.HandleGetDeviceForRange);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest,
            HandleMultiAgentWrite);

        HFunc(TEvNonreplPartitionPrivate::TEvReadBlocksCompleted, HandleReadBlocksCompleted);
        HFunc(TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted, HandleWriteBlocksCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted,
            HandleMultiAgentWriteBlocksCompleted);
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
        HFunc(TEvVolume::TEvCheckRangeRequest, HandleCheckRange);

        HFunc(TEvVolumePrivate::TEvDeviceTimedOutResponse, HandleDeviceTimedOutResponse);
        HFunc(TEvNonreplPartitionPrivate::TEvAgentIsUnavailable, HandleAgentIsUnavailable);
        HFunc(TEvNonreplPartitionPrivate::TEvAgentIsBackOnline, HandleAgentIsBackOnline);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TNonreplicatedPartitionActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);

        HFunc(TEvService::TEvReadBlocksRequest, RejectReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, RejectWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, RejectZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, RejectReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, RejectWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, RejectDrain);
        HFunc(NPartition::TEvPartition::TEvWaitForInFlightWritesRequest, RejectWaitForInFlightWrites);
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            GetDeviceForRangeCompanion.RejectGetDeviceForRange);

        HFunc(TEvNonreplPartitionPrivate::TEvReadBlocksCompleted, HandleReadBlocksCompleted);
        HFunc(TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted, HandleWriteBlocksCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted,
            HandleMultiAgentWriteBlocksCompleted);
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
        IgnoreFunc(TEvVolumePrivate::TEvDeviceTimedOutResponse);
        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
