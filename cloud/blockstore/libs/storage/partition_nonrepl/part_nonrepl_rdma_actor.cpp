#include "part_nonrepl_rdma_actor.h"

#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool NeedToNotifyAboutDeviceRequestError(const NProto::TError& err)
{
    switch (err.GetCode()) {
        case E_RDMA_UNAVAILABLE:
        case E_TIMEOUT:
        case E_REJECTED:
            return true;
    }
    return false;
}

}   // namespace

TNonreplicatedPartitionRdmaActor::TNonreplicatedPartitionRdmaActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TNonreplicatedPartitionConfigPtr partConfig,
        NRdma::IClientPtr rdmaClient,
        TActorId statActorId)
    : Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , PartConfig(std::move(partConfig))
    , RdmaClient(std::move(rdmaClient))
    , StatActorId(statActorId)
    , PartCounters(CreatePartitionDiskCounters(
          EPublishingPolicy::DiskRegistryBased,
          DiagnosticsConfig->GetHistogramCounterOptions()))
{}

TNonreplicatedPartitionRdmaActor::~TNonreplicatedPartitionRdmaActor()
{
}

void TNonreplicatedPartitionRdmaActor::Bootstrap(const TActorContext& ctx)
{
    for (const auto& d: PartConfig->GetDevices()) {
        auto& ep = AgentId2EndpointFuture[d.GetAgentId()];
        if (ep.Initialized()) {
            continue;
        }

        ep = RdmaClient->StartEndpoint(
            d.GetAgentId(),
            d.GetRdmaEndpoint().GetPort());
    }

    Become(&TThis::StateWork);
    if (!Config->GetUsePullSchemeForVolumeStatistics()) {
        ScheduleCountersUpdate(ctx);
    }
    ctx.Schedule(
        Config->GetNonReplicatedMinRequestTimeoutSSD(),
        new TEvents::TEvWakeup());
}

bool TNonreplicatedPartitionRdmaActor::CheckReadWriteBlockRange(
    const TBlockRange64& range) const
{
    return range.End >= range.Start && PartConfig->GetBlockCount() > range.End;
}

ui32 TNonreplicatedPartitionRdmaActor::GetFlags() const
{
    ui32 flags = 0;
    if (RdmaClient->IsAlignedDataEnabled()) {
        SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
    }
    return flags;
}

void TNonreplicatedPartitionRdmaActor::ScheduleCountersUpdate(
    const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvNonreplPartitionPrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
bool TNonreplicatedPartitionRdmaActor::InitRequests(
    const typename TMethod::TRequest& msg,
    const NActors::TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests)
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
        deviceRequests);
}

template <typename TRequest, typename TResponse>
bool TNonreplicatedPartitionRdmaActor::InitRequests(
    const char* methodName,
    const bool isWriteMethod,
    const TRequest& msg,
    const NActors::TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests)
{
    auto reply = [methodName] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        NProto::TError error)
    {
        auto response = std::make_unique<TResponse>(
            std::move(error));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            methodName,
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (!CheckReadWriteBlockRange(blockRange)) {
        reply(ctx, requestInfo, PartConfig->MakeError(
            E_ARGUMENT,
            TStringBuilder() << "invalid block range ["
                "index: " << blockRange.Start
                << ", count: " << blockRange.Size()
                << "]"));
        return false;
    }

    if (!msg.Record.GetHeaders().GetIsBackgroundRequest() &&
        isWriteMethod && PartConfig->IsReadOnly())
    {
        reply(ctx, requestInfo, PartConfig->MakeIOError("disk in error state"));
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

    for (auto& r: *deviceRequests) {
        if (PartConfig->GetOutdatedDeviceIds().contains(
                r.Device.GetDeviceUUID()))
        {
            reply(
                ctx,
                requestInfo,
                PartConfig->MakeError(
                    E_REJECTED,
                    TStringBuilder() << "Device " << r.Device.GetDeviceUUID()
                                     << " is lagging behind on data. All IO "
                                        "operations are prohibited."));
            return false;
        }

        auto& ep = AgentId2Endpoint[r.Device.GetAgentId()];
        if (!ep) {
            auto* f = AgentId2EndpointFuture.FindPtr(r.Device.GetAgentId());
            if (!f) {
                Y_DEBUG_ABORT_UNLESS(0);

                reply(ctx, requestInfo, PartConfig->MakeError(
                    E_INVALID_STATE,
                    TStringBuilder() << "endpoint not found for agent: "
                    << r.Device.GetAgentId()));
                return false;
            }

            if (f->HasException()) {
                SendRdmaUnavailableIfNeeded(ctx);

                auto [_, error] = ResultOrError(*f);

                reply(
                    ctx,
                    requestInfo,
                    PartConfig->MakeError(
                        E_REJECTED,
                        TStringBuilder() << "endpoint init failed for agent "
                                         << r.Device.GetAgentId().Quote()
                                         << ": " << FormatError(error)));
                return false;
            }

            if (!f->HasValue()) {
                reply(ctx, requestInfo, PartConfig->MakeError(
                    E_REJECTED,
                    TStringBuilder() << "endpoint not initialized for agent: "
                    << r.Device.GetAgentId()));
                return false;
            }

            ep = f->GetValue();
        }
    }

    return true;
}

template bool TNonreplicatedPartitionRdmaActor::InitRequests<TEvService::TWriteBlocksMethod>(
    const TEvService::TWriteBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests);

template bool TNonreplicatedPartitionRdmaActor::InitRequests<TEvService::TWriteBlocksLocalMethod>(
    const TEvService::TWriteBlocksLocalMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests);

template bool TNonreplicatedPartitionRdmaActor::InitRequests<TEvService::TZeroBlocksMethod>(
    const TEvService::TZeroBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests);

template bool TNonreplicatedPartitionRdmaActor::InitRequests<TEvService::TReadBlocksMethod>(
    const TEvService::TReadBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests);

template bool TNonreplicatedPartitionRdmaActor::InitRequests<TEvService::TReadBlocksLocalMethod>(
    const TEvService::TReadBlocksLocalMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests);

template bool TNonreplicatedPartitionRdmaActor::InitRequests<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
    const TEvNonreplPartitionPrivate::TChecksumBlocksMethod::TRequest& msg,
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests);

template bool TNonreplicatedPartitionRdmaActor::InitRequests<
    TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest,
    TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
    const char* methodName,
    const bool isWriteMethod,
    const TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest& msg,
    const NActors::TActorContext& ctx,
    TRequestInfo& requestInfo,
    const TBlockRange64& blockRange,
    TVector<TDeviceRequest>* deviceRequests);

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TNonreplicatedPartitionRdmaActor::TRequestContext>
TNonreplicatedPartitionRdmaActor::SendReadRequests(
    const NActors::TActorContext& ctx,
    TCallContextPtr callContext,
    const NProto::THeaders& headers,
    NRdma::IClientHandlerPtr handler,
    const TVector<TDeviceRequest>& deviceRequests)
{
    struct TDeviceRequestInfo
    {
        NRdma::IClientEndpointPtr Endpoint;
        NRdma::TClientRequestPtr ClientRequest;
    };

    TVector<TDeviceRequestInfo> requests;

    TRequestContext sentRequestCtx;

    ui64 startBlockIndexOffset = 0;
    for (auto& r: deviceRequests) {
        auto ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_ABORT_UNLESS(ep);
        auto dr = std::make_unique<TDeviceReadRequestContext>();

        ui64 sz = r.DeviceBlockRange.Size() * PartConfig->GetBlockSize();
        dr->StartIndexOffset = startBlockIndexOffset;
        dr->BlockCount = r.DeviceBlockRange.Size();
        dr->DeviceIdx = r.DeviceIdx;
        startBlockIndexOffset += r.DeviceBlockRange.Size();

        sentRequestCtx.emplace_back(r.DeviceIdx);

        NProto::TReadDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->CopyFrom(headers);
        deviceRequest.SetDeviceUUID(r.Device.GetDeviceUUID());
        deviceRequest.SetStartIndex(r.DeviceBlockRange.Start);
        deviceRequest.SetBlockSize(PartConfig->GetBlockSize());
        deviceRequest.SetBlocksCount(r.DeviceBlockRange.Size());

        auto [req, err] = ep->AllocateRequest(
            handler,
            std::move(dr),
            NRdma::TProtoMessageSerializer::MessageByteSize(deviceRequest, 0),
            4_KB + sz);

        if (HasError(err)) {
            LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
                "Failed to allocate rdma memory for ReadDeviceBlocksRequest"
                ", error: %s",
                FormatError(err).c_str());

            NotifyDeviceTimedOutIfNeeded(ctx, r.Device.GetDeviceUUID());
            return err;
        }

        ui32 flags = 0;
        if (RdmaClient->IsAlignedDataEnabled()) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

        NRdma::TProtoMessageSerializer::Serialize(
            req->RequestBuffer,
            TBlockStoreProtocol::ReadDeviceBlocksRequest,
            flags,
            deviceRequest);

        requests.push_back({std::move(ep), std::move(req)});
    }

    for (size_t i = 0; i < requests.size(); ++i) {
        auto& request = requests[i];
        sentRequestCtx[i].SentRequestId = request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            callContext);
    }

    return sentRequestCtx;
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::NotifyDeviceTimedOutIfNeeded(
    const NActors::TActorContext& ctx,
    const TString& deviceUUID)
{
    if (!PartConfig->GetLaggingDevicesAllowed()) {
        return;
    }
    auto [it, inserted] = TimedOutDeviceCtxByDeviceUUID.try_emplace(
        deviceUUID,
        TTimedOutDeviceCtx{});
    auto& timedOutDeviceCtx = it->second;

    if (inserted) {
        timedOutDeviceCtx.FirstErrorTs = ctx.Now();
        timedOutDeviceCtx.VolumeWasNotified = false;
        return;
    }

    if (timedOutDeviceCtx.VolumeWasNotified) {
        return;
    }

    auto timePassedSinceFirstError = ctx.Now() - timedOutDeviceCtx.FirstErrorTs;
    if (timePassedSinceFirstError >= Config->GetLaggingDeviceTimeoutThreshold())
    {
        NCloud::Send(
            ctx,
            PartConfig->GetParentActorId(),
            std::make_unique<TEvVolumePrivate::TEvDeviceTimedOutRequest>(
                deviceUUID));
        timedOutDeviceCtx.VolumeWasNotified = true;
    }
}

void TNonreplicatedPartitionRdmaActor::ProcessOperationCompleted(
    const NActors::TActorContext& ctx,
    const TEvNonreplPartitionPrivate::TOperationCompleted& opCompleted)
{
    const auto& devices = PartConfig->GetDevices();
    for (const auto& [idx, error]: opCompleted.RequestResults) {
        Y_ABORT_UNLESS(idx < static_cast<ui32>(devices.size()));

        const auto& deviceUUID = devices[idx].GetDeviceUUID();

        if (!NeedToNotifyAboutDeviceRequestError(error)) {
            TimedOutDeviceCtxByDeviceUUID.erase(deviceUUID);
            continue;
        }

        NotifyDeviceTimedOutIfNeeded(ctx, deviceUUID);
    }
}

void TNonreplicatedPartitionRdmaActor::SendRdmaUnavailableIfNeeded(
    const TActorContext& ctx)
{
    if (SentRdmaUnavailableNotification) {
        return;
    }

    NCloud::Send(
        ctx,
        PartConfig->GetParentActorId(),
        std::make_unique<TEvVolume::TEvRdmaUnavailable>());

    ReportRdmaError("RDMA Unavailable");

    SentRdmaUnavailableNotification = true;
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::HandleReadBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvReadBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Complete read blocks", SelfId().ToString().c_str());

    UpdateStats(msg->Stats);

    ProcessOperationCompleted(ctx, *msg);

    const auto requestBytes = msg->Stats.GetUserReadCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ReadBlocks.AddRequest(time, requestBytes);
    PartCounters->Rdma.ReadBytes.Increment(requestBytes);
    PartCounters->Rdma.ReadCount.Increment(1);
    const ui64 nonVoidBytes =
        static_cast<ui64>(msg->NonVoidBlockCount) * PartConfig->GetBlockSize();
    const ui64 voidBytes =
        static_cast<ui64>(msg->VoidBlockCount) * PartConfig->GetBlockSize();
    PartCounters->RequestCounters.ReadBlocks.RequestNonVoidBytes +=
        nonVoidBytes;
    PartCounters->RequestCounters.ReadBlocks.RequestVoidBytes += voidBytes;

    // TODO(komarevtsev-d): As of now, RDMA always transfers zero over the
    // network. Once this behaviour is optimized, "nonVoidBytes" should be added
    // here instead of "requestBytes".
    NetworkBytes += requestBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    const auto requestId = ev->Cookie;
    RequestsInProgress.RemoveRequest(requestId);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

void TNonreplicatedPartitionRdmaActor::HandleWriteBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Complete write blocks", SelfId().ToString().c_str());

    UpdateStats(msg->Stats);

    ProcessOperationCompleted(ctx, *msg);

    const auto requestBytes = msg->Stats.GetUserWriteCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.WriteBlocks.AddRequest(time, requestBytes);
    PartCounters->Rdma.WriteBytes.Increment(requestBytes);
    PartCounters->Rdma.WriteCount.Increment(1);
    NetworkBytes += requestBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    const auto requestId = ev->Cookie;
    RequestsInProgress.RemoveRequest(requestId);
    DrainActorCompanion.ProcessDrainRequests(ctx);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

void TNonreplicatedPartitionRdmaActor::HandleMultiAgentWriteBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Complete multi-agent write blocks",
        SelfId().ToString().c_str());

    UpdateStats(msg->Stats);

    ProcessOperationCompleted(ctx, *msg);

    const auto requestBytes =
        msg->Stats.GetUserWriteCounters().GetBlocksCount() *
        PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();

    PartCounters->RequestCounters.WriteBlocksMultiAgent.AddRequest(time, requestBytes);
    PartCounters->Rdma.WriteBytesMultiAgent.Increment(requestBytes);
    PartCounters->Rdma.WriteCountMultiAgent.Increment(1);

    NetworkBytes += requestBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    const auto requestId = ev->Cookie;
    RequestsInProgress.RemoveRequest(requestId);
    DrainActorCompanion.ProcessDrainRequests(ctx);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

void TNonreplicatedPartitionRdmaActor::HandleZeroBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Complete zero blocks", SelfId().ToString().c_str());

    UpdateStats(msg->Stats);

    ProcessOperationCompleted(ctx, *msg);

    const auto requestBytes = msg->Stats.GetUserWriteCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ZeroBlocks.AddRequest(time, requestBytes);
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    const auto requestId = ev->Cookie;
    RequestsInProgress.RemoveRequest(requestId);
    DrainActorCompanion.ProcessDrainRequests(ctx);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

void TNonreplicatedPartitionRdmaActor::HandleChecksumBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Complete checksum blocks", SelfId().ToString().c_str());

    UpdateStats(msg->Stats);

    ProcessOperationCompleted(ctx, *msg);

    const auto requestBytes = msg->Stats.GetSysChecksumCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ChecksumBlocks.AddRequest(time, requestBytes);

    NetworkBytes += sizeof(ui64);   //  Checksum is sent as a 64-bit integer.
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    const auto requestId = ev->Cookie;
    RequestsInProgress.RemoveRequest(requestId);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::HandleUpdateCounters(
    const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCountersScheduled = false;

    SendStats(ctx);
    ScheduleCountersUpdate(ctx);
}

void TNonreplicatedPartitionRdmaActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    // TODO timeout logic?

    ctx.Schedule(
        Config->GetNonReplicatedMinRequestTimeoutSSD(),
        new TEvents::TEvWakeup());
}

void TNonreplicatedPartitionRdmaActor::ReplyAndDie(const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Reply and die",
        SelfId().ToString().c_str());

    for (auto& [_, endpoint]: AgentId2EndpointFuture) {
        endpoint.Subscribe([](auto& future) {
            if (future.HasValue()) {
                future.GetValue()->Stop();
            }
        });
    }
    NCloud::Reply(ctx, *Poisoner, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

void TNonreplicatedPartitionRdmaActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Become(&TThis::StateZombie);

    Poisoner = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    if (!RequestsInProgress.Empty()) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Postpone PoisonPill response. Wait for requests in progress",
            SelfId().ToString().c_str());

        return;
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::HandleAgentIsUnavailable(
    const TEvNonreplPartitionPrivate::TEvAgentIsUnavailable::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& laggingAgentId = msg->LaggingAgent.GetAgentId();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Agent %s has become unavailable (lagging)",
        PartConfig->GetName().c_str(),
        laggingAgentId.Quote().c_str());

    if (!PartConfig->GetLaggingDevicesAllowed()) {
        return;
    }

    TSet<ui32> laggingRows;
    for (const auto& laggingDevice: msg->LaggingAgent.GetDevices()) {
        laggingRows.insert(laggingDevice.GetRowIndex());
    }

    const auto& devices = PartConfig->GetDevices();

    // Cancel all write/zero requests that intersects with the rows of the lagging
    // agent. And read requests to the lagging replica.
    for (const auto& [_, requestInfo]: RequestsInProgress.AllRequests()) {
        const auto& requestCtx = requestInfo.Value;
        bool needToCancel = AnyOf(
            requestCtx,
            [&](const auto& ctx)
            {
                return laggingRows.contains(ctx.DeviceIndex) &&
                       (requestInfo.IsWrite ||
                        devices[ctx.DeviceIndex].GetAgentId() ==
                            laggingAgentId);
            });

        if (!needToCancel) {
            continue;
        }

        for (auto [deviceIdx, rdmaRequestId]: requestCtx) {
            Y_ABORT_UNLESS(deviceIdx < static_cast<ui64>(devices.size()));
            auto agentId = devices[deviceIdx].GetAgentId();

            auto& endpoint = AgentId2Endpoint[agentId];
            endpoint->CancelRequest(rdmaRequestId);
        }
    }
}

void TNonreplicatedPartitionRdmaActor::HandleAgentIsBackOnline(
    const TEvNonreplPartitionPrivate::TEvAgentIsBackOnline::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& agentId = msg->AgentId;
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Lagging agent %s is back online",
        PartConfig->GetName().c_str(),
        agentId.Quote().c_str());

    const auto* ep = AgentId2Endpoint.FindPtr(agentId);
    if (!ep) {
        return;
    }
    // Agent is back online via interconnect. We should force the reconnect
    // attempt via RDMA.
    ep->get()->TryForceReconnect();

    for (const auto& device: PartConfig->GetDevices()) {
        if (device.GetAgentId() == msg->AgentId) {
            TimedOutDeviceCtxByDeviceUUID.erase(device.GetDeviceUUID());
        }
    }
}

void TNonreplicatedPartitionRdmaActor::HandleDeviceTimedOutResponse(
    const TEvVolumePrivate::TEvDeviceTimedOutResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Attempted to mark device %s as lagging. Result: %s",
        PartConfig->GetName().c_str(),
        PartConfig->GetDevices()[ev->Cookie].GetDeviceUUID().c_str(),
        FormatError(msg->GetError()).c_str());
}

////////////////////////////////////////////////////////////////////////////////

bool TNonreplicatedPartitionRdmaActor::HandleRequests(STFUNC_SIG)
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
    void TNonreplicatedPartitionRdmaActor::Handle##name(                       \
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

STFUNC(TNonreplicatedPartitionRdmaActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, DrainActorCompanion.HandleDrain);
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            GetDeviceForRangeCompanion.HandleGetDeviceForRange);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest,
            HandleMultiAgentWrite);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);

        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest,
            HandleChecksumBlocks);

        HFunc(
            TEvNonreplPartitionPrivate::TEvReadBlocksCompleted,
            HandleReadBlocksCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted,
            HandleWriteBlocksCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted,
            HandleMultiAgentWriteBlocksCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted,
            HandleZeroBlocksCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted,
            HandleChecksumBlocksCompleted);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, HandleGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, HandleCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, HandleRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, HandleGetRebuildMetadataStatus);
        HFunc(TEvVolume::TEvScanDiskRequest, HandleScanDisk);
        HFunc(TEvVolume::TEvGetScanDiskStatusRequest, HandleGetScanDiskStatus);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);

        HFunc(
            TEvNonreplPartitionPrivate::TEvAgentIsBackOnline,
            HandleAgentIsBackOnline);
        HFunc(
            TEvNonreplPartitionPrivate::TEvAgentIsUnavailable,
            HandleAgentIsUnavailable);
        HFunc(
            TEvVolumePrivate::TEvDeviceTimedOutResponse,
            HandleDeviceTimedOutResponse);

        HFunc(
            TEvNonreplPartitionPrivate::
                TEvGetDiskRegistryBasedPartCountersRequest,
            HandleGetDiskRegistryBasedPartCounters);

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

STFUNC(TNonreplicatedPartitionRdmaActor::StateZombie)
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
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            GetDeviceForRangeCompanion.RejectGetDeviceForRange);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest,
            RejectMultiAgentWrite);

        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest, RejectChecksumBlocks);

        HFunc(TEvNonreplPartitionPrivate::TEvReadBlocksCompleted, HandleReadBlocksCompleted);
        HFunc(TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted, HandleWriteBlocksCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted,
            HandleMultiAgentWriteBlocksCompleted);
        HFunc(TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted, HandleZeroBlocksCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted,
            HandleChecksumBlocksCompleted);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, RejectDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, RejectGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, RejectCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, RejectRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, RejectGetRebuildMetadataStatus);
        HFunc(TEvVolume::TEvScanDiskRequest, RejectScanDisk);
        HFunc(TEvVolume::TEvGetScanDiskStatusRequest, RejectGetScanDiskStatus);

        IgnoreFunc(TEvents::TEvPoisonPill);
        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);
        IgnoreFunc(TEvVolumePrivate::TEvDeviceTimedOutResponse);

        IgnoreFunc(TEvNonreplPartitionPrivate::
                       TEvGetDiskRegistryBasedPartCountersRequest);

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
