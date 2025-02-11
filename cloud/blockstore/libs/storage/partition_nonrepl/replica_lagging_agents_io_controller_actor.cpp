#include "replica_lagging_agents_io_controller_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/agent_availability_monitoring_actor.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_common.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////
namespace {

using namespace NActors;
using namespace NKikimr;

template <typename TMethod>
class TSplitRequestSenderActor final
    : public NActors::TActorBootstrapped<TSplitRequestSenderActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    TVector<TSplitRequest> Requests;
    const NActors::TActorId ParentActorId;
    const ui64 RequestId;

    ui32 Responses = 0;
    typename TMethod::TResponse::ProtoRecordType Record;

    using TBase =
        NActors::TActorBootstrapped<TSplitRequestSenderActor<TMethod>>;

public:
    TSplitRequestSenderActor(
        TRequestInfoPtr requestInfo,
        TVector<TSplitRequest> requests,
        NActors::TActorId parentActorId,
        ui64 requestId);
    ~TSplitRequestSenderActor() override = default;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendRequests(const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

template <typename TMethod>
TSplitRequestSenderActor<TMethod>::TSplitRequestSenderActor(
    TRequestInfoPtr requestInfo,
    TVector<TSplitRequest> requests,
    NActors::TActorId parentActorId,
    ui64 requestId)
    : RequestInfo(std::move(requestInfo))
    , Requests(std::move(requests))
    , ParentActorId(parentActorId)
    , RequestId(requestId)
{
    Y_DEBUG_ABORT_UNLESS(!Requests.empty());
    Y_DEBUG_ABORT_UNLESS(ParentActorId);
}

template <typename TMethod>
void TSplitRequestSenderActor<TMethod>::Bootstrap(
    const NActors::TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    SendRequests(ctx);
}

template <typename TMethod>
void TSplitRequestSenderActor<TMethod>::SendRequests(
    const NActors::TActorContext& ctx)
{
    for (auto& request: Requests) {
        Y_DEBUG_ABORT_UNLESS(request.CallContext);

        if (!request.RecipientActorId) {
            ++Responses;
            continue;
        }

        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "xxxxx AID[%s] TSplitRequestSenderActor<%s> SendRequest %lu to %s",
            ctx.SelfID.ToString().c_str(),
            TMethod::Name,
            RequestId,
            request.RecipientActorId.ToString().c_str());

        auto event = std::make_unique<NActors::IEventHandle>(
            request.RecipientActorId,
            ctx.SelfID,
            request.Request.release(),
            NActors::IEventHandle::FlagForwardOnNondelivery,
            RequestInfo->Cookie,   // cookie
            &ctx.SelfID            // forwardOnNondelivery
        );

        ctx.Send(std::move(event));
    }

    Y_DEBUG_ABORT_UNLESS(Responses < Requests.size());
}

template <typename TMethod>
void TSplitRequestSenderActor<TMethod>::Done(const NActors::TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(Record);

    auto& callContext = *RequestInfo->CallContext;
    for (auto& request: Requests) {
        callContext.LWOrbit.Join(request.CallContext->LWOrbit);
    }

    // TODO: TRACING?
    // LWTRACK(
    //     ResponseSent_PartitionWorker,
    //     RequestInfo->CallContext->LWOrbit,
    //     TMethod::Name,
    //     RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    // TODO: NOT NEEDED, right?
    // using TCompletion =
    //     TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted;
    // auto completion =
    //     std::make_unique<TCompletion>(
    //         NonreplicatedRequestCounter,
    //         RequestInfo->GetTotalCycles());

    // NCloud::Send(ctx, ParentActorId, std::move(completion));

    TBase::Die(ctx);
}

template <typename TMethod>
void CopyCommonRequestData(
    const typename TMethod::TRequest::TPtr& ev,
    typename TMethod::TRequest& dst)
{
    const auto* msg = ev->Get();

    auto& callContext = *msg->CallContext;
    if (!callContext.LWOrbit.Fork(dst.CallContext->LWOrbit)) {
        LWTRACK(
            ForkFailed,
            callContext.LWOrbit,
            TMethod::Name,
            callContext.RequestId);
    }
    dst.Record.MutableHeaders()->CopyFrom(msg->Record.GetHeaders());
    dst.Record.SetDiskId(msg->Record.GetDiskId());
    dst.Record.SetFlags(msg->Record.GetFlags());
    dst.Record.SetSessionId(msg->Record.GetSessionId());
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TSplitRequestSenderActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    // TODO: FIX log
    // LOG_WARN(ctx, TBlockStoreComponents::PARTITION_WORKER,
    //     "[%s] %s request undelivered to some nonrepl partitions",
    //     DiskId.c_str(),
    //     TMethod::Name);

    Record.MutableError()->CopyFrom(MakeError(
        E_REJECTED,
        TStringBuilder() << TMethod::Name
                         << " request undelivered to some nonrepl partitions"));

    if (++Responses < Requests.size()) {
        return;
    }

    Done(ctx);
}

template <typename TMethod>
void TSplitRequestSenderActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    // if (HasError(msg->Record)) {
    //     // TODO: FIX log
    //     LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_WORKER,
    //         "[%s] %s got error from nonreplicated partition: %s",
    //         DiskId.c_str(),
    //         TMethod::Name,
    //         FormatError(msg->Record.GetError()).c_str());
    // }

    if (!HasError(Record)) {
        Record = std::move(msg->Record);
    }

    if (++Responses < Requests.size()) {
        return;
    }

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "xxxxx AID[%s] Handle response %s with request id: %lu",
        this->SelfId().ToString().c_str(),
        TMethod::Name,
        RequestId);

    Done(ctx);
}

template <typename TMethod>
void TSplitRequestSenderActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    *Record.MutableError() =
        MakeError(E_REJECTED, "TSplitRequestSenderActor is dead");
    Done(ctx);
}

template <typename TMethod>
STFUNC(TSplitRequestSenderActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TReplicaLaggingAgentsIOControllerActor::TReplicaLaggingAgentsIOControllerActor(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TNonreplicatedPartitionConfigPtr partConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    TString rwClientId,
    TActorId partNonreplActorId,
    TActorId statActorId,
    TActorId mirrorPartitionActor)
    : Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , PartConfig(std::move(partConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , RwClientId(std::move(rwClientId))
    , PartNonreplActorId(partNonreplActorId)
    , StatActorId(statActorId)
    , MirrorPartitionActor(mirrorPartitionActor)
    , PoisonPillHelper(this)
{}

TReplicaLaggingAgentsIOControllerActor::
    ~TReplicaLaggingAgentsIOControllerActor() = default;

void TReplicaLaggingAgentsIOControllerActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);
}

void TReplicaLaggingAgentsIOControllerActor::HandleAgentIsUnavailable(
    const TEvNonreplPartitionPrivate::TEvAgentIsUnavailable::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "HandleAgentIsUnavailable %s",
        msg->LaggingAgent.GetAgentId().c_str());

    const auto& agentId = msg->LaggingAgent.GetAgentId();
    TAgentState* state = AgentState.FindPtr(agentId);
    if (!state || state->State == EAgentState::Resyncing) {
        auto& state = AgentState[agentId];
        state.State = EAgentState::Unavailable;
        state.LaggingAgent = msg->LaggingAgent;
        state.AgentAvailabilityMonitoring = NCloud::Register(
            ctx,
            std::make_unique<TAgentAvailabilityMonitoringActor>(
                Config,
                PartConfig,
                PartNonreplActorId,
                SelfId(),
                StatActorId,
                agentId));
        PoisonPillHelper.TakeOwnership(ctx, state.AgentAvailabilityMonitoring);

        if (state.SmartMigrationActor) {
            Y_DEBUG_ABORT_UNLESS(state.CleanBlocksMap);

            PoisonPillHelper.ReleaseOwnership(ctx, state.SmartMigrationActor);
            // TODO: Either add a lock to sturcture, or handle poison taken
            // event.
            // ????
            NCloud::Send<TEvents::TEvPoisonPill>(
                ctx,
                state.SmartMigrationActor);
            state.SmartMigrationActor = TActorId();
        } else {
            state.CleanBlocksMap = std::make_shared<TCompressedBitmap>(
                PartConfig->GetBlockCount());
            state.CleanBlocksMap->Set(0, PartConfig->GetBlockCount());
        }
    } else {
        Y_DEBUG_ABORT_UNLESS(AgentState[agentId].AgentAvailabilityMonitoring);
        Y_DEBUG_ABORT_UNLESS(!AgentState[agentId].SmartMigrationActor);
    }

    NCloud::Send(
        ctx,
        PartNonreplActorId,
        std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsUnavailable>(
            msg->LaggingAgent));
}

void TReplicaLaggingAgentsIOControllerActor::HandleAgentIsBackOnline(
    const TEvNonreplPartitionPrivate::TEvAgentIsBackOnline::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "HandleAgentIsBackOnline %s",
        msg->AgentId.c_str());

    Y_DEBUG_ABORT_UNLESS(AgentState.contains(msg->AgentId));
    if (!AgentState.contains(msg->AgentId)) {
        return;
    }

    NCloud::Send(
        ctx,
        PartNonreplActorId,
        std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsBackOnline>(
            msg->AgentId));

    switch (AgentState[msg->AgentId].State) {
        case EAgentState::Unavailable: {
            TAgentState& state = AgentState[msg->AgentId];
            Y_DEBUG_ABORT_UNLESS(!state.SmartMigrationActor);
            Y_DEBUG_ABORT_UNLESS(state.AgentAvailabilityMonitoring);

            PoisonPillHelper.ReleaseOwnership(
                ctx,
                state.AgentAvailabilityMonitoring);
            NCloud::Send<TEvents::TEvPoisonPill>(
                ctx,
                state.AgentAvailabilityMonitoring);
            state.AgentAvailabilityMonitoring = TActorId();

            Cerr << "map block count = " << state.CleanBlocksMap->Count()
                 << "; disk bc = " << PartConfig->GetBlockCount() << Endl;

            TCompressedBitmap cleanBlocksCopy(PartConfig->GetBlockCount());
            cleanBlocksCopy.Update(*state.CleanBlocksMap, 0);

            Cerr << "cleanBlocksCopy dirty = " << cleanBlocksCopy.Count() << Endl;

            state.SmartMigrationActor = NCloud::Register(
                ctx,
                std::make_unique<TSmartMigrationActor>(
                    Config,
                    DiagnosticsConfig,
                    PartConfig,
                    ProfileLog,
                    BlockDigestGenerator,
                    RwClientId,
                    PartNonreplActorId,
                    MirrorPartitionActor,
                    StatActorId,
                    std::move(cleanBlocksCopy),
                    msg->AgentId));
            PoisonPillHelper.TakeOwnership(ctx, state.SmartMigrationActor);
            state.State = EAgentState::Resyncing;
            break;
        }
        case EAgentState::Resyncing: {
            const TAgentState& state = AgentState[msg->AgentId];
            Y_DEBUG_ABORT_UNLESS(state.SmartMigrationActor);
            Y_DEBUG_ABORT_UNLESS(!state.AgentAvailabilityMonitoring);
            // no-op?
            break;
        }
    }
}

bool TReplicaLaggingAgentsIOControllerActor::AgentIsUnavailable(
    const TString& agentId) const
{
    const TAgentState* state = AgentState.FindPtr(agentId);
    return state && state->State == EAgentState::Unavailable;
}

void TReplicaLaggingAgentsIOControllerActor::MarkBlocksAsDirty(
    const TString& unavailableAgentId,
    TBlockRange64 range)
{
    Y_DEBUG_ABORT_UNLESS(AgentState.contains(unavailableAgentId));
    auto& state = AgentState[unavailableAgentId];
    Y_DEBUG_ABORT_UNLESS(state.CleanBlocksMap);
    state.CleanBlocksMap->Unset(range.Start, range.End + 1);
}

template <typename TMethod>
void TReplicaLaggingAgentsIOControllerActor::WriteRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "xxxxx AID[%s] Handle "
        "TReplicaLaggingAgentsIOControllerActor::WriteRequest "
        "requestid = %lu, sender = %s",
        SelfId().ToString().c_str(),
        ev->Cookie,
        ev->Sender.ToString().c_str());

    const ui32 requestBlockCount = CalculateWriteRequestBlockCount(
        msg->Record,
        PartConfig->GetBlockSize());
    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        requestBlockCount);
    auto deviceRequests = PartConfig->ToDeviceRequests(blockRange);
    ui32 requestsToUnavailable = 0;
    for (const auto& deviceRequest: deviceRequests) {
        const auto& agentId = deviceRequest.Device.GetAgentId();
        if (AgentIsUnavailable(agentId)) {
            requestsToUnavailable++;
            MarkBlocksAsDirty(agentId, deviceRequest.BlockRange);
        }
    }

    if (requestsToUnavailable == deviceRequests.size()) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "xxxxx Respond with fake success. diskid = %s, requestid = %lu",
            PartConfig->GetName().c_str(),
            ev->Cookie);

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<typename TMethod::TResponse>());
        return;
    }

    auto requests = SplitRequest<TMethod>(ev, deviceRequests);
    Y_DEBUG_ABORT_UNLESS(!requests.empty());
    Y_DEBUG_ABORT_UNLESS(requests.size() <= deviceRequests.size());

    NCloud::Register(
        ctx,
        std::make_unique<TSplitRequestSenderActor<TMethod>>(
            CreateRequestInfo<TMethod>(
                ev->Sender,
                ev->Cookie,
                msg->CallContext),
            std::move(requests),
            SelfId(),
            GetRequestId(msg->Record)));
}

template <typename TMethod>
TVector<TSplitRequest> TReplicaLaggingAgentsIOControllerActor::SplitRequest(
    const TMethod::TRequest::TPtr& ev,
    const TVector<TDeviceRequest>& deviceRequests)
{
    if (ShouldSplitWriteRequest(deviceRequests)) {
        return DoSplitRequest(ev, deviceRequests);
    }

    auto* msg = ev->Get();
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->Record = std::move(msg->Record);
    request->CallContext = msg->CallContext;

    TVector<TSplitRequest> result;
    result.emplace_back(
        std::move(request),
        msg->CallContext,
        GetRecipientActorId(deviceRequests[0].Device.GetAgentId()));
    return result;
}

bool TReplicaLaggingAgentsIOControllerActor::ShouldSplitWriteRequest(
    const TVector<TDeviceRequest>& requests) const
{
    Y_DEBUG_ABORT_UNLESS(!requests.empty());

    THashSet<NActors::TActorId> recipientActors;
    for (const auto& request: requests) {
        recipientActors.insert(
            GetRecipientActorId(request.Device.GetAgentId()));
    }
    return recipientActors.size() > 1;
}

TVector<TSplitRequest> TReplicaLaggingAgentsIOControllerActor::DoSplitRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TVector<TDeviceRequest>& deviceRequests)
{
    TVector<TSplitRequest> result;
    auto* msg = ev->Get();

    TDeviceRequestBuilder builder(
        deviceRequests,
        PartConfig->GetBlockSize(),
        msg->Record);

    for (const auto& deviceRequest: deviceRequests) {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        CopyCommonRequestData<TEvService::TWriteBlocksMethod>(ev, *request);

        request->Record.SetStartIndex(deviceRequest.BlockRange.Start);
        builder.BuildNextRequest(request->Record);

        auto forkedCallContext = request->CallContext;
        result.emplace_back(
            std::move(request),
            forkedCallContext,
            GetRecipientActorId(deviceRequest.Device.GetAgentId()));
    }

    return result;
}

TVector<TSplitRequest> TReplicaLaggingAgentsIOControllerActor::DoSplitRequest(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TVector<TDeviceRequest>& deviceRequests)
{
    TVector<TSplitRequest> result;
    auto* msg = ev->Get();

    auto guard = msg->Record.Sglist.Acquire();
    TSgListBlockRange src(guard.Get(), msg->Record.BlockSize);

    for (const auto& deviceRequest: deviceRequests) {
        auto request =
            std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
        CopyCommonRequestData<TEvService::TWriteBlocksLocalMethod>(
            ev,
            *request);

        request->Record.SetStartIndex(deviceRequest.BlockRange.Start);
        request->Record.BlocksCount = deviceRequest.BlockRange.Size();
        request->Record.BlockSize = msg->Record.BlockSize;

        Y_DEBUG_ABORT_UNLESS(src.HasNext());
        TSgList sglist = src.Next(deviceRequest.BlockRange.Size());
        Y_DEBUG_ABORT_UNLESS(!sglist.empty());
        request->Record.Sglist =
            msg->Record.Sglist.CreateDepender(std::move(sglist));

        auto forkedCallContext = request->CallContext;
        result.emplace_back(
            std::move(request),
            forkedCallContext,
            GetRecipientActorId(deviceRequest.Device.GetAgentId()));
    }

    return result;
}

TVector<TSplitRequest> TReplicaLaggingAgentsIOControllerActor::DoSplitRequest(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TVector<TDeviceRequest>& deviceRequests)
{
    TVector<TSplitRequest> result;
    for (const auto& deviceRequest: deviceRequests) {
        auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
        CopyCommonRequestData<TEvService::TZeroBlocksMethod>(ev, *request);

        request->Record.SetStartIndex(deviceRequest.BlockRange.Start);
        request->Record.SetBlocksCount(deviceRequest.BlockRange.Size());

        auto forkedCallContext = request->CallContext;
        result.emplace_back(
            std::move(request),
            forkedCallContext,
            GetRecipientActorId(deviceRequest.Device.GetAgentId()));
    }

    return result;
}

NActors::TActorId TReplicaLaggingAgentsIOControllerActor::GetRecipientActorId(
    const TString& agentId) const
{
    if (!AgentState.contains(agentId)) {
        return PartNonreplActorId;
    }

    switch (AgentState.at(agentId).State) {
        case EAgentState::Unavailable:
            return {};
        case EAgentState::Resyncing:
            Y_DEBUG_ABORT_UNLESS(AgentState.at(agentId).SmartMigrationActor);
            return AgentState.at(agentId).SmartMigrationActor;
    }
}

void TReplicaLaggingAgentsIOControllerActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteRequest<TEvService::TWriteBlocksMethod>(ev, ctx);
}

void TReplicaLaggingAgentsIOControllerActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteRequest<TEvService::TWriteBlocksLocalMethod>(ev, ctx);
}

void TReplicaLaggingAgentsIOControllerActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteRequest<TEvService::TZeroBlocksMethod>(ev, ctx);
}

void TReplicaLaggingAgentsIOControllerActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const TActorContext& ctx)
{
    RwClientId = ev->Get()->RWClientId;
    auto partitionRequest =
        std::make_unique<TEvVolume::TEvRWClientIdChanged>(RwClientId);
    ctx.Send(PartNonreplActorId, std::move(partitionRequest));

    for (const auto& [_, state]: AgentState) {
        Y_DEBUG_ABORT_UNLESS(
            !state.AgentAvailabilityMonitoring || !state.SmartMigrationActor);

        if (state.SmartMigrationActor) {
            ctx.Send(
                state.SmartMigrationActor,
                std::make_unique<TEvVolume::TEvRWClientIdChanged>(RwClientId));
        }
    }
}

void TReplicaLaggingAgentsIOControllerActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Become(&TThis::StateZombie);
    PoisonPillHelper.HandlePoisonPill(ev, ctx);
    AgentState.clear();
}

void TReplicaLaggingAgentsIOControllerActor::Die(
    const NActors::TActorContext& ctx)
{
    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TReplicaLaggingAgentsIOControllerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(
            TEvNonreplPartitionPrivate::TEvAgentIsUnavailable,
            HandleAgentIsUnavailable);
        HFunc(
            TEvNonreplPartitionPrivate::TEvAgentIsBackOnline,
            HandleAgentIsBackOnline);
        HFunc(TEvVolume::TEvRWClientIdChanged, HandleRWClientIdChanged);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        // case TEvService::EvReadBlocksLocalRequest:
        // case TEvService::EvReadBlocksRequest: {
        //     // NOTE: We handle all unexpected requests and proxy them to
        //     // nonreplicated partititon.
        //     break;
        // }

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            // ForwardUnhandledEvent(ev, ActorContext());
            break;
    }
}

void TReplicaLaggingAgentsIOControllerActor::ForwardUnhandledEvent(
    TAutoPtr<IEventHandle>& ev,
    const TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, PartNonreplActorId);
}

STFUNC(TReplicaLaggingAgentsIOControllerActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvScrubbingNextRange);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse);

        HFunc(TEvService::TEvWriteBlocksRequest, RejectWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, RejectZeroBlocks);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, RejectWriteBlocksLocal);

        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);
        IgnoreFunc(TEvents::TEvPoisonPill);
        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            // ForwardUnhandledEvent(ev, ActorContext());
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
