#include "incomplete_mirror_rw_mode_controller_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/agent_availability_waiter_actor.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_common.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////
namespace {

using namespace NActors;
using namespace NKikimr;

template <typename TEvent>
void ForwardMessageToActor(
    TEvent& ev,
    const NActors::TActorContext& ctx,
    NActors::TActorId destActor)
{
    NActors::TActorId nondeliveryActor = ev->GetForwardOnNondeliveryRecipient();
    auto message = std::make_unique<IEventHandle>(
        destActor,
        ev->Sender,
        ev->ReleaseBase().Release(),
        ev->Flags,
        ev->Cookie,
        ev->Flags & NActors::IEventHandle::FlagForwardOnNondelivery
            ? &nondeliveryActor
            : nullptr);
    ctx.Send(std::move(message));
}

template <typename TMethod>
class TSplitRequestSenderActor final
    : public NActors::TActorBootstrapped<TSplitRequestSenderActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    TVector<TSplitRequest> Requests;
    const TString DiskId;
    const NActors::TActorId ParentActorId;
    const ui64 RequestId;

    ui32 Responses = 0;
    typename TMethod::TResponse::ProtoRecordType Record;

    using TBase = NActors::TActorBootstrapped<TSplitRequestSenderActor<TMethod>>;

public:
    TSplitRequestSenderActor(
        TRequestInfoPtr requestInfo,
        TVector<TSplitRequest> requests,
        TString diskId,
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
    TString diskId,
    NActors::TActorId parentActorId,
    ui64 requestId)
    : RequestInfo(std::move(requestInfo))
    , Requests(std::move(requests))
    , DiskId(std::move(diskId))
    , ParentActorId(parentActorId)
    , RequestId(requestId)
{
    Y_DEBUG_ABORT_UNLESS(!requests.empty());
    Y_DEBUG_ABORT_UNLESS(!DiskId.empty());
    Y_DEBUG_ABORT_UNLESS(parentActorId);
    Y_DEBUG_ABORT_UNLESS(!DiskId.empty());
}

template <typename TMethod>
void TSplitRequestSenderActor<TMethod>::Bootstrap(const NActors::TActorContext& ctx) {
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
void TSplitRequestSenderActor<TMethod>::SendRequests(const NActors::TActorContext& ctx) {
    for (auto& request: Requests) {
        Y_DEBUG_ABORT_UNLESS(request.CallContext);

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
    LOG_WARN(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s request undelivered to some nonrepl partitions",
        DiskId.c_str(),
        TMethod::Name);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " request undelivered to some nonrepl partitions"));

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

    if (HasError(msg->Record)) {
        // TODO: FIX log
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "[%s] %s got error from nonreplicated partition: %s",
            DiskId.c_str(),
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());
    }

    if (!HasError(Record)) {
        Record = std::move(msg->Record);
    }

    if (++Responses < Requests.size()) {
        return;
    }

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
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TIncompleteMirrorRWModeControllerActor::TIncompleteMirrorRWModeControllerActor(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString rwClientId,
        TActorId partNonreplActorId,
        TActorId statActorId,
        TActorId mirrorPartitionActor)
    : Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , RwClientId(std::move(rwClientId))
    , PartNonreplActorId(partNonreplActorId)
    , StatActorId(statActorId)
    , MirrorPartitionActor(mirrorPartitionActor)
    , PoisonPillHelper(this)
{
}

TIncompleteMirrorRWModeControllerActor::
    ~TIncompleteMirrorRWModeControllerActor() = default;

void TIncompleteMirrorRWModeControllerActor::Bootstrap(const TActorContext& ctx)
{
    PoisonPillHelper.TakeOwnership(ctx, PartNonreplActorId);
    Become(&TThis::StateWork);
}

bool TIncompleteMirrorRWModeControllerActor::ShouldSplitWriteRequest(
    const TVector<TDeviceRequest>& requests) const
{
    Y_DEBUG_ABORT_UNLESS(!requests.empty());

    TSet<TString> agents;
    for (const auto& request: requests) {
        agents.insert(request.Device.GetAgentId());
    }

    if (agents.size() == 1) {
        return false;
    }

    TVector<std::optional<TIncompleteMirrorRWModeControllerActor::EAgentState>>
        states;
    for (const auto& agent: agents) {
        if (AgentState.contains(agent)) {
            states.push_back(AgentState.at(agent).State);
        } else {
            states.push_back(std::nullopt);
        }
    }

    Unique(states.begin(), states.end());
    return states.size() != 1;
}

void TIncompleteMirrorRWModeControllerActor::OnMigrationProgress(
    const TString& agentId,
    ui64 processedBlockCount,
    ui64 blockCountNeedToBeProcessed)
{
    Y_UNUSED(agentId);
    Y_UNUSED(processedBlockCount);
    Y_UNUSED(blockCountNeedToBeProcessed);
    Y_DEBUG_ABORT_UNLESS(false);
    // Y_DEBUG_ABORT_UNLESS(AgentState.contains(agentId));
    // auto& state = AgentState[agentId];
    // Y_DEBUG_ABORT_UNLESS(state.SmartResyncActor);

    // NCloud::Send(
    //     ActorContext(),
    //     PartConfig->GetParentActorId(),
    //     std::make_unique<TEvVolume::TEvUpdateSmartResyncState>(
    //         processedBlockCount,
    //         blockCountNeedToBeProcessed));
}

void TIncompleteMirrorRWModeControllerActor::OnMigrationFinished(
    const TString& agentId)
{
    Y_UNUSED(agentId);
    Y_DEBUG_ABORT_UNLESS(false);
    // Y_DEBUG_ABORT_UNLESS(AgentState.contains(agentId));
    // auto& state = AgentState[agentId];
    // Y_DEBUG_ABORT_UNLESS(state.SmartResyncActor);

    // NCloud::Send(
    //     ActorContext(),
    //     PartConfig->GetParentActorId(),
    //     std::make_unique<TEvVolume::TEvSmartResyncFinished>(agentId));
}

void TIncompleteMirrorRWModeControllerActor::OnMigrationError(
    const TString& agentId)
{
    Y_UNUSED(agentId);
    Y_DEBUG_ABORT_UNLESS(false);
    // Y_DEBUG_ABORT_UNLESS(AgentState.contains(agentId));
    // auto& state = AgentState[agentId];
    // Y_DEBUG_ABORT_UNLESS(state.SmartResyncActor);

    // // TODO: Abort this and start real resync?
}

void TIncompleteMirrorRWModeControllerActor::HandleAgentIsUnavailable(
    const NPartition::TEvPartition::TEvAgentIsUnavailable::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "HandleAgentIsUnavailable %s",
        msg->AgentId.c_str());

    TAgentState* state = AgentState.FindPtr(msg->AgentId);
    if (!state || state->State == EAgentState::Resyncing) {
        auto& state = AgentState[msg->AgentId];
        state.State = EAgentState::Unavailable;
        state.AgentAvailabilityWaiter = NCloud::Register(
            ctx,
            std::make_unique<TAgentAvailabilityWaiterActor>(
                Config,
                PartConfig,
                PartNonreplActorId,
                SelfId(),
                StatActorId,
                msg->AgentId));
        PoisonPillHelper.TakeOwnership(ctx, state.AgentAvailabilityWaiter);

        if (state.SmartResyncActor) {
            Y_DEBUG_ABORT_UNLESS(state.CleanBlocksMap);

            PoisonPillHelper.ReleaseOwnership(ctx, state.SmartResyncActor);
            // TODO: Either add a lock to sturcture, or handle poisontaken event.
            NCloud::Send<TEvents::TEvPoisonPill>(ctx, state.SmartResyncActor);
            state.SmartResyncActor = TActorId();
        } else {
            state.CleanBlocksMap =
                std::make_shared<TCompressedBitmap>(PartConfig->GetBlockCount());
            state.CleanBlocksMap->Set(0, PartConfig->GetBlockCount());
        }
    } else {
        Y_DEBUG_ABORT_UNLESS(AgentState[msg->AgentId].AgentAvailabilityWaiter);
        Y_DEBUG_ABORT_UNLESS(!AgentState[msg->AgentId].SmartResyncActor);
    }

    NCloud::Send(
        ctx,
        PartNonreplActorId,
        std::make_unique<NPartition::TEvPartition::TEvAgentIsUnavailable>(
            msg->AgentId));
}

void TIncompleteMirrorRWModeControllerActor::HandleAgentIsBackOnline(
    const NPartition::TEvPartition::TEvAgentIsBackOnline::TPtr& ev,
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

    switch (AgentState[msg->AgentId].State) {
        case EAgentState::Unavailable: {
            TAgentState& state = AgentState[msg->AgentId];
            Y_DEBUG_ABORT_UNLESS(!state.SmartResyncActor);
            Y_DEBUG_ABORT_UNLESS(state.AgentAvailabilityWaiter);

            PoisonPillHelper.ReleaseOwnership(
                ctx,
                state.AgentAvailabilityWaiter);
            NCloud::Send<TEvents::TEvPoisonPill>(
                ctx,
                state.AgentAvailabilityWaiter);
            state.AgentAvailabilityWaiter = TActorId();

            state.SmartResyncActor = NCloud::Register(
                ctx,
                std::make_unique<TSmartResyncActor>(
                    this,
                    Config,
                    PartConfig,
                    ProfileLog,
                    BlockDigestGenerator,
                    RwClientId,
                    PartNonreplActorId,
                    StatActorId,
                    MirrorPartitionActor,
                    SelfId(),
                    state.CleanBlocksMap,
                    msg->AgentId));
            PoisonPillHelper.TakeOwnership(ctx, state.SmartResyncActor);
            state.State = EAgentState::Resyncing;
            break;
        }
        case EAgentState::Resyncing: {
            const TAgentState& state = AgentState[msg->AgentId];
            Y_DEBUG_ABORT_UNLESS(state.SmartResyncActor);
            Y_DEBUG_ABORT_UNLESS(!state.AgentAvailabilityWaiter);
            // no-op?
            break;
        }
    }

    NCloud::Send(
        ctx,
        PartNonreplActorId,
        std::make_unique<NPartition::TEvPartition::TEvAgentIsBackOnline>(
            msg->AgentId));
}

bool TIncompleteMirrorRWModeControllerActor::AgentIsUnavailable(
    const TString& agentId) const
{
    const TAgentState* state = AgentState.FindPtr(agentId);
    return state && state->State == EAgentState::Unavailable;
}

void TIncompleteMirrorRWModeControllerActor::MarkBlocksAsDirty(
    const TString& unavailableAgentId,
    TBlockRange64 range)
{
    Y_DEBUG_ABORT_UNLESS(AgentState.contains(unavailableAgentId));
    auto& state = AgentState[unavailableAgentId];
    Y_DEBUG_ABORT_UNLESS(state.CleanBlocksMap);
    state.CleanBlocksMap->Unset(range.Start, range.End);
}

template <typename TMethod>
void TIncompleteMirrorRWModeControllerActor::WriteRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui32 requestBlockCount = CalculateWriteRequestBlockCount(
        msg->Record,
        PartConfig->GetBlockSize());
    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        requestBlockCount);
    auto deviceRequests = PartConfig->ToDeviceRequests(blockRange);

    const bool shouldRespondWithSuccess = AllOf(
        deviceRequests,
        [this](const auto& deviceRequest)
        { return AgentIsUnavailable(deviceRequest.Device.GetAgentId()); });
    if (shouldRespondWithSuccess) {
        MarkBlocksAsDirty(deviceRequests[0].Device.GetAgentId(), blockRange);
        NCloud::Reply(ctx, *ev, std::make_unique<typename TMethod::TResponse>());
        return;
    }

    auto requests = SplitRequest<TMethod>(ev, deviceRequests);
    Y_DEBUG_ABORT_UNLESS(requests.size() == deviceRequests.size());
    for (const auto& deviceRequest: deviceRequests) {
        const auto& agentId = deviceRequest.Device.GetAgentId();
        if (AgentIsUnavailable(agentId)) {
            MarkBlocksAsDirty(agentId, deviceRequest.BlockRange);
        }
    }
    NCloud::Register(
        ctx,
        std::make_unique<TSplitRequestSenderActor<TMethod>>(
            CreateRequestInfo<TMethod>(
                ev->Sender,
                ev->Cookie,
                msg->CallContext),
            std::move(requests),
            msg->Record.GetDiskId(),
            SelfId(),
            GetRequestId(msg->Record)));
}

template <typename TMethod>
TVector<TSplitRequest> TIncompleteMirrorRWModeControllerActor::SplitRequest(
    const TMethod::TRequest::TPtr& ev,
    const TVector<TDeviceRequest>& deviceRequests)
{
    auto* msg = ev->Get();
    if (!ShouldSplitWriteRequest(deviceRequests)) {
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

    return DoSplitRequest(ev, deviceRequests);
}

TVector<TSplitRequest> TIncompleteMirrorRWModeControllerActor::DoSplitRequest(
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

TVector<TSplitRequest> TIncompleteMirrorRWModeControllerActor::DoSplitRequest(
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
        CopyCommonRequestData<TEvService::TWriteBlocksLocalMethod>(ev, *request);

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

TVector<TSplitRequest> TIncompleteMirrorRWModeControllerActor::DoSplitRequest(
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

NActors::TActorId TIncompleteMirrorRWModeControllerActor::GetRecipientActorId(
    const TString& agentId) const
{
    if (!AgentState.contains(agentId)) {
        return PartNonreplActorId;
    }

    switch(AgentState.at(agentId).State) {
        case EAgentState::Unavailable:
            return {};
        case EAgentState::Resyncing:
            Y_DEBUG_ABORT_UNLESS(AgentState.at(agentId).SmartResyncActor);
            return AgentState.at(agentId).SmartResyncActor;
    }
}

void TIncompleteMirrorRWModeControllerActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteRequest<TEvService::TWriteBlocksMethod>(ev, ctx);
    auto request = std::make_unique<TEvService::TWriteBlocksMethod::TRequest>();
}

void TIncompleteMirrorRWModeControllerActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteRequest<TEvService::TWriteBlocksLocalMethod>(ev, ctx);
}

void TIncompleteMirrorRWModeControllerActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteRequest<TEvService::TZeroBlocksMethod>(ev, ctx);
}

void TIncompleteMirrorRWModeControllerActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const TActorContext& ctx)
{
    RwClientId = ev->Get()->RWClientId;
    auto& callContext = *ev->Get()->CallContext;

    {
        auto partitionRequest =
            std::make_unique<TEvVolume::TEvRWClientIdChanged>(RwClientId);
        if (!callContext.LWOrbit.Fork(partitionRequest->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                callContext.LWOrbit,
                "TEvVolume::TEvRWClientIdChanged",
                callContext.RequestId);
        }
        ctx.Send(PartNonreplActorId, std::move(partitionRequest));
    }

    for (const auto& state: AgentState) {
        Y_DEBUG_ABORT_UNLESS(
            !state.second.AgentAvailabilityWaiter ||
            !state.second.SmartResyncActor);

        auto companionRequest =
            std::make_unique<TEvVolume::TEvRWClientIdChanged>(RwClientId);
        if (!callContext.LWOrbit.Fork(companionRequest->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                callContext.LWOrbit,
                "TEvVolume::TEvRWClientIdChanged",
                callContext.RequestId);
        }

        if (state.second.AgentAvailabilityWaiter) {
            ctx.Send(
                state.second.AgentAvailabilityWaiter,
                std::move(companionRequest));
        } else if (state.second.SmartResyncActor) {
            ctx.Send(
                state.second.SmartResyncActor,
                std::move(companionRequest));
        }
    }
}

void TIncompleteMirrorRWModeControllerActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Become(&TThis::StateZombie);
    PoisonPillHelper.HandlePoisonPill(ev, ctx);
    AgentState.clear();
}

void TIncompleteMirrorRWModeControllerActor::Die(
    const NActors::TActorContext& ctx)
{
    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TIncompleteMirrorRWModeControllerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(
            NPartition::TEvPartition::TEvAgentIsUnavailable,
            HandleAgentIsUnavailable);
        HFunc(
            NPartition::TEvPartition::TEvAgentIsBackOnline,
            HandleAgentIsBackOnline);
        HFunc(TEvVolume::TEvRWClientIdChanged, HandleRWClientIdChanged);
        // HFunc(NPartition::TEvPartition::TEvDrainRequest, HandleDrain);

        // HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);

        // HFunc(TEvPartition::TEvDrainRequest,
        // DrainActorCompanion.HandleDrain);
        // HFunc(TEvPartition::TEvEnterIncompleteMirrorRWModeRequest,
        // HandleEnterIncompleteMirrorRWMode);

        // HFunc(
        //     TEvService::TEvGetChangedBlocksRequest,
        //     GetChangedBlocksCompanion.HandleGetChangedBlocks);

        // HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocks);
        // HFunc(TEvVolume::TEvGetCompactionStatusRequest,
        // HandleGetCompactionStatus); HFunc(TEvVolume::TEvCompactRangeRequest,
        // HandleCompactRange); HFunc(TEvVolume::TEvRebuildMetadataRequest,
        // HandleRebuildMetadata);
        // HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest,
        // HandleGetRebuildMetadataStatus); HFunc(TEvVolume::TEvScanDiskRequest,
        // HandleScanDisk); HFunc(TEvVolume::TEvGetScanDiskStatusRequest,
        // HandleGetScanDiskStatus);

        // HFunc(
        //     TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted,
        //     HandleWriteOrZeroCompleted);

        // HFunc(
        //     TEvVolume::TEvDiskRegistryBasedPartitionCounters,
        //     HandlePartCounters);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            // NOTE: We handle all unexpected requests and proxy them to
            // nonreplicated partititon.
            ForwardUnexpectedEvent(ev, ActorContext());
            break;
    }
}

void TIncompleteMirrorRWModeControllerActor::ForwardUnexpectedEvent(
    TAutoPtr<::NActors::IEventHandle>& ev,
    const TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, PartNonreplActorId);
}

STFUNC(TIncompleteMirrorRWModeControllerActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvScrubbingNextRange);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse);

        HFunc(TEvService::TEvWriteBlocksRequest, RejectWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, RejectZeroBlocks);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, RejectWriteBlocksLocal);

        // HFunc(NPartition::TEvPartition::TEvDrainRequest, RejectDrain);

        // HFunc(TEvVolume::TEvDescribeBlocksRequest, RejectDescribeBlocks);
        // HFunc(TEvVolume::TEvGetCompactionStatusRequest,
        // RejectGetCompactionStatus); HFunc(TEvVolume::TEvCompactRangeRequest,
        // RejectCompactRange); HFunc(TEvVolume::TEvRebuildMetadataRequest,
        // RejectRebuildMetadata);
        // HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest,
        // RejectGetRebuildMetadataStatus); HFunc(TEvVolume::TEvScanDiskRequest,
        // RejectScanDisk); HFunc(TEvVolume::TEvGetScanDiskStatusRequest,
        // RejectGetScanDiskStatus);

        // IgnoreFunc(TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted);

        // IgnoreFunc(TEvVolume::TEvDiskRegistryBasedPartitionCounters);

        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);
        IgnoreFunc(TEvents::TEvPoisonPill);
        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        default:
            ForwardUnexpectedEvent(ev, ActorContext());
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
