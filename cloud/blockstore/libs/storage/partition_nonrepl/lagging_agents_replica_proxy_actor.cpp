#include "lagging_agents_replica_proxy_actor.h"

#include "agent_availability_monitoring_actor.h"
#include "lagging_agent_migration_actor.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/storage/core/libs/common/sglist_block_range.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/library/actors/core/executor_thread.h>

namespace NCloud::NBlockStore::NStorage {
namespace {

using namespace NActors;
using namespace NKikimr;
using TSplitRequest = TLaggingAgentsReplicaProxyActor::TSplitRequest;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TSplitRequestSenderActor final
    : public NActors::TActorBootstrapped<TSplitRequestSenderActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    TVector<TSplitRequest> Requests;
    const NActors::TActorId ParentActorId;
    const TString DiskId;
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
        TString diskId,
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
        TString diskId,
        ui64 requestId)
    : RequestInfo(std::move(requestInfo))
    , Requests(std::move(requests))
    , ParentActorId(parentActorId)
    , DiskId(std::move(diskId))
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

        auto event = std::make_unique<NActors::IEventHandle>(
            request.RecipientActorId,
            ctx.SelfID,
            request.Request.release(),
            NActors::IEventHandle::FlagForwardOnNondelivery,
            RequestInfo->Cookie,   // cookie
            &ctx.SelfID            // forwardOnNondelivery
        );
        ctx.Send(std::move(event));

        LOG_TRACE(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "[%s] Splitted %s request #%lu has been sent to %s",
            DiskId.c_str(),
            TMethod::Name,
            RequestId,
            request.DeviceUUID.c_str());
    }

    if (Responses == Requests.size()) {
        Done(ctx);
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

    LWTRACK(
        ResponseSent_PartitionWorker,
        callContext.LWOrbit,
        TMethod::Name,
        callContext.RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

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

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s request %lu undelivered to some nonrepl partitions",
        DiskId.c_str(),
        TMethod::Name,
        RequestId);

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

    if (HasError(msg->Record)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
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
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TLaggingAgentsReplicaProxyActor::TLaggingAgentsReplicaProxyActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TNonreplicatedPartitionConfigPtr partConfig,
        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString rwClientId,
        TActorId nonreplPartitionActorId,
        TActorId mirrorPartitionActorId)
    : Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , PartConfig(std::move(partConfig))
    , Migrations(std::move(migrations))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , RwClientId(std::move(rwClientId))
    , NonreplPartitionActorId(nonreplPartitionActorId)
    , MirrorPartitionActorId(mirrorPartitionActorId)
    , PoisonPillHelper(this)
{}

TLaggingAgentsReplicaProxyActor::~TLaggingAgentsReplicaProxyActor() = default;

void TLaggingAgentsReplicaProxyActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);
}

bool TLaggingAgentsReplicaProxyActor::AgentIsUnavailable(
    const TString& agentId) const
{
    const TAgentState* state = AgentState.FindPtr(agentId);
    return state && state->State == EAgentState::Unavailable;
}

void TLaggingAgentsReplicaProxyActor::MarkBlocksAsDirty(
    const TActorContext& ctx,
    const TString& unavailableAgentId,
    TBlockRange64 range)
{
    // We artificially lower the resolution of the "CleanBlocksMap". This is
    // done because the migration actor migrates by ranges of
    // "ProcessingRangeSize" anyway. The full resolution here would lead to
    // migrating not aligned ranges which is not ideal.
    const ui64 blocksPerRange =
        ProcessingRangeSize / PartConfig->GetBlockSize();
    auto alignedStart = AlignDown<ui64>(range.Start, blocksPerRange);
    auto alignedEnd = AlignUp<ui64>(range.End + 1, blocksPerRange);

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Lower block range resolution %s -> %s and mark blocks as dirty "
        "for lagging agent %s",
        PartConfig->GetName().c_str(),
        range.Print().c_str(),
        TBlockRange64::MakeClosedInterval(alignedStart, alignedEnd)
            .Print()
            .c_str(),
        unavailableAgentId.Quote().c_str());

    auto& state = AgentState[unavailableAgentId];
    Y_ABORT_UNLESS(state.CleanBlocksMap);
    state.CleanBlocksMap->Unset(alignedStart, alignedEnd);
}

void TLaggingAgentsReplicaProxyActor::DestroyChildActor(
    const TActorContext& ctx,
    TActorId* actorId)
{
    Y_DEBUG_ABORT_UNLESS(actorId);

    if (!*actorId) {
        return;
    }

    PoisonPillHelper.ReleaseOwnership(ctx, *actorId);
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, *actorId);
    *actorId = TActorId();
}

template <typename TMethod>
void TLaggingAgentsReplicaProxyActor::ReadBlocks(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // Make sure that read requests are not going to lagging agents.
    const auto* msg = ev->Get();
    const auto blockRange =
        BuildRequestBlockRange(*msg, PartConfig->GetBlockSize());
    const auto deviceRequests = PartConfig->ToDeviceRequests(blockRange);
    for (const auto& deviceRequest: deviceRequests) {
        const auto& agentId = deviceRequest.Device.GetAgentId();
        const TActorId recipient = GetRecipientActorId(
            deviceRequest.BlockRange,
            true   // forRead
        );
        if (recipient != NonreplPartitionActorId) {
            TString message = TStringBuilder()
                              << "Desired recipient actor is not "
                                 "nonreplicated partition for device: "
                              << deviceRequest.Device.GetDeviceUUID()
                              << ", lagging agent: " << agentId
                              << ", disk id: " << PartConfig->GetName();
            ReportLaggingAgentsProxyWrongRecipientActor(message);
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<typename TMethod::TResponse>(
                    MakeError(E_REJECTED, std::move(message))));
            return;
        }
    }

    ForwardMessageToActor(ev, ctx, NonreplPartitionActorId);
}

template <typename TMethod>
void TLaggingAgentsReplicaProxyActor::WriteBlocks(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo<TMethod>(ev->Sender, ev->Cookie, msg->CallContext);
    TRequestScope timer(*requestInfo);

    const ui32 requestBlockCount = CalculateWriteRequestBlockCount(
        msg->Record,
        PartConfig->GetBlockSize());
    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        requestBlockCount);
    auto deviceRequests = PartConfig->ToDeviceRequests(blockRange);
    for (const auto& deviceRequest: deviceRequests) {
        const auto& blockRangeData =
            GetBlockRangeDataByBlockRange(deviceRequest.BlockRange);
        if (AgentIsUnavailable(blockRangeData.LaggingAgentId)) {
            MarkBlocksAsDirty(
                ctx,
                blockRangeData.LaggingAgentId,
                deviceRequest.BlockRange);
        }
    }

    auto requests = SplitRequest<TMethod>(ev, deviceRequests);
    Y_DEBUG_ABORT_UNLESS(!requests.empty());
    Y_DEBUG_ABORT_UNLESS(requests.size() <= deviceRequests.size());

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s request #%lu with range %s has been split into %u parts. %u "
        "of them will be dropped.",
        PartConfig->GetName().c_str(),
        TMethod::Name,
        GetRequestId(msg->Record),
        blockRange.Print().c_str(),
        requests.size(),
        CountIf(
            requests,
            [](const TSplitRequest& request)
            { return !request.RecipientActorId; }));

    NCloud::Register(
        ctx,
        std::make_unique<TSplitRequestSenderActor<TMethod>>(
            requestInfo,
            std::move(requests),
            SelfId(),
            PartConfig->GetName(),
            GetRequestId(msg->Record)));
}

template <typename TMethod>
TVector<TSplitRequest> TLaggingAgentsReplicaProxyActor::SplitRequest(
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
        GetRecipientActorId(
            deviceRequests[0].BlockRange,
            false),   // forRead

        deviceRequests[0].Device.GetDeviceUUID());

    return result;
}

bool TLaggingAgentsReplicaProxyActor::ShouldSplitWriteRequest(
    const TVector<TDeviceRequest>& deviceRequests) const
{
    Y_DEBUG_ABORT_UNLESS(!deviceRequests.empty());

    THashSet<NActors::TActorId> recipientActors;
    for (const auto& deviceRequest: deviceRequests) {
        recipientActors.insert(GetRecipientActorId(
            deviceRequest.BlockRange,
            false));   // forRead
    }
    return recipientActors.size() > 1;
}

TVector<TSplitRequest> TLaggingAgentsReplicaProxyActor::DoSplitRequest(
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
            GetRecipientActorId(
                deviceRequest.BlockRange,
                false),   // forRead

            deviceRequest.Device.GetDeviceUUID());
    }

    return result;
}

TVector<TSplitRequest> TLaggingAgentsReplicaProxyActor::DoSplitRequest(
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
            GetRecipientActorId(
                deviceRequest.BlockRange,
                false),   // forRead

            deviceRequest.Device.GetDeviceUUID());
    }

    return result;
}

TVector<TSplitRequest> TLaggingAgentsReplicaProxyActor::DoSplitRequest(
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
            GetRecipientActorId(
                deviceRequest.BlockRange,
                false),   // forRead

            deviceRequest.Device.GetDeviceUUID());
    }

    return result;
}

NActors::TActorId TLaggingAgentsReplicaProxyActor::GetRecipientActorId(
    const TBlockRange64& requestBlockRange,
    bool forRead) const
{
    const auto& blockRangeData =
        GetBlockRangeDataByBlockRange(requestBlockRange);
    const auto& agentId = blockRangeData.LaggingAgentId;
    // Lagging target migration reads can always be sent directly to partition.
    if (agentId.empty() || (blockRangeData.IsTargetMigration && forRead)) {
        return NonreplPartitionActorId;
    }

    const auto* agentState = AgentState.FindPtr(agentId);
    if (!agentState) {
        return NonreplPartitionActorId;
    }

    switch (agentState->State) {
        case EAgentState::Unavailable:
            if (blockRangeData.IsTargetMigration) {
                return NonreplPartitionActorId;
            }
            return {};
        case EAgentState::WaitingForDrain:
        case EAgentState::Resyncing:
            Y_ABORT_UNLESS(agentState->MigrationActorId);
            return agentState->MigrationActorId;
    }
    Y_ABORT("Unknown enum value: %u", static_cast<ui8>(agentState->State));
}

auto TLaggingAgentsReplicaProxyActor::GetBlockRangeDataByBlockRange(
    const TBlockRange64& requestBlockRange) const -> const TBlockRangeData&
{
    const auto it =
        LaggingAgentIdByBlockRangeEnd.lower_bound(requestBlockRange.End);
    Y_ABORT_UNLESS(it != LaggingAgentIdByBlockRangeEnd.end());
    Y_DEBUG_ABORT_UNLESS(
        LaggingAgentIdByBlockRangeEnd.lower_bound(requestBlockRange.Start) ==
        it);
    return it->second;
}

void TLaggingAgentsReplicaProxyActor::RecalculateLaggingAgentIdByBlockRangeEnd()
{
    ui64 blockIndex = 0;
    for (const auto& device: PartConfig->GetDevices()) {
        blockIndex += device.GetBlocksCount();

        if (AgentState.contains(device.GetAgentId())) {
            LaggingAgentIdByBlockRangeEnd[blockIndex - 1] = {
                .LaggingAgentId = device.GetAgentId(),
                .IsTargetMigration = false};
            // There can be only one lagging device per disk "row". So it is
            // safe to continue here.
            continue;
        }

        // Migration target can also be lagging.
        const auto* migration = FindIfPtr(
            Migrations,
            [&](const NProto::TDeviceMigration& migration)
            {
                return migration.GetSourceDeviceId() == device.GetDeviceUUID();
            });
        if (migration) {
            const auto& targetAgentId =
                migration->GetTargetDevice().GetAgentId();
            if (AgentState.contains(targetAgentId)) {
                LaggingAgentIdByBlockRangeEnd[blockIndex - 1] = {
                    .LaggingAgentId = targetAgentId,
                    .IsTargetMigration = true};
                continue;
            }
        }

        LaggingAgentIdByBlockRangeEnd[blockIndex - 1] = {};
    }
}

void TLaggingAgentsReplicaProxyActor::HandleAgentIsUnavailable(
    const TEvNonreplPartitionPrivate::TEvAgentIsUnavailable::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Agent %s went unavailable. Creating availability monitor",
        PartConfig->GetName().c_str(),
        msg->LaggingAgent.GetAgentId().Quote().c_str());

    NCloud::Send(
        ctx,
        NonreplPartitionActorId,
        std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsUnavailable>(
            msg->LaggingAgent));

    const auto& agentId = msg->LaggingAgent.GetAgentId();
    if (TAgentState* state = AgentState.FindPtr(agentId);
        state && state->State == EAgentState::Unavailable)
    {
        return;
    }

    auto& state = AgentState[agentId];
    state.State = EAgentState::Unavailable;
    state.LaggingAgent = msg->LaggingAgent;

    RecalculateLaggingAgentIdByBlockRangeEnd();

    if (state.MigrationActorId) {
        Y_DEBUG_ABORT_UNLESS(state.CleanBlocksMap);
        DestroyChildActor(ctx, &state.MigrationActorId);
    } else if (!state.CleanBlocksMap) {
        state.CleanBlocksMap =
            std::make_unique<TCompressedBitmap>(PartConfig->GetBlockCount());
        state.CleanBlocksMap->Set(0, PartConfig->GetBlockCount());
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Agent %s block count: %lu, dirty block count: %lu",
        PartConfig->GetName().c_str(),
        agentId.c_str(),
        PartConfig->GetBlockCount(),
        PartConfig->GetBlockCount() - state.CleanBlocksMap->Count());

    state.AvailabilityMonitoringActorId = NCloud::Register(
        ctx,
        std::make_unique<TAgentAvailabilityMonitoringActor>(
            Config,
            PartConfig,
            Migrations,
            NonreplPartitionActorId,
            SelfId(),
            state.LaggingAgent));
    PoisonPillHelper.TakeOwnership(ctx, state.AvailabilityMonitoringActorId);
}

void TLaggingAgentsReplicaProxyActor::HandleAgentIsBackOnline(
    const TEvNonreplPartitionPrivate::TEvAgentIsBackOnline::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto* state = AgentState.FindPtr(msg->AgentId);
    Y_DEBUG_ABORT_UNLESS(state);
    if (!state) {
        return;
    }
    switch (state->State) {
        case EAgentState::Unavailable:
            break;
        case EAgentState::WaitingForDrain:
            return;
        case EAgentState::Resyncing:
            Y_DEBUG_ABORT_UNLESS(false);
            return;
    }
    state->State = EAgentState::WaitingForDrain;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Lagging agent %s is back online. Draining other partitions",
        PartConfig->GetName().c_str(),
        msg->AgentId.Quote().c_str());

    NCloud::Send(
        ctx,
        NonreplPartitionActorId,
        std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsBackOnline>(
            msg->AgentId));

    DrainRequestCounter++;
    Y_ABORT_UNLESS(!CurrentDrainingAgents.contains(DrainRequestCounter));
    CurrentDrainingAgents[DrainRequestCounter] = msg->AgentId;
    NCloud::Send<NPartition::TEvPartition::TEvWaitForInFlightWritesRequest>(
        ctx,
        MirrorPartitionActorId,
        DrainRequestCounter);

    Y_DEBUG_ABORT_UNLESS(!state->MigrationActorId);
    Y_DEBUG_ABORT_UNLESS(state->AvailabilityMonitoringActorId);

    DestroyChildActor(ctx, &state->AvailabilityMonitoringActorId);

    TCompressedBitmap cleanBlocksCopy(PartConfig->GetBlockCount());
    cleanBlocksCopy.Update(*state->CleanBlocksMap, 0);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Preparing lagging agent %s migration. Block count: %lu, dirty "
        "block count: %lu",
        PartConfig->GetName().c_str(),
        msg->AgentId.Quote().c_str(),
        PartConfig->GetBlockCount(),
        PartConfig->GetBlockCount() - cleanBlocksCopy.Count());

    state->MigrationActorId = NCloud::Register(
        ctx,
        std::make_unique<TLaggingAgentMigrationActor>(
            Config,
            DiagnosticsConfig,
            PartConfig,
            SelfId(),
            ProfileLog,
            BlockDigestGenerator,
            RwClientId,
            NonreplPartitionActorId,
            MirrorPartitionActorId,
            std::move(cleanBlocksCopy),
            msg->AgentId));
    PoisonPillHelper.TakeOwnership(ctx, state->MigrationActorId);
}

void TLaggingAgentsReplicaProxyActor::HandleLaggingAgentMigrationFinished(
    const TEvVolumePrivate::TEvLaggingAgentMigrationFinished::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto it = AgentState.find(msg->AgentId);
    Y_DEBUG_ABORT_UNLESS(it != AgentState.end());
    if (it == AgentState.end()) {
        return;
    }

    auto& state = it->second;
    switch (state.State) {
        case EAgentState::Unavailable:
            return;
        case EAgentState::WaitingForDrain:
            Y_DEBUG_ABORT_UNLESS(false);
            return;
        case EAgentState::Resyncing:
            break;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Lagging agent %s migration has been finished",
        PartConfig->GetName().c_str(),
        msg->AgentId.Quote().c_str());

    Y_DEBUG_ABORT_UNLESS(state.MigrationActorId);
    Y_DEBUG_ABORT_UNLESS(!state.AvailabilityMonitoringActorId);
    DestroyChildActor(ctx, &state.MigrationActorId);
    AgentState.erase(it);
    RecalculateLaggingAgentIdByBlockRangeEnd();

    ctx.Send(std::make_unique<NActors::IEventHandle>(
        PartConfig->GetParentActorId(),
        SelfId(),
        ev->ReleaseBase().Release()));
}

void TLaggingAgentsReplicaProxyActor::HandleWaitForInFlightWrites(
    const NPartition::TEvPartition::TEvWaitForInFlightWritesRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, NonreplPartitionActorId);
}

void TLaggingAgentsReplicaProxyActor::HandleWaitForInFlightWritesResponse(
    const NPartition::TEvPartition::TEvWaitForInFlightWritesResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto it = CurrentDrainingAgents.find(ev->Cookie);
    if (it == CurrentDrainingAgents.end()) {
        return;
    }

    TString agentId = std::move(it->second);
    CurrentDrainingAgents.erase(it);

    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "[%s] Failed to drain partitions for lagging agent %s: %s",
            PartConfig->GetName().c_str(),
            agentId.Quote().c_str(),
            FormatError(msg->GetError()).c_str());

        DrainRequestCounter++;
        Y_ABORT_UNLESS(!CurrentDrainingAgents.contains(DrainRequestCounter));
        CurrentDrainingAgents[DrainRequestCounter] = agentId;

        ctx.ExecutorThread.Schedule(
            TDuration::Seconds(1),
            new IEventHandle(
                MirrorPartitionActorId,
                SelfId(),
                new NPartition::TEvPartition::
                    TEvWaitForInFlightWritesRequest()));
        return;
    }

    auto* state = AgentState.FindPtr(agentId);
    Y_DEBUG_ABORT_UNLESS(state);
    if (!state) {
        return;
    }

    state->DrainIsFinished = true;
    if (state->MigrationIsDisabled) {
        return;
    }

    switch (state->State) {
        case EAgentState::Unavailable:
            return;
        case EAgentState::WaitingForDrain:
            break;
        case EAgentState::Resyncing:
            Y_DEBUG_ABORT_UNLESS(false);
            return;
    }
    StartLaggingResync(ctx, agentId, state);
}

void TLaggingAgentsReplicaProxyActor::HandleLaggingMigrationDisabled(
    const TEvNonreplPartitionPrivate::TEvLaggingMigrationDisabled::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto* state = AgentState.FindPtr(msg->AgentId);
    if (!state) {
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Lagging agent %s is a target for a migration. Writes to it are "
        "disabled.",
        PartConfig->GetName().c_str(),
        msg->AgentId.Quote().c_str());

    state->MigrationIsDisabled = true;
}

void TLaggingAgentsReplicaProxyActor::HandleLaggingMigrationEnabled(
    const TEvNonreplPartitionPrivate::TEvLaggingMigrationEnabled::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto* state = AgentState.FindPtr(msg->AgentId);
    if (!state || !state->MigrationIsDisabled) {
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Migration to lagging agent %s is enabled",
        PartConfig->GetName().c_str(),
        msg->AgentId.Quote().c_str());

    state->MigrationIsDisabled = false;
    if (!state->DrainIsFinished) {
        return;
    }

    switch (state->State) {
        case EAgentState::Unavailable:
            return;
        case EAgentState::WaitingForDrain:
            break;
        case EAgentState::Resyncing:
            Y_DEBUG_ABORT_UNLESS(false);
            return;
    }
    StartLaggingResync(ctx, msg->AgentId, state);
}

void TLaggingAgentsReplicaProxyActor::StartLaggingResync(
    const NActors::TActorContext& ctx,
    const TString& agentId,
    TAgentState* state)
{
    Y_ABORT_UNLESS(state->DrainIsFinished && !state->MigrationIsDisabled);

    state->State = EAgentState::Resyncing;
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Starting lagging agent %s migration. Block count: %lu, dirty "
        "block count: %lu",
        PartConfig->GetName().c_str(),
        agentId.Quote().c_str(),
        PartConfig->GetBlockCount(),
        PartConfig->GetBlockCount() - state->CleanBlocksMap->Count());

    Y_ABORT_UNLESS(state->MigrationActorId);
    NCloud::Send<TEvNonreplPartitionPrivate::TEvStartLaggingAgentMigration>(
        ctx,
        state->MigrationActorId);
    state->State = EAgentState::Resyncing;
}

void TLaggingAgentsReplicaProxyActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteBlocks<TEvService::TWriteBlocksMethod>(ev, ctx);
}

void TLaggingAgentsReplicaProxyActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteBlocks<TEvService::TWriteBlocksLocalMethod>(ev, ctx);
}

void TLaggingAgentsReplicaProxyActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteBlocks<TEvService::TZeroBlocksMethod>(ev, ctx);
}

void TLaggingAgentsReplicaProxyActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadBlocks<TEvService::TReadBlocksMethod>(ev, ctx);
}

void TLaggingAgentsReplicaProxyActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadBlocks<TEvService::TReadBlocksLocalMethod>(ev, ctx);
}

void TLaggingAgentsReplicaProxyActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const TActorContext& ctx)
{
    RwClientId = ev->Get()->RWClientId;
    auto partitionRequest =
        std::make_unique<TEvVolume::TEvRWClientIdChanged>(RwClientId);
    ctx.Send(NonreplPartitionActorId, std::move(partitionRequest));

    for (const auto& [_, state]: AgentState) {
        Y_DEBUG_ABORT_UNLESS(
            !state.AvailabilityMonitoringActorId || !state.MigrationActorId);

        if (state.MigrationActorId) {
            ctx.Send(
                state.MigrationActorId,
                std::make_unique<TEvVolume::TEvRWClientIdChanged>(RwClientId));
        }
    }
}

void TLaggingAgentsReplicaProxyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Become(&TThis::StateZombie);
    AgentState.clear();
    RecalculateLaggingAgentIdByBlockRangeEnd();
    PoisonPillHelper.HandlePoisonPill(ev, ctx);
}

void TLaggingAgentsReplicaProxyActor::Die(const NActors::TActorContext& ctx)
{
    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TLaggingAgentsReplicaProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(
            TEvNonreplPartitionPrivate::TEvAgentIsUnavailable,
            HandleAgentIsUnavailable);
        HFunc(
            TEvNonreplPartitionPrivate::TEvAgentIsBackOnline,
            HandleAgentIsBackOnline);
        HFunc(
            TEvNonreplPartitionPrivate::TEvLaggingMigrationDisabled,
            HandleLaggingMigrationDisabled);
        HFunc(
            TEvNonreplPartitionPrivate::TEvLaggingMigrationEnabled,
            HandleLaggingMigrationEnabled);
        HFunc(
            TEvVolumePrivate::TEvLaggingAgentMigrationFinished,
            HandleLaggingAgentMigrationFinished);
        HFunc(
            NPartition::TEvPartition::TEvWaitForInFlightWritesRequest,
            HandleWaitForInFlightWrites);
        HFunc(
            NPartition::TEvPartition::TEvWaitForInFlightWritesResponse,
            HandleWaitForInFlightWritesResponse);
        HFunc(TEvVolume::TEvRWClientIdChanged, HandleRWClientIdChanged);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

void TLaggingAgentsReplicaProxyActor::ForwardUnhandledEvent(
    TAutoPtr<IEventHandle>& ev,
    const TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, NonreplPartitionActorId);
}

STFUNC(TLaggingAgentsReplicaProxyActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvWriteBlocksRequest, RejectWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, RejectZeroBlocks);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, RejectWriteBlocksLocal);
        HFunc(TEvService::TEvReadBlocksRequest, RejectReadBlocks);
        HFunc(TEvService::TEvReadBlocksLocalRequest, RejectReadBlocksLocal);
        HFunc(
            NPartition::TEvPartition::TEvWaitForInFlightWritesRequest,
            RejectWaitForInFlightWrites);

        IgnoreFunc(NPartition::TEvPartition::TEvWaitForInFlightWritesResponse);
        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);
        IgnoreFunc(TEvents::TEvPoisonPill);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvLaggingMigrationDisabled);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvLaggingMigrationEnabled);

        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
