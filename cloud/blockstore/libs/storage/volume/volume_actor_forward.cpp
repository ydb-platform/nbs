#include "volume_actor.h"

#include "multi_partition_requests.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/volume/model/merge.h>
#include <cloud/blockstore/libs/storage/volume/model/stripe.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

#include <util/generic/guid.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

Y_HAS_MEMBER(SetThrottlerDelay);

template <typename TResponse>
void StoreThrottlerDelay(TResponse& response, TDuration delay)
{
    using TProtoType = decltype(TResponse::Record);

    if constexpr (THasSetThrottlerDelay<TProtoType>::value) {
        response.Record.SetThrottlerDelay(delay.MicroSeconds());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void RejectVolumeRequest(
    const TActorContext& ctx,
    NActors::TActorId caller,
    ui64 callerCookie,
    TCallContext& callContext,
    NProto::TError error)
{
    auto response =
        std::make_unique<typename TMethod::TResponse>(std::move(error));

    StoreThrottlerDelay(
        *response,
        callContext.Time(EProcessingStage::Postponed));

    NCloud::Send(ctx, caller, std::move(response), callerCookie);
}

////////////////////////////////////////////////////////////////////////////////

void CopySgListIntoRequestBuffers(
    TEvService::TEvWriteBlocksLocalRequest& request)
{
    auto& record = request.Record;
    auto g = record.Sglist.Acquire();
    if (!g) {
        return;
    }

    const auto& sgList = g.Get();
    STORAGE_VERIFY_C(
        record.GetBlocks().BuffersSize() == 0,
        TWellKnownEntityTypes::DISK,
        record.GetDiskId(),
        TStringBuilder() << "Buffers: " << record.GetBlocks().BuffersSize());
    TSgList newSgList;
    newSgList.reserve(sgList.size());
    for (const auto& block: sgList) {
        auto& buffer = *record.MutableBlocks()->AddBuffers();
        buffer.ReserveAndResize(block.Size());
        memcpy(buffer.begin(), block.Data(), block.Size());
        newSgList.emplace_back(buffer.data(), buffer.size());
    }
    record.Sglist.SetSgList(std::move(newSgList));
}

template <typename T>
void CopySgListIntoRequestBuffers(T& t)
{
    Y_UNUSED(t);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TVolumeActor::UpdateIngestTimeStats(
    const typename TMethod::TRequest::TPtr& ev,
    TInstant now)
{
    if constexpr (!IsReadOrWriteMethod<TMethod>) {
        return;
    }

    const auto& headers = ev->Get()->Record.GetHeaders();
    if (headers.GetRetryNumber() > 0) {
        return;
    }

    const auto ingestTime =
        now - TInstant::MicroSeconds(headers.GetTimestamp());

    if constexpr (IsReadMethod<TMethod>) {
        VolumeSelfCounters->IngestTimeRequestCounters.ReadBlocks.Increment(
            ingestTime.MicroSeconds());
    } else if constexpr (IsExactlyWriteMethod<TMethod>) {
        VolumeSelfCounters->IngestTimeRequestCounters.WriteBlocks.Increment(
            ingestTime.MicroSeconds());
    } else if constexpr (IsZeroMethod<TMethod>) {
        VolumeSelfCounters->IngestTimeRequestCounters.ZeroBlocks.Increment(
            ingestTime.MicroSeconds());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
bool TVolumeActor::HandleMultipartitionVolumeRequest(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    ui64 volumeRequestId,
    bool isTraced,
    ui64 traceTs)
{
    static_assert(!IsCheckpointMethod<TMethod>);
    Y_ABORT_UNLESS(!State->IsDiskRegistryMediaKind());
    Y_ABORT_UNLESS(State->GetPartitions().size() > 1);

    const auto blocksPerStripe =
        State->GetMeta().GetVolumeConfig().GetBlocksPerStripe();
    Y_ABORT_UNLESS(blocksPerStripe);

    TVector<TPartitionRequest<TMethod>> partitionRequests;
    TBlockRange64 blockRange;

    bool ok = ToPartitionRequests<TMethod>(
        State->GetPartitions(),
        State->GetBlockSize(),
        blocksPerStripe,
        ev,
        &partitionRequests,
        &blockRange);

    if (!ok) {
        return false;
    }

    // For DescribeBlocks should always forward request to
    // TMultiPartitionRequestActor
    if (partitionRequests.size() == 1 && !IsDescribeBlocksMethod<TMethod>) {
        ev->Get()->Record = std::move(partitionRequests.front().Event->Record);
        SendRequestToPartition<TMethod>(
            ctx,
            ev,
            volumeRequestId,
            blockRange,
            partitionRequests.front().PartitionId,
            traceTs);

        return true;
    }

    for (const auto& partitionRequest: partitionRequests) {
        LOG_TRACE(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Forward %s request to partition: %lu %s",
            LogTitle.GetWithTime().c_str(),
            TMethod::Name,
            partitionRequest.TabletId,
            ToString(partitionRequest.ActorId).data());
    }

    auto wrappedRequest = WrapRequest<TMethod>(
        ev,
        TActorId{},
        volumeRequestId,
        blockRange,
        traceTs,
        false,
        IsWriteMethod<TMethod>);

    NCloud::Register<TMultiPartitionRequestActor<TMethod>>(
        ctx,
        CreateRequestInfo(
            wrappedRequest->Sender,
            wrappedRequest->Cookie,
            wrappedRequest->Get()->CallContext),
        blockRange,
        blocksPerStripe,
        State->GetBlockSize(),
        State->GetPartitions().size(),
        std::move(partitionRequests),
        TRequestTraceInfo(isTraced, traceTs, TraceSerializer));

    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
typename TMethod::TRequest::TPtr TVolumeActor::WrapRequest(
    const typename TMethod::TRequest::TPtr& ev,
    NActors::TActorId newRecipient,
    ui64 volumeRequestId,
    TBlockRange64 blockRange,
    ui64 traceTime,
    bool forkTraces,
    bool isMultipartitionWriteOrZero)
{
    auto* msg = ev->Get();

    auto originalContext = msg->CallContext;
    if (forkTraces) {
        msg->CallContext =
            MakeIntrusive<TCallContext>(originalContext->RequestId);

        if (!originalContext->LWOrbit.Fork(msg->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                originalContext->LWOrbit,
                TMethod::Name,
                originalContext->RequestId);
        }
    }

    msg->Record.MutableHeaders()->SetVolumeRequestId(volumeRequestId);
    // We wrap the original message so that the response goes through
    // TVolumeActor
    auto selfId = SelfId();
    auto newEvent = typename TMethod::TRequest::TPtr(
        static_cast<typename TMethod::TRequest::THandle*>(new IEventHandle(
            newRecipient,
            selfId,
            ev->ReleaseBase().Release(),
            IEventHandle::FlagForwardOnNondelivery,   // flags
            volumeRequestId,                          // cookie
            &selfId                                   // forwardOnNondelivery
            )));

    // We save the original sender to reply to him when we receive a response
    // from the partition.
    VolumeRequests.emplace(
        volumeRequestId,
        TVolumeRequest(
            ev->Sender,
            ev->Cookie,
            std::move(originalContext),
            forkTraces ? msg->CallContext : nullptr,
            traceTime,
            &RejectVolumeRequest<TMethod>,
            isMultipartitionWriteOrZero));

    if (isMultipartitionWriteOrZero) {
        ++MultipartitionWriteAndZeroRequestsInProgress;
    }

    if constexpr (IsReadMethod<TMethod>) {
        RequestTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Read,
            volumeRequestId,
            blockRange,
            traceTime);
    } else if constexpr (IsExactlyWriteMethod<TMethod>) {
        RequestTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Write,
            volumeRequestId,
            blockRange,
            traceTime);
    } else if constexpr (IsZeroMethod<TMethod>) {
        RequestTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Zero,
            volumeRequestId,
            blockRange,
            traceTime);
    }  else if constexpr (IsDescribeBlocksMethod<TMethod>) {
        RequestTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Describe,
            volumeRequestId,
            blockRange,
            traceTime);
    }

    return newEvent;
}

template <typename TMethod>
void TVolumeActor::SendRequestToPartition(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    ui64 volumeRequestId,
    TBlockRange64 blockRange,
    ui32 partitionId,
    ui64 traceTime)
{
    STORAGE_VERIFY_C(
        State->IsDiskRegistryMediaKind() || State->GetPartitions(),
        TWellKnownEntityTypes::TABLET,
        TabletID(),
        "Empty partition list");

    auto partActorId = State->IsDiskRegistryMediaKind()
        ? State->GetDiskRegistryBasedPartitionActor()
        : State->GetPartitions()[partitionId].GetTopActorId();

    if (State->GetPartitions()) {
        LOG_TRACE(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Sending %s request to partition: %lu %s",
            LogTitle.GetWithTime().c_str(),
            TMethod::Name,
            State->GetPartitions()[partitionId].TabletId,
            ToString(partActorId).data());

        if constexpr (IsExactlyWriteMethod<TMethod>) {
            CombineChecksumsInPlace(*ev->Get()->Record.MutableChecksums());
        }
    }

    auto wrappedRequest = WrapRequest<TMethod>(
        ev,
        partActorId,
        volumeRequestId,
        blockRange,
        traceTime,
        true,
        false);

    if (SendRequestToPartitionWithUsedBlockTracking<TMethod>(
            ctx,
            wrappedRequest,
            partActorId,
            volumeRequestId))
    {
        // The request was sent to the partition with tracking of used blocks.
        return;
    }

    // Send request to the partition.
    ctx.Send(wrappedRequest.Release());
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TVolumeActor::FillResponse(
    typename TMethod::TResponse& response,
    TCallContext& callContext,
    ui64 startTime)
{
    LWTRACK(
        ResponseSent_Volume,
        callContext.LWOrbit,
        TMethod::Name,
        callContext.RequestId);

    if (TraceSerializer->IsTraced(callContext.LWOrbit)) {
        TraceSerializer->BuildTraceInfo(
            *response.Record.MutableTrace(),
            callContext.LWOrbit,
            startTime,
            GetCycleCount());
    }

    StoreThrottlerDelay(
        response,
        callContext.Time(EProcessingStage::Postponed));
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
NProto::TError TVolumeActor::ProcessAndValidateReadFromCheckpoint(
    typename TMethod::TRequest::ProtoRecordType& record) const
{
    if (!State->IsDiskRegistryMediaKind()) {
        return {};
    }

    const auto& checkpointId = record.GetCheckpointId();
    if (!checkpointId) {
        return {};
    }

    const auto checkpointInfo =
        State->GetCheckpointStore().GetCheckpoint(checkpointId);

    auto makeError = [](TString message)
    {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_SILENT);
        return MakeError(E_NOT_FOUND, std::move(message), flags);
    };

    if (!checkpointInfo) {
        return makeError(
            TStringBuilder()
            << "Checkpoint id=" << checkpointId.Quote() << " not found");
    }

    // For light checkpoints read from the disk itself.
    if (checkpointInfo->Type == ECheckpointType::Light) {
        record.ClearCheckpointId();
        return {};
    }

    switch (checkpointInfo->Data) {
        case ECheckpointData::DataDeleted: {
            // Return error when reading from deleted checkpoint.
            return makeError(
                TStringBuilder() << "Data for checkpoint id="
                                 << checkpointId.Quote() << " deleted");
        } break;
        case ECheckpointData::DataPresent: {
            if (checkpointInfo->ShadowDiskId.empty()) {
                // For write blocking checkpoint clear the checkpoint ID to
                // read data from the disk itself.
                record.ClearCheckpointId();
                return {};
            }
        } break;
    }

    // Will read checkpoint data from shadow disk.
    return {};
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TVolumeActor::ForwardResponse(
    const NActors::TActorContext& ctx,
    const typename TMethod::TResponse::TPtr& ev)
{
    const ui64 volumeRequestId = ev->Cookie;

    auto response = std::unique_ptr<typename TMethod::TResponse>(
        static_cast<typename TMethod::TResponse*>(ev->ReleaseBase().Release()));

    ReplyToOriginalRequest<TMethod>(
        ctx,
        ev->Sender,
        ev->Flags,
        volumeRequestId,
        std::move(response));
}

void TVolumeActor::HandleWriteOrZeroCompleted(
    const TEvVolumePrivate::TEvWriteOrZeroCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

template <typename TMethod>
bool TVolumeActor::ReplyToOriginalRequest(
    const NActors::TActorContext& ctx,
    NActors::TActorId sender,
    IEventHandle::TEventFlags flags,
    ui64 volumeRequestId,
    std::unique_ptr<typename TMethod::TResponse> response)
{
    const bool success = !HasError(response->Record.GetError());
    if constexpr (IsWriteMethod<TMethod>) {
        ReplyToDuplicateRequests(
            ctx,
            volumeRequestId,
            response->Record.GetError().GetCode());
    }

    auto it = VolumeRequests.find(volumeRequestId);
    if (it == VolumeRequests.end()) {
        return false;
    }

    const TVolumeRequest& volumeRequest = it->second;

    if (volumeRequest.ForkedContext) {
        volumeRequest.CallContext->LWOrbit.Join(
            volumeRequest.ForkedContext->LWOrbit);
    }

    FillResponse<TMethod>(
        *response,
        *volumeRequest.CallContext,
        volumeRequest.ReceiveTime);

    // forward response to the caller
    auto event = std::make_unique<IEventHandle>(
        volumeRequest.Caller,
        sender,
        response.release(),
        flags,
        volumeRequest.CallerCookie);
    ctx.Send(std::move(event));

    if (volumeRequest.IsMultipartitionWriteOrZero) {
        Y_DEBUG_ABORT_UNLESS(MultipartitionWriteAndZeroRequestsInProgress > 0);
        --MultipartitionWriteAndZeroRequestsInProgress;
        ProcessCheckpointRequests(ctx);
    }

    VolumeRequests.erase(it);
    const auto firstSuccess = RequestTimeTracker.OnRequestFinished(
        volumeRequestId,
        success,
        GetCycleCount());

    if (firstSuccess) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s The first successful %s request was started at %s finished at "
            "%s. The very first request was started at %s. Failed requests: "
            "%lu",
            LogTitle.GetWithTime().c_str(),
            ToString(firstSuccess->RequestType).c_str(),
            FormatDuration(firstSuccess->SuccessfulRequestStartTime).c_str(),
            FormatDuration(firstSuccess->SuccessfulRequestFinishTime).c_str(),
            FormatDuration(firstSuccess->FirstRequestStartTime).c_str(),
            firstSuccess->FailCount);
    }

    return true;
}

void TVolumeActor::ReplyToDuplicateRequests(
    const TActorContext& ctx,
    ui64 volumeRequestId,
    ui32 resultCode)
{
    WriteAndZeroRequestsInFlight.RemoveRequest(volumeRequestId);

    auto it = DuplicateWriteAndZeroRequests.find(volumeRequestId);
    if (it == DuplicateWriteAndZeroRequests.end()) {
        return;
    }

    NProto::TError error;
    error.SetCode(resultCode);
    if (HasError(error)) {
        error.SetMessage(TStringBuilder()
            << "Duplicate request intersects with a failed inflight write or"
            << " zero request");
    }

    for (auto& duplicateRequest: it->second) {
        NActors::IEventBasePtr response;
        switch (duplicateRequest.EventType) {
            case TEvService::EvWriteBlocksRequest: {
                auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>(error);
                FillResponse<TEvService::TWriteBlocksMethod>(
                    *response,
                    *duplicateRequest.CallContext,
                    duplicateRequest.ReceiveTime);

                NCloud::Reply(ctx, *duplicateRequest.Event, std::move(response));
                break;
            }

            case TEvService::EvWriteBlocksLocalRequest: {
                auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);
                FillResponse<TEvService::TWriteBlocksLocalMethod>(
                    *response,
                    *duplicateRequest.CallContext,
                    duplicateRequest.ReceiveTime);
                NCloud::Reply(ctx, *duplicateRequest.Event, std::move(response));
                break;
            }

            case TEvService::EvZeroBlocksRequest: {
                auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>(error);
                FillResponse<TEvService::TZeroBlocksMethod>(
                    *response,
                    *duplicateRequest.CallContext,
                    duplicateRequest.ReceiveTime);
                NCloud::Reply(ctx, *duplicateRequest.Event, std::move(response));
                break;
            }

            default: {
                STORAGE_VERIFY_C(
                    0,
                    TWellKnownEntityTypes::TABLET,
                    TabletID(),
                    TStringBuilder() << "unexpected duplicate event type: "
                        << static_cast<ui32>(duplicateRequest.EventType));
            }
        }
    }

    DuplicateRequestCount -= it->second.size();
    DuplicateWriteAndZeroRequests.erase(it);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TVolumeActor::ForwardRequest(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev)
{
    // PartitionRequests undelivery handing
    if (ev->Sender == SelfId()) {
        const ui64 volumeRequestId = ev->Cookie;
        auto response = std::make_unique<typename TMethod::TResponse>(MakeError(
            E_REJECTED,
            TStringBuilder()
                << "Volume  " << State->GetDiskId().Quote() << " not ready. "
                << TMethod::Name << " is undelivered to partition"));

        if (ReplyToOriginalRequest<TMethod>(
                ctx,
                SelfId(),
                0,   // flags
                volumeRequestId,
                std::move(response)))
        {
            return;
        }
    }

    auto* msg = ev->Get();
    auto now = GetCycleCount();

    // Fill block range.
    TBlockRange64 blockRange;
    if constexpr (
        IsReadOrWriteMethod<TMethod> || IsDescribeBlocksMethod<TMethod>)
    {
        blockRange = BuildRequestBlockRange(*msg, State->GetBlockSize());
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s ForwardRequest %s %s",
        LogTitle.GetWithTime().c_str(),
        TMethod::Name,
        blockRange.Print().c_str());

    bool isTraced = false;

    if (ev->Recipient != ev->GetRecipientRewrite())
    {
        if (TraceSerializer->IsTraced(msg->CallContext->LWOrbit)) {
            isTraced = true;
            now = msg->Record.GetHeaders().GetInternal().GetTraceTs();
        } else if (TraceSerializer->HandleTraceRequest(
            msg->Record.GetHeaders().GetInternal().GetTrace(),
            msg->CallContext->LWOrbit))
        {
            isTraced = true;
            msg->Record.MutableHeaders()->MutableInternal()->SetTraceTs(now);
        }
    }

    LWTRACK(
        RequestReceived_Volume,
        msg->CallContext->LWOrbit,
        TMethod::Name,
        msg->CallContext->RequestId);

    UpdateIngestTimeStats<TMethod>(ev, ctx.Now());

    auto replyError = [&] (NProto::TError error)
    {
        auto response = std::make_unique<typename TMethod::TResponse>(
            std::move(error));

        FillResponse<TMethod>(*response, *msg->CallContext, now);

        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if (!VolumeRequestIdGenerator->CanAdvance()) {
        replyError(MakeError(
            E_REJECTED,
            "VolumeRequestId overflow. Going to restart tablet."));
        NCloud::Send(ctx, SelfId(), std::make_unique<TEvents::TEvPoisonPill>());
        return;
    }
    const ui64 volumeRequestId = VolumeRequestIdGenerator->Advance();

    if (ShuttingDown) {
        replyError(MakeError(E_REJECTED, "Shutting down"));
        return;
    }

    if (IsReadOrWriteMethod<TMethod> && HasError(State->GetReadWriteError())) {
        replyError(State->GetReadWriteError());
        return;
    }

    if (State->IsDiskRegistryMediaKind()) {
        if (State->GetMeta().GetDevices().empty()) {
            replyError(MakeError(E_REJECTED, TStringBuilder()
                << "Storage not allocated for volume: "
                << State->GetDiskId().Quote()));
            return;
        }
    }

    if (State->GetPartitionsState() != TPartitionInfo::READY) {
        StartPartitionsIfNeeded(ctx);

        if (!State->Ready()) {
            if constexpr (RejectRequestIfNotReady<TMethod>) {
                replyError(MakeError(
                    E_REJECTED,
                    TStringBuilder() << "Volume " << State->GetDiskId().Quote()
                                     << " not ready by partition state"));
            } else {
                LOG_DEBUG(
                    ctx,
                    TBlockStoreComponents::VOLUME,
                    "%s %s request delayed until volume and partitions are "
                    "ready",
                    LogTitle.GetWithTime().c_str(),
                    TMethod::Name);

                auto requestInfo = CreateRequestInfo<TMethod>(
                    ev->Sender,
                    ev->Cookie,
                    ev->Get()->CallContext);

                PendingRequests.emplace_back(
                    NActors::IEventHandlePtr(ev.Release()),
                    requestInfo);
            }
            return;
        }
    }

    const auto& clientId = GetClientId(*msg);
    auto& clients = State->AccessClients();
    auto clientsIt = clients.end();

    bool throttlingDisabled = false;
    bool forceWrite = false;
    bool predefinedClient = false;
    if constexpr (RequiresMount<TMethod>) {
        clientsIt = clients.find(clientId);
        if (clientsIt == clients.end()) {
            if (clientId == CopyVolumeClientId) {
                if (!clients.empty()) {
                    replyError(MakeError(
                        E_BS_INVALID_SESSION,
                        TStringBuilder()
                            << "Invalid session. Can't use " << clientId.Quote()
                            << " when other clients exists"));
                    return;
                }
                predefinedClient = true;
                throttlingDisabled = true;
                VolumeSelfCounters->Cumulative.ThrottlerSkippedRequests
                    .Increment(1);
            } else {
                replyError(MakeError(
                    E_BS_INVALID_SESSION,
                    TStringBuilder() << "Invalid session (" << clientId.Quote()
                                     << " not found)"));
                return;
            }
        } else {
            const auto& clientInfo = clientsIt->second;

            throttlingDisabled = HasProtoFlag(
                clientInfo.GetVolumeClientInfo().GetMountFlags(),
                NProto::MF_THROTTLING_DISABLED);

            if (RequiresThrottling<TMethod> && throttlingDisabled) {
                VolumeSelfCounters->Cumulative.ThrottlerSkippedRequests
                    .Increment(1);
            }

            forceWrite = HasProtoFlag(
                clientInfo.GetVolumeClientInfo().GetMountFlags(),
                NProto::MF_FORCE_WRITE);
        }
    }

    if (RequiresReadWriteAccess<TMethod>
            && State->GetRejectWrite()
            && !forceWrite)
    {
        replyError(MakeError(E_REJECTED, "Writes blocked"));
        return;
    }

    {
        auto error = Throttle<TMethod>(ctx, ev, throttlingDisabled);
        if (HasError(error)) {
            replyError(std::move(error));
            return;
        } else if (!ev) {
            // request postponed
            return;
        }
    }

    /*
     *  Mount-related validation.
     */
    if constexpr (RequiresMount<TMethod>) {
        Y_ABORT_UNLESS(clientsIt != clients.end() || predefinedClient);

        if (clientsIt != clients.end()) {
            auto& clientInfo = clientsIt->second;
            NProto::TError error;

            if (ev->Recipient != ev->GetRecipientRewrite()) {
                error = clientInfo.CheckPipeRequest(
                    ev->Recipient,
                    RequiresReadWriteAccess<TMethod>,
                    TMethod::Name,
                    State->GetDiskId());
            } else {
                error = clientInfo.CheckLocalRequest(
                    ev->Sender.NodeId(),
                    RequiresReadWriteAccess<TMethod>,
                    TMethod::Name,
                    State->GetDiskId());
            }
            if (FAILED(error.GetCode())) {
                replyError(std::move(error));
                return;
            }
        }

        if (RequiresReadWriteAccess<TMethod> && !CanExecuteWriteRequest()) {
            replyError(MakeError(
                E_REJECTED,
                TStringBuilder() // NBS-4447. Do not change message.
                    << "Checkpoint reject request. " << TMethod::Name << " is not allowed "
                    << (State->IsDiskRegistryMediaKind()
                            ? "if a checkpoint exists"
                            : "during checkpoint creation")));
            return;
        }
    }

    /*
     *  Read from checkpoint processing and validation.
     */
    if constexpr (IsReadMethod<TMethod>) {
        if (auto error =
                ProcessAndValidateReadFromCheckpoint<TMethod>(msg->Record);
            HasError(error))
        {
            replyError(std::move(error));
            return;
        }
    }

    /*
     *  Validation of the request blocks range
     */
    if constexpr (IsReadOrWriteMethod<TMethod>) {
        if (!CheckReadWriteBlockRange(blockRange)) {
            replyError(MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "invalid block range " << DescribeRange(blockRange)));
            return;
        }
    }

    /*
     *  Support for copying request data from the user-supplied buffer to some
     *  buffers which we own to protect the layer below Volume from bugged
     *  guests which modify buffer contents before receiving write response.
     *
     *  Impacts performance (due to extra copying) and is thus not switched on
     *  by default.
     *
     *  See https://github.com/ydb-platform/nbs/issues/2421
     */
    if constexpr (IsExactlyWriteMethod<TMethod>) {
        if (State->GetUseIntermediateWriteBuffer()) {
            CopySgListIntoRequestBuffers(*msg);
        }
    }

    /*
     *  Processing overlapping writes. Overlapping writes should not be sent
     *  to the underlying (storage) layer.
     */
    if constexpr (IsWriteMethod<TMethod>) {
        auto addResult = WriteAndZeroRequestsInFlight.TryAddRequest(
            volumeRequestId,
            blockRange);

        if (!addResult.Added) {
            if (addResult.DuplicateRequestId
                    == TRequestsInFlight::InvalidRequestId)
            {
                replyError(MakeError(E_REJECTED, TStringBuilder()
                    << "Request " << TMethod::Name
                    << " intersects with inflight write or zero request"
                    << " (block range: " << DescribeRange(blockRange) << ")"));
                return;
            }

            LWTRACK(
                DuplicatedRequestReceived_Volume,
                msg->CallContext->LWOrbit,
                TMethod::Name,
                msg->CallContext->RequestId,
                addResult.DuplicateRequestId);

            DuplicateWriteAndZeroRequests[addResult.DuplicateRequestId].push_back({
                ev->Get()->CallContext,
                static_cast<TEvService::EEvents>(TMethod::TRequest::EventType),
                NActors::IEventHandlePtr(ev.Release()),
                now
            });
            ++DuplicateRequestCount;

            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s DuplicateRequestCount=%lu",
                LogTitle.GetWithTime().c_str(),
                DuplicateRequestCount);

            return;
        }
    }

    // Now we are ready to send the request to the underlying layer for
    // processing. We need to save information about all sent requests in
    // VolumeRequests to respond with E_REJECTED if the partition is stopped.
    // When the underlying layer responds, we should remove the request from
    // VolumeRequests.
    // To do this, all requests before sending to the underlying actor are
    // prepared by the WrapRequest<TMethod>() method, which replaces the sender
    // and receiver.

    const bool isSinglePartitionVolume = State->GetPartitions().size() <= 1;
    if constexpr (IsCheckpointMethod<TMethod>) {
        HandleCheckpointRequest<TMethod>(ctx, ev, isTraced, now);
    } else if (isSinglePartitionVolume) {
        SendRequestToPartition<TMethod>(
            ctx,
            ev,
            volumeRequestId,
            blockRange,
            0,
            now);
    } else {
        if (!HandleMultipartitionVolumeRequest<TMethod>(
                ctx,
                ev,
                volumeRequestId,
                isTraced,
                now))
        {
            if constexpr (IsWriteMethod<TMethod>) {
                WriteAndZeroRequestsInFlight.RemoveRequest(volumeRequestId);
            }
            replyError(MakeError(E_REJECTED, "Sglist destroyed"));
        }
    }
}

#define BLOCKSTORE_FORWARD_REQUEST(name, ns)                                   \
    void TVolumeActor::Handle##name(                                           \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        BLOCKSTORE_VOLUME_COUNTER(name);                                       \
        ForwardRequest<ns::T##name##Method>(ctx, ev);                          \
    }                                                                          \
                                                                               \
    void TVolumeActor::Handle##name##Response(                                 \
        const ns::TEv##name##Response::TPtr& ev,                               \
        const NActors::TActorContext& ctx)                                     \
    {                                                                          \
        ForwardResponse<ns::T##name##Method>(ctx, ev);                         \
    }                                                                          \
// BLOCKSTORE_FORWARD_REQUEST

BLOCKSTORE_FORWARD_REQUEST(ReadBlocks,               TEvService)
BLOCKSTORE_FORWARD_REQUEST(WriteBlocks,              TEvService)
BLOCKSTORE_FORWARD_REQUEST(ZeroBlocks,               TEvService)
BLOCKSTORE_FORWARD_REQUEST(CreateCheckpoint,         TEvService)
BLOCKSTORE_FORWARD_REQUEST(DeleteCheckpoint,         TEvService)
BLOCKSTORE_FORWARD_REQUEST(GetChangedBlocks,         TEvService)
BLOCKSTORE_FORWARD_REQUEST(GetCheckpointStatus,      TEvService)
BLOCKSTORE_FORWARD_REQUEST(ReadBlocksLocal,          TEvService)
BLOCKSTORE_FORWARD_REQUEST(WriteBlocksLocal,         TEvService)

BLOCKSTORE_FORWARD_REQUEST(DescribeBlocks,           TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(GetUsedBlocks,            TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(GetPartitionInfo,         TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(CompactRange,             TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(GetCompactionStatus,      TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(DeleteCheckpointData,     TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(RebuildMetadata,          TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(GetRebuildMetadataStatus, TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(ScanDisk,                 TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(GetScanDiskStatus,        TEvVolume)
BLOCKSTORE_FORWARD_REQUEST(CheckRange,               TEvVolume)


#undef BLOCKSTORE_FORWARD_REQUEST

}   // namespace NCloud::NBlockStore::NStorage
