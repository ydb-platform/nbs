#include "part_mirror_actor.h"

#include "mirror_request_actor.h"
#include "part_mirror_split_read_blocks_actor.h"
#include "part_mirror_split_request_helpers.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/common/verify.h>

#include <util/string/join.h>

#include <algorithm>
#include <ranges>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 CalculateChecksum(
    const TEvService::TEvReadBlocksRequest::ProtoRecordType& request,
    const TEvService::TEvReadBlocksResponse::ProtoRecordType& response)
{
    Y_UNUSED(request);

    TBlockChecksum checksum;
    for (const auto& buffer: response.GetBlocks().GetBuffers()) {
        checksum.Extend(buffer.data(), buffer.size());
    }
    return checksum.GetValue();
}

ui32 CalculateChecksum(
    const TEvService::TEvReadBlocksLocalRequest::ProtoRecordType& request,
    const TEvService::TEvReadBlocksLocalResponse::ProtoRecordType& response)
{
    Y_UNUSED(response);

    auto g = request.Sglist.Acquire();
    if (!g) {
        return 0;
    }

    const auto& sgList = g.Get();
    TBlockChecksum checksum;
    for (auto blockData: sgList) {
        checksum.Extend(blockData.Data(), blockData.Size());
    }
    return checksum.GetValue();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TRequestActor final
    : public TActorBootstrapped<TRequestActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TVector<TActorId> Partitions;
    typename TMethod::TRequest::ProtoRecordType Request;
    const TBlockRange64 Range;
    const TString DiskId;
    const NActors::TActorId ParentActorId;
    const ui64 RequestIdentityKey;

    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;
    using TBase = TActorBootstrapped<TRequestActor<TMethod>>;

    TVector<ui32> ResponseChecksums;
    ui32 ResponseCount = 0;
    TResponseProto Response;
    bool ChecksumMismatchObserved = false;

public:
    TRequestActor(
        TRequestInfoPtr requestInfo,
        const TVector<TActorId>& partitions,
        typename TMethod::TRequest::ProtoRecordType request,
        const TBlockRange64 range,
        TString diskId,
        TActorId parentActorId,
        ui64 requestIdentityKey);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendRequests(const TActorContext& ctx);
    bool HandleError(const TActorContext& ctx, NProto::TError error);
    void CompareChecksums(const TActorContext& ctx);
    void Done(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleChecksumUndelivery(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleChecksumResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TRequestActor<TMethod>::TRequestActor(
        TRequestInfoPtr requestInfo,
        const TVector<TActorId>& partitions,
        typename TMethod::TRequest::ProtoRecordType request,
        const TBlockRange64 range,
        TString diskId,
        TActorId parentActorId,
        ui64 requestIdentityKey)
    : RequestInfo(std::move(requestInfo))
    , Partitions(partitions)
    , Request(std::move(request))
    , Range(range)
    , DiskId(std::move(diskId))
    , ParentActorId(parentActorId)
    , RequestIdentityKey(requestIdentityKey)
    , ResponseChecksums(Partitions.size(), 0)
{}

template <typename TMethod>
void TRequestActor<TMethod>::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    SendRequests(ctx);
}

template <typename TMethod>
void TRequestActor<TMethod>::SendRequests(const TActorContext& ctx)
{
    using TChecksumRequest =
        TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest;
    for (ui32 i = 1; i < Partitions.size(); ++i) {
        auto request = std::make_unique<TChecksumRequest>();
        request->Record.SetStartIndex(Request.GetStartIndex());
        request->Record.SetBlocksCount(GetBlocksCount(Request));
        *request->Record.MutableHeaders() = Request.GetHeaders();

        auto event = std::make_unique<IEventHandle>(
            Partitions[i],
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            RequestInfo->Cookie + i,
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(event.release());
    }

    auto request = std::make_unique<typename TMethod::TRequest>();
    request->CallContext = RequestInfo->CallContext;
    if (Partitions.size() > 1) {
        // Request will be used during checksum calculation
        request->Record = Request;
    } else {
        request->Record = std::move(Request);
    }

    auto event = std::make_unique<IEventHandle>(
        Partitions[0],
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        RequestInfo->Cookie,
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());
}

template <typename TMethod>
void TRequestActor<TMethod>::CompareChecksums(const TActorContext& ctx)
{
    std::unordered_map<ui32, std::vector<TString>> checksumMap;

    for (ui32 i = 0; i < ResponseChecksums.size(); ++i) {
        const auto checksum = ResponseChecksums[i];
        if (checksum) {
            checksumMap[checksum].push_back(Partitions[i].ToString());
        }
    }

    if (checksumMap.size() > 1) {
        TStringBuilder errorMessege;
        errorMessege << "Checksum mismatch detected: ";

        for (const auto& [checksum, partitions]: checksumMap) {
            errorMessege << checksum << " (";
            for (size_t i = 0; i < partitions.size(); ++i) {
                if (i > 0) {
                    errorMessege << ", ";
                }
                errorMessege << partitions[i];
            }
            errorMessege << ") ";
        }

        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Read range %s: %s",
            DiskId.c_str(),
            DescribeRange(Range).c_str(),
            errorMessege.c_str());

        *Response.MutableError() = MakeError(E_REJECTED, errorMessege);
        ChecksumMismatchObserved = true;
    }
}

template <typename TMethod>
void TRequestActor<TMethod>::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(Response);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvMirroredReadCompleted>(
            RequestIdentityKey,
            ChecksumMismatchObserved);
    NCloud::Send(ctx, ParentActorId, std::move(completion));

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TRequestActor<TMethod>::HandleChecksumUndelivery(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s (ChecksumBlocks) request undelivered to some nonrepl"
        " partitions",
        DiskId.c_str(),
        TMethod::Name);

    *Response.MutableError() = MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " (ChecksumBlocks) request undelivered to some"
        << " nonrepl partitions");
    Done(ctx);
}

template <typename TMethod>
void TRequestActor<TMethod>::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;
    ProcessMirrorActorError(*record.MutableError());

    if (HasError(record.GetError())) {
        *Response.MutableError() = *record.MutableError();
        Done(ctx);
        return;
    }

    ui32 responseIdx = ev->Cookie - RequestInfo->Cookie;
    STORAGE_VERIFY(
        responseIdx < ResponseChecksums.size(),
        TWellKnownEntityTypes::DISK,
        DiskId);
    ResponseChecksums[responseIdx] = record.GetChecksum();

    STORAGE_VERIFY(
        ResponseCount < ResponseChecksums.size(),
        TWellKnownEntityTypes::DISK,
        DiskId);

    if (++ResponseCount == ResponseChecksums.size()) {
        CompareChecksums(ctx);
        Done(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TRequestActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s request undelivered to some nonrepl partitions",
        DiskId.c_str(),
        TMethod::Name);

    *Response.MutableError() = MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " request undelivered to some nonrepl partitions");
    Done(ctx);
}

template <typename TMethod>
void TRequestActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;
    ProcessMirrorActorError(*record.MutableError());

    if (HasError(record)) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "[%s] %s got error from nonreplicated partition: %s",
            DiskId.c_str(),
            TMethod::Name,
            FormatError(record.GetError()).c_str());

        *Response.MutableError() = *record.MutableError();
        Done(ctx);
        return;
    }

    if (ResponseChecksums.size() > 1) {
        ui32 responseIdx = ev->Cookie - RequestInfo->Cookie;
        STORAGE_VERIFY(
            responseIdx < ResponseChecksums.size(),
            TWellKnownEntityTypes::DISK,
            DiskId);
        ResponseChecksums[responseIdx] = CalculateChecksum(Request, record);
    }

    STORAGE_VERIFY(
        ResponseCount < ResponseChecksums.size(),
        TWellKnownEntityTypes::DISK,
        DiskId);

    Response = std::move(record);

    if (++ResponseCount == ResponseChecksums.size()) {
        CompareChecksums(ctx);
        Done(ctx);
    }
}

template <typename TMethod>
void TRequestActor<TMethod>::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    *Response.MutableError() = MakeError(E_REJECTED, "Dead");
    Done(ctx);
}

template <typename TMethod>
STFUNC(TRequestActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest,
            HandleChecksumUndelivery);
        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse,
            HandleChecksumResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);
        HFunc(TMethod::TResponse, HandleResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

auto TMirrorPartitionActor::SelectReplicasToReadFrom(
    std::optional<ui32> replicaIndex,
    std::optional<ui32> replicaCount,
    TBlockRange64 blockRange,
    const TStringBuf methodName) -> TResultOrError<TVector<TActorId>>
{
    const auto& replicaInfos = State.GetReplicaInfos();

    if (replicaIndex && *replicaIndex >= replicaInfos.size()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Request " << methodName << " has incorrect ReplicaIndex "
                << *replicaIndex << " disk has " << replicaInfos.size()
                << " replicas");
    }
    if (replicaCount && *replicaCount > replicaInfos.size()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Request " << methodName
                             << " has incorrect replica count: " << replicaCount
                             << ". Disk " << DiskId << " has only "
                             << replicaInfos.size() << " replicas");
    }

    ui32 readReplicaCount =
        replicaCount ? *replicaCount
                     : Min<ui32>(
                           Max<ui32>(1, Config->GetMirrorReadReplicaCount()),
                           replicaInfos.size());

    if (replicaIndex) {
        State.SetReadReplicaIndex(*replicaIndex);
        readReplicaCount = 1;
    }
    TVector<ui32> replicaIndexes;
    replicaIndexes.reserve(readReplicaCount);
    for (ui32 i = 0; i < readReplicaCount; ++i) {
        ui32 replicaIndex = 0;
        const auto error = State.NextReadReplica(blockRange, replicaIndex);
        if (HasError(error)) {
            return error;
        }

        if (FindPtr(replicaIndexes, replicaIndex)) {
            break;
        }

        replicaIndexes.emplace_back(replicaIndex);
    }

    if (replicaIndex && *replicaIndexes.begin() != *replicaIndex) {
        return MakeError(
            E_REJECTED,
            TStringBuilder()
                << "Cannot process " << methodName << " cause replica "
                << *replicaIndex << " has not ready devices");
    }

    if (replicaCount && replicaIndexes.size() != *replicaCount) {
        auto allIndexes = std::views::iota(0U, *replicaCount);

        Sort(replicaIndexes);
        TVector<ui32> unreadyActorIndexes;
        std::ranges::set_difference(
            allIndexes,
            replicaIndexes,
            std::back_inserter(unreadyActorIndexes));

        return MakeError(
            E_REJECTED,
            TStringBuilder()
                << "Cannot process " << methodName << " on " << replicaCount
                << " replicas, since devices of the following replicas "
                   "are not ready: ["
                << JoinSeq(",", unreadyActorIndexes) << "]");
    }

    TVector<TActorId> replicaActorIds;
    for (const auto& replicaIndex: replicaIndexes) {
        replicaActorIds.emplace_back(State.GetReplicaActor(replicaIndex));
    }

    return replicaActorIds;
}

template <typename TMethod>
void TMirrorPartitionActor::ReadBlocks(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TMethod::TResponse;

    if (HasError(Status)) {
        Reply(ctx, *ev, std::make_unique<TResponse>(Status));

        return;
    }

    auto& record = ev->Get()->Record;

    const auto blockRange = TBlockRange64::WithLength(
        record.GetStartIndex(),
        record.GetBlocksCount());

    if (ResyncRangeStarted && GetScrubbingRange().Overlaps(blockRange)) {
        auto response = std::make_unique<TResponse>(MakeError(
            E_REJECTED,
            TStringBuilder() << "Request " << TMethod::Name
                             << " intersects with currently resyncing range"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const std::optional<ui32> replicaIndex =
        record.GetHeaders().GetReplicaIndex() > 0
            ? std::make_optional(record.GetHeaders().GetReplicaIndex() - 1)
            : std::nullopt;
    const std::optional<ui32> replicaCount =
        record.GetHeaders().GetReplicaCount() > 0
            ? std::make_optional(record.GetHeaders().GetReplicaCount())
            : std::nullopt;

    auto [replicaActorIds, error] = SelectReplicasToReadFrom(
        replicaIndex,
        replicaCount,
        blockRange,
        TMethod::Name);

    bool tryToSplitRequest =
        error.GetCode() == E_INVALID_STATE && !replicaIndex;
    if (HasError(error) && !tryToSplitRequest) {
        NCloud::Reply(ctx, *ev, std::make_unique<TResponse>(std::move(error)));
        return;
    }
    if (tryToSplitRequest) {
        auto splitError = SplitReadBlocks<TMethod>(ev, ctx);

        if (HasError(splitError)) {
            LOG_LOG(
                ctx,
                splitError.GetCode() == E_ARGUMENT ? NLog::PRI_ERROR
                                                   : NLog::PRI_DEBUG,
                TBlockStoreComponents::PARTITION,
                "Can't split read request: %s",
                FormatError(splitError).c_str());

            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TResponse>(std::move(error)));
        }
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Will read %s from %u replicas",
        DiskId.c_str(),
        DescribeRange(blockRange).c_str(),
        replicaActorIds.size());

    const auto requestIdentityKey = TakeNextRequestIdentifier();
    RequestsInProgress.AddReadRequest(
        requestIdentityKey,
        {blockRange, record.GetHeaders().GetVolumeRequestId()});

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    NCloud::Register<TRequestActor<TMethod>>(
        ctx,
        std::move(requestInfo),
        std::move(replicaActorIds),
        std::move(record),
        blockRange,
        State.GetReplicaInfos()[0].Config->GetName(),
        SelfId(),
        requestIdentityKey);
}

template <typename TMethod>
NProto::TError TMirrorPartitionActor::SplitReadBlocks(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& record = ev->Get()->Record;

    const auto blockRange = TBlockRange64::WithLength(
        record.GetStartIndex(),
        record.GetBlocksCount());

    auto blockRangeSplitByDeviceBorders =
        State.SplitRangeByDeviceBorders(blockRange);
    // There is no sense to split request if it covers only one device or
    // replica.
    if (blockRangeSplitByDeviceBorders.size() == 1) {
        return MakeError(E_INVALID_STATE, "no sense to split such request");
    }

    for (auto range: blockRangeSplitByDeviceBorders) {
        ui32 replicaIndex = 0;
        auto error = State.NextReadReplica(range, replicaIndex);
        if (HasError(error)) {
            return error;
        }
    }

    auto [splitRequest, error] =
        SplitReadRequest(record, blockRangeSplitByDeviceBorders);
    if (HasError(error)) {
        return error;
    }

    const auto requestIdentityKey = TakeNextRequestIdentifier();
    RequestsInProgress.AddReadRequest(
        requestIdentityKey,
        {blockRange, record.GetHeaders().GetVolumeRequestId()});

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Split original range %s by device borders. Will try to read "
        "with few requests",
        DiskId.c_str(),
        DescribeRange(blockRange).c_str());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    NCloud::Register<TSplitReadBlocksActor<TMethod>>(
        ctx,
        std::move(requestInfo),
        std::move(splitRequest),
        SelfId(),
        State.GetBlockSize(),
        requestIdentityKey);

    return {};
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadBlocks<TEvService::TReadBlocksMethod>(ev, ctx);
}

void TMirrorPartitionActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadBlocks<TEvService::TReadBlocksLocalMethod>(ev, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
