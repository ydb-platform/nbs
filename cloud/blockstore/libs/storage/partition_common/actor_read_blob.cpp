#include "actor_read_blob.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/public.h>

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/wilson_trace_compatibility.h>

#include <util/string/builder.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TString GetBrokenDataMarkerBlock(ui32 blockSize)
{
    return TString(GetBrokenDataMarker()).resize(blockSize, '\0');
}

std::optional<ui32> GetRepairBlockChecksum(
    EStorageAccessMode storageAccessMode,
    ui32 blockSize)
{
    if (storageAccessMode != EStorageAccessMode::Repair) {
        return std::nullopt;
    }

    const auto marker = GetBrokenDataMarkerBlock(blockSize);
    return ComputeDefaultDigest({marker.data(), marker.size()});
}

////////////////////////////////////////////////////////////////////////////////

TReadBlobActor::TReadBlobActor(
    TRequestInfoPtr requestInfo,
    const TActorId& partitionActorId,
    const TActorId& volumeActorId,
    ui64 partitionTabletId,
    ui32 blockSize,
    bool shouldCalculateChecksums,
    const EStorageAccessMode storageAccessMode,
    std::unique_ptr<TRequest> request,
    TDuration longRunningThreshold,
    ui64 bsGroupOperationId,
    bool passTraceIdToBlobstorage)
    : TLongRunningOperationCompanion(
          partitionActorId,
          volumeActorId,
          longRunningThreshold,
          TLongRunningOperationCompanion::EOperation::ReadBlob,
          request->GroupId)
    , RequestInfo(std::move(requestInfo))
    , PartitionActorId(partitionActorId)
    , PartitionTabletId(partitionTabletId)
    , BlockSize(blockSize)
    , ShouldCalculateChecksums(shouldCalculateChecksums)
    , StorageAccessMode(storageAccessMode)
    , Request(std::move(request))
    , BSGroupOperationId(bsGroupOperationId)
    , PassTraceIdToBlobstorage(passTraceIdToBlobstorage)
{}

void TReadBlobActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker_DSProxy,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlob",
        RequestInfo->CallContext->RequestId,
        Request->GroupId);

    TLongRunningOperationCompanion::RequestStarted(ctx);

    std::optional<ui16> lastOffset;
    for (const auto offset: Request->BlobOffsets) {
        if (lastOffset && *lastOffset >= offset) {
            auto error = MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Offsets should be sorted in strict "
                                    "ascending order: "
                                 << JoinSeq(", ", Request->BlobOffsets)
                                 << " for blob " << Request->BlobId);
            ReportInvalidReadBlobOffsets(
                error.GetMessage(),
                {{"tabletId", PartitionTabletId}});
            ReplyAndDie(ctx, std::make_unique<TResponse>(error));
            return;
        }
        lastOffset = offset;
    }

    SendGetRequest(ctx);
}

TReadPlan TReadBlobActor::GetQueries() const
{
    using TEvGetQuery = TEvBlobStorage::TEvGet::TQuery;

    size_t blocksCount = Request->BlobOffsets.size();

    TArrayHolder<TEvGetQuery> queries(new TEvGetQuery[blocksCount]);
    size_t queriesCount = 0;

    TVector<TBlockRange32> blockRangesForEachRequest;

    for (size_t i = 0; i < blocksCount; ++i) {
        if (i && Request->BlobOffsets[i] == Request->BlobOffsets[i - 1] + 1) {
            // extend range
            queries[queriesCount - 1].Size += BlockSize;
            blockRangesForEachRequest.back().End = Request->BlobOffsets[i];
        } else {
            queries[queriesCount++].Set(
                Request->BlobId,
                Request->BlobOffsets[i] * BlockSize,
                BlockSize);
            blockRangesForEachRequest.push_back(
                TBlockRange32::MakeOneBlock(Request->BlobOffsets[i]));
        }
    }

    return TReadPlan{
        .Queries = std::move(queries),
        .QueriesCount = queriesCount,
        .BlockRangesForEachRequest = std::move(blockRangesForEachRequest)};
}

void TReadBlobActor::SendGetRequest(const TActorContext& ctx)
{
    auto [queries, queriesCount, blockRangesForEachRequest] = GetQueries();
    BlockRangesForEachRequest = std::move(blockRangesForEachRequest);

    auto request = std::make_unique<TEvBlobStorage::TEvGet>(
        queries,
        queriesCount,
        Request->Deadline,
        Request->Async ? NKikimrBlobStorage::AsyncRead
                       : NKikimrBlobStorage::FastRead);

    NWilson::TTraceId traceId;
    if (PassTraceIdToBlobstorage) {
        traceId = GetTraceIdForRequestId(
            RequestInfo->CallContext->LWOrbit,
            RequestInfo->CallContext->RequestId);
    }
    request->Orbit = std::move(RequestInfo->CallContext->LWOrbit);

    RequestSent = ctx.Now();

    SendToBSProxy(
        ctx,
        Request->Proxy,
        request.release(),
        RequestInfo->Cookie,
        std::move(traceId));
}

void TReadBlobActor::NotifyCompleted(
    const NActors::TActorContext& ctx,
    const TResponse& response)
{
    auto request =
        std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobCompleted>(
            response.GetError());

    request->BlobId = Request->BlobId;
    request->BytesCount = Request->BlobOffsets.size() * BlockSize;
    request->RequestTime = ResponseReceived - RequestSent;
    request->GroupId = Request->GroupId;
    request->BSGroupOperationId = BSGroupOperationId;
    request->BlobStorageError = response.BlobStorageError;

    if (DeadlineSeen) {
        request->DeadlineSeen = true;
    }

    NCloud::Send(ctx, PartitionActorId, std::move(request));
}

void TReadBlobActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TResponse> response)
{
    NotifyCompleted(ctx, *response);

    if (ResponseReceived) {
        LWTRACK(
            ResponseSent_Partition,
            RequestInfo->CallContext->LWOrbit,
            "ReadBlob",
            RequestInfo->CallContext->RequestId);
    }

    TLongRunningOperationCompanion::RequestFinished(ctx, response->GetError());

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TReadBlobActor::ReplyError(
    const NActors::TActorContext& ctx,
    const NKikimr::TEvBlobStorage::TEvGetResult& response,
    const NProto::TError& error,
    bool blobStorageError)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::PARTITION_COMMON,
        "[%lu] TEvBlobStorage::TEvGet failed: %s\n%s",
        PartitionTabletId,
        FormatError(error).data(),
        response.Print(false).data());

    if (response.Status == NKikimrProto::DEADLINE) {
        DeadlineSeen = true;
    }

    auto reply = std::make_unique<TResponse>(error);
    reply->BlobStorageError = blobStorageError;
    ReplyAndDie(ctx, std::move(reply));
}

void TReadBlobActor::ReplyError(
    const TActorContext& ctx,
    const TEvBlobStorage::TEvGetResult& response,
    const TString& description,
    bool blobStorageError)
{
    ReplyError(
        ctx,
        response,
        MakeError(E_REJECTED, "TEvBlobStorage::TEvGet failed: " + description),
        /*blobStorageError=*/blobStorageError);
}

auto TReadBlobActor::FillBrokenDataAndFilterItOut(
    const NKikimr::TEvBlobStorage::TEvGetResult* msg,
    const TSgList& sglist,
    const NActors::TActorContext& ctx) -> TResultOrError<TDataThatCanBeProcessed>
{
    if (StorageAccessMode != EStorageAccessMode::Repair) {
        return MakeError(S_ALREADY);
    }

    TVector<TEvBlobStorage::TEvGetResult::TResponse> responses;
    responses.reserve(msg->ResponseSz);
    for (size_t i = 0; i < msg->ResponseSz; ++i) {
        responses.push_back(msg->Responses[i]);
    }

    const auto markerBlock = GetBrokenDataMarkerBlock(BlockSize);
    STORAGE_VERIFY(
        markerBlock.size() == BlockSize,
        TWellKnownEntityTypes::TABLET,
        PartitionTabletId);

    auto isBroken = [](const auto& response)
    {
        return response.Status != NKikimrProto::OK &&
               NCloud::IsUnrecoverable(response.Status);
    };

    TSgList sglistForBlocksWithNoBrokenData;
    TVector<ui16> blockOffsetsWithNoBrokenData;
    size_t blockIndex = 0;
    for (size_t requestIndex = 0; requestIndex < responses.size();
         ++requestIndex)
    {
        const auto& response = responses[requestIndex];
        if (isBroken(response)) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::PARTITION_COMMON,
                "[%lu] Repairing TEvBlobStorage::TEvGet %s error (%s)",
                PartitionTabletId,
                NKikimrProto::EReplyStatus_Name(response.Status).data(),
                msg->Print(false).data());
        }

        const auto& range = BlockRangesForEachRequest[requestIndex];
        for (; blockIndex < sglist.size() &&
               Request->BlobOffsets[blockIndex] <= range.End;
             ++blockIndex)
        {
            const auto& block = sglist[blockIndex];
            if (isBroken(response)) {
                STORAGE_VERIFY(
                    block.Data(),
                    TWellKnownEntityTypes::TABLET,
                    PartitionTabletId);
                STORAGE_VERIFY(
                    block.Size() == BlockSize,
                    TWellKnownEntityTypes::TABLET,
                    PartitionTabletId);
                memcpy(
                    const_cast<char*>(block.Data()),
                    markerBlock.data(),
                    markerBlock.size());
            } else {
                sglistForBlocksWithNoBrokenData.push_back(block);
                blockOffsetsWithNoBrokenData.push_back(
                    Request->BlobOffsets[blockIndex]);
            }
        }
    }

    auto toRemove = std::ranges::remove_if(responses, isBroken);

    responses.erase(toRemove.begin(), toRemove.end());

    return TDataThatCanBeProcessed{
        .Sglist = std::move(sglistForBlocksWithNoBrokenData),
        .BlockOffsets = std::move(blockOffsetsWithNoBrokenData),
        .Responses = std::move(responses)};
}

TResultOrError<TVector<ui32>> TReadBlobActor::CopyReadResultToSglist(
    std::span<TEvBlobStorage::TEvGetResult::TResponse> responses,
    std::span<const TBlockDataRef> sglist,
    std::span<const ui16> blockOffsets)
{
    size_t sglistIndex = 0;
    TVector<ui32> blockChecksums;

    for (auto& response: responses) {
        STORAGE_VERIFY(
            response.Status == NKikimrProto::OK,
            TWellKnownEntityTypes::TABLET,
            PartitionTabletId);

        if (response.Buffer.empty() || response.Buffer.size() % BlockSize != 0)
        {
            return MakeError(E_REJECTED, "invalid response received");
        }

        for (auto iter = response.Buffer.begin(); iter.Valid();) {
            if (sglistIndex >= sglist.size()) {
                return MakeError(E_REJECTED, "response is out of range");
            }

            Y_ABORT_UNLESS(sglist[sglistIndex].Size() == BlockSize);
            void* to = const_cast<char*>(sglist[sglistIndex].Data());
            if (ShouldCalculateChecksums) {
                auto block = TString::Uninitialized(BlockSize);
                iter.ExtractPlainDataAndAdvance(block.begin(), BlockSize);
                blockChecksums.push_back(
                    ComputeDefaultDigest({block.data(), BlockSize}));

                memcpy(to, block.data(), BlockSize);
            } else {
                iter.ExtractPlainDataAndAdvance(to, BlockSize);
            }
            ++sglistIndex;
        }
    }

    if (sglistIndex != blockOffsets.size()) {
        return MakeError(E_REJECTED, "invalid response received");
    }

    return std::move(blockChecksums);
}

TVector<ui32> TReadBlobActor::FillBlockChecksumsForRecoveredBlocksIfNeeded(
    std::span<const ui16> originalBlockOffsets,
    std::span<const ui16> newBlockOffsets,
    TVector<ui32> newBlockChecksums)
{
    if (!ShouldCalculateChecksums ||
        originalBlockOffsets.size() == newBlockOffsets.size())
    {
        return newBlockChecksums;
    }

    TVector<ui32> blockChecksums;
    blockChecksums.reserve(originalBlockOffsets.size());

    STORAGE_VERIFY(
        originalBlockOffsets.size() >= newBlockOffsets.size(),
        TWellKnownEntityTypes::TABLET,
        PartitionTabletId);

    const auto markerBlock = GetBrokenDataMarkerBlock(BlockSize);
    const ui32 markerBlockChecksum =
        ComputeDefaultDigest({markerBlock.data(), markerBlock.size()});

    size_t originalIdx = 0;
    size_t newIdx = 0;
    while (originalIdx < originalBlockOffsets.size() &&
           newIdx < newBlockOffsets.size())
    {
        if (originalBlockOffsets[originalIdx] == newBlockOffsets[newIdx]) {
            blockChecksums.push_back(newBlockChecksums[newIdx]);
            ++originalIdx;
            ++newIdx;
        } else {
            blockChecksums.push_back(markerBlockChecksum);
            ++originalIdx;
        }
    }

    STORAGE_VERIFY(
        newIdx == newBlockOffsets.size(),
        TWellKnownEntityTypes::TABLET,
        PartitionTabletId);

    for (; originalIdx < originalBlockOffsets.size(); ++originalIdx) {
        blockChecksums.push_back(markerBlockChecksum);
    }

    return blockChecksums;
}

////////////////////////////////////////////////////////////////////////////////

void TReadBlobActor::HandleGetResult(
    const TEvBlobStorage::TEvGetResult::TPtr& ev,
    const TActorContext& ctx)
{
    ResponseReceived = ctx.Now();

    auto* msg = ev->Get();

    RequestInfo->CallContext->LWOrbit = std::move(msg->Orbit);

    if (msg->Status != NKikimrProto::OK) {
        ReplyError(ctx, *msg, msg->ErrorReason, /*blobStorageError=*/true);
        return;
    }

    TVector<ui32> blockChecksums;

    if (BlockRangesForEachRequest.size() != msg->ResponseSz) {
        ReplyError(
            ctx,
            *msg,
            "Number of responses does not match number of requests",
            /*blobStorageError=*/false);
        return;
    }

    if (auto guard = Request->Sglist.Acquire()) {
        const auto& sglist = guard.Get();

        auto [dataThatCanBeProcessed, error] =
            FillBrokenDataAndFilterItOut(msg, sglist, ctx);

        std::span<TEvBlobStorage::TEvGetResult::TResponse> responsesToProcess;
        std::span<const TBlockDataRef> sglistToProcess;
        std::span<const ui16> blockOffsetsToProcess;

        if (HasError(error)) {
            ReplyError(
                ctx,
                *msg,
                error.GetMessage(),
                /*blobStorageError=*/false);
            return;
        }

        if (error.GetCode() == S_ALREADY) {
            responsesToProcess = {msg->Responses.Get(), msg->ResponseSz};
            sglistToProcess = sglist;
            blockOffsetsToProcess = Request->BlobOffsets;
        } else {
            responsesToProcess = dataThatCanBeProcessed.Responses;
            sglistToProcess = dataThatCanBeProcessed.Sglist;
            blockOffsetsToProcess = dataThatCanBeProcessed.BlockOffsets;
        }

        for (auto& response: responsesToProcess) {
            if (response.Status != NKikimrProto::OK) {
                ReplyError(ctx, *msg, "read error", /*blobStorageError=*/true);
                return;
            }
            if (response.Id != Request->BlobId) {
                ReplyError(
                    ctx,
                    *msg,
                    "invalid response received",
                    /*blobStorageError=*/false);
                return;
            }
        }

        auto checksumsOrError = CopyReadResultToSglist(
            responsesToProcess,
            sglistToProcess,
            blockOffsetsToProcess);

        if (HasError(checksumsOrError.GetError())) {
            ReplyError(
                ctx,
                *msg,
                checksumsOrError.GetError().GetMessage(),
                /*blobStorageError=*/false);
            return;
        }

        blockChecksums = FillBlockChecksumsForRecoveredBlocksIfNeeded(
            Request->BlobOffsets,
            blockOffsetsToProcess,
            checksumsOrError.ExtractResult());
    } else {
        ReplyAndDie(
            ctx,
            std::make_unique<TResponse>(MakeError(
                E_CANCELLED,
                "failed to acquire sglist in ReadBlobActor")));
        return;
    }

    auto response = std::make_unique<TResponse>();
    response->BlockChecksums = std::move(blockChecksums);
    response->ExecCycles = RequestInfo->GetExecCycles();
    ReplyAndDie(ctx, std::move(response));
}

void TReadBlobActor::HandleUndelivered(
    const NActors::TEvents::TEvUndelivered::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto response = std::make_unique<TResponse>(
        MakeError(E_REJECTED, "Get event undelivered %s", ev->Get()->Reason));

    ReplyAndDie(ctx, std::move(response));
}

void TReadBlobActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto response = std::make_unique<TResponse>(
        MakeError(E_REJECTED, "tablet is shutting down"));

    ReplyAndDie(ctx, std::move(response));
}

STFUNC(TReadBlobActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvents::TEvWakeup,
            TLongRunningOperationCompanion::HandleTimeout);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvGetResult, HandleGetResult);
        HFunc(TEvents::TEvUndelivered, HandleUndelivered);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_COMMON,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
