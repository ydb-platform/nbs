#include "actor_read_blob.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/public.h>

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/wilson_trace_compatibility.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetBrokenDataMarkerBlock(ui32 blockSize)
{
    return TString(GetBrokenDataMarker()).resize(blockSize, '\0');
}

}   // namespace

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
        bool passTraceIdToBlobstorage,
        std::shared_ptr<TBlobCompressor> blobCompressor)
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
    , BlobCompressor(std::move(blobCompressor))
{
    Y_ABORT_UNLESS(!Request->CompressionInfo || BlobCompressor);
}

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
    SendGetRequest(ctx);
}

TResultOrError<TBlobCompressor::TReadPlan> TReadBlobActor::GetQueries() const
{
    using TEvGetQuery = TEvBlobStorage::TEvGet::TQuery;

    if (Request->CompressionInfo) {
        return BlobCompressor->PlanReadBlob(
            Request->BlobId,
            *Request->CompressionInfo,
            Request->BlobOffsets);
    }

    size_t blocksCount = Request->BlobOffsets.size();

    TArrayHolder<TEvGetQuery> queries(new TEvGetQuery[blocksCount]);
    size_t queriesCount = 0;

    TVector<TVector<ui32>> blockOffsetsForEachRequest;

    for (size_t i = 0; i < blocksCount; ++i) {
        if (i && Request->BlobOffsets[i] == Request->BlobOffsets[i-1] + 1) {
            // extend range
            queries[queriesCount-1].Size += BlockSize;
        } else {
            queries[queriesCount++].Set(
                Request->BlobId,
                Request->BlobOffsets[i] * BlockSize,
                BlockSize);
            blockOffsetsForEachRequest.emplace_back();
        }

        blockOffsetsForEachRequest.back().push_back(Request->BlobOffsets[i]);
    }

    return TBlobCompressor::TReadPlan{
        .Queries = std::move(queries),
        .QueriesCount = queriesCount,
        .BlockOffsetsForEachRequest = std::move(blockOffsetsForEachRequest)};
}

void TReadBlobActor::SendGetRequest(const TActorContext& ctx)
{
    auto planOrError = GetQueries();
    if (HasError(planOrError.GetError())) {
        ReportCompressedBlobReadError(
            "Failed to plan compressed blob read",
            {{"tabletId", PartitionTabletId},
             {"blobId", Request->BlobId.ToString()},
             {"error", FormatError(planOrError.GetError())}});
        ReplyAndDie(ctx, std::make_unique<TResponse>(planOrError.GetError()));
        return;
    }
    auto [queries, queriesCount, blockOffsetsForEachRequest] =
        planOrError.ExtractResult();
    BlockOffsetsForEachRequest = std::move(blockOffsetsForEachRequest);

    auto request = std::make_unique<TEvBlobStorage::TEvGet>(
        queries,
        queriesCount,
        Request->Deadline,
        Request->Async
            ? NKikimrBlobStorage::AsyncRead
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
    const NProto::TError& error)
{
    auto request =
        std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobCompleted>(error);

    request->BlobId = Request->BlobId;
    request->BytesCount = Request->BlobOffsets.size() * BlockSize;
    request->RequestTime = ResponseReceived - RequestSent;
    request->GroupId = Request->GroupId;
    request->BSGroupOperationId = BSGroupOperationId;

    if (DeadlineSeen) {
        request->DeadlineSeen = true;
    }

    NCloud::Send(ctx, PartitionActorId, std::move(request));
}

void TReadBlobActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TResponse> response)
{
    NotifyCompleted(
        ctx,
        response->GetError());

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
    const NProto::TError& error)
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

    ReplyAndDie(ctx, std::make_unique<TResponse>(error));
}

void TReadBlobActor::ReplyError(
    const TActorContext& ctx,
    const TEvBlobStorage::TEvGetResult& response,
    const TString& description)
{
    ReplyError(ctx, response, MakeError(E_REJECTED, description));
}

////////////////////////////////////////////////////////////////////////////////

auto TReadBlobActor::FillBrokenDataAndFilterItOut(
    TVector<NKikimr::TEvBlobStorage::TEvGetResult::TResponse> responses,
    const TSgList& sglist,
    const NActors::TActorContext& ctx) -> TDataThatCanBeProcessed
{
    if (StorageAccessMode != EStorageAccessMode::Repair) {
        return {
            .Sglist = sglist,
            .BlockOffsets = Request->BlobOffsets,
            .Responses = std::move(responses)};
    }

    STORAGE_VERIFY(
        BlockOffsetsForEachRequest.size() == responses.size(),
        TWellKnownEntityTypes::TABLET,
        PartitionTabletId);


    THashMap<ui16, size_t> responseIndexForEachBlockOffset;
    for (size_t i = 0; i < BlockOffsetsForEachRequest.size(); ++i) {
        for (const auto& blockOffset: BlockOffsetsForEachRequest[i]) {
            responseIndexForEachBlockOffset[blockOffset] = i;
        }
    }

    const auto markerBlock = GetBrokenDataMarkerBlock(BlockSize);
    STORAGE_VERIFY(
        markerBlock.size() == BlockSize,
        TWellKnownEntityTypes::TABLET,
        PartitionTabletId);

    TSgList sglistForBlocksWithNoBrokenData;
    TVector<ui16> blockOffsetsWithNoBrokenData;
    for (size_t i = 0; i < sglist.size(); ++i) {
        const size_t blockOffset = Request->BlobOffsets[i];

        size_t responseIndex = responseIndexForEachBlockOffset[blockOffset];

        auto& response = responses[responseIndex];
        if (response.Status != NKikimrProto::OK &&
            NCloud::IsUnrecoverable(response.Status))
        {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::PARTITION_COMMON,
                "[%lu] Repairing TEvBlobStorage::TEvGet %s error (%s)",
                PartitionTabletId,
                NKikimrProto::EReplyStatus_Name(response.Status).data(),
                "TODO: add response print");

            auto& block = sglist[i];
            Y_ABORT_UNLESS(block.Data());
            memcpy(
                const_cast<char*>(block.Data()),
                markerBlock.data(),
                markerBlock.size());
            continue;
        }

        sglistForBlocksWithNoBrokenData.push_back(sglist[i]);
        blockOffsetsWithNoBrokenData.push_back(Request->BlobOffsets[i]);
    }

    auto toRemove = std::ranges::remove_if(
        responses,
        [](const auto& response)
        {
            return response.Status != NKikimrProto::OK &&
                   NCloud::IsUnrecoverable(response.Status);
        });

    responses.erase(toRemove.begin(), toRemove.end());

    return {
        .Sglist = std::move(sglistForBlocksWithNoBrokenData),
        .BlockOffsets = std::move(blockOffsetsWithNoBrokenData),
        .Responses = std::move(responses)};
}

TResultOrError<TVector<ui32>> TReadBlobActor::DecompressReadResultToSglist(
    TVector<TEvBlobStorage::TEvGetResult::TResponse>& responses,
    const TSgList& sglist,
    const TVector<ui16>& blockOffsets,
    const NActors::TActorContext&)
{
    TVector<TRope> compressedBuffers;
    for (auto& response: responses) {
        STORAGE_VERIFY(
            response.Status == NKikimrProto::OK,
            TWellKnownEntityTypes::TABLET,
            PartitionTabletId);
        compressedBuffers.push_back(std::move(response.Buffer));
    }

    TVector<ui32> blockChecksums;

    auto error = BlobCompressor->DecompressBlob(
        compressedBuffers,
        sglist,
        *Request->CompressionInfo,
        blockOffsets,
        ShouldCalculateChecksums ? &blockChecksums : nullptr);
    if (HasError(error)) {
        ReportCompressedBlobReadError(
            "Failed to decompress blob",
            {{"tabletId", PartitionTabletId},
             {"blobId", Request->BlobId.ToString()},
             {"error", FormatError(error)}});
        return error;
    }

    return std::move(blockChecksums);
}

TResultOrError<TVector<ui32>> TReadBlobActor::CopyReadResultToSglist(
    TVector<TEvBlobStorage::TEvGetResult::TResponse>& responses,
    const TSgList& sglist,
    const TVector<ui16>& blockOffsets,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    size_t sglistIndex = 0;
    TVector<ui32> blockChecksums;

    for (auto& response: responses) {
        STORAGE_VERIFY(
            response.Status == NKikimrProto::OK,
            TWellKnownEntityTypes::TABLET,
            PartitionTabletId);

        if (response.Buffer.empty() || response.Buffer.size() % BlockSize != 0) {
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
    const TVector<ui16>& originalBlockOffsets,
    const TVector<ui16>& newBlockOffsets,
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

void TReadBlobActor::HandleGetResult(
    const TEvBlobStorage::TEvGetResult::TPtr& ev,
    const TActorContext& ctx)
{
    ResponseReceived = ctx.Now();

    auto* msg = ev->Get();

    RequestInfo->CallContext->LWOrbit = std::move(msg->Orbit);

    if (msg->Status != NKikimrProto::OK) {
        ReplyError(ctx, *msg, msg->ErrorReason);
        return;
    }

    TVector<ui32> blockChecksums;

    if (auto guard = Request->Sglist.Acquire()) {
        const auto& sglist = guard.Get();

        TVector<TEvBlobStorage::TEvGetResult::TResponse> responses;
        responses.reserve(msg->ResponseSz);
        for (size_t i = 0; i < msg->ResponseSz; ++i) {
            responses.push_back(msg->Responses[i]);
        }

        auto [newSglist, newBlockOffsets, newResponses] =
            FillBrokenDataAndFilterItOut(std::move(responses), sglist, ctx);

        for (auto& response: newResponses) {
            if (response.Status != NKikimrProto::OK) {
                ReplyError(ctx, *msg, "read error");
                return;
            }
            if (response.Id != Request->BlobId) {
                ReplyError(ctx, *msg, "invalid response received");
                return;
            }
        }

        TResultOrError<TVector<ui32>> checksumsOrError = [&]()
        {
            if (Request->CompressionInfo) {
                return DecompressReadResultToSglist(
                    newResponses,
                    newSglist,
                    newBlockOffsets,
                    ctx);
            }
            return CopyReadResultToSglist(
                newResponses,
                newSglist,
                newBlockOffsets,
                ctx);
        }();

        if (HasError(checksumsOrError.GetError())) {
            ReplyError(ctx, *msg, checksumsOrError.GetError().GetMessage());
            return;
        }

        blockChecksums = FillBlockChecksumsForRecoveredBlocksIfNeeded(
            Request->BlobOffsets,
            newBlockOffsets,
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
    const NActors::TEvents::TEvUndelivered::TPtr &ev,
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
        HFunc(TEvents::TEvWakeup, TLongRunningOperationCompanion::HandleTimeout);
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
