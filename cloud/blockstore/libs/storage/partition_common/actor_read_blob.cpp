#include "actor_read_blob.h"

#include <cloud/blockstore/libs/storage/partition/model/merged_blob_compression_policy.h>

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/api/public.h>

#include <cloud/storage/core/libs/diagnostics/wilson_trace_compatibility.h>

#include <util/system/datetime.h>

#include <algorithm>
#include <numeric>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

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
    SendGetRequest(ctx);
}

void TReadBlobActor::SendGetRequest(const TActorContext& ctx)
{
    using TEvGetQuery = TEvBlobStorage::TEvGet::TQuery;

    size_t blocksCount = Request->BlobOffsets.size();

    const auto& format = Request->Format;
    CompressionStats.Background = Request->Async;
    if (format.Compression || format.Invalid) {
        CompressionCpuStart = ThreadCPUTime();
    }
    const auto reject = [&](TStringBuf reason) {
        CompressionStats.FormatErrors = 1;
        ReplyAndDie(ctx, std::make_unique<TResponse>(MakeError(E_IO, TString(reason))));
    };
    if (format.Invalid ||
        (format.LogicalBlocks && !format.Compression &&
         ui64(format.LogicalBlocks) * BlockSize != Request->BlobId.BlobSize()))
    {
        reject("Missing or inconsistent Merged blob metadata");
        return;
    }
    if (format.Compression) {
        if (ui64(blocksCount) * BlockSize > NPartition::MaxMergedBlobLogicalBytes ||
            ui64(format.LogicalBlocks) * BlockSize !=
                format.Compression->GetLogicalSize())
        {
            reject("Invalid compressed Merged logical read size");
            return;
        }
        auto error = NPartition::ValidateMergedBlobCompression(
            *format.Compression, Request->BlobId.BlobSize(), BlockSize);
        if (HasError(error)) {
            CompressionStats.FormatErrors = 1;
            ReplyAndDie(ctx, std::make_unique<TResponse>(std::move(error)));
            return;
        }
    }

    if (format.Compression) {
        if (!Request->Sglist.Acquire()) {
            ReplyAndDie(ctx, std::make_unique<TResponse>(
                MakeError(E_CANCELLED, "Compressed read buffer was released")));
            return;
        }
        // Reserve before allocating the planner, scatter order or GET queries.
        // Include vector growth and the logical checksum result, in addition
        // to codec and descriptor workspace.
        const ui64 vectorBytes =
            2 * ui64(blocksCount) * (sizeof(size_t) + sizeof(ui32)) +
            ui64(format.Compression->ChunkSizesSize()) *
                (2 * sizeof(NPartition::TCompressedBlobChunk) +
                 sizeof(TEvGetQuery));
        CompressionBudget = NPartition::TryAcquireMergedBlobBudget(
            Request->Async,
            true,
            ui64(blocksCount) * BlockSize + Request->BlobId.BlobSize() +
                NPartition::MergedBlobCompressionWorkspaceBytes + vectorBytes);
        if (!CompressionBudget) {
            CompressionStats.ReadAdmissionRejected = 1;
            ReplyAndDie(ctx, std::make_unique<TResponse>(
                MakeError(E_REJECTED, "Compressed read admission limit")));
            return;
        }
    }

    if (format.Compression) {
        auto error = NPartition::PlanCompressedBlobRead(
            *format.Compression, Request->BlobId.BlobSize(), BlockSize,
            Request->BlobOffsets, CompressedChunks);
        if (HasError(error)) {
            CompressionStats.FormatErrors = 1;
            ReplyAndDie(ctx, std::make_unique<TResponse>(std::move(error)));
            return;
        }
        SortedBlockPositions.resize(blocksCount);
        std::iota(SortedBlockPositions.begin(), SortedBlockPositions.end(), 0);
        std::sort(
            SortedBlockPositions.begin(), SortedBlockPositions.end(),
            [&](size_t a, size_t b) {
                return Request->BlobOffsets[a] < Request->BlobOffsets[b];
            });
    }

    const size_t queryCapacity =
        format.Compression ? CompressedChunks.size() : blocksCount;
    TArrayHolder<TEvGetQuery> queries(new TEvGetQuery[queryCapacity]);
    size_t queriesCount = 0;

    if (format.Compression) {
        for (const auto& chunk: CompressedChunks) {
            queries[queriesCount++].Set(
                Request->BlobId, chunk.Offset, chunk.Size);
            PhysicalBytes += chunk.Size;
        }
    } else {
        PhysicalBytes = blocksCount * BlockSize;
    }
    for (size_t i = 0; !format.Compression && i < blocksCount; ++i) {
        if (i && Request->BlobOffsets[i] == Request->BlobOffsets[i-1] + 1) {
            // extend range
            queries[queriesCount-1].Size += BlockSize;
        } else {
            queries[queriesCount++].Set(
                Request->BlobId,
                Request->BlobOffsets[i] * BlockSize,
                BlockSize);
        }
    }

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
    if (CompressionCpuStart) {
        CompressionStats.DecodeCpuMicros += ThreadCPUTime() - CompressionCpuStart;
        CompressionCpuStart = 0;
        CompressionStats.ReadPhysicalBytes = PhysicalBytes;
    }

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
    request->BytesCount = PhysicalBytes;
    request->CompressionStats = CompressionStats;
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
    if (CompressionCpuStart) {
        CompressionStats.DecodeCpuMicros += ThreadCPUTime() - CompressionCpuStart;
        CompressionCpuStart = 0;
    }
    if ((Request->Format.Compression || Request->Format.Invalid) &&
        HasError(response->GetError()) &&
        !CompressionStats.ReadAdmissionRejected)
    {
        CompressionStats.DecodeErrors = 1;
    }
    if (!HasError(response->GetError()) && Request->Format.LogicalBlocks &&
        !Request->Format.Compression)
    {
        CompressionStats.RawMergedReadLogicalBytes =
            ui64(Request->BlobOffsets.size()) * BlockSize;
    }
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
    const TActorContext& ctx,
    const TEvBlobStorage::TEvGetResult& response,
    const TString& description)
{
    LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_COMMON,
        "[%lu] TEvBlobStorage::TEvGet failed: %s\n%s",
        PartitionTabletId,
        description.data(),
        response.Print(false).data());

    if (response.Status == NKikimrProto::DEADLINE) {
        DeadlineSeen = true;
    }

    ReplyAndDie(
        ctx,
        std::make_unique<TResponse>(MakeError(
            E_REJECTED,
            "TEvBlobStorage::TEvGet failed: " + description)));
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
        ReplyError(ctx, *msg, msg->ErrorReason);
        return;
    }

    if (Request->Format.Compression) {
        HandleCompressedResult(*msg, ctx);
        return;
    }

    const auto& blobId = Request->BlobId;
    size_t blocksCount = Request->BlobOffsets.size();
    TVector<ui32> blockChecksums;

    if (auto guard = Request->Sglist.Acquire()) {
        const auto& sglist = guard.Get();
        size_t sglistIndex = 0;

        for (size_t i = 0; i < msg->ResponseSz; ++i) {
            auto& response = msg->Responses[i];

            if (response.Status != NKikimrProto::OK) {
                if (NCloud::IsUnrecoverable(response.Status)
                        && StorageAccessMode == EStorageAccessMode::Repair)
                {
                    LOG_WARN(ctx, TBlockStoreComponents::PARTITION_COMMON,
                        "[%lu] Repairing TEvBlobStorage::TEvGet %s error (%s)",
                        PartitionTabletId,
                        NKikimrProto::EReplyStatus_Name(response.Status).data(),
                        msg->Print(false).data());

                    const auto marker = GetBrokenDataMarker();
                    auto& block = sglist[sglistIndex];
                    Y_ABORT_UNLESS(block.Data());
                    memcpy(
                        const_cast<char*>(block.Data()),
                        marker.data(),
                        Min(block.Size(), marker.size())
                    );
                    ++sglistIndex;

                    while (sglistIndex < sglist.size()) {
                        const ui16 offset = Request->BlobOffsets[sglistIndex];
                        const ui16 prevOffset = Request->BlobOffsets[sglistIndex - 1];
                        if (offset != prevOffset + 1) {
                            break;
                        }

                        auto& block = sglist[sglistIndex];
                        Y_ABORT_UNLESS(block.Data());
                        memcpy(
                            const_cast<char*>(block.Data()),
                            marker.data(),
                            Min(block.Size(), marker.size())
                        );

                        ++sglistIndex;
                    }

                    continue;
                } else {
                    ReplyError(ctx, *msg, "read error");
                    return;
                }
            }

            if (response.Id != blobId ||
                response.Buffer.empty() ||
                response.Buffer.size() % BlockSize != 0)
            {
                ReplyError(ctx, *msg, "invalid response received");
                return;
            }

            for (auto iter = response.Buffer.begin(); iter.Valid(); ) {
                if (sglistIndex >= sglist.size()) {
                    ReplyError(ctx, *msg, "response is out of range");
                    return;
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

        if (sglistIndex != blocksCount) {
            ReplyError(ctx, *msg, "invalid response received");
            return;
        }
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

void TReadBlobActor::HandleCompressedResult(
    const TEvBlobStorage::TEvGetResult& result,
    const TActorContext& ctx)
{
    CompressionCpuStart = ThreadCPUTime();
    const auto fail = [&](TStringBuf reason) {
        CompressionStats.FormatErrors = 1;
        ReplyAndDie(ctx, std::make_unique<TResponse>(MakeError(E_IO, TString(reason))));
    };
    if (result.ResponseSz != CompressedChunks.size()) {
        fail("Invalid compressed blob response count");
        return;
    }

    // Assemble complete logical blocks privately. Neither corrupt fragments nor
    // a late failure may expose a partially decoded result to a local client.
    auto logical = TString::Uninitialized(
        Request->BlobOffsets.size() * BlockSize);
    for (size_t i = 0; i < CompressedChunks.size(); ++i) {
        if (!Request->Sglist.Acquire()) {
            ReplyAndDie(ctx, std::make_unique<TResponse>(
                MakeError(E_CANCELLED, "Compressed read buffer was released")));
            return;
        }
        const auto& chunk = CompressedChunks[i];
        const auto& response = result.Responses[i];
        if (response.Status != NKikimrProto::OK) {
            ReplyError(ctx, result, "compressed chunk read error");
            return;
        }
        if (response.Id != Request->BlobId ||
            response.Shift != chunk.Offset ||
            response.Buffer.size() != chunk.Size)
        {
            fail("Invalid compressed blob fragment");
            return;
        }
        auto payload = TString::Uninitialized(chunk.Size);
        response.Buffer.Begin().ExtractPlainDataAndAdvance(
            payload.begin(), chunk.Size);
        TString decoded;
        auto error = NPartition::DecodeMergedBlobChunk(
            *Request->Format.Compression, chunk.Index, payload, decoded);
        if (HasError(error)) {
            CompressionStats.FormatErrors = 1;
            ReplyAndDie(ctx, std::make_unique<TResponse>(std::move(error)));
            return;
        }

        ++CompressionStats.DecodedChunks;
        const ui64 chunkBegin =
            ui64(chunk.Index) * NPartition::MergedBlobCompressionChunkSize;
        const ui64 chunkEnd = chunkBegin + decoded.size();
        auto position = std::lower_bound(
            SortedBlockPositions.begin(), SortedBlockPositions.end(), chunkBegin,
            [&](size_t j, ui64 begin) {
                return (ui64(Request->BlobOffsets[j]) + 1) * BlockSize <= begin;
            });
        for (; position != SortedBlockPositions.end(); ++position) {
            const size_t j = *position;
            const ui64 blockBegin = ui64(Request->BlobOffsets[j]) * BlockSize;
            if (blockBegin >= chunkEnd) {
                break;
            }
            const ui64 begin = Max(blockBegin, chunkBegin);
            const ui64 end = Min(blockBegin + BlockSize, chunkEnd);
            if (begin < end) {
                memcpy(
                    logical.begin() + j * BlockSize + begin - blockBegin,
                    decoded.data() + begin - chunkBegin,
                    end - begin);
            }
        }
    }

    auto guard = Request->Sglist.Acquire();
    if (!guard) {
        ReplyAndDie(ctx, std::make_unique<TResponse>(
            MakeError(E_CANCELLED, "Compressed read buffer was released")));
        return;
    }
    const auto& sglist = guard.Get();
    if (sglist.size() != Request->BlobOffsets.size()) {
        fail("Invalid compressed read destination count");
        return;
    }
    for (const auto& block: sglist) {
        if (!block.Data() || block.Size() != BlockSize) {
            fail("Invalid compressed read destination");
            return;
        }
    }
    CompressionStats.ReadLogicalBytes = logical.size();
    auto response = std::make_unique<TResponse>();
    for (size_t i = 0; i < sglist.size(); ++i) {
        const char* data = logical.data() + i * BlockSize;
        if (ShouldCalculateChecksums) {
            response->BlockChecksums.push_back(
                ComputeDefaultDigest({data, BlockSize}));
        }
        memcpy(const_cast<char*>(sglist[i].Data()), data, BlockSize);
    }
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
