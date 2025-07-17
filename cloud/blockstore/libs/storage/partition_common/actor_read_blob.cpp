#include "actor_read_blob.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/api/public.h>

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
        TDuration longRunningThreshold)
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

    SendGetRequest(ctx);
    TLongRunningOperationCompanion::RequestStarted(ctx);
}

void TReadBlobActor::SendGetRequest(const TActorContext& ctx)
{
    using TEvGetQuery = TEvBlobStorage::TEvGet::TQuery;

    size_t blocksCount = Request->BlobOffsets.size();

    TArrayHolder<TEvGetQuery> queries(new TEvGetQuery[blocksCount]);
    size_t queriesCount = 0;

    for (size_t i = 0; i < blocksCount; ++i) {
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

    request->Orbit = std::move(RequestInfo->CallContext->LWOrbit);

    RequestSent = ctx.Now();

    SendToBSProxy(
        ctx,
        Request->Proxy,
        request.release());
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

    const auto& blobId = Request->BlobId;
    size_t blocksCount = Request->BlobOffsets.size();
    TVector<ui32> blockChecksums;

    if (auto guard = Request->Sglist.Acquire()) {
        const auto& sglist = guard.Get();
        size_t sglistIndex = 0;

        for (size_t i = 0; i < msg->ResponseSz; ++i) {
            auto& response = msg->Responses[i];

            if (response.Status != NKikimrProto::OK) {
                if (IsUnrecoverable(response.Status)
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
                        const auto offset = Request->BlobOffsets[sglistIndex];
                        const auto prevOffset = Request->BlobOffsets[sglistIndex - 1];
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
