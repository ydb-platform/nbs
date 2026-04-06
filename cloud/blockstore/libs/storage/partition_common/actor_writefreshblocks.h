#pragma once

#include "events_private.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

enum class EFreshRequestType
{
    WriteBlocks,
    WriteBlocksLocal,
    ZeroBlocks
};

template <typename... T>
NActors::IEventBasePtr CreateWriteBlocksResponse(bool replyLocal, T&&... args)
{
    if (replyLocal) {
        return std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            std::forward<T>(args)...);
    }
    return std::make_unique<TEvService::TEvWriteBlocksResponse>(
        std::forward<T>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

class TWriteFreshBlocksActor final
    : public NActors::TActorBootstrapped<
          TWriteFreshBlocksActor>
{
public:
    struct TRequest
    {
        TRequestInfoPtr RequestInfo;
        EFreshRequestType RequestType;

        TRequest(TRequestInfoPtr requestInfo, EFreshRequestType requestType)
            : RequestInfo(std::move(requestInfo))
            , RequestType(requestType)
        {}
    };

private:
    const NActors::TActorId Owner;
    const NActors::TActorId ActorToAddFreshBlocks;
    const ui64 CommitId;
    const ui32 Channel;
    const ui32 BlockCount;
    const TVector<TRequest> Requests;
    TVector<TBlockRange32> BlockRanges;
    TVector<IWriteBlocksHandlerPtr> WriteHandlers;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const bool IsZeroRequest;
    const bool WaitForAddFreshBlocksResponseBeforeResponse;
    const ui64 TabletId;

    std::shared_ptr<std::atomic<ui64>> UnflushedFreshBlobByteCount;

    TString BlobContent;
    ui64 BlobSize = 0;

    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

    TCallContextPtr CombinedContext = MakeIntrusive<TCallContext>();

    bool ReplySent = false;

public:
    TWriteFreshBlocksActor(
        const NActors::TActorId& owner,
        const NActors::TActorId& actorToAddFreshBlocks,
        ui64 commitId,
        ui32 channel,
        ui32 blockCount,
        TVector<TRequest> requests,
        TVector<TBlockRange32> blockRanges,
        TVector<IWriteBlocksHandlerPtr> writeHandlers,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        bool waitForAddFreshBlocksResponseBeforeResponse,
        ui64 tabletId,
        std::shared_ptr<std::atomic<ui64>> unflushedFreshBlobByteCount);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    NProto::TError BuildBlobContentAndComputeDigest();

    void WriteBlob(const NActors::TActorContext& ctx);
    void AddBlocks(const NActors::TActorContext& ctx);

    template <typename TEvent>
    void NotifyCompleted(
        const NActors::TActorContext& ctx,
        std::unique_ptr<TEvent> ev);

    bool HandleError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void ReplyWrite(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);
    void ReplyZero(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void Reply(const NActors::TActorContext& ctx, const NProto::TError& error);

    void NotifyWrite(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void NotifyZero(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void Notify(const NActors::TActorContext& ctx, const NProto::TError& error);

    void ReplyAllAndDie(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleWriteBlobResponse(
        const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddFreshBlocksResponse(
        const TEvPartitionCommonPrivate::TEvAddFreshBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
