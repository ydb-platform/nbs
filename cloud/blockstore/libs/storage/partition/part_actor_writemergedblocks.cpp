#include "part_actor.h"

#include "model/merged_blob_compression.h"
#include "model/merged_blob_compression_policy.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/vector.h>
#include <util/system/datetime.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename ...T>
IEventBasePtr CreateWriteBlocksResponse(bool replyLocal, T&& ...args)
{
    if (replyLocal) {
        return std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            std::forward<T>(args)...);
    } else {
        return std::make_unique<TEvService::TEvWriteBlocksResponse>(
            std::forward<T>(args)...);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TWriteMergedBlocksActor final
    : public TActorBootstrapped<TWriteMergedBlocksActor>
{
public:
    struct TWriteBlobRequest
    {
        TPartialBlobId BlobId;
        const TBlockRange32 WriteRange;
        TVector<ui32> Checksums;
        bool Compress = false;
        TMergedBlobCompressionStats CompressionStats;
        std::shared_ptr<const NProto::TBlobCompression> Compression;
        TGuardedBuffer<TString> Encoded;
        TGuardedSgList Content;

        TWriteBlobRequest(
                const TPartialBlobId& blobId,
                const TBlockRange32& writeRange,
                bool compress)
            : BlobId(blobId)
            , WriteRange(writeRange)
            , Compress(compress)
        {}
    };

private:
    const ui64 TabletId;
    const TActorId Tablet;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ui64 CommitId;
    const TRequestInfoPtr RequestInfo;
    std::shared_ptr<void> CompressionBudget;
    TVector<TWriteBlobRequest> WriteBlobRequests;
    TVector<TBlobToConfirm> BlobsToConfirm;
    const bool ReplyLocal;
    const bool ShouldAddUnconfirmedBlobs = false;
    const IWriteBlocksHandlerPtr WriteHandler;
    const ui32 BlockSizeForChecksums;
    const ui32 BlockSize;
    const ui32 MinSavingsPercentage;

    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
    size_t WriteBlobRequestsCompleted = 0;

    TVector<TCallContextPtr> ForkedCallContexts;
    bool SafeToUseOrbit = true;

    bool UnconfirmedBlobsAdded = false;

public:
    TWriteMergedBlocksActor(
        const ui64 tabletId,
        const TActorId& tablet,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui64 commitId,
        TRequestInfoPtr requestInfo,
        TVector<TWriteBlobRequest> writeBlobRequests,
        bool replyLocal,
        bool shouldAddUnconfirmedBlobs,
        IWriteBlocksHandlerPtr writeHandler,
        ui32 blockSizeForChecksums,
        ui32 blockSize,
        ui32 minSavingsPercentage);

    void Bootstrap(const TActorContext& ctx);

private:
    TGuardedSgList BuildBlobContentAndComputeChecksums(TWriteBlobRequest& request);

    NProto::TError PrepareBlobs();
    void WriteBlobs(const TActorContext& ctx);
    void AddBlobs(const TActorContext& ctx, bool confirmed);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

    void Reply(
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        IEventBasePtr response) const;

private:
    STFUNC(StateWork);

    void HandleWriteBlobResponse(
        const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddBlobsResponse(
        const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddUnconfirmedBlobsResponse(
        const TEvPartitionPrivate::TEvAddUnconfirmedBlobsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TWriteMergedBlocksActor::TWriteMergedBlocksActor(
        const ui64 tabletId,
        const TActorId& tablet,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui64 commitId,
        TRequestInfoPtr requestInfo,
        TVector<TWriteBlobRequest> writeBlobRequests,
        bool replyLocal,
        bool shouldAddUnconfirmedBlobs,
        IWriteBlocksHandlerPtr writeHandler,
        ui32 blockSizeForChecksums,
        ui32 blockSize,
        ui32 minSavingsPercentage)
    : TabletId(tabletId)
    , Tablet(tablet)
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , CommitId(commitId)
    , RequestInfo(std::move(requestInfo))
    , WriteBlobRequests(std::move(writeBlobRequests))
    , ReplyLocal(replyLocal)
    , ShouldAddUnconfirmedBlobs(shouldAddUnconfirmedBlobs)
    , WriteHandler(std::move(writeHandler))
    , BlockSizeForChecksums(blockSizeForChecksums)
    , BlockSize(blockSize)
    , MinSavingsPercentage(minSavingsPercentage)
{}

void TWriteMergedBlocksActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "WriteMergedBlocks",
        RequestInfo->CallContext->RequestId);

    Become(&TThis::StateWork);

    auto error = PrepareBlobs();
    if (HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }
    WriteBlobs(ctx);
    if (ShouldAddUnconfirmedBlobs) {
        AddBlobs(ctx, false /* confirmed */);
    }
}

TGuardedSgList TWriteMergedBlocksActor::BuildBlobContentAndComputeChecksums(
    TWriteBlobRequest& request)
{
    auto guardedSgList =
        WriteHandler->GetBlocks(ConvertRangeSafe(request.WriteRange));

    if (auto guard = guardedSgList.Acquire()) {
        const auto& sgList = guard.Get();

        for (size_t index = 0; index < sgList.size(); ++index) {
            const auto& block = sgList[index];

            size_t blockIndex = request.WriteRange.Start + index;
            const auto digest = BlockDigestGenerator->ComputeDigest(
                blockIndex,
                block);

            if (digest.Defined()) {
                AffectedBlockInfos.push_back({blockIndex, *digest});
            }
        }
    }
    return guardedSgList;
}

NProto::TError TWriteMergedBlocksActor::PrepareBlobs()
{
    ui64 reservation = 0;
    for (const auto& req: WriteBlobRequests) {
        if (req.Compress) {
            reservation += ui64(req.BlobId.BlobSize()) * 3 +
                MergedBlobCompressionWorkspaceBytes;
        }
    }
    if (reservation) {
        CompressionBudget = TryAcquireMergedBlobBudget(
            false, false, reservation);
    }
    for (auto& req: WriteBlobRequests) {
        req.Content = BuildBlobContentAndComputeChecksums(req);
        if (!req.Compress) {
            continue;
        }
        auto& stats = req.CompressionStats;
        stats.Attempts = 1;
        stats.LogicalBytes = req.BlobId.BlobSize();
        stats.PhysicalBytes = req.BlobId.BlobSize();
        if (!CompressionBudget) {
            stats.RawFallback = 1;
            stats.AdmissionRejected = 1;
            continue;
        }
        const ui64 cpuStart = ThreadCPUTime();
        auto guard = req.Content.Acquire();
        if (!guard) {
            return MakeError(E_CANCELLED, "Merged write buffer was released");
        }
        const auto& sglist = guard.Get();
        const ui64 size = SgListGetSize(sglist);
        if (size != req.BlobId.BlobSize() ||
            size > MaxMergedBlobLogicalBytes)
        {
            return MakeError(E_ARGUMENT, "Invalid logical Merged write size");
        }
        auto raw = TString::Uninitialized(size);
        SgListCopy(sglist, {raw.data(), raw.size()});
        NProto::TBlobMeta meta;
        meta.MutableMergedBlocks()->SetStart(req.WriteRange.Start);
        meta.MutableMergedBlocks()->SetEnd(req.WriteRange.End);
        meta.MutableMergedBlocks()->SetSkipped(0);
        if (BlockSizeForChecksums) {
            for (size_t offset = 0; offset < raw.size(); offset += BlockSize) {
                const ui32 checksum =
                    ComputeDefaultDigest({raw.data() + offset, BlockSize});
                req.Checksums.push_back(checksum);
                meta.AddBlockChecksums(checksum);
            }
        }
        TCompressedMergedBlob compressed;
        auto error = CompressMergedBlob(
            raw, BlockSize, MinSavingsPercentage, meta, compressed);
        stats.EncodeCpuMicros = ThreadCPUTime() - cpuStart;
        if (HasError(error)) {
            return error;
        }
        if (compressed.Payload.empty()) {
            stats.RawFallback = 1;
            continue;
        }
        stats.Accepted = 1;
        stats.PhysicalBytes = compressed.Payload.size();
        stats.MetadataBytes = compressed.MetadataBytes;
        // Channel selection already happened once. Preserve every id field
        // except the physical byte size, before PUT and Unconfirmed publication.
        req.BlobId = TPartialBlobId(
            req.BlobId.Generation(), req.BlobId.Step(), req.BlobId.Channel(),
            compressed.Payload.size(), req.BlobId.Cookie(), req.BlobId.PartId());
        req.Compression = std::make_shared<NProto::TBlobCompression>(
            std::move(compressed.Compression));
        req.Encoded = TGuardedBuffer<TString>(std::move(compressed.Payload));
        req.Content = req.Encoded.GetGuardedSgList();
    }
    return {};
}

void TWriteMergedBlocksActor::WriteBlobs(const TActorContext& ctx)
{
    for (ui32 i = 0; i < WriteBlobRequests.size(); ++i) {
        auto& req = WriteBlobRequests[i];
        auto request = std::make_unique<TEvPartitionCommonPrivate::TEvWriteBlobRequest>(
            req.BlobId,
            req.Content,
            req.Compression ? 0 : BlockSizeForChecksums,
            false); // async
        request->IsCompressed = bool(req.Compression);
        request->CompressionStats = req.CompressionStats;

        if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                RequestInfo->CallContext->LWOrbit,
                "TEvPartitionCommonPrivate::TEvWriteBlobRequest",
                RequestInfo->CallContext->RequestId);
        }
        request->CallContext->RequestId = RequestInfo->CallContext->RequestId;

        ForkedCallContexts.emplace_back(request->CallContext);

        NCloud::Send(
            ctx,
            Tablet,
            std::move(request),
            i);
    }
}

void TWriteMergedBlocksActor::AddBlobs(
    const TActorContext& ctx,
    bool confirmed)
{
    Y_DEBUG_ABORT_UNLESS(RequestInfo);

    IEventBasePtr request;

    if (confirmed) {
        TVector<TAddMergedBlob> blobs(Reserve(WriteBlobRequests.size()));

        for (auto& req: WriteBlobRequests) {
            blobs.emplace_back(
                req.BlobId,
                req.WriteRange,
                TBlockMask(), // skipMask
                std::move(req.Checksums),
                req.Compression);
        }

        request = std::make_unique<TEvPartitionPrivate::TEvAddBlobsRequest>(
            RequestInfo->CallContext,
            CommitId,
            TVector<TAddMixedBlob>(),
            std::move(blobs),
            TVector<TAddFreshBlob>(),
            ADD_WRITE_RESULT
        );
    } else {
        BlobsToConfirm.reserve(WriteBlobRequests.size());

        for (const auto& req: WriteBlobRequests) {
            BlobsToConfirm.emplace_back(
                req.BlobId.UniqueId(),
                req.WriteRange,
                req.Checksums,
                req.Compression);
        }

        request = std::make_unique<TEvPartitionPrivate::TEvAddUnconfirmedBlobsRequest>(
            RequestInfo->CallContext,
            CommitId,
            BlobsToConfirm);
    }

    SafeToUseOrbit = false;

    NCloud::Send(
        ctx,
        Tablet,
        std::move(request));
}

void TWriteMergedBlocksActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    using TEvent = TEvPartitionPrivate::TEvWriteBlocksCompleted;
    using TCompleted = TEvPartitionPrivate::TWriteBlocksCompleted;
    if (!BlockSizeForChecksums) {
        // this structure is needed only to transfer block checksums to
        // PartState - passing it without checksums will trigger a couple
        // of debug asserts and is actually pointless even if we didn't have
        // those asserts
        BlobsToConfirm.clear();
    }
    auto ev = std::make_unique<TEvent>(
        error,
        TCompleted::CreateMergedBlocksCompleted(
            ShouldAddUnconfirmedBlobs,
            std::move(BlobsToConfirm)));

    ev->ExecCycles = RequestInfo->GetExecCycles();
    ev->TotalCycles = RequestInfo->GetTotalCycles();

    ev->CommitId = CommitId;
    ev->AffectedBlockInfos = std::move(AffectedBlockInfos);

    ui64 waitCycles = RequestInfo->GetWaitCycles();

    ui32 blocksCount = 0;
    for (const auto& r: WriteBlobRequests) {
        blocksCount += r.WriteRange.Size();
    }

    auto execTime = CyclesToDurationSafe(ev->ExecCycles);
    auto waitTime = CyclesToDurationSafe(waitCycles);

    auto& counters = *ev->Stats.MutableUserWriteCounters();
    counters.SetRequestsCount(1);
    counters.SetBatchCount(1);
    counters.SetBlocksCount(blocksCount);
    counters.SetExecTime(execTime.MicroSeconds());
    counters.SetWaitTime(waitTime.MicroSeconds());

    NCloud::Send(ctx, Tablet, std::move(ev));
}

bool TWriteMergedBlocksActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAndDie(ctx, error);
        return true;
    }

    return false;
}

void TWriteMergedBlocksActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    NotifyCompleted(ctx, error);

    auto response = CreateWriteBlocksResponse(ReplyLocal, error);
    Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TWriteMergedBlocksActor::Reply(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    IEventBasePtr response) const
{
    if (SafeToUseOrbit) {
        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "WriteMergedBlocks",
            requestInfo.CallContext->RequestId);
    }

    NCloud::Reply(ctx, requestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TWriteMergedBlocksActor::HandleWriteBlobResponse(
    const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    RequestInfo->AddExecCycles(msg->ExecCycles);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    STORAGE_VERIFY(
        ev->Cookie < WriteBlobRequests.size(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    if (WriteBlobRequests[ev->Cookie].Compression) {
        // Encoded PUTs do not compute logical checksums; they were persisted
        // together with the final format and id before sending the request.
    } else if (BlobsToConfirm.empty()) {
        WriteBlobRequests[ev->Cookie].Checksums =
            std::move(msg->BlockChecksums);
    } else {
        STORAGE_VERIFY(
            BlobsToConfirm.size() == WriteBlobRequests.size(),
            TWellKnownEntityTypes::TABLET,
            TabletId);

        BlobsToConfirm[ev->Cookie].Checksums = std::move(msg->BlockChecksums);
    }

    STORAGE_VERIFY(
        WriteBlobRequestsCompleted < WriteBlobRequests.size(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    if (++WriteBlobRequestsCompleted < WriteBlobRequests.size()) {
        return;
    }

    for (const auto& context: ForkedCallContexts) {
        RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
    }

    if (ShouldAddUnconfirmedBlobs) {
        if (UnconfirmedBlobsAdded) {
            ReplyAndDie(ctx, MakeError(S_OK));
        }
    } else {
        AddBlobs(ctx, true /* confirmed */);
    }
}

void TWriteMergedBlocksActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    SafeToUseOrbit = true;

    RequestInfo->AddExecCycles(msg->ExecCycles);

    const auto& error = msg->GetError();
    if (HandleError(ctx, error)) {
        return;
    }

    ReplyAndDie(ctx, error);
}

void TWriteMergedBlocksActor::HandleAddUnconfirmedBlobsResponse(
    const TEvPartitionPrivate::TEvAddUnconfirmedBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    SafeToUseOrbit = true;

    RequestInfo->AddExecCycles(msg->ExecCycles);

    const auto& error = msg->GetError();
    if (HandleError(ctx, error)) {
        return;
    }

    UnconfirmedBlobsAdded = true;

    if (WriteBlobRequestsCompleted < WriteBlobRequests.size()) {
        return;
    }

    ReplyAndDie(ctx, error);
}

void TWriteMergedBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    ReplyAndDie(ctx, error);
}

STFUNC(TWriteMergedBlocksActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionCommonPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvPartitionPrivate::TEvAddBlobsResponse, HandleAddBlobsResponse);
        HFunc(TEvPartitionPrivate::TEvAddUnconfirmedBlobsResponse, HandleAddUnconfirmedBlobsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::WriteMergedBlocks(
    const TActorContext& ctx,
    TRequestInBuffer<TWriteBufferRequestData> requestInBuffer)
{
    const ui64 commitId = State->GenerateCommitId();

    if (commitId == InvalidCommitId) {
        requestInBuffer.Data.RequestInfo->CancelRequest(ctx);
        RebootPartitionOnCommitIdOverflow(ctx, "WriteMergedBlocks");
        return;
    }

    State->AccessCommitQueue()->AcquireBarrier(commitId);
    State->GetGarbageQueue().AcquireBarrier(commitId);

    const auto writeRange = requestInBuffer.Data.Range;
    const ui32 maxBlocksInBlob = State->GetMaxBlocksInBlob();

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Writing merged blocks @%lu (range: %s)",
        LogTitle.GetWithTime().c_str(),
        commitId,
        DescribeRange(writeRange).c_str());

    ui32 blobIndex = 0;

    TVector<TWriteMergedBlocksActor::TWriteBlobRequest> requests(
        Reserve(1 + writeRange.Size() / maxBlocksInBlob));

    for (ui64 blockIndex: xrange(writeRange, maxBlocksInBlob)) {
        auto range = TBlockRange32::MakeClosedIntervalWithLimit(
            blockIndex,
            blockIndex + maxBlocksInBlob - 1,
            writeRange.End);

        auto blobId = State->GenerateBlobId(
            EChannelDataKind::Merged,
            EChannelPermission::UserWritesAllowed,
            commitId,
            range.Size() * State->GetBlockSize(),
            blobIndex++);

        const bool compress = SelectMergedBlobCompression(
            *Config,
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId(),
            commitId,
            blobIndex - 1,
            false);
        requests.emplace_back(blobId, range, compress);
    }

    Y_ABORT_UNLESS(requests);

    const ui32 checksumBoundary =
        Config->GetDiskPrefixLengthWithBlockChecksumsInBlobs()
        / State->GetBlockSize();
    const bool checksumsEnabled = writeRange.Start < checksumBoundary;

    const bool addingUnconfirmedBlobsEnabledForCloud = Config->IsAddingUnconfirmedBlobsFeatureEnabled(
        PartitionConfig.GetCloudId(),
        PartitionConfig.GetFolderId(),
        PartitionConfig.GetDiskId());
    bool shouldAddUnconfirmedBlobs = Config->GetAddingUnconfirmedBlobsEnabled()
        || addingUnconfirmedBlobsEnabledForCloud;
    if (shouldAddUnconfirmedBlobs) {
        // we take confirmed blobs into account because they have not yet been
        // added to the index, so we treat them as unconfirmed while counting
        // the limit
        const ui32 blobCount =
            State->GetUnconfirmedBlobCount() + State->GetConfirmedBlobCount();
        shouldAddUnconfirmedBlobs =
            blobCount < Config->GetUnconfirmedBlobCountHardLimit();
    }

    auto actor = NCloud::Register<TWriteMergedBlocksActor>(
        ctx,
        TabletID(),
        SelfId(),
        BlockDigestGenerator,
        commitId,
        requestInBuffer.Data.RequestInfo,
        std::move(requests),
        requestInBuffer.Data.ReplyLocal,
        shouldAddUnconfirmedBlobs,
        std::move(requestInBuffer.Data.Handler),
        checksumsEnabled ? State->GetBlockSize() : 0,
        State->GetBlockSize(),
        Config->GetMergedBlobCompressionMinSavingsPercentage()
    );
    Actors.Insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
