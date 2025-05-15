#include "backup_volume.h"

#include "volume_manipulation_params.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCopyVolumeCommand
    : public TCommand
{
protected:
    TString DiskId;
    TString BackupDiskId;
    TString CheckpointId;
    TString MountToken;
    bool MountLocal = false;
    ui32 IoDepth = 1;

    ui32 BatchSize = 4 * 1024 * 1024;
    ui32 BatchBlocksCount = 0;
    ui32 ChangedBlocksCount = 1024 * 1024;

    ui32 TabletVersion = 0;
    ui32 BlockSize = 0;
    ui64 BlocksCount = 0;

    NCloud::NProto::EStorageMediaKind MainDiskStorageMediaKind =
        NCloud::NProto::STORAGE_MEDIA_DEFAULT;

    TString StorageMediaKindArg;
    NCloud::NProto::EStorageMediaKind BackupDiskStorageMediaKind =
        NCloud::NProto::STORAGE_MEDIA_DEFAULT;

    NProto::TEncryptionSpec EncryptionSpec;

    ui64 StartIndex = 0;
    TAtomic BlocksLeft = 0;

    ISessionPtr SrcSession;
    ISessionPtr DstSession;

    TVector<NThreading::TFuture<NProto::TWriteBlocksLocalResponse>> Write;
    NThreading::TFuture<NProto::TGetChangedBlocksResponse> ChangedBlocks;
    NThreading::TBlockingQueue<ui32> Queue;

    struct TRange
    {
        ui64 Empty;
        ui64 Changed;
    };

public:
    TCopyVolumeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
        , Queue(0)
    {}

protected:
    bool Fail(const NProto::TError& error)
    {
        STORAGE_ERROR(FormatError(error));
        return false;
    }

    bool DescribeVolume()
    {
        STORAGE_DEBUG("Describing volume");

        auto request = std::make_shared<NProto::TDescribeVolumeRequest>();
        request->SetDiskId(DiskId);

        auto requestId = GetRequestId(*request);
        auto response = WaitFor(ClientEndpoint->DescribeVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        if (HasError(response)) {
            return Fail(response.GetError());
        }

        const auto& volume = response.GetVolume();
        BlockSize = volume.GetBlockSize();
        BlocksCount = volume.GetBlocksCount();
        MainDiskStorageMediaKind = volume.GetStorageMediaKind();

        if (BackupDiskStorageMediaKind == NCloud::NProto::STORAGE_MEDIA_DEFAULT) {
            BackupDiskStorageMediaKind = MainDiskStorageMediaKind;
        }

        const auto& encryptionDesc = volume.GetEncryptionDesc();
        if (encryptionDesc.GetMode() != NProto::NO_ENCRYPTION) {
            EncryptionSpec.SetMode(encryptionDesc.GetMode());
            EncryptionSpec.SetKeyHash(encryptionDesc.GetKeyHash());
        }

        if (TabletVersion == 0) {
            TabletVersion = volume.GetTabletVersion();
        }

        BatchBlocksCount = Max<ui32>(1, BatchSize / BlockSize);
        BatchSize = BatchBlocksCount * BlockSize;

        return true;
    }

    bool MountVolumes(const TString& srcDiskId, const TString& dstDiskId)
    {
        bool throttlingDisabled = true;

        auto response = MountVolume(
            srcDiskId,
            MountToken,
            SrcSession,
            NProto::VOLUME_ACCESS_READ_ONLY,
            MountLocal,
            throttlingDisabled,
            EncryptionSpec);

        if (HasError(response)) {
            return Fail(response.GetError());
        }

        response = MountVolume(
            dstDiskId,
            MountToken,
            DstSession,
            NProto::VOLUME_ACCESS_READ_WRITE,
            MountLocal,
            throttlingDisabled,
            EncryptionSpec);

        if (HasError(response)) {
            UnmountVolume(*SrcSession);
            return Fail(response.GetError());
        }

        return true;
    }

    bool UnmountVolumes()
    {
        bool result = UnmountVolume(*SrcSession);

        if (!UnmountVolume(*DstSession)) {
            result = false;
        }

        return result;
    }

    void GetChangedBlocks(ui64 startIndex, ui32 blocksCount)
    {
        STORAGE_DEBUG("Getting changed blocks " << startIndex << "-" << startIndex + blocksCount);

        auto request = std::make_shared<NProto::TGetChangedBlocksRequest>();
        request->SetDiskId(DiskId);
        request->SetStartIndex(startIndex);
        request->SetBlocksCount(blocksCount);
        request->SetHighCheckpointId(CheckpointId);
        request->SetIgnoreBaseDisk(true);

        auto requestId = GetRequestId(*request);
        auto future = ClientEndpoint->GetChangedBlocks(
            MakeIntrusive<TCallContext>(requestId), std::move(request));

        ChangedBlocks = future.Subscribe([=, this](const auto& future) {
            const auto& response = future.GetValue();
            if (HasError(response)) {
                STORAGE_ERROR(FormatError(response.GetError()));
                ShouldContinue.ShouldStop(1);
            }
        });
    }

    TRange NextRange(const char* mask, ui64 i, ui64 n)
    {
        ui64 empty = 0;
        ui32 changed = 0;

        while (i < n && (mask[i / CHAR_BIT] & (1 << i % CHAR_BIT)) == 0) {
            empty++;
            i++;
        }

        while (i < n && (mask[i / CHAR_BIT] & (1 << i % CHAR_BIT)) != 0 && changed < BatchBlocksCount) {
            changed++;
            i++;
        }

        return TRange{empty, changed};
    }

    TRange GetNextRange()
    {
        if (IsDiskRegistryMediaKind(MainDiskStorageMediaKind) || !CheckpointId) {
            return {0, Min<ui64>(BatchBlocksCount, BlocksCount - StartIndex)};
        }

        ui64 i = StartIndex % ChangedBlocksCount;
        ui64 n = Min<ui32>(BlocksCount - StartIndex + i, ChangedBlocksCount);

        if (i == 0) {
            GetChangedBlocks(StartIndex, n);
        }

        auto changedBlocks = WaitFor(ChangedBlocks);
        if (HasError(changedBlocks)) {
            STORAGE_ERROR(FormatError(changedBlocks.GetError()));
            return TRange{0, 0};
        }

        return NextRange(changedBlocks.GetMask().data(), i, n);
    }

    void ReadBlocks(ui32 bucket)
    {
        if (StartIndex >= BlocksCount) {
            return;
        }

        auto range = GetNextRange();

        if (range.Empty) {
            STORAGE_DEBUG("Skipping zero blocks " << StartIndex << "-" << StartIndex + range.Empty);
            StartIndex += range.Empty;

            if (AtomicSub(BlocksLeft, range.Empty) == 0) {
                ShouldContinue.ShouldStop(0);
            }
        }

        if (range.Changed) {
            DoReadBlocks(bucket, StartIndex, range.Changed);
            StartIndex += range.Changed;
            return;
        }

        Queue.Push(bucket);
    }

    void DoReadBlocks(ui32 bucket, ui64 startIndex, ui32 blocksCount)
    {
        STORAGE_DEBUG("Reading blocks " << startIndex << "-" << startIndex + blocksCount);

        TString buffer;
        buffer.ReserveAndResize(BlockSize * blocksCount);
        TGuardedBuffer holder(std::move(buffer));
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetStartIndex(startIndex);
        request->SetBlocksCount(blocksCount);
        request->SetCheckpointId(CheckpointId);
        request->BlockSize = BlockSize;
        request->Sglist = holder.GetGuardedSgList();

        auto future = SrcSession->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        future.Subscribe([=, this, holder = std::move(holder)](const auto& future) mutable {
            const auto& response = future.GetValue();
            if (HasError(response)) {
                STORAGE_ERROR(FormatError(response.GetError()));
                ShouldContinue.ShouldStop(1);
                return;
            }
            Write[bucket].Subscribe([=, this, holder = std::move(holder)](const auto& future) mutable {
                Y_UNUSED(future);
                WriteBlocks(bucket, startIndex, blocksCount, holder.Extract());
                Queue.Push(bucket);
            });
        });
    }

    void WriteBlocks(ui32 bucket, ui64 startIndex, ui32 blocksCount, TString buffer)
    {
        STORAGE_DEBUG("Writing blocks " << startIndex << "-" << startIndex + blocksCount);

        TGuardedBuffer holder(std::move(buffer));
        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request->SetStartIndex(startIndex);
        request->BlocksCount = blocksCount;
        request->BlockSize = BlockSize;
        request->Sglist = holder.GetGuardedSgList();

        Write[bucket] = DstSession->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        Write[bucket].Subscribe([=, this, holder = std::move(holder)](const auto& future) mutable {
            holder.Extract();

            const auto& response = future.GetValue();
            if (HasError(response)) {
                STORAGE_ERROR(FormatError(response.GetError()));
                ShouldContinue.ShouldStop(1);
                return;
            }
            if (AtomicSub(BlocksLeft, blocksCount) == 0) {
                ShouldContinue.ShouldStop(0);
            }
        });
    }

    bool CopyBlocks()
    {
        StartIndex = 0;
        BlocksLeft = BlocksCount;

        for (ui32 bucket = 0; bucket < IoDepth; bucket++) {
            Write.emplace_back(NThreading::MakeFuture(NProto::TWriteBlocksLocalResponse()));
            ReadBlocks(bucket);
        }

        while (ShouldContinue.PollState() != TProgramShouldContinue::Stop) {
            if (auto bucket = Queue.Pop(WaitTimeout)) {
                ReadBlocks(*bucket);
            }
        }

        if (ShouldContinue.GetReturnCode()) {
            STORAGE_ERROR("Command has been cancelled");
            return false;
        }

        return true;
    }

    bool CheckOpts() const
    {
        bool result = true;

        if (IoDepth < 1) {
            STORAGE_ERROR("--io-depth should be positive");
            result = false;
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBackupVolumeCommand final
    : public TCopyVolumeCommand
{
public:
    TBackupVolumeCommand(IBlockStorePtr client)
        : TCopyVolumeCommand(std::move(client))
    {
        Opts.AddLongOption("checkpoint-id", "checkpoint identifier")
            .RequiredArgument("STR")
            .StoreResult(&CheckpointId);

        Opts.AddLongOption("disk-id", "volume identifier")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("backup-disk-id", "backup volume identifier")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&BackupDiskId);

        Opts.AddLongOption("storage-media-kind", "backup volume media kind")
            .RequiredArgument("STR")
            .StoreResult(&StorageMediaKindArg);

        Opts.AddLongOption("token", "mount token")
            .RequiredArgument("STR")
            .StoreResult(&MountToken);

        Opts.AddLongOption("mount-local")
            .Help("mount the volume locally")
            .NoArgument()
            .SetFlag(&MountLocal);

        Opts.AddLongOption("io-depth", "number of I/O units to keep in flight")
            .RequiredArgument("NUM")
            .StoreResult(&IoDepth);

        Opts.AddLongOption("changed-blocks-count", "GetChangedBlocks request blocks count")
            .RequiredArgument("NUM")
            .StoreResult(&ChangedBlocksCount);

        Opts.AddLongOption("tablet-version", "Tablet version")
            .RequiredArgument("NUM")
            .StoreResult(&TabletVersion);
    }

protected:
    bool DoExecute() override
    {
        STORAGE_DEBUG("Backup volume");

        if (!CheckOpts()) {
            return false;
        }

        if (StorageMediaKindArg) {
            ParseStorageMediaKind(
                *ParseResultPtr,
                StorageMediaKindArg,
                BackupDiskStorageMediaKind
            );
        }

        if (!DescribeVolume()) {
            return false;
        }

        Y_DEFER {
            Cleanup();
        };

        if (!CreateVolume()) {
            return false;
        }

        if (!CreateCheckpoint()) {
            return false;
        }

        if (!MountVolumes(DiskId, BackupDiskId)) {
            return false;
        }

        if (!CopyBlocks()) {
            return false;
        }

        return true;
    }

private:
    void Cleanup()
    {
        // We don't want to leave any junk laying around. Reset state and wait
        // for cleanup operations to complete. If this gets cancelled, continue
        // without waiting

        STORAGE_INFO("Cleaning up");

        auto code = ShouldContinue.GetReturnCode();
        ShouldContinue.Reset();

        UnmountVolumes();
        DeleteCheckpoint();

        if (code) {
            DestroyBackup();
        }

        ShouldContinue.ShouldStop(code);
    }

    bool CheckOpts() const
    {
        bool result = TCopyVolumeCommand::CheckOpts();

        auto* version = ParseResultPtr->FindLongOptParseResult("tablet-version");

        if (version && TabletVersion != 1 && TabletVersion != 2) {
            STORAGE_ERROR("Tablet version should be either 1 or 2");
            result = false;
        }

        return result;
    }

    bool CreateVolume()
    {
        STORAGE_DEBUG("Creating backup volume");

        auto request = std::make_shared<NProto::TCreateVolumeRequest>();
        request->SetDiskId(BackupDiskId);
        request->SetBlockSize(BlockSize);
        request->SetBlocksCount(BlocksCount);
        request->SetStorageMediaKind(BackupDiskStorageMediaKind);
        request->MutableEncryptionSpec()->CopyFrom(EncryptionSpec);

        auto requestId = GetRequestId(*request);
        auto response = WaitFor(ClientEndpoint->CreateVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        if (HasError(response)) {
            return Fail(response.GetError());
        }

        return true;
    }

    bool CreateCheckpoint()
    {
        if (IsDiskRegistryMediaKind(MainDiskStorageMediaKind)) {
            STORAGE_WARN("Skip checkpoint");
            return true;
        }

        STORAGE_DEBUG("Creating checkpoint");

        if (!CheckpointId) {
            STORAGE_ERROR("Empty checkpoint-id");
            return false;
        }

        auto request = std::make_shared<NProto::TCreateCheckpointRequest>();
        request->SetDiskId(DiskId);
        request->SetCheckpointId(CheckpointId);

        auto requestId = GetRequestId(*request);
        auto response = WaitFor(ClientEndpoint->CreateCheckpoint(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        if (HasError(response)) {
            return Fail(response.GetError());
        }

        return true;
    }

    bool DestroyBackup()
    {
        STORAGE_DEBUG("Deleting unfinished backup");

        auto request = std::make_shared<NProto::TDestroyVolumeRequest>();
        request->SetDiskId(BackupDiskId);

        auto requestId = GetRequestId(*request);
        auto response = WaitFor(ClientEndpoint->DestroyVolume(
            MakeIntrusive<TCallContext>(requestId), std::move(request)));

        if (HasError(response)) {
            return Fail(response.GetError());
        }

        return true;
    }

    bool DeleteCheckpoint()
    {
        if (CheckpointId.empty()) {
            return true;
        }

        STORAGE_DEBUG("Deleting checkpoint");

        auto request = std::make_shared<NProto::TDeleteCheckpointRequest>();
        request->SetDiskId(DiskId);
        request->SetCheckpointId(CheckpointId);

        auto requestId = GetRequestId(*request);
        auto response = WaitFor(ClientEndpoint->DeleteCheckpoint(
            MakeIntrusive<TCallContext>(requestId), std::move(request)));

        if (HasError(response)) {
            return Fail(response.GetError());
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRestoreVolumeCommand final
    : public TCopyVolumeCommand
{
public:
    TRestoreVolumeCommand(IBlockStorePtr client)
        : TCopyVolumeCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("backup-disk-id", "backup volume identifier")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&BackupDiskId);

        Opts.AddLongOption("token", "mount token")
            .RequiredArgument("STR")
            .StoreResult(&MountToken);

        Opts.AddLongOption("mount-local")
            .Help("mount the volume locally")
            .NoArgument()
            .SetFlag(&MountLocal);

        Opts.AddLongOption("io-depth", "number of I/O units to keep in flight")
            .RequiredArgument("NUM")
            .StoreResult(&IoDepth);

        Opts.AddLongOption("changed-blocks-count", "GetChangedBlocks request blocks count")
            .RequiredArgument("NUM")
            .StoreResult(&ChangedBlocksCount);
    }

protected:
    bool DoExecute() override
    {
        STORAGE_DEBUG("Restore volume");

        if (!CheckOpts()) {
            return false;
        }

        if (!DescribeVolume()) {
            return false;
        }

        if (!ValidateBackupVolume()) {
            return false;
        }

        if (!MountVolumes(BackupDiskId, DiskId)) {
            return false;
        }

        Y_DEFER {
            Cleanup();
        };

        if (!CopyBlocks()) {
            return false;
        }

        return true;
    }

private:
    void Cleanup()
    {
        STORAGE_INFO("Cleaning up");

        auto code = ShouldContinue.GetReturnCode();
        ShouldContinue.Reset();

        UnmountVolumes();

        ShouldContinue.ShouldStop(code);
    }

    bool ValidateBackupVolume()
    {
        STORAGE_DEBUG("Validating backup volume");

        auto request = std::make_shared<NProto::TDescribeVolumeRequest>();
        request->SetDiskId(BackupDiskId);

        auto requestId = GetRequestId(*request);
        auto response = WaitFor(ClientEndpoint->DescribeVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        if (HasError(response)) {
            return Fail(response.GetError());
        }

        bool result = true;

        if (auto bs = response.GetVolume().GetBlockSize(); bs != BlockSize) {
            STORAGE_ERROR("Unexpected block size: " << bs << " != " << BlockSize);
            result = false;
        }

        if (auto bc = response.GetVolume().GetBlocksCount(); bc != BlocksCount) {
            STORAGE_ERROR("Unexpected block count: " << bc << " != " << BlocksCount);
            result = false;
        }

        return result;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewBackupVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TBackupVolumeCommand>(std::move(client));
}

TCommandPtr NewRestoreVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TRestoreVolumeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
