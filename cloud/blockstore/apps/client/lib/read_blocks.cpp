#include "read_blocks.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/encryption/model/utils.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

// XXX bad dep
#include <cloud/blockstore/libs/storage/api/public.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/set.h>
#include <util/string/escape.h>
#include <util/system/mutex.h>
#include <util/thread/lfstack.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TReadRange
{
    ui64 StartIndex;
    ui32 BlocksCount;
};

////////////////////////////////////////////////////////////////////////////////

struct IReadRangeIterator
{
    virtual ~IReadRangeIterator() = default;

    virtual TMaybe<TReadRange> Next() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSingleReadRangeIterator final
    : public IReadRangeIterator
{
private:
    const ui64 StartIndex;
    const ui32 BlocksCount;

    bool Completed = false;

public:
    TSingleReadRangeIterator(ui64 startIndex, ui32 blocksCount)
        : StartIndex(startIndex)
        , BlocksCount(blocksCount)
    {}

    TMaybe<TReadRange> Next() override
    {
        if (!Completed) {
            Completed = true;
            return TReadRange { StartIndex, BlocksCount };
        }

        return Nothing();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMultiReadRangeIterator final
    : public IReadRangeIterator
{
private:
    ui64 StartIndex;
    const ui64 EndIndex;
    const ui32 BatchBlocksCount;

public:
    TMultiReadRangeIterator(
            ui64 startIndex,
            ui64 endIndex,
            ui32 batchBlocksCount)
        : StartIndex(startIndex)
        , EndIndex(endIndex)
        , BatchBlocksCount(batchBlocksCount)
    {}

    TMaybe<TReadRange> Next() override
    {
        if (StartIndex < EndIndex) {
            auto readRange = TReadRange {
                StartIndex,
                Min<ui32>(EndIndex - StartIndex, BatchBlocksCount)
            };

            StartIndex += readRange.BlocksCount;
            return readRange;
        }

        return Nothing();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IResponseHandler
{
    virtual ~IResponseHandler() = default;

    virtual void HandleResponse(
        const NProto::TError& error,
        const TString& buffer) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TProtoResponseHandler final
    : public IResponseHandler
{
private:
    IOutputStream& Out;

public:
    TProtoResponseHandler(IOutputStream& out)
        : Out(out)
    {}

    void HandleResponse(
        const NProto::TError& error,
        const TString& buffer) override
    {
        NProto::TReadBlocksResponse response;
        response.MutableError()->CopyFrom(error);

        if (SUCCEEDED(error.GetCode())) {
            auto& blocks = *response.MutableBlocks();
            blocks.AddBuffers(buffer);
        }

        SerializeToTextFormat(response, Out);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRawResponseHandler final
    : public IResponseHandler
{
private:
    TLog& Log;
    IOutputStream& Out;

public:
    TRawResponseHandler(TLog& log, IOutputStream& out)
        : Log(log)
        , Out(out)
    {}

    void HandleResponse(
        const NProto::TError& error,
        const TString& buffer) override
    {
        if (SUCCEEDED(error.GetCode())) {
            Out << buffer;
        } else {
            STORAGE_ERROR("ReadBlocks failed: " << FormatError(error));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TReadResponse
{
    ui32 BatchId = 0;
    TString Buffer;
    NProto::TError Error;

    bool operator<(const TReadResponse& response) const {
        return BatchId < response.BatchId;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksCommand final
    : public TCommand
{
private:
    TString DiskId;
    TString MountToken;
    bool ThrottlingDisabled = false;
    bool MountLocal = false;
    NProto::EEncryptionMode EncryptionMode = NProto::NO_ENCRYPTION;
    TString EncryptionKeyPath;
    TString EncryptionKeyHash;
    TString CheckpointId;
    ui64 StartIndex = 0;
    ui64 BlocksCount = 0;
    ui32 IODepth = 0;
    bool ReadAll = false;
    bool Repair = false;
    ui32 ReplicaIndex = 0;

    TLockFreeStack<TReadResponse> ReadyBatches;
    TAutoEvent Ready;
    TMutex OutputError;

    ISessionPtr Session;
    NProto::TVolume Volume;

public:
    TReadBlocksCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("token", "Mount token")
            .RequiredArgument("STR")
            .StoreResult(&MountToken);

        Opts.AddLongOption("throttling-disabled")
            .Help("Use ThrottlingDisabled mount flag")
            .NoArgument()
            .SetFlag(&ThrottlingDisabled);

        Opts.AddLongOption("mount-local")
            .Help("mount the volume locally")
            .NoArgument()
            .SetFlag(&MountLocal);

        Opts.AddLongOption("encryption-mode", "encryption mode [no|aes-xts|test]")
            .RequiredArgument("STR")
            .Handler1T<TString>([this] (const auto& s) {
                EncryptionMode = EncryptionModeFromString(s);
            });

        Opts.AddLongOption("encryption-key-path", "path to file with encryption key")
            .RequiredArgument("STR")
            .StoreResult(&EncryptionKeyPath);

        Opts.AddLongOption("encryption-key-hash", "key hash for snapshot mode")
            .RequiredArgument("STR")
            .StoreResult(&EncryptionKeyHash);

        Opts.AddLongOption("checkpoint-id", "Checkpoint identifier")
            .RequiredArgument("STR")
            .StoreResult(&CheckpointId);

        Opts.AddLongOption("start-index", "start block index")
            .RequiredArgument("NUM")
            .StoreResult(&StartIndex);

        Opts.AddLongOption("blocks-count", "maximum number of blocks stored in volume")
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("read-all", "Read all data from volume")
            .NoArgument()
            .SetFlag(&ReadAll);

        Opts.AddLongOption("repair", "Repair blocks (fill lost blocks with a specific marker)")
            .NoArgument()
            .SetFlag(&Repair);

        Opts.AddLongOption("io-depth", "number of reads to keep in flight")
            .RequiredArgument("NUM")
            .StoreResult(&IODepth)
            .DefaultValue(1);

        Opts.AddLongOption(
                "replica-index",
                "from which replica(numerate from 1) read data, only for "
                "mirror* disks")
            .RequiredArgument("NUM")
            .StoreResult(&ReplicaIndex);
    }

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        if (Proto) {
            NProto::TReadBlocksRequest request;
            ParseFromTextFormat(GetInputStream(), request);

            DiskId = request.GetDiskId();
            CheckpointId = request.GetCheckpointId();
            StartIndex = request.GetStartIndex();
            BlocksCount = request.GetBlocksCount();
        }

        auto encryptionSpec = CreateEncryptionSpec(
            EncryptionMode,
            EncryptionKeyPath,
            EncryptionKeyHash);

        auto mountResponse = MountVolume(
            DiskId,
            MountToken,
            Session,
            Repair ? NProto::VOLUME_ACCESS_REPAIR : NProto::VOLUME_ACCESS_READ_ONLY,
            MountLocal,
            ThrottlingDisabled,
            encryptionSpec
        );
        if (HasError(mountResponse)) {
            return false;
        }

        Volume = mountResponse.GetVolume();

        if (ReadAll) {
            StartIndex = 0;
            BlocksCount = Volume.GetBlocksCount();
        }

        std::unique_ptr<IReadRangeIterator> rangeIter;
        std::unique_ptr<IResponseHandler> responseHandler;
        ui32 batchCount = 0;

        if (Proto) {
            batchCount = 1;
            rangeIter = std::make_unique<TSingleReadRangeIterator>(
                StartIndex,
                BlocksCount);

            responseHandler = std::make_unique<TProtoResponseHandler>(
                GetOutputStream());
        } else {
            batchCount = (BlocksCount + BatchBlocksCount - 1) / BatchBlocksCount;
            rangeIter = std::make_unique<TMultiReadRangeIterator>(
                StartIndex,
                StartIndex + BlocksCount,
                BatchBlocksCount);

            responseHandler = std::make_unique<TRawResponseHandler>(
                Log,
                GetOutputStream());
        }

        ui32 readBatchId = 0;
        ui32 currentInflight = 0;
        ui32 writeBatchId = 0;
        TVector<TReadResponse> buf;
        TSet<TReadResponse> batches;
        bool result = true;
        while (result && writeBatchId < batchCount &&
            ShouldContinue.PollState() != TProgramShouldContinue::Stop)
        {
            if (currentInflight != 0) {
                ui32 readyCount = batches.size();
                ReadyBatches.DequeueAllSingleConsumer(&buf);
                batches.insert(buf.begin(), buf.end());
                buf.clear();
                ui32 requestsDone = batches.size() - readyCount;
                currentInflight -= requestsDone;
            }
            if (batches.size() > 2 * IODepth) {
                STORAGE_WARN("One of requests hangs");
            } else {
                while (result && readBatchId < batchCount && currentInflight < IODepth) {
                    result = ReadBatch(*rangeIter, readBatchId);
                    ++readBatchId;
                    ++currentInflight;
                }
            }
            if (!result) {
                break;
            }
            if (batches.empty() || writeBatchId != batches.begin()->BatchId) {
                Ready.WaitI();
                continue;
            }
            while (!batches.empty() && writeBatchId == batches.begin()->BatchId) {
                result = OutputBatch(*batches.begin(), *responseHandler);
                batches.erase(batches.begin());
                ++writeBatchId;
            }
        }

        UnmountVolume(*Session);

        if (ShouldContinue.GetReturnCode()) {
            return false;
        }

        return result;
    }

private:
    bool CheckOpts() const
    {
        if (!DiskId && !Proto) {
            STORAGE_ERROR("--disk-id option is required");
            return false;
        }

        if (ReadAll && Proto) {
            STORAGE_ERROR("--read-all option cannot be used along with --proto option");
            return false;
        }

        if (ReadAll && (StartIndex || BlocksCount)) {
            STORAGE_ERROR("--read-all option cannot be used along with --start-index or --blocks-count options");
            return false;
        }

        if (Proto && (StartIndex || BlocksCount)) {
            STORAGE_ERROR("--proto option cannot be used along with --start-index or --blocks-count options");
            return false;
        }

        if (!Proto && !ReadAll && !BlocksCount) {
            STORAGE_ERROR("--blocks-count option is required");
            return false;
        }

        if (Proto && IODepth) {
            STORAGE_ERROR("--proto cannot be used along with --io-depth");
            return false;
        }

        return true;
    }

    bool OutputBatch(
        const TReadResponse& response,
        IResponseHandler& responseHandler)
    {
       responseHandler.HandleResponse(response.Error, response.Buffer);
       if (HasError(response.Error)) {
           return false;
       }
       return true;
    }


    bool ReadBatch(IReadRangeIterator& rangeIter, ui32 batchId)
    {
        auto error = SafeExecute<NProto::TError>([&] {
            auto readRange = rangeIter.Next();

            TString buffer;
            buffer.ReserveAndResize(
                Volume.GetBlockSize() * readRange->BlocksCount);
            TGuardedBuffer<TString> holder(std::move(buffer));

            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetStartIndex(readRange->StartIndex);
            request->SetBlocksCount(readRange->BlocksCount);
            request->SetCheckpointId(CheckpointId);
            request->BlockSize = Volume.GetBlockSize();
            request->Sglist = holder.GetGuardedSgList();
            PrepareHeaders(*request->MutableHeaders());
            request->MutableHeaders()->SetReplicaIndex(ReplicaIndex);

            auto future = Session->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            future.Subscribe([=, this, holder = std::move(holder)] (const auto& f) mutable {
                const auto& response = f.GetValue();
                auto buffer = holder.Extract();

                auto brokenDataMarker = NStorage::GetBrokenDataMarker();
                if (brokenDataMarker.size() > Volume.GetBlockSize()) {
                    brokenDataMarker = brokenDataMarker.SubString(
                        0,
                        Volume.GetBlockSize());
                }
                for (ui32 i = 0; i < buffer.size(); i += Volume.GetBlockSize()) {
                    TStringBuf view(buffer);
                    if (view.SubString(i, brokenDataMarker.size()) == brokenDataMarker) {
                        with_lock (OutputError) {
                            GetErrorStream()
                                << "NODATA@"
                                << (readRange->StartIndex + (i / Volume.GetBlockSize()))
                                << Endl;
                        }
                    }
                }
                ReadyBatches.Enqueue({
                    batchId,
                    std::move(buffer),
                    response.GetError()
                });
                Ready.Signal();
            });

            return NProto::TError();
        });

        if (HasError(error)) {
            STORAGE_ERROR(FormatError(error));
            return false;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadBlocksCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TReadBlocksCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
