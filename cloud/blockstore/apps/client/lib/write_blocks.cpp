#include "write_blocks.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/encryption/model/utils.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TWriteRange
{
    ui64 StartIndex;
    ui32 BlocksCount;
    TGuardedSgList SgList;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteRangeIterator
{
    virtual ~IWriteRangeIterator() = default;

    virtual TMaybe<TWriteRange> Next() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSingleWriteRangeIterator final
    : public IWriteRangeIterator
{
private:
    TLog& Log;
    const ui64 StartIndex;
    const ui32 BlockSize;
    const TGuardedBuffer<NProto::TWriteBlocksRequest> BlocksHolder;

    bool Completed = false;

public:
    TSingleWriteRangeIterator(
            TLog& log,
            ui64 startIndex,
            ui32 blockSize,
            TGuardedBuffer<NProto::TWriteBlocksRequest> blocksHolder)
        : Log(log)
        , StartIndex(startIndex)
        , BlockSize(blockSize)
        , BlocksHolder(std::move(blocksHolder))
    {}

    TMaybe<TWriteRange> Next() override
    {
        if (!Completed) {
            Completed = true;

            auto sgListOrError = SgListNormalize(
                GetSgList(BlocksHolder.Get()),
                BlockSize);

            if (HasError(sgListOrError)) {
                STORAGE_ERROR("Failed to get sglist from buffer: "
                    << FormatError(sgListOrError.GetError()));
                return Nothing();
            }

            auto sglist = sgListOrError.ExtractResult();
            auto blocksCount = static_cast<ui32>(sglist.size());

            return TWriteRange {
                StartIndex,
                blocksCount,
                BlocksHolder.CreateGuardedSgList(std::move(sglist))
            };
        }

        return Nothing();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMultiWriteRangeIterator final
    : public IWriteRangeIterator
{
private:
    ui64 StartIndex;
    IInputStream& Input;
    const ui32 BlockSize;
    const ui32 BatchBlocksCount;
    TGuardedBuffer<TString> BufferHolder;

public:
    TMultiWriteRangeIterator(
            ui64 startIndex,
            IInputStream& input,
            ui32 blockSize,
            ui32 batchBlocksCount)
        : StartIndex(startIndex)
        , Input(input)
        , BlockSize(blockSize)
        , BatchBlocksCount(batchBlocksCount)
    {}

    TMaybe<TWriteRange> Next() override
    {
        TString buffer;
        buffer.ReserveAndResize(BlockSize * BatchBlocksCount);

        auto bytesRead = Input.Load((void*)buffer.data(), buffer.size());
        if (!bytesRead) {
            return Nothing();
        }

        Y_ENSURE(bytesRead % BlockSize == 0);

        BufferHolder = TGuardedBuffer(std::move(buffer));
        TSgList sglist = {{ BufferHolder.Get().data(), bytesRead }};

        auto writeRange = TWriteRange {
            StartIndex,
            static_cast<ui32>(bytesRead / BlockSize),
            BufferHolder.CreateGuardedSgList(std::move(sglist))
        };

        StartIndex += writeRange.BlocksCount;
        return writeRange;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IResponseHandler
{
    virtual ~IResponseHandler() = default;

    virtual void HandleResponse(const NProto::TError& error) = 0;
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

    void HandleResponse(const NProto::TError& error) override
    {
        NProto::TWriteBlocksResponse response;
        response.MutableError()->CopyFrom(error);

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

    void HandleResponse(const NProto::TError& error) override
    {
        if (FAILED(error.GetCode())) {
            STORAGE_ERROR("WriteBlocks failed: " << FormatError(error));
        } else {
            Out << "OK" << Endl;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBlocksCommand final
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
    ui64 StartIndex = 0;

    ISessionPtr Session;
    NProto::TVolume Volume;

public:
    TWriteBlocksCommand(IBlockStorePtr client)
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

        Opts.AddLongOption("start-index", "start block index")
            .RequiredArgument("NUM")
            .StoreResult(&StartIndex);
    }

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        NProto::TWriteBlocksRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);

            DiskId = request.GetDiskId();
            StartIndex = request.GetStartIndex();
        }

        auto encryptionSpec = CreateEncryptionSpec(
            EncryptionMode,
            EncryptionKeyPath,
            EncryptionKeyHash);

        auto mountResponse = MountVolume(
            DiskId,
            MountToken,
            Session,
            NProto::VOLUME_ACCESS_READ_WRITE,
            MountLocal,
            ThrottlingDisabled,
            encryptionSpec
        );
        if (HasError(mountResponse)) {
            return false;
        }

        Volume = mountResponse.GetVolume();

        std::unique_ptr<IWriteRangeIterator> rangeIter;
        std::unique_ptr<IResponseHandler> responseHandler;

        if (Proto) {
            TGuardedBuffer blocksHolder(std::move(request));

            rangeIter = std::make_unique<TSingleWriteRangeIterator>(
                Log,
                StartIndex,
                Volume.GetBlockSize(),
                std::move(blocksHolder));

            responseHandler = std::make_unique<TProtoResponseHandler>(
                GetOutputStream());
        } else {
            rangeIter = std::make_unique<TMultiWriteRangeIterator>(
                StartIndex,
                GetInputStream(),
                Volume.GetBlockSize(),
                BatchBlocksCount);

            responseHandler = std::make_unique<TRawResponseHandler>(
                Log,
                GetOutputStream());
        }

        bool result = WriteBlocks(*rangeIter, *responseHandler);
        UnmountVolume(*Session);
        return result;
    }

private:
    bool CheckOpts() const
    {
        if (!DiskId && !Proto) {
            STORAGE_ERROR("--disk-id option is required");
            return false;
        }

        if (Proto && StartIndex) {
            STORAGE_ERROR("--proto option cannot be used along with --start-index option");
            return false;
        }

        return true;
    }

    bool WriteBlocks(
        IWriteRangeIterator& rangeIter,
        IResponseHandler& responseHandler)
    {
        for (;;) {
            auto writeRange = rangeIter.Next();
            if (!writeRange) {
                return true;
            }

            auto error = SafeExecute<NProto::TError>([&] {
                auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
                request->SetStartIndex(writeRange->StartIndex);
                request->BlocksCount = writeRange->BlocksCount;
                request->BlockSize = Volume.GetBlockSize();
                request->Sglist = writeRange->SgList;
                PrepareHeaders(*request->MutableHeaders());

                auto future = Session->WriteBlocksLocal(
                    MakeIntrusive<TCallContext>(),
                    std::move(request));
                auto response = WaitFor(std::move(future));
                return response.GetError();
            });

            responseHandler.HandleResponse(error);

            if (FAILED(error.GetCode())) {
                return false;
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewWriteBlocksCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TWriteBlocksCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
