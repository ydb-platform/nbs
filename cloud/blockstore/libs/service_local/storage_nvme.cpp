#include "storage_nvme.h"

#include <cloud/blockstore/libs/nvme/spec.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/io_uring/context.h>

#include <library/cpp/regex/pcre/regexp.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/file.h>
#include <util/system/fs.h>

#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>

#include <regex>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace NCloud::NBlockStore::NNvme;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 SectorSizeBytes = 512;

////////////////////////////////////////////////////////////////////////////////

struct TFree
{
    void operator () (void* ptr) const
    {
        std::free(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

nvme_ns_data NVMeIdentifyNs(TFileHandle& device, ui32 nsId)
{
    nvme_ns_data ns = {};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .nsid = nsId,
        .addr = static_cast<ui64>(std::bit_cast<uintptr_t>(&ns)),
        .data_len = sizeof(ns),
        .cdw10 = NVME_IDENTIFY_NS
    };

    int err = ioctl(device, NVME_IOCTL_ADMIN_CMD, &cmd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeIdentifyNs failed: " << strerror(err);
    }

    return ns;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
struct TCompletion
    : TFileIOCompletion
{
    TPromise<TResponse> Promise;
    TGuardedSgList::TGuard Guard;

    explicit TCompletion(TGuardedSgList::TGuard guard)
        : TFileIOCompletion{.Func = &TCompletion::HandleCompletion}
        , Promise(NewPromise<TResponse>())
        , Guard(std::move(guard))
    {}

    static void HandleCompletion(
        TFileIOCompletion* obj,
        const NProto::TError& error,
        ui32 bytesTransferred)
    {
        Y_UNUSED(bytesTransferred);

        std::unique_ptr<TCompletion> self(static_cast<TCompletion*>(obj));

        TResponse response;
        response.MutableError()->CopyFrom(error);
        self->Promise.SetValue(std::move(response));
    }
};

size_t SgListCopyUnsafe(const char* src, const TSgList& dstList)
{
    size_t bytesCount = 0;
    for (TBlockDataRef buf: dstList) {
        memcpy(const_cast<char*>(buf.Data()), src, buf.Size());
        bytesCount += buf.Size();
        src += buf.Size();
    }

    return bytesCount;
}

struct TReadCompletion
    : TFileIOCompletion
{
    using TResponse = NProto::TReadBlocksLocalResponse;

    TPromise<TResponse> Promise;
    TGuardedSgList::TGuard Guard;
    std::unique_ptr<char, TFree> BounceBuffer;

    explicit TReadCompletion(TGuardedSgList::TGuard guard)
        : TFileIOCompletion{.Func = &TReadCompletion::HandleCompletionFunc}
        , Promise(NewPromise<TResponse>())
        , Guard(std::move(guard))
    {}

    static void HandleCompletionFunc(
        TFileIOCompletion* obj,
        const NProto::TError& error,
        ui32 bytesTransferred)
    {
        Y_UNUSED(bytesTransferred);

        std::unique_ptr<TReadCompletion> self(
            static_cast<TReadCompletion*>(obj));

        self->HandleCompletion(error);
    }

    void HandleCompletion(const NProto::TError& error)
    {
        TResponse response;
        response.MutableError()->CopyFrom(error);
        if (!HasError(error) && BounceBuffer) {
            SgListCopyUnsafe(BounceBuffer.get(), Guard.Get());
        }

        Promise.SetValue(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNvmePartLayout
{
    ui64 Offset = 0;
    ui64 Size = 0;
};

struct TNvmePartIds
{
    ui32 CtrlId = 0;
    ui32 NsId = 0;
    ui32 PartId = 0;
};

struct TNvmePart
{
    TFileHandle Fd;
    ui32 LbaShift = 0;
    TNvmePartIds Ids;
    TNvmePartLayout Layout;
};

TNvmePartIds GetNvmePartIds(TFsPath path)
{
    path = path.RealLocation();

    const std::regex re(R"(^/dev/nvme(\d+)n(\d+)p(\d+)$)");
    std::smatch m;

    Y_ENSURE(std::regex_match(path.GetPath().c_str(), m, re) && m.size() == 4);

    const ui32 ctrlId = FromString<ui32>(m[1]);
    const ui32 nsId = FromString<ui32>(m[2]);
    const ui32 partId = FromString<ui32>(m[3]);

    Y_ENSURE(nsId && partId);

    return {.CtrlId = ctrlId, .NsId = nsId, .PartId = partId};
}

ui64 ReadUint64FromFile(const TFsPath& path)
{
    const auto data = Strip(TFileInput(path).ReadAll());
    Y_ENSURE(!data.empty(), "empty file: " << path);

    return FromString<ui64>(data);
}

TNvmePartLayout GetPartLayout(const TNvmePartIds& ids)
{
    const TFsPath path = TStringBuilder()
                         << "/sys/block/nvme" << ids.CtrlId << "n" << ids.NsId
                         << "/nvme" << ids.CtrlId << "n" << ids.NsId << "p"
                         << ids.PartId;

    return {
        .Offset = ReadUint64FromFile(path / "start") * SectorSizeBytes,
        .Size = ReadUint64FromFile(path / "size") * SectorSizeBytes,
    };
}

TResultOrError<TNvmePart> CreateNvmePartition(const TFsPath& path)
{
    try {
        const TNvmePartIds ids = GetNvmePartIds(path);

        const TString ngPath = TStringBuilder()
                               << "/dev/ng" << ids.CtrlId << "n" << ids.NsId;

        TFileHandle fd{ngPath, EOpenModeFlag::OpenExisting};
        if (!fd.IsOpen()) {
            int ec = errno;
            return MakeError(
                MAKE_SYSTEM_ERROR(ec),
                TStringBuilder() << "Unable to open " << ngPath.Quote());
        }

        nvme_ns_data ns = NVMeIdentifyNs(fd, ids.NsId);
        if (ns.lbaf[ns.flbas.format].ms) {
            return MakeError(E_ARGUMENT, "invalid metadata size");
        }

        const ui32 lbaSize = 1 << ns.lbaf[ns.flbas.format].lbads;
        if (lbaSize < SectorSizeBytes) {
            return MakeError(E_ARGUMENT, "invalid LBA size");
        }

        return TNvmePart{
            .Fd = std::move(fd),
            .LbaShift = static_cast<ui32>(std::bit_width(lbaSize) - 1),
            .Ids = ids,
            .Layout = GetPartLayout(ids),
        };
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }
}

struct TNvmePartIO
{
    TNvmePart Part;
    NIoUring::TContext Context;

    TNvmePartIO(TNvmePart part, NIoUring::TContext::TParams params)
        : Part(std::move(part))
        , Context(std::move(params))
    {}
};

using TNvmePartIOPtr = std::shared_ptr<TNvmePartIO>;

////////////////////////////////////////////////////////////////////////////////

bool IsProperlyAligned(ui32 lbaSize, const TSgList& sglist)
{
    return AllOf(
        sglist,
        [mask = lbaSize - 1](TBlockDataRef buf)
        {
            return std::bit_cast<ui64>(buf.Data()) & mask == 0 &&
                   buf.Size() & mask == 0;
        });
}

TArrayRef<const TArrayRef<char>> AsArrayRef(const TSgList& sglist)
{
    static_assert(sizeof(TSgList::value_type) == sizeof(TArrayRef<char>));

    return {
        std::bit_cast<const TArrayRef<char>*>(sglist.data()),
        sglist.size()};
}

////////////////////////////////////////////////////////////////////////////////

// name0n1: [ ( |......|  |......|  |......| )  ( ... ) ]
//            |              |                  |
//   nvme0n1p1               |                nvme0n1p2
//                        storage

class TNvmeStorage
    : public IStorage
{
private:
    TNvmePartIOPtr NvmePartIO;
    const ui64 StartBytes;
    const ui64 EndBytes;
    const ui32 BlockSize;
    const bool ReadOnly;

public:
    TNvmeStorage(
            TNvmePartIOPtr nvmeIO,
            ui64 offset,
            ui64 size,
            ui32 blockSize,
            bool readOnly)
        : NvmePartIO(std::move(nvmeIO))
        , StartBytes(offset + nvmeIO->Part.Layout.Offset)
        , EndBytes(StartBytes + size)
        , BlockSize(blockSize)
        , ReadOnly(readOnly)
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) final
    {
        Y_UNUSED(callContext, request);

        NProto::TZeroBlocksResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);

        // TODO

        Y_UNUSED(ReadOnly);

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) final
    {
        Y_UNUSED(callContext);

        auto guard = request->Sglist.Acquire();
        if (!guard) {
            NProto::TReadBlocksLocalResponse response;
            *response.MutableError() =
                MakeError(E_CANCELLED, "failed to acquire sglist");
            return MakeFuture(std::move(response));
        }

        const ui64 offset = StartBytes + (request->GetStartIndex() * BlockSize);
        const ui64 size = SgListGetSize(guard.Get());

        if (offset + size >= EndBytes) {
            NProto::TReadBlocksLocalResponse response;
            *response.MutableError() = MakeError(E_ARGUMENT);
            return MakeFuture(std::move(response));
        }

        auto completion = std::make_unique<TReadCompletion>(std::move(guard));

        const ui32 lbaSize = 1U << NvmePartIO->Part.LbaShift;

        if (IsProperlyAligned(lbaSize, completion->Guard.Get())) {
            NvmePartIO->Context.AsyncNvmeReadV(
                NvmePartIO->Part.Fd,
                NvmePartIO->Part.Ids.NsId,
                NvmePartIO->Part.LbaShift,
                AsArrayRef(completion->Guard.Get()),
                offset,
                completion.get());
        } else {
            completion->BounceBuffer.reset(
                static_cast<char*>(std::aligned_alloc(lbaSize, size)));

            NvmePartIO->Context.AsyncNvmeRead(
                NvmePartIO->Part.Fd,
                NvmePartIO->Part.Ids.NsId,
                NvmePartIO->Part.LbaShift,
                {completion->BounceBuffer.get(), size},
                offset,
                completion.get());
        }

        auto future = completion->Promise.GetFuture();

        auto* ptr = completion.release();
        Y_UNUSED(ptr);

        return future;
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) final
    {
        Y_UNUSED(callContext, request);

        NProto::TWriteBlocksLocalResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);

        // TODO

        Y_UNUSED(ReadOnly);

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TError> EraseDevice(NProto::EDeviceEraseMethod method) final
    {
        Y_UNUSED(method);

        // TODO

        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) final
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() final
    {
        // TODO
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNvmeStorageProvider
    : public IStorageProvider
{
private:
    THashMap<TString, TNvmePartIOPtr> NvmePartIOs;
    IStorageProviderPtr Fallback;

    ui32 Index = 0;

public:
    explicit TNvmeStorageProvider(IStorageProviderPtr fallback)
        : Fallback(std::move(fallback))
    {}

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) final
    {
        const TString& filePath = volume.GetDiskId();

        TNvmePartIOPtr nvmePartIO;

        if (auto it = NvmePartIOs.find(filePath); it != NvmePartIOs.end()) {
            nvmePartIO = it->second;
        }

        if (!nvmePartIO) {
            auto [part, error] = CreateNvmePartition(TFsPath{filePath});
            if (HasError(error)) {
                return Fallback->CreateStorage(volume, clientId, accessMode);
            }

            const ui32 index = Index++;

            NIoUring::TContext::TParams params {
                .SubmissionThreadName = TStringBuilder() << "NV.SQ" << index,
                .CompletionThreadName = TStringBuilder() << "NV.CQ" << index,
                .EnableNvmePassthrough = true,
            };

            nvmePartIO = std::make_shared<TNvmePartIO>(
                std::move(part),
                std::move(params));

            nvmePartIO->Context.Start();
            NvmePartIOs.emplace(filePath, nvmePartIO);
        }

        const ui32 blockSize =
            volume.GetBlockSize() ? volume.GetBlockSize() : DefaultBlockSize;

        return MakeFuture<IStoragePtr>(std::make_shared<TNvmeStorage>(
            nvmePartIO,
            volume.GetStartIndex() * blockSize,
            volume.GetBlocksCount() * blockSize,
            blockSize,
            !IsReadWriteMode(accessMode)));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateNvmeStorageProvider(IStorageProviderPtr fallback)
{
    if (!NIoUring::IsNvmePassthroughSupported()) {
        return fallback;
    }

    return std::make_shared<TNvmeStorageProvider>(std::move(fallback));
}

}   // namespace NCloud::NBlockStore::NServer
