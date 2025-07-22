#include "storage_nvme.h"

#include "safe_deallocator.h"

#include <cloud/blockstore/libs/nvme/spec.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/io_uring/context.h>

#include <library/cpp/regex/pcre/regexp.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
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
        .addr = std::bit_cast<ui64>(&ns),
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

size_t SgListCopyUnsafe(const char* src, const TSgList& dst)
{
    size_t bytesCount = 0;
    for (TBlockDataRef buf: dst) {
        memcpy(const_cast<char*>(buf.Data()), src, buf.Size());
        bytesCount += buf.Size();
        src += buf.Size();
    }

    return bytesCount;
}

size_t SgListCopyUnsafe(const TSgList& src, char* dst)
{
    size_t bytesCount = 0;
    for (TBlockDataRef buf: src) {
        memcpy(dst, const_cast<char*>(buf.Data()), buf.Size());
        bytesCount += buf.Size();
        dst += buf.Size();
    }

    return bytesCount;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived, typename TResponse>
struct TCompletion
    : TFileIOCompletion
{
    TCallContextPtr CallContext;
    TGuardedSgList SgList;
    TGuardedSgList::TGuard Guard;
    std::unique_ptr<char, TFree> BounceBuffer;

    TPromise<TResponse> Promise;

    TCompletion(
            TCallContextPtr callContext,
            TGuardedSgList sglist)
        : TFileIOCompletion{.Func = &TCompletion::HandleCompletionFunc}
        , CallContext(std::move(callContext))
        , SgList(std::move(sglist))
        , Guard(SgList.Acquire())
        , Promise(NewPromise<TResponse>())
    {}

    void Finish(ui32 errorCode, TString message)
    {
        static_cast<TDerived*>(this)->HandleCompletion(
            MakeError(errorCode, std::move(message)));
    }

    static void HandleCompletionFunc(
        TFileIOCompletion* obj,
        const NProto::TError& error,
        ui32 bytesTransferred)
    {
        Y_UNUSED(bytesTransferred);

        std::unique_ptr<TDerived> self(static_cast<TDerived*>(obj));
        self->HandleCompletion(error);
    }
};

struct TReadCompletion
    : TCompletion<TReadCompletion, NProto::TReadBlocksLocalResponse>
{
    using TCompletion::TCompletion;

    void HandleCompletion(const NProto::TError& error)
    {
        NProto::TReadBlocksLocalResponse response;
        response.MutableError()->CopyFrom(error);
        if (!HasError(error) && BounceBuffer) {
            SgListCopyUnsafe(BounceBuffer.get(), Guard.Get());
        }

        Promise.SetValue(std::move(response));
    }
};

struct TWriteCompletion
    : TCompletion<TWriteCompletion, NProto::TWriteBlocksLocalResponse>
{
    using TCompletion::TCompletion;

    void HandleCompletion(const NProto::TError& error)
    {
        NProto::TWriteBlocksLocalResponse response;
        response.MutableError()->CopyFrom(error);
        Promise.SetValue(std::move(response));
    }
};

struct TWriteZeroesCompletion
    : TCompletion<TWriteZeroesCompletion, NProto::TZeroBlocksResponse>
{
    explicit TWriteZeroesCompletion(TCallContextPtr callContext)
        : TCompletion(std::move(callContext), TGuardedSgList{})
    {}

    void HandleCompletion(const NProto::TError& error)
    {
        NProto::TZeroBlocksResponse response;
        response.MutableError()->CopyFrom(error);
        Promise.SetValue(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadOnlyFileIO final: public IFileIOService
{
private:
    std::shared_ptr<NIoUring::TContext> Context;

public:
    explicit TReadOnlyFileIO(std::shared_ptr<NIoUring::TContext> context)
        : Context(std::move(context))
    {}

    void Start() final
    {}

    void Stop() final
    {}

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        Context->AsyncRead(file, buffer, offset, completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        completion->Func(completion, MakeError(E_NOT_IMPLEMENTED), 0);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffer);

        completion->Func(completion, MakeError(E_NOT_IMPLEMENTED), 0);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        completion->Func(completion, MakeError(E_NOT_IMPLEMENTED), 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

// location of partition in nvme namespace
struct TNvmePartLayout
{
    ui64 Offset = 0;
    ui64 Size = 0;
};

struct TNvmePartIds
{
    // controller id, 0 for nvme0, 1 for nvme1 etc. Can be 0.
    ui32 CtrlId = 0;
    // namespace id, 1 for nvme0n1, 2 for nvme0n2 etc. Can't be 0.
    ui32 NsId = 0;
    // partition id, 1 for nvme0n1p1, 2 for nvme0n1p2 etc. 0 means no partiton.
    ui32 PartId = 0;
};

struct TNvmePart
{
    TFileHandle Fd;
    ui32 LbaShift = 0;   // LBA size == 1 << LbaShift
    TNvmePartIds Ids;
    TNvmePartLayout Layout;
};

TNvmePartIds GetNvmePartIds(TFsPath path)
{
    path = path.RealLocation();

    const std::regex re(R"(^/dev/nvme(\d+)n(\d+)(p(\d+))?$)");
    std::smatch m;

    Y_ENSURE(std::regex_match(path.GetPath().c_str(), m, re) && m.size() == 5);

    const ui32 ctrlId = FromString<ui32>(m[1]);
    const ui32 nsId = FromString<ui32>(m[2]);
    const ui32 partId = m[4].matched
        ? FromString<ui32>(m[4])
        : 0;

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
    const TFsPath nvmePath =
        Sprintf("/sys/block/nvme%dn%d", ids.CtrlId, ids.NsId);

    if (!ids.PartId) {
        return {
            .Offset = 0,
            .Size = ReadUint64FromFile(nvmePath / "size") * SectorSizeBytes,
        };
    }

    const TFsPath partPath =
        nvmePath / Sprintf("nvme%dn%dp%d", ids.CtrlId, ids.NsId, ids.PartId);

    return {
        .Offset = ReadUint64FromFile(partPath / "start") * SectorSizeBytes,
        .Size = ReadUint64FromFile(partPath / "size") * SectorSizeBytes,
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
    } catch (const TServiceError& e) {
        return MakeError(e.GetCode(), TString(e.GetMessage()));
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
            const ui64 addr = std::bit_cast<ui64>(buf.Data());
            return (addr & mask) == 0 && (buf.Size() & mask) == 0;
        });
}

////////////////////////////////////////////////////////////////////////////////

TArrayRef<TArrayRef<char>> AsArrayRef(const TSgList& sglist)
{
    static_assert(sizeof(TSgList::value_type) == sizeof(TArrayRef<char>));

    return {std::bit_cast<TArrayRef<char>*>(sglist.data()), sglist.size()};
}

TArrayRef<TArrayRef<const char>> AsArrayRefConst(const TSgList& sglist)
{
    static_assert(sizeof(TSgList::value_type) == sizeof(TArrayRef<const char>));

    return {
        std::bit_cast<TArrayRef<const char>*>(sglist.data()),
        sglist.size()};
}

////////////////////////////////////////////////////////////////////////////////
//                          nvme0n1p1
//              ________________|______________
//             /                               \
// nvme0n1: [ ( |.......|  |.......|  |......|  )  ...
//               \_____/
//                  |
//               storage

class TNvmeStorage
    : public IStorage
{
private:
    const TString Filename;
    const NNvme::INvmeManagerPtr NvmeManager;
    const TNvmePartIOPtr NvmePartIO;
    const ui64 StartBytes;
    const ui64 EndBytes;
    const ui32 BlockSize;
    const bool ReadOnly;

public:
    TNvmeStorage(
            TString filename,
            NNvme::INvmeManagerPtr nvmeManager,
            TNvmePartIOPtr nvmeIO,
            ui64 offset,
            ui64 size,
            ui32 blockSize,
            bool readOnly)
        : Filename(std::move(filename))
        , NvmeManager(std::move(nvmeManager))
        , NvmePartIO(std::move(nvmeIO))
        , StartBytes(offset + NvmePartIO->Part.Layout.Offset)
        , EndBytes(StartBytes + size)
        , BlockSize(blockSize)
        , ReadOnly(readOnly)
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) final
    {
        auto completion =
            std::make_unique<TWriteZeroesCompletion>(std::move(callContext));
        auto future = completion->Promise.GetFuture();

        if (ReadOnly) {
            completion->Finish(E_ARGUMENT, "storage is in read only mode");
            return future;
        }

        const ui64 offset = StartBytes + (request->GetStartIndex() * BlockSize);
        const ui64 size =
            static_cast<ui64>(request->GetBlocksCount()) * BlockSize;

        if (offset >= EndBytes || offset + size >= EndBytes) {
            completion->Finish(
                E_ARGUMENT,
                "Write zeroes beyond storage boundary");
            return future;
        }

        NvmePartIO->Context.AsyncNvmeWriteZeroes(
            NvmePartIO->Part.Fd,
            NvmePartIO->Part.Ids.NsId,
            NvmePartIO->Part.LbaShift,
            size,
            offset,
            completion.get());

        auto* ptr = completion.release();
        Y_UNUSED(ptr);

        return future;
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) final
    {
        auto completion = std::make_unique<TReadCompletion>(
            std::move(callContext),
            std::move(request->Sglist));
        auto future = completion->Promise.GetFuture();

        if (!completion->Guard) {
            completion->Finish(
                E_CANCELLED,
                "failed to acquire sglist in NVME storage");
            return future;
        }

        const auto& sglist = completion->Guard.Get();
        const ui64 offset = StartBytes + (request->GetStartIndex() * BlockSize);
        const ui64 size = SgListGetSize(sglist);

        if (offset >= EndBytes || offset + size >= EndBytes) {
            completion->Finish(E_ARGUMENT, "Read beyond storage boundary");
            return future;
        }

        const ui32 lbaSize = 1U << NvmePartIO->Part.LbaShift;

        if (IsProperlyAligned(lbaSize, sglist)) {
            NvmePartIO->Context.AsyncNvmeReadV(
                NvmePartIO->Part.Fd,
                NvmePartIO->Part.Ids.NsId,
                NvmePartIO->Part.LbaShift,
                AsArrayRef(sglist),
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

        auto* ptr = completion.release();
        Y_UNUSED(ptr);

        return future;
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) final
    {
        auto completion = std::make_unique<TWriteCompletion>(
            std::move(callContext),
            std::move(request->Sglist));
        auto future = completion->Promise.GetFuture();

        if (ReadOnly) {
            completion->Finish(E_ARGUMENT, "storage is in read only mode");
            return future;
        }

        if (!completion->Guard) {
            completion->Finish(
                E_CANCELLED,
                "failed to acquire sglist in NVME storage");
            return future;
        }

        const auto& sglist = completion->Guard.Get();
        const ui64 offset = StartBytes + (request->GetStartIndex() * BlockSize);
        const ui64 size = SgListGetSize(sglist);

        if (offset >= EndBytes || offset + size >= EndBytes) {
            completion->Finish(E_ARGUMENT, "Write beyond storage boundary");
            return future;
        }

        const ui32 lbaSize = 1U << NvmePartIO->Part.LbaShift;

        if (IsProperlyAligned(lbaSize, sglist)) {
            NvmePartIO->Context.AsyncNvmeWriteV(
                NvmePartIO->Part.Fd,
                NvmePartIO->Part.Ids.NsId,
                NvmePartIO->Part.LbaShift,
                AsArrayRefConst(sglist),
                offset,
                completion.get());
        } else {
            completion->BounceBuffer.reset(
                static_cast<char*>(std::aligned_alloc(lbaSize, size)));
            SgListCopyUnsafe(sglist, completion->BounceBuffer.get());

            NvmePartIO->Context.AsyncNvmeWrite(
                NvmePartIO->Part.Fd,
                NvmePartIO->Part.Ids.NsId,
                NvmePartIO->Part.LbaShift,
                {completion->BounceBuffer.get(), size},
                offset,
                completion.get());
        }

        auto* ptr = completion.release();
        Y_UNUSED(ptr);

        return future;
    }

    TFuture<NProto::TError> EraseDevice(NProto::EDeviceEraseMethod method) final
    {
        if (method != NProto::DEVICE_ERASE_METHOD_DEALLOCATE) {
            return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
        }

        // TODO(sharpeye): get rid of NvmeManager, use io_uring

        const EOpenMode flags =
            EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly |
            EOpenModeFlag::DirectAligned | EOpenModeFlag::Sync;

        IFileIOServicePtr fileIO = std::make_shared<TReadOnlyFileIO>(
            std::shared_ptr<NIoUring::TContext>(
                NvmePartIO,
                &NvmePartIO->Context));

        return SafeDeallocateDevice(
            Filename,
            TFileHandle{Filename, flags},
            std::move(fileIO),
            StartBytes / BlockSize,
            (EndBytes - StartBytes) / BlockSize,
            BlockSize,
            NvmeManager);
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
    const ILoggingServicePtr Logging;
    TLog Log;
    THashMap<TString, TNvmePartIOPtr> NvmePartIOs;
    IStorageProviderPtr Fallback;
    NNvme::INvmeManagerPtr NvmeManager;

    ui32 Index = 0;

public:
    TNvmeStorageProvider(
            ILoggingServicePtr logging,
            IStorageProviderPtr fallback,
            NNvme::INvmeManagerPtr nvmeManager)
        : Logging(std::move(logging))
        , Log(Logging->CreateLog("BLOCKSTORE_SERVER"))
        , Fallback(std::move(fallback))
        , NvmeManager(std::move(nvmeManager))
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
                STORAGE_INFO(
                    "Unableto create an NVME storage for "
                    << filePath.Quote() << ": " << FormatError(error)
                    << ". Fallback.");

                return Fallback->CreateStorage(volume, clientId, accessMode);
            }

            const ui32 index = Index++;

            NIoUring::TContext::TParams params {
                .SubmissionThreadName = TStringBuilder() << "NV.SQ" << index,
                .CompletionThreadName = TStringBuilder() << "NV.CQ" << index,
                .EnableNvmePassthrough = true,
            };

            STORAGE_INFO(
                "Create an NVME IO backend for "
                << filePath.Quote() << ". ctrl: " << part.Ids.CtrlId
                << " ns: " << part.Ids.NsId << " partition: " << part.Ids.PartId
                << " offset: " << part.Layout.Offset
                << " size: " << part.Layout.Size << "("
                << FormatByteSize(part.Layout.Size) << ")");

            nvmePartIO = std::make_shared<TNvmePartIO>(
                std::move(part),
                std::move(params));

            nvmePartIO->Context.Start();
            NvmePartIOs.emplace(filePath, nvmePartIO);
        }

        const ui32 blockSize =
            volume.GetBlockSize() ? volume.GetBlockSize() : DefaultBlockSize;

        if (nvmePartIO->Part.Layout.Offset % blockSize != 0) {
            STORAGE_WARN(
                "Unableto create an NVME storage for "
                << filePath.Quote() << ": partition offset ("
                << nvmePartIO->Part.Layout.Offset
                << ") is not divisible by the block size (" << blockSize
                << "). Fallback.");

            return Fallback->CreateStorage(volume, clientId, accessMode);
        }

        const ui64 offset = volume.GetStartIndex() * blockSize;
        const ui64 size = volume.GetBlocksCount() * blockSize;

        STORAGE_INFO(
            "Create an NVME storage for "
            << filePath.Quote() << ". offset: " << offset << " size: " << size
            << "(" << FormatByteSize(size) << ")");

        return MakeFuture<IStoragePtr>(std::make_shared<TNvmeStorage>(
            filePath,
            NvmeManager,
            nvmePartIO,
            offset,
            size,
            blockSize,
            !IsReadWriteMode(accessMode)));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateNvmeStorageProvider(
    ILoggingServicePtr logging,
    IStorageProviderPtr fallback,
    NNvme::INvmeManagerPtr nvmeManager)
{
    Y_ENSURE(fallback);
    Y_ENSURE(nvmeManager);
    Y_ENSURE(logging);

    auto [ok, error] = NIoUring::IsNvmePassthroughSupported();

    if (!ok || HasError(error)) {
        auto Log = logging->CreateLog("BLOCKSTORE_SERVER");
        STORAGE_INFO(
            "NVME passthrough is not supported. Use the fallback storage "
            "provider. "
            << FormatError(error));

        return fallback;
    }

    return std::make_shared<TNvmeStorageProvider>(
        std::move(logging),
        std::move(fallback),
        std::move(nvmeManager));
}

}   // namespace NCloud::NBlockStore::NServer
