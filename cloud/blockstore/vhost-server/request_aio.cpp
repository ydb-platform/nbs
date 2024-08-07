#include "request_aio.h"

#include "critical_event.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/strbuf.h>
#include <util/string/builder.h>
#include <util/system/sanitizers.h>

#include <algorithm>
#include <memory>
#include <span>

namespace NCloud::NBlockStore::NVHostServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <bool DoDecrypt>
[[nodiscard]] bool DoCryptoOperation(
    IEncryptor& encryptor,
    TBlockDataRef src,
    TBlockDataRef dst,
    ui64 startSector)
{
    const size_t sectorCount = src.Size() / VHD_SECTOR_SIZE;

    for (size_t i = 0; i < sectorCount; ++i) {
        TBlockDataRef srcRef(src.Data() + i * VHD_SECTOR_SIZE, VHD_SECTOR_SIZE);
        TBlockDataRef dstRef(dst.Data() + i * VHD_SECTOR_SIZE, VHD_SECTOR_SIZE);

        if constexpr (DoDecrypt) {
            if (IsAllZeroes(srcRef.Data(), srcRef.Size())) {
                // If there was a reading from a block that has not yet been
                // written, then we return a block consisting of only zeros.
                memset(const_cast<char*>(dstRef.Data()), 0, dstRef.Size());
                continue;
            }

            if (!encryptor.Decrypt(srcRef, dstRef, startSector + i)) {
                // Something went wrong inside the decryption operation.
                return false;
            }
        } else {
            if (!encryptor.Encrypt(srcRef, dstRef, startSector + i)) {
                // Something went wrong inside the encryption operation.
                return false;
            }

            if (IsAllZeroes(dstRef.Data(), dstRef.Size())) {
                ReportCriticalEvent(
                    "EncryptorGeneratedZeroBlock",
                    TStringBuilder() << "Encryptor has generated a zero block #"
                                     << startSector + i << " !");
                return false;
            }
        }
    }
    return true;
}

void PrepareCompoundIO(
    IEncryptor* encryptor,
    TLog& Log,
    const TVector<TAioDevice>& devices,
    vhd_io* io,
    TVector<iocb*>& batch,
    TCpuCycles now,
    TSimpleStats& queueStats)
{
    auto* bio = vhd_get_bdev_io(io);
    const i64 logicalOffset = bio->first_sector * VHD_SECTOR_SIZE;
    i64 totalBytes = bio->total_sectors * VHD_SECTOR_SIZE;

    auto it = std::lower_bound(
        devices.begin(),
        devices.end(),
        logicalOffset,
        [] (const TAioDevice& device, i64 offset) {
            return device.EndOffset <= offset;
        });

    const auto end = std::lower_bound(
        it,
        devices.end(),
        logicalOffset + totalBytes,
        [] (const TAioDevice& device, i64 offset) {
            return device.StartOffset < offset;
        });

    const ui32 n = static_cast<ui32>(std::distance(it, end));

    Y_DEBUG_ABORT_UNLESS(n > 1);

    STORAGE_DEBUG(
        "%s compound request, %u parts: start block %" PRIu64 ", blocks count %" PRIu64,
        bio->type == VHD_BDEV_READ ? "Read" : "Write",
        n,
        bio->first_sector,
        bio->total_sectors);

    auto req = TAioCompoundRequest::CreateNew(n, io, totalBytes, now);

    if (bio->type == VHD_BDEV_WRITE) {
        const bool success = SgListCopyWithOptionalEncryption(
            Log,
            bio->sglist,
            req->Buffer.get(),
            encryptor,
            bio->first_sector);
        if (!success) {
            ++queueStats.EncryptorErrors;
            vhd_complete_bio(req->Io, VHD_BDEV_IOERR);
            return;
        }
    }

    ui64 deviceOffset = logicalOffset - it->StartOffset;
    char* ptr = req->Buffer.get();
    for (; it != end; ++it) {
        auto subRequest = TAioSubRequest::CreateNew();

        const ui64 count = Min<ui64>(
            totalBytes,
            it->EndOffset - it->StartOffset - deviceOffset);

        if (bio->type == VHD_BDEV_READ) {
            io_prep_pread(
                subRequest.get(),
                it->File,
                ptr,
                count,
                it->FileOffset + deviceOffset);
        } else {
            io_prep_pwrite(
                subRequest.get(),
                it->File,
                ptr,
                count,
                it->FileOffset + deviceOffset);
        }

        // Save the address of TAioCompoundRequest in each subrequest to share
        // ownership among all subrequests.
        subRequest->data = req.get();

        NSan::Release(subRequest.get());
        batch.push_back(subRequest.release());

        ptr += count;
        totalBytes -= count;
        deviceOffset = 0;
    }

    // Ownership transferred to subrequests.
    NSan::Release(req.get());
    req.release();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool SgListCopyWithOptionalDecryption(
    TLog& Log,
    const char* src,
    const vhd_sglist& dst,
    IEncryptor* encryptor,
    ui64 startSector)
{
    auto buffers = std::span<vhd_buffer>{dst.buffers, dst.nbuffers};

    if (!encryptor) {
        for (auto& buffer: buffers) {
            std::memcpy(buffer.base, src, buffer.len);
            src += buffer.len;
        }
        return true;
    }

    for (auto& buffer: buffers) {
        TBlockDataRef srcRef{src, buffer.len};
        TBlockDataRef dstRef{static_cast<const char*>(buffer.base), buffer.len};
        if (!DoCryptoOperation<true>(*encryptor, srcRef, dstRef, startSector)) {
            STORAGE_ERROR(
                "Decryption error. Start block %" PRIu64
                ", blocks count %" PRIu64,
                startSector,
                buffers.size());
            return false;
        }
        startSector += buffer.len / VHD_SECTOR_SIZE;
        src += buffer.len;
    }
    return true;
}

bool SgListCopyWithOptionalEncryption(
    TLog& Log,
    const vhd_sglist& src,
    char* dst,
    IEncryptor* encryptor,
    ui64 startSector)
{
    auto buffers = std::span<vhd_buffer>{src.buffers, src.nbuffers};

    if (!encryptor) {
        for (auto& buffer: buffers) {
            std::memcpy(dst, buffer.base, buffer.len);
            dst += buffer.len;
        }
        return true;
    }

    for (auto& buffer: buffers) {
        TBlockDataRef srcRef{static_cast<const char*>(buffer.base), buffer.len};
        TBlockDataRef dstRef{dst, buffer.len};
        if (!DoCryptoOperation<false>(*encryptor, srcRef, dstRef, startSector))
        {
            STORAGE_ERROR(
                "Encryption error. Start block %" PRIu64
                ", blocks count %" PRIu64,
                startSector,
                buffers.size());
            return false;
        }
        startSector += buffer.len / VHD_SECTOR_SIZE;
        dst += buffer.len;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

void PrepareIO(
    TLog& Log,
    IEncryptor* encryptor,
    const TVector<TAioDevice>& devices,
    vhd_io* io,
    TVector<iocb*>& batch,
    TCpuCycles now,
    TSimpleStats& queueStats)
{
    auto* bio = vhd_get_bdev_io(io);
    const i64 logicalOffset = bio->first_sector * VHD_SECTOR_SIZE;
    const i64 totalBytes = bio->total_sectors * VHD_SECTOR_SIZE;

    const auto *it = std::lower_bound(
        devices.begin(),
        devices.end(),
        logicalOffset,
        [] (const TAioDevice& device, i64 offset) {
            return device.EndOffset <= offset;
        });

    Y_ABORT_UNLESS(it != devices.end());

    if (it->EndOffset < logicalOffset + totalBytes) {
        // The request is cross-device, so we split it into two.
        PrepareCompoundIO(encryptor, Log, devices, io, batch, now, queueStats);
        return;
    }

    STORAGE_DEBUG(
        "%s request, %u parts: start block %" PRIu64 ", blocks count %" PRIu64,
        bio->type == VHD_BDEV_READ ? "Read" : "Write",
        bio->sglist.nbuffers,
        bio->first_sector,
        bio->total_sectors);

    auto buffers =
        std::span<vhd_buffer>{bio->sglist.buffers, bio->sglist.nbuffers};

    // Windows allows i/o with buffers not aligned to i/o block size, but
    // Linux doesn't, so use bounce buffer in this case.
    // Note: the required alignment is the logical block size of the
    // underlying storage; assume it to equal the sector size as BIOS
    // requires sector-granular i/o anyway.
    const bool isAllBuffersAligned = AllOf(
        buffers,
        [](const vhd_buffer& buffer)
        {
            return VHD_IS_ALIGNED((uintptr_t)buffer.base, VHD_SECTOR_SIZE) &&
                   VHD_IS_ALIGNED(buffer.len, VHD_SECTOR_SIZE);
        }
    );

    const bool needToAllocateBuffer =
        !isAllBuffersAligned || (encryptor && bio->type == VHD_BDEV_WRITE);

    auto req = TAioRequest::CreateNew(
        needToAllocateBuffer ? 1 : buffers.size(),
        needToAllocateBuffer ? totalBytes : 0,
        io,
        now);

    if (needToAllocateBuffer) {
        req->Unaligned = !isAllBuffersAligned;
        if (bio->type == VHD_BDEV_WRITE) {
            const bool success = SgListCopyWithOptionalEncryption(
                Log,
                bio->sglist,
                static_cast<char*>(req->Data[0].iov_base),
                encryptor,
                bio->first_sector);
            if (!success) {
                ++queueStats.EncryptorErrors;
                vhd_complete_bio(req->Io, VHD_BDEV_IOERR);
                return;
            }
        }
        // Instead of multiple buffers, we have allocated one large buffer.
        buffers = buffers.subspan(0, 1);
    } else {
        for (ui32 i = 0; i != buffers.size(); ++i) {
            req->Data[i].iov_base = buffers[i].base;
            req->Data[i].iov_len = buffers[i].len;
        }
    }

    const auto offset = it->FileOffset + logicalOffset - it->StartOffset;

    if (bio->type == VHD_BDEV_READ) {
        io_prep_preadv(req.get(), it->File, req->Data, buffers.size(), offset);
    } else {
        io_prep_pwritev(req.get(), it->File, req->Data, buffers.size(), offset);
    }

    STORAGE_DEBUG("Prepared IO request with addr: %p", req.get());

    NSan::Release(req.get());
    batch.push_back(req.release());
}

////////////////////////////////////////////////////////////////////////////////

void TFreeDeleter::operator()(void* obj)
{
    std::free(obj);
}

void TAioRequestDeleter::operator()(TAioRequest* obj)
{
    if (obj->BufferAllocated) {
        std::free(obj->Data[0].iov_base);
    }
    std::free(obj);
}

TAioRequest::TAioRequest(
        size_t allocatedBufferSize,
        vhd_io* io,
        TCpuCycles submitTs)
    : iocb()
    , Io(io)
    , SubmitTs(submitTs)
    , BufferAllocated(allocatedBufferSize != 0)
{
    if (allocatedBufferSize) {
        Data[0].iov_len = allocatedBufferSize;
        Data[0].iov_base =
            std::aligned_alloc(VHD_SECTOR_SIZE, allocatedBufferSize);
    }
}

// static
TAioRequestHolder TAioRequest::CreateNew(
    size_t bufferCount,
    size_t allocatedBufferSize,
    vhd_io* io,
    TCpuCycles submitTs)
{
    const size_t totalSize = sizeof(TAioRequest) + sizeof(iovec) * bufferCount;
    return TAioRequestHolder{
        new (std::calloc(1, totalSize))
            TAioRequest(allocatedBufferSize, io, submitTs)};
}

// static
TAioRequestHolder TAioRequest::FromIocb(iocb* cb)
{
    NSan::Acquire(cb);
    Y_ABORT_UNLESS(cb->data == nullptr);
    return TAioRequestHolder{static_cast<TAioRequest*>(cb)};
}

// static
TAioSubRequestHolder TAioSubRequest::CreateNew()
{
    const size_t size = sizeof(TAioSubRequest);
    return TAioSubRequestHolder{new (std::calloc(1, size)) TAioSubRequest};
}

// static
TAioSubRequestHolder TAioSubRequest::FromIocb(iocb* cb) {
    NSan::Acquire(cb);
    Y_ABORT_UNLESS(cb->data != nullptr);
    return TAioSubRequestHolder{static_cast<TAioSubRequest*>(cb)};
}

TAioCompoundRequest* TAioSubRequest::GetParentRequest() const
{
    NSan::Acquire(data);
    Y_ABORT_UNLESS(data != nullptr);
    return static_cast<TAioCompoundRequest*>(data);
}

TAioCompoundRequestHolder TAioSubRequest::TakeParentRequest()
{
    NSan::Acquire(data);
    Y_ABORT_UNLESS(data != nullptr);
    auto result =
        TAioCompoundRequestHolder{static_cast<TAioCompoundRequest*>(data)};
    data = nullptr;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TAioCompoundRequest::TAioCompoundRequest(
        int inflight,
        vhd_io* io,
        size_t bufferSize,
        TCpuCycles submitTs)
    : Inflight(inflight)
    , Io(io)
    , SubmitTs(submitTs)
    , Buffer{
          static_cast<char*>(std::aligned_alloc(VHD_SECTOR_SIZE, bufferSize)),
      }
{}

// static
std::unique_ptr<TAioCompoundRequest> TAioCompoundRequest::CreateNew(
    int inflight,
    vhd_io* io,
    size_t bufferSize,
    TCpuCycles submitTs)
{
    return std::make_unique<TAioCompoundRequest>(
        inflight,
        io,
        bufferSize,
        submitTs);
}

}   // namespace NCloud::NBlockStore::NVHostServer
