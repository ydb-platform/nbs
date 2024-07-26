#include "request_aio.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/strbuf.h>
#include <util/system/sanitizers.h>

#include <algorithm>
#include <memory>

namespace NCloud::NBlockStore::NVHostServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

void PrepareCompoundIO(
    TLog& Log,
    const TVector<TAioDevice>& devices,
    vhd_io* io,
    TVector<iocb*>& batch,
    TCpuCycles now)
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
        // TODO(drbasic): encryption
        SgListCopy(bio->sglist, req->Buffer.get());
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

        batch.push_back(subRequest.release());

        ptr += count;
        totalBytes -= count;
        deviceOffset = 0;
    }

    // Ownership transferred to subrequests.
    req.release();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void SgListCopy(const char* src, const vhd_sglist& dst)
{
    auto [count, bufs] = dst;

    for (ui32 i = 0; i != count; ++i) {
        std::memcpy(bufs[i].base, src, bufs[i].len);
        src += bufs[i].len;
    }
}

void SgListCopy(const vhd_sglist& src, char* dst)
{
    auto [count, bufs] = src;

    for (ui32 i = 0; i != count; ++i) {
        std::memcpy(dst, bufs[i].base, bufs[i].len);
        dst += bufs[i].len;
    }
}

////////////////////////////////////////////////////////////////////////////////

void PrepareIO(
    TLog& Log,
    const TVector<TAioDevice>& devices,
    vhd_io* io,
    TVector<iocb*>& batch,
    TCpuCycles now)
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
        PrepareCompoundIO(Log, devices, io, batch, now);
        return;
    }

    STORAGE_DEBUG(
        "%s request, %u parts: start block %" PRIu64 ", blocks count %" PRIu64,
        bio->type == VHD_BDEV_READ ? "Read" : "Write",
        bio->sglist.nbuffers,
        bio->first_sector,
        bio->total_sectors);

    auto [nbufs, buffers] = bio->sglist;

    auto req = TAioRequest::CreateNew(nbufs, io, now);

    for (ui32 i = 0; i != nbufs; ++i) {
        // Windows allows i/o with buffers not aligned to i/o block size, but
        // Linux doesn't, so use bounce buffer in this case.
        // Note: the required alignment is the logical block size of the
        // underlying storage; assume it to equal the sector size as BIOS
        // requires sector-granular i/o anyway.
        if (!VHD_IS_ALIGNED((uintptr_t) buffers[i].base, VHD_SECTOR_SIZE) ||
            !VHD_IS_ALIGNED(buffers[i].len, VHD_SECTOR_SIZE))
        {
            req->BounceBuf = true;
            req->Data[0].iov_len = totalBytes;
            req->Data[0].iov_base = std::aligned_alloc(
                VHD_SECTOR_SIZE,
                req->Data[0].iov_len);

            if (bio->type == VHD_BDEV_WRITE) {
                char* dst = static_cast<char*>(req->Data[0].iov_base);
                for (ui32 i = 0; i != nbufs; ++i) {
                    // TODO(drbasic) encrypt
                    std::memcpy(dst, buffers[i].base, buffers[i].len);
                    dst += buffers[i].len;
                }
            }

            nbufs = 1;

            break;
        }

        req->Data[i].iov_base = buffers[i].base;
        req->Data[i].iov_len = buffers[i].len;
    }

    const auto offset = it->FileOffset + logicalOffset - it->StartOffset;

    if (bio->type == VHD_BDEV_READ) {
        io_prep_preadv(req.get(), it->File, req->Data, nbufs, offset);
    } else {
        io_prep_pwritev(req.get(), it->File, req->Data, nbufs, offset);
    }

    STORAGE_DEBUG("Prepared IO request with addr: %p", req.get());

    batch.push_back(req.release());
}

////////////////////////////////////////////////////////////////////////////////

void TFreeDeleter::operator()(void* obj)
{
    std::free(obj);
}

void TAioRequestDeleter::operator()(TAioRequest* obj)
{
    if (obj->BounceBuf) {
        std::free(obj->Data[0].iov_base);
    }
    std::free(obj);
}

TAioRequest::TAioRequest(vhd_io* io, TCpuCycles submitTs)
    : iocb()
    , Io(io)
    , SubmitTs(submitTs)
{}

// static
TAioRequestHolder
TAioRequest::CreateNew(size_t bufferCount, vhd_io* io, TCpuCycles submitTs)
{
    const size_t totalSize = sizeof(TAioRequest) + sizeof(iovec) * bufferCount;
    return TAioRequestHolder{new (std::calloc(1, totalSize))
                                 TAioRequest(io, submitTs)};
}

// static
TAioRequestHolder TAioRequest::FromIocb(iocb* cb)
{
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
    Y_ABORT_UNLESS(cb->data != nullptr);
    return TAioSubRequestHolder{static_cast<TAioSubRequest*>(cb)};
}

TAioCompoundRequest* TAioSubRequest::GetParentRequest() const
{
    Y_ABORT_UNLESS(data != nullptr);
    return static_cast<TAioCompoundRequest*>(data);
}

TAioCompoundRequestHolder TAioSubRequest::TakeParentRequest()
{
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
