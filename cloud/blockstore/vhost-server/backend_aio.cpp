#include "backend_aio.h"

#include "backend.h"
#include "request_aio.h"

#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/system/sanitizers.h>

#include <libaio.h>

#include <thread>

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

void CompleteRequest(
    TAioRequestHolder req,
    vhd_bdev_io_result status,
    TSimpleStats& stats,
    TCpuCycles now)
{
    auto* bio = vhd_get_bdev_io(req->Io);
    const ui64 bytes = bio->total_sectors * VHD_SECTOR_SIZE;

    stats.Requests[bio->type].Errors += status != VHD_BDEV_SUCCESS;
    stats.Requests[bio->type].Count += status == VHD_BDEV_SUCCESS;
    stats.Requests[bio->type].Bytes += bytes;
    stats.Requests[bio->type].Unaligned += req->BounceBuf;

    if (status == VHD_BDEV_SUCCESS) {
        stats.Times[bio->type].Increment(now - req->SubmitTs);
        stats.Sizes[bio->type].Increment(bytes);
    }

    if (req->BounceBuf) {
        if (bio->type == VHD_BDEV_READ && status == VHD_BDEV_SUCCESS) {
            // TODO(drbasic): decrypt
            SgListCopy(
                static_cast<const char*>(req->Data[0].iov_base),
                bio->sglist);
        }
    }
    vhd_complete_bio(req->Io, status);
}

void CompleteCompoundRequest(
    TAioSubRequestHolder sub,
    vhd_bdev_io_result status,
    TSimpleStats& stats,
    TCpuCycles now)
{
    auto* req = sub->GetParentRequest();

    req->Errors += status != VHD_BDEV_SUCCESS;

    if (req->Inflight.fetch_sub(1) == 1) {
        // This is the last subrequest. Take ownership of the parent request and
        // release it when leave the scope.
        auto holder = sub->TakeParentRequest();

        auto* bio = vhd_get_bdev_io(req->Io);
        const ui64 bytes = bio->total_sectors * VHD_SECTOR_SIZE;

        stats.Requests[bio->type].Errors += req->Errors != 0;
        stats.Requests[bio->type].Count += 1;
        stats.Requests[bio->type].Bytes += bytes;

        if (status == VHD_BDEV_SUCCESS) {
            stats.Times[bio->type].Increment(now - req->SubmitTs);
            stats.Sizes[bio->type].Increment(bytes);
        }

        if (bio->type == VHD_BDEV_READ && status == VHD_BDEV_SUCCESS) {
            // TODO(drbasic): decrypt
            SgListCopy(req->Buffer.get(), bio->sglist);
        }
        vhd_complete_bio(req->Io, status);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TAioBackend final: public IBackend
{
private:
    const ILoggingServicePtr Logging;
    TLog Log;

    TVector<TAioDevice> Devices;

    io_context_t Io = {};

    ui32 BatchSize = 0;
    TVector<TVector<iocb*>> Batches;

    std::thread CompletionThread;

    ICompletionStatsPtr CompletionStats;

public:
    explicit TAioBackend(ILoggingServicePtr logging);

    vhd_bdev_info Init(const TOptions& options) override;
    void Start() override;
    void Stop() override;
    void ProcessQueue(
        ui32 queueIndex,
        vhd_request_queue* queue,
        TSimpleStats& queueStats) override;
    std::optional<TSimpleStats> GetCompletionStats(TDuration timeout) override;

private:
    // Dequeue requests from |queue| and start processing them. Returns the
    // number of dequeued requests.
    // Puts started io-requests iocb to |batch|. Some requests may splitted for
    // cross-device read/write and produce two iocb.
    size_t PrepareBatch(
        vhd_request_queue* queue,
        TVector<iocb*>& batch,
        TCpuCycles now);

    void CompletionThreadFunc();
};

////////////////////////////////////////////////////////////////////////////////

TAioBackend::TAioBackend(ILoggingServicePtr logging)
    : Logging{std::move(logging)}
    , CompletionStats(CreateCompletionStats())
{
    Log = Logging->CreateLog("AIO");
}

vhd_bdev_info TAioBackend::Init(const TOptions& options)
{
    STORAGE_INFO("Initializing AIO backend");

    BatchSize = options.BatchSize;

    Y_ABORT_UNLESS(io_setup(BatchSize, &Io) >= 0, "io_setup");

    for (ui32 i = 0; i < options.QueueCount; i++) {
        Batches.emplace_back();
        Batches.back().reserve(BatchSize);
    }

    EOpenMode flags =
        EOpenModeFlag::OpenExisting | EOpenModeFlag::DirectAligned |
        (options.ReadOnly ? EOpenModeFlag::RdOnly : EOpenModeFlag::RdWr);

    if (!options.NoSync) {
        flags |= EOpenModeFlag::Sync;
    }

    Devices.reserve(options.Layout.size());

    i64 totalBytes = 0;

    for (const auto& chunk: options.Layout) {
        TFileHandle file{chunk.DevicePath, flags};

        if (!file.IsOpen()) {
            int ret = errno;
            Y_ABORT("can't open %s: %s", chunk.DevicePath.c_str(), strerror(ret));
        }

        i64 fileLen = file.Seek(0, sEnd);

        Y_ABORT_UNLESS(
            fileLen,
            "unable to retrive size of file %s",
            chunk.DevicePath.Quote().c_str());

        Y_ABORT_UNLESS(
            !chunk.Offset || fileLen > chunk.Offset,
            "%s: file is too small (%ld B) or the offset is too big (%ld B)",
            chunk.DevicePath.Quote().c_str(),
            fileLen,
            chunk.Offset);

        fileLen -= chunk.Offset;

        if (chunk.ByteCount) {
            Y_ABORT_UNLESS(
                fileLen >= chunk.ByteCount,
                "%s: file is too small (%ld B) expected at least %ld B",
                chunk.DevicePath.Quote().c_str(),
                fileLen,
                chunk.ByteCount);

            fileLen = chunk.ByteCount;
        }

        Y_ABORT_UNLESS(
            fileLen % VHD_SECTOR_SIZE == 0,
            "%s: file size is not a multiple of the block size.",
            chunk.DevicePath.Quote().c_str());

        STORAGE_INFO(
            "File %s (%s) %s, offset: %ld",
            chunk.DevicePath.Quote().c_str(),
            DecodeOpenMode(flags).c_str(),
            FormatByteSize(fileLen).c_str(),
            chunk.Offset);

        Devices.push_back(TAioDevice{
            .StartOffset = totalBytes,
            .EndOffset = totalBytes + fileLen,
            .File = std::move(file),
            .FileOffset = chunk.Offset});

        totalBytes += fileLen;
    }

    return {
        .serial = options.Serial.c_str(),
        .socket_path = options.SocketPath.c_str(),
        .block_size = VHD_SECTOR_SIZE,
        .num_queues = options.QueueCount,   // Max count of virtio queues
        .total_blocks = totalBytes / VHD_SECTOR_SIZE,
        .features = options.ReadOnly ? VHD_BDEV_F_READONLY : 0};
}

void TAioBackend::Start()
{
    STORAGE_INFO("Starting AIO backend");

    CompletionThread = std::thread([this] { CompletionThreadFunc(); });
}

void TAioBackend::Stop()
{
    STORAGE_INFO("Stopping AIO backend");

    pthread_kill(CompletionThread.native_handle(), SIGUSR2);
    CompletionThread.join();

    io_destroy(Io);
    Devices.clear();
}

void TAioBackend::ProcessQueue(
    ui32 queueIndex,
    vhd_request_queue* queue,
    TSimpleStats& queueStats)
{
    int ret;

    auto& batch = Batches[queueIndex];

    for (;;) {
        const TCpuCycles now = GetCycleCount();

        // append new requests to the tail of the batch
        queueStats.Dequeued += PrepareBatch(queue, batch, now);

        if (batch.empty()) {
            break;
        }

        do {
            ret = io_submit(Io, batch.size(), batch.data());
        } while (ret == -EINTR);

        // kernel queue full, punt the re-submission to later event loop
        // iterations, woken up by completions
        if (ret == -EAGAIN) {
            break;
        }

        // submission failed for other reasons, fail the first request but
        // keep the rest of the batch
        if (ret < 0) {
            STORAGE_ERROR("io_submit: %s", strerror(-ret));

            ++queueStats.SubFailed;

            if (batch[0]->data) {
                CompleteCompoundRequest(
                    TAioSubRequest::FromIocb(batch[0]),
                    VHD_BDEV_IOERR,
                    queueStats,
                    now);
            } else {
                CompleteRequest(
                    TAioRequest::FromIocb(batch[0]),
                    VHD_BDEV_IOERR,
                    queueStats,
                    now);
            }

            ret = 1;
        }

        queueStats.Submitted += ret;

        // remove submitted items from the batch
        batch.erase(batch.begin(), batch.begin() + ret);
    }
}

std::optional<TSimpleStats> TAioBackend::GetCompletionStats(TDuration timeout)
{
    return CompletionStats->Get(timeout);
}

size_t TAioBackend::PrepareBatch(
    vhd_request_queue* queue,
    TVector<iocb*>& batch,
    TCpuCycles now)
{
    const size_t initialSize = batch.size();

    vhd_request req{};
    while (batch.size() < BatchSize && vhd_dequeue_request(queue, &req)) {
        Y_DEBUG_ABORT_UNLESS(vhd_vdev_get_priv(req.vdev) == this);

        PrepareIO(Log, Devices, req.io, batch, now);
    }

    return batch.size() - initialSize;
}

void TAioBackend::CompletionThreadFunc()
{
    NCloud::SetCurrentThreadName("AIO");

    thread_local sig_atomic_t shouldStop;

    struct sigaction stopAction = {};
    stopAction.sa_handler = [](int) { shouldStop = 1; };
    sigaction(SIGUSR2, &stopAction, nullptr);

    TVector<io_event> events(BatchSize);

    TSimpleStats stats;
    timespec timeout{.tv_sec = 1, .tv_nsec = 0};

    for (;;) {
        // TODO: try AIO_RING_MAGIC trick
        // (https://github.com/axboe/fio/blob/master/engines/libaio.c#L272)
        int ret = io_getevents(Io, 1, events.size(), events.data(), &timeout);

        if (ret < 0 && ret != -EINTR) {
            Y_ABORT("io_getevents: %s", strerror(-ret));
        }

        CompletionStats->Sync(stats);

        if (shouldStop) {
            break;
        }

        if (ret == -EINTR) {
            continue;
        }

        const TCpuCycles now = GetCycleCount();

        for (int i = 0; i != ret; ++i) {
            if (events[i].data) {
                auto sub = TAioSubRequest::FromIocb(events[i].obj);

                vhd_bdev_io_result result = VHD_BDEV_SUCCESS;

                if (events[i].res2 != 0 || events[i].res != sub->u.c.nbytes) {
                    stats.CompFailed += 1;
                    result = VHD_BDEV_IOERR;
                    STORAGE_ERROR(
                        "IO request: opcode=%d, offset=%lld size=%lu, res=%lu, "
                        "res2=%lu, %s",
                        sub->aio_lio_opcode,
                        sub->u.c.offset,
                        sub->u.c.nbytes,
                        events[i].res,
                        events[i].res2,
                        strerror(-events[i].res));
                }

                CompleteCompoundRequest(
                    std::move(sub),
                    result,
                    stats,
                    now);

                continue;
            }

            auto req = TAioRequest::FromIocb(events[i].obj);

            vhd_bdev_io_result result = VHD_BDEV_SUCCESS;
            auto* bio = vhd_get_bdev_io(req->Io);

            if (events[i].res2 != 0 ||
                events[i].res != bio->total_sectors * VHD_SECTOR_SIZE)
            {
                stats.CompFailed += 1;
                result = VHD_BDEV_IOERR;
                STORAGE_ERROR(
                    "IO request: opcode=%d, offset=%lld, size=%llu, res=%lu, "
                    "res2=%lu, %s",
                    req->aio_lio_opcode,
                    req->u.c.offset,
                    bio->total_sectors * VHD_SECTOR_SIZE,
                    events[i].res,
                    events[i].res2,
                    strerror(-events[i].res));
            }

            CompleteRequest(std::move(req), result, stats, now);
        }

        stats.Completed += ret;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateAioBackend(ILoggingServicePtr logging)
{
    return std::make_shared<TAioBackend>(std::move(logging));
}

}   // namespace NCloud::NBlockStore::NVHostServer
