#include "server.h"

#include "request.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/contrib/vhost/include/vhost/blockdev.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/contrib/vhost/include/vhost/types.h>
#include <cloud/contrib/vhost/platform.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/iterator_range.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/stream/printf.h>
#include <util/system/datetime.h>
#include <util/system/event.h>
#include <util/system/file.h>
#include <util/system/sysstat.h>

#include <atomic>
#include <span>
#include <thread>

#include <libaio.h>
#include <signal.h>

using namespace NThreading;

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

TLog Log;

////////////////////////////////////////////////////////////////////////////////

void LogStderr(LogLevel level, const char* fmt, ...)
{
    static ELogPriority levelToPriority[] {
        TLOG_ERR,
        TLOG_WARNING,
        TLOG_INFO,
        TLOG_DEBUG
    };

    auto priority = levelToPriority[level];

    va_list args;
    va_start(args, fmt);

    if (Log.IsOpen() && priority <= Log.FiltrationLevel()) {
        auto elem = Log << priority;
        Printf(elem, fmt, args);
    }

    va_end(args);
}

void DeleteSocket(const TString& path)
{
    if (unlink(path.c_str()) != -1) {
        return;
    }

    if (errno == ENOENT) {
        return;
    }

    const int err = errno;
    STORAGE_WARN(
        "Can't delete socket file %s: %s (%d)",
        path.c_str(),
        strerror(err),
        err);
}

////////////////////////////////////////////////////////////////////////////////

#ifndef NDEBUG

TString ToString(std::span<iocb*> batch)
{
    TStringStream ss;

    const char* op[] {
        "pread",
        "pwrite",
        "",
        "",
        "",
        "",
        "",
        "preadv",
        "pwritev",
    };

    ss << "[ ";
    for (iocb* cb: batch) {
        ss << "{ " << op[cb->aio_lio_opcode] << ":" << cb->aio_fildes << " ";
        switch (cb->aio_lio_opcode) {
            case IO_CMD_PREAD:
            case IO_CMD_PWRITE:
                ss << cb->u.c.buf << " " << cb->u.c.nbytes
                    << ":" << cb->u.c.offset;
                break;
            case IO_CMD_PREADV:
            case IO_CMD_PWRITEV: {
                iovec* iov = static_cast<iovec*>(cb->u.c.buf);
                for (unsigned i = 0; i != cb->u.c.nbytes; ++i) {
                    ss << "(" << iov[i].iov_base << " " << iov[i].iov_len << ") ";
                }
                break;
            }
        }
        ss << "} ";
    }

    ss << "]";

    return ss.Str();
}

#endif  // NDEBUG

////////////////////////////////////////////////////////////////////////////////

void CompleteRequest(
    TRequest* req,
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
            SgListCopy(
                static_cast<const char*>(req->Data[0].iov_base),
                bio->sglist);
        }
        std::free(req->Data[0].iov_base);
    }
    vhd_complete_bio(req->Io, status);
    std::free(req);
}

void CompleteRequest(
    iocb* sub,
    TCompoundRequest* req,
    vhd_bdev_io_result status,
    TSimpleStats& stats,
    TCpuCycles now)
{
    req->Errors += status != VHD_BDEV_SUCCESS;

    auto* bio = vhd_get_bdev_io(req->Io);

    if (req->Inflight.fetch_sub(1) == 1) {
        const ui64 bytes = bio->total_sectors * VHD_SECTOR_SIZE;

        stats.Requests[bio->type].Errors += req->Errors != 0;
        stats.Requests[bio->type].Count += 1;
        stats.Requests[bio->type].Bytes += bytes;

        if (status == VHD_BDEV_SUCCESS) {
            stats.Times[bio->type].Increment(now - req->SubmitTs);
            stats.Sizes[bio->type].Increment(bytes);
        }

        if (bio->type == VHD_BDEV_READ && status == VHD_BDEV_SUCCESS) {
            SgListCopy(req->Buffer, bio->sglist);
        }
        std::free(req->Buffer);

        vhd_complete_bio(req->Io, status);
        std::free(req);
    }

    std::free(sub);
}

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public IServer
{
private:
    const ILoggingServicePtr Logging;

    ui32 BatchSize = 0;
    TString SocketPath;

    io_context_t Io = {};

    vhd_vdev* Handler = nullptr;
    vhd_bdev_info Info = {};

    TVector<TDevice> Devices;

    TVector<vhd_request_queue*> Queues;
    std::unique_ptr<TAtomicStats[]> QueueStats;
    TSimpleStats CompletionStats;

    std::thread CompletionThread;
    TVector<std::thread> QueueThreads;

    std::atomic_bool NeedUpdateCompletionStats;
    TAutoEvent CompletionStatsEvent;

public:
    explicit TServer(ILoggingServicePtr logging);

    void Start(const TOptions& options) override;
    void Stop() override;

    TSimpleStats GetStats() override;

private:
    void InitBackend(const TOptions& options);

    void QueueThreadFunc(ui32 queueIndex);
    void CompletionThreadFunc();

    void SyncQueueStats(ui32 queueIndex, const TSimpleStats& queueStats);
    void SyncCompletionStats(const TSimpleStats& stats);

    size_t PrepareBatch(
        vhd_request_queue* queue,
        TVector<iocb*>& batch,
        TCpuCycles now);
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(ILoggingServicePtr logging)
    : Logging {std::move(logging)}
{}

void TServer::Start(const TOptions& options)
{
    Log = Logging->CreateLog("");

    BatchSize = options.BatchSize;
    SocketPath = options.SocketPath;

    InitBackend(options);

    Y_ABORT_UNLESS(io_setup(BatchSize, &Io) >= 0, "io_setup");

    for (ui32 i = 0; i != options.QueueCount; ++i) {
        auto* queue = vhd_create_request_queue();
        Y_ABORT_UNLESS(queue, "vhd_create_request_queue failed");
        Queues.push_back(queue);
    }

    QueueStats = std::make_unique<TAtomicStats[]>(options.QueueCount);

    Y_ABORT_UNLESS(
        vhd_start_vhost_server(LogStderr) >= 0,
        "vhd_start_vhost_server failed");

    DeleteSocket(SocketPath);

    // Register backend for existing queue and add backend to it.
    Handler = vhd_register_blockdev(
        &Info,
        Queues.data(),
        Queues.size(),
        this);
    Y_ABORT_UNLESS(Handler, "vhd_register_blockdev: Can't register device");

    if (!options.NoChmod) {
        const int mode = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;
        if (int err = Chmod(SocketPath.c_str(), mode)) {
            Y_ABORT(
                "failed to chmod socket %s: %s",
                SocketPath.c_str(),
                strerror(err)
            );
        }
    }

    STORAGE_INFO("Server started");

    CompletionThread = std::thread([this] {
        CompletionThreadFunc();
    });

    // start libvhost request queue runner thread
    QueueThreads.reserve(options.QueueCount);

    for (ui32 i = 0; i != options.QueueCount; ++i) {
        QueueThreads.emplace_back([this, i] {
            QueueThreadFunc(i);
        });
    }
}

void TServer::Stop()
{
    STORAGE_INFO("Stopping the server");

    {
        auto promise = NewPromise();
        vhd_unregister_blockdev(Handler, [] (void* opaque) {
            static_cast<TPromise<void>*>(opaque)->SetValue();
        }, &promise);

        promise.GetFuture().Wait();
    }

    // 2. Stop request queues. For each do:
    // 2.1 Stop a request queue
    for (vhd_request_queue* queue: Queues) {
        vhd_stop_queue(queue);
    }

    // 2.2 Wait for queue's thread to join
    for (auto& queueThread: QueueThreads) {
        queueThread.join();
    }

    // 3. Stop the completion thread
    pthread_kill(CompletionThread.native_handle(), SIGUSR2);
    CompletionThread.join();

    // 4. Release request queues and stop the vhost server in any order
    for (vhd_request_queue* queue: Queues) {
        vhd_release_request_queue(queue);
    }
    vhd_stop_vhost_server();

    // 5. Release related resources
    io_destroy(Io);
    Devices.clear();

    STORAGE_INFO("Server has been stopped.");

    DeleteSocket(SocketPath);
}

TSimpleStats TServer::GetStats()
{
    NeedUpdateCompletionStats = true;
    CompletionStatsEvent.WaitI();

    TSimpleStats stats = CompletionStats;

    for (ui32 i = 0; i != Queues.size(); ++i) {
        stats += QueueStats[i];
    }

    return stats;
}

void TServer::QueueThreadFunc(ui32 queueIndex)
{
    NCloud::SetCurrentThreadName("VHOST");

    vhd_request_queue* queue = Queues[queueIndex];

    TVector<iocb*> batch;
    batch.reserve(BatchSize);

    TSimpleStats queueStats;

    for (;;) {
        int ret = vhd_run_queue(queue);

        if (ret != -EAGAIN) {
            if (ret < 0) {
                STORAGE_ERROR("vhd_run_queue error: %d", ret);
            }
            break;
        }

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

                queueStats.SubFailed++;

                if (batch[0]->data) {
                    CompleteRequest(
                        batch[0],
                        static_cast<TCompoundRequest*>(batch[0]->data),
                        VHD_BDEV_IOERR,
                        queueStats,
                        now);
                } else {
                    CompleteRequest(
                        static_cast<TRequest*>(batch[0]),
                        VHD_BDEV_IOERR,
                        queueStats,
                        now);
                }

                ret = 1;
            }

            queueStats.Submitted += ret;

#ifndef NDEBUG
            STORAGE_DEBUG("submitted " << ret << ": "
                << ToString(std::span(batch.data(), ret)));
#endif
            // remove submitted items from the batch
            batch.erase(batch.begin(), batch.begin() + ret);
        }

        SyncQueueStats(queueIndex, queueStats);
    }
}

void TServer::CompletionThreadFunc()
{
    NCloud::SetCurrentThreadName("AIO");

    thread_local sig_atomic_t shouldStop;

    struct sigaction stopAction = {};
    stopAction.sa_handler = [] (int) {
        shouldStop = 1;
    };
    sigaction(SIGUSR2, &stopAction, nullptr);

    TVector<io_event> events(BatchSize);

    TSimpleStats stats;
    timespec timeout { .tv_sec = 1 };

    for (;;) {
        // TODO: try AIO_RING_MAGIC trick (https://github.com/axboe/fio/blob/master/engines/libaio.c#L272)
        int ret = io_getevents(Io, 1, events.size(), events.data(), &timeout);

        if (ret < 0 && ret != -EINTR) {
            Y_ABORT("io_getevents: %s", strerror(-ret));
        }

        SyncCompletionStats(stats);

        if (shouldStop) {
            break;
        }

        if (ret == -EINTR) {
            continue;
        }

        const TCpuCycles now = GetCycleCount();

        for (int i = 0; i != ret; ++i) {
            if (events[i].data) {
                auto* req = static_cast<TCompoundRequest*>(events[i].data);
                iocb* sub = events[i].obj;

                vhd_bdev_io_result result = VHD_BDEV_SUCCESS;

                if (events[i].res2 != 0 ||
                    events[i].res != sub->u.c.nbytes)
                {
                    stats.CompFailed += 1;
                    result = VHD_BDEV_IOERR;
                    STORAGE_ERROR("IO request: %s", strerror(-events[i].res));
                }

                CompleteRequest(sub, req, result, stats, now);

                continue;
            }

            auto* req = static_cast<TRequest*>(events[i].obj);

            vhd_bdev_io_result result = VHD_BDEV_SUCCESS;
            auto* bio = vhd_get_bdev_io(req->Io);

            if (events[i].res2 != 0 ||
                events[i].res != bio->total_sectors * VHD_SECTOR_SIZE)
            {
                stats.CompFailed += 1;
                result = VHD_BDEV_IOERR;
                STORAGE_ERROR("IO request: %s", strerror(-events[i].res));
            }

            CompleteRequest(req, result, stats, now);
        }

        stats.Completed += ret;
    }
}
void TServer::InitBackend(const TOptions& options)
{
    EOpenMode flags =
          EOpenModeFlag::OpenExisting
        | EOpenModeFlag::DirectAligned
        | (options.ReadOnly
            ? EOpenModeFlag::RdOnly
            : EOpenModeFlag::RdWr);

    if (!options.NoSync) {
        flags |= EOpenModeFlag::Sync;
    }

    Devices.reserve(options.Layout.size());

    i64 totalBytes = 0;

    for (auto& chunk: options.Layout) {
        TFileHandle file {
            chunk.FilePath,
            flags
        };

        if (!file.IsOpen()) {
            int ret = errno;
            Y_ABORT("can't open %s: %s", chunk.FilePath.c_str(), strerror(ret));
        }

        i64 fileLen = file.Seek(0, sEnd);

        Y_ABORT_UNLESS(fileLen,
            "unable to retrive size of file %s",
            chunk.FilePath.Quote().c_str());

        Y_ABORT_UNLESS(
            !chunk.Offset || fileLen > chunk.Offset,
            "%s: file is too small (%ld B) or the offset is too big (%ld B)",
            chunk.FilePath.Quote().c_str(),
            fileLen,
            chunk.Offset);

        fileLen -= chunk.Offset;

        if (chunk.ByteCount) {
            Y_ABORT_UNLESS(
                fileLen >= chunk.ByteCount,
                "%s: file is too small (%ld B) expected at least %ld B",
                chunk.FilePath.Quote().c_str(),
                fileLen,
                chunk.ByteCount);

            fileLen = chunk.ByteCount;
        }

        Y_ABORT_UNLESS(
            fileLen % VHD_SECTOR_SIZE == 0,
            "%s: file size is not a multiple of the block size.",
            chunk.FilePath.Quote().c_str());

        STORAGE_INFO(
            "File %s (%s) %s, offset: %ld",
            chunk.FilePath.Quote().c_str(),
            DecodeOpenMode(flags).c_str(),
            FormatByteSize(fileLen).c_str(),
            chunk.Offset);

        Devices.push_back(TDevice {
            .StartOffset = totalBytes,
            .EndOffset = totalBytes + fileLen,
            .File = std::move(file),
            .FileOffset = chunk.Offset
        });

        totalBytes += fileLen;
    }

    Info = {
        .serial = options.Serial.c_str(),
        .socket_path = options.SocketPath.c_str(),
        .block_size = VHD_SECTOR_SIZE,
        .num_queues = options.QueueCount, // Max count of virtio queues
        .total_blocks = totalBytes / VHD_SECTOR_SIZE,
        .features = options.ReadOnly ? VHD_BDEV_F_READONLY : 0
    };
}


void TServer::SyncQueueStats(ui32 queueIndex, const TSimpleStats& queueStats)
{
    auto& stats = QueueStats[queueIndex];

    stats.Dequeued = queueStats.Dequeued;
    stats.Submitted = queueStats.Submitted;
    stats.SubFailed = queueStats.SubFailed;

    stats.Requests[0] = queueStats.Requests[VHD_BDEV_READ];
    stats.Requests[1] = queueStats.Requests[VHD_BDEV_WRITE];
}

void TServer::SyncCompletionStats(const TSimpleStats& stats)
{
    if (!NeedUpdateCompletionStats) {
        return;
    }

    CompletionStats.Completed = stats.Completed;
    CompletionStats.CompFailed = stats.CompFailed;

    CompletionStats.Requests = stats.Requests;
    CompletionStats.Times = stats.Times;
    CompletionStats.Sizes = stats.Sizes;

    NeedUpdateCompletionStats = false;
    CompletionStatsEvent.Signal();
}

size_t TServer::PrepareBatch(
    vhd_request_queue* queue,
    TVector<iocb*>& batch,
    TCpuCycles now)
{
    const size_t size = batch.size();

    vhd_request req;
    while (batch.size() < BatchSize && vhd_dequeue_request(queue, &req)) {
        Y_DEBUG_ABORT_UNLESS(vhd_vdev_get_priv(req.vdev) == this);

        PrepareIO(Log, Devices, req.io, batch, now);
    }

    return batch.size() - size;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IServer> CreateServer(ILoggingServicePtr logging)
{
    return std::make_shared<TServer>(std::move(logging));
}

}   // namespace NCloud::NBlockStore::NVHostServer
