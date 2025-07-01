#include "backend_aio.h"

#include "backend.h"
#include "request_aio.h"

#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/stream/file.h>
#include <util/system/sanitizers.h>

#include <libaio.h>

#include <thread>
#include <utility>

namespace NCloud::NBlockStore::NVHostServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

void CompleteRequest(
    TLog& log,
    IEncryptor* encryptor,
    TAioRequestHolder req,
    vhd_bdev_io_result status,
    TSimpleStats& stats,
    TCpuCycles now)
{
    auto* bio = vhd_get_bdev_io(req->Io);
    const ui64 bytes = bio->total_sectors * VHD_SECTOR_SIZE;

    auto& requestStat = stats.Requests[bio->type];
    requestStat.Errors += status != VHD_BDEV_SUCCESS;
    requestStat.Count += status == VHD_BDEV_SUCCESS;
    requestStat.Bytes += bytes;
    requestStat.Unaligned += req->Unaligned;

    if (status == VHD_BDEV_SUCCESS) {
        stats.Times[bio->type].Increment(now - req->SubmitTs);
        stats.Sizes[bio->type].Increment(bytes);
    }

    if (req->BufferAllocated || encryptor) {
        if (bio->type == VHD_BDEV_READ && status == VHD_BDEV_SUCCESS) {
            NSan::Unpoison(req->Data[0].iov_base, req->Data[0].iov_len);
            const bool success = SgListCopyWithOptionalDecryption(
                log,
                static_cast<const char*>(req->Data[0].iov_base),
                bio->sglist,
                encryptor,
                bio->first_sector);
            if (!success) {
                status = VHD_BDEV_IOERR;
                stats.EncryptorErrors++;
            }
        }
    }
    vhd_complete_bio(req->Io, status);
}

void CompleteCompoundRequest(
    TLog& log,
    IEncryptor* encryptor,
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

        auto& requestStat = stats.Requests[bio->type];
        requestStat.Errors += req->Errors != 0;
        requestStat.Count += 1;
        requestStat.Bytes += bytes;

        if (status == VHD_BDEV_SUCCESS) {
            stats.Times[bio->type].Increment(now - req->SubmitTs);
            stats.Sizes[bio->type].Increment(bytes);
        }

        if (bio->type == VHD_BDEV_READ && status == VHD_BDEV_SUCCESS) {
            NSan::Unpoison(req->Buffer.get(), bytes);
            const bool success = SgListCopyWithOptionalDecryption(
                log,
                req->Buffer.get(),
                bio->sglist,
                encryptor,
                bio->first_sector);
            if (!success) {
                status = VHD_BDEV_IOERR;
                stats.EncryptorErrors++;
            }
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

    IEncryptorPtr Encryptor;
    TVector<TAioDevice> Devices;

    io_context_t Io = {};
    ui32 BlockSize = 0;

    ui32 BatchSize = 0;
    TVector<TVector<iocb*>> Batches;

    std::thread CompletionThread;

    ICompletionStatsPtr CompletionStats;

public:
    TAioBackend(IEncryptorPtr encryptor, ILoggingServicePtr logging);

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
        TCpuCycles now,
        TSimpleStats& queueStats);

    void CompletionThreadFunc();

    void IoSetup();
};

////////////////////////////////////////////////////////////////////////////////

TAioBackend::TAioBackend(
        IEncryptorPtr encryptor,
        ILoggingServicePtr logging)
    : Logging{std::move(logging)}
    , Encryptor(std::move(encryptor))
    , CompletionStats(CreateCompletionStats())
{
    Log = Logging->CreateLog("AIO");
}

void TAioBackend::IoSetup()
{
    const auto waitTime = TDuration::MilliSeconds(100);
    const int maxIterations = 1000;

    int iterations = 0;
    int error = 0;

    for (; iterations != maxIterations; ++iterations) {
        error = io_setup(BatchSize, &Io);
        if (error != -EAGAIN) {
            break;
        }

        const auto aioNr = TIFStream("/proc/sys/fs/aio-nr").ReadLine();
        const auto aioMaxNr = TIFStream("/proc/sys/fs/aio-max-nr").ReadLine();

        STORAGE_WARN(
            "retrying EAGAIN from io_setup, BatchSize: "
            << BatchSize << ", aio-nr/aio-max-nr: " << aioNr << "/"
            << aioMaxNr);

        Sleep(waitTime);
    }
    Y_ABORT_UNLESS(!error, "io_setup: %d, iterations: %d", error, iterations);
}

vhd_bdev_info TAioBackend::Init(const TOptions& options)
{
    STORAGE_INFO("Initializing AIO backend");
    if (Encryptor) {
        STORAGE_INFO("Encryption enabled");
    }
    BatchSize = options.BatchSize;

    IoSetup();

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

    BlockSize = options.BlockSize;
    STORAGE_VERIFY(
        BlockSize >= 512 && IsPowerOf2(BlockSize),
        TWellKnownEntityTypes::ENDPOINT,
        options.ClientId);

    Devices.reserve(options.Layout.size());

    ui64 totalBytes = 0;

    for (const auto& chunk: options.Layout) {
        TFileHandle file{chunk.DevicePath, flags};

        if (!file.IsOpen()) {
            int ret = errno;
            STORAGE_ERROR(
                "can't open %s: %s",
                chunk.DevicePath.c_str(),
                strerror(ret));

            Devices.push_back(TAioDevice{
                .StartOffset = totalBytes,
                .EndOffset = totalBytes + chunk.ByteCount,
                .File = std::move(file),
                .FileOffset = chunk.Offset,
                .BlockSize = BlockSize,
                .NullBackend = true});

            totalBytes += chunk.ByteCount;
            continue;
        }

        ui64 fileLen = file.Seek(0, sEnd);

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
            fileLen % BlockSize == 0,
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
            .FileOffset = chunk.Offset,
            .BlockSize = BlockSize});

        totalBytes += fileLen;
    }

    return {
        .serial = options.Serial.c_str(),
        .socket_path = options.SocketPath.c_str(),
        .block_size = BlockSize,
        .num_queues = options.QueueCount,   // Max count of virtio queues
        .total_blocks = totalBytes / BlockSize,
        .features = options.ReadOnly ? VHD_BDEV_F_READONLY : 0,
        .pte_flush_byte_threshold = options.PteFlushByteThreshold};
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
        queueStats.Dequeued += PrepareBatch(queue, batch, now, queueStats);

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
                    Log,
                    Encryptor.get(),
                    TAioSubRequest::FromIocb(batch[0]),
                    VHD_BDEV_IOERR,
                    queueStats,
                    now);
            } else {
                CompleteRequest(
                    Log,
                    Encryptor.get(),
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
    TCpuCycles now,
    TSimpleStats& queueStats)
{
    const size_t initialSize = batch.size();

    vhd_request req{};
    while (batch.size() < BatchSize && vhd_dequeue_request(queue, &req)) {
        Y_DEBUG_ABORT_UNLESS(vhd_vdev_get_priv(req.vdev) == this);

        PrepareIO(
            Log,
            Encryptor.get(),
            Devices,
            req.io,
            batch,
            now,
            queueStats);
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
                    Log,
                    Encryptor.get(),
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

            CompleteRequest(
                Log,
                Encryptor.get(),
                std::move(req),
                result,
                stats,
                now);
        }

        stats.Completed += ret;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateAioBackend(
    IEncryptorPtr encryptor,
    ILoggingServicePtr logging)
{
    return std::make_shared<TAioBackend>(
        std::move(encryptor),
        std::move(logging));
}

}   // namespace NCloud::NBlockStore::NVHostServer
