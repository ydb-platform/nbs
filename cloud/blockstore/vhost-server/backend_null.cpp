#include "backend_null.h"

#include "backend.h"
#include "latency_tracker.h"

#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNullBackend final: public IBackend
{
private:
    const ILoggingServicePtr Logging;
    TLog Log;

    ui32 BlockSize = 0;
    TLatencyTracker LatencyTracker;

public:
    explicit TNullBackend(ILoggingServicePtr logging);

    vhd_bdev_info Init(const TOptions& options) override;
    void Start() override;
    void Stop() override;
    void ProcessQueue(
        ui32 queueIndex,
        vhd_request_queue* queue,
        TSimpleStats& queueStats) override;
    std::optional<TSimpleStats> GetCompletionStats(TDuration timeout) override;
};

////////////////////////////////////////////////////////////////////////////////

TNullBackend::TNullBackend(ILoggingServicePtr logging)
    : Logging{std::move(logging)}
{
    Log = Logging->CreateLog("NULL");
}

vhd_bdev_info TNullBackend::Init(const TOptions& options)
{
    Y_UNUSED(options);

    ui64 totalBytes = 0;

    for (const auto& chunk: options.Layout) {
        totalBytes += chunk.ByteCount;
    }
    BlockSize = options.BlockSize;
    LatencyTracker = TLatencyTracker(
        options.LatencyTrackingEnabled,
        options.LatencyThresholds);

    return {
        .serial = options.Serial.c_str(),
        .socket_path = options.SocketPath.c_str(),
        .block_size = BlockSize,
        .num_queues = options.QueueCount,   // Max count of virtio queues
        .total_blocks = totalBytes / BlockSize,
        .features = options.ReadOnly ? VHD_BDEV_F_READONLY : 0,
        .pte_flush_byte_threshold = options.PteFlushByteThreshold};
}

void TNullBackend::Start()
{}

void TNullBackend::Stop()
{}

void TNullBackend::ProcessQueue(
    ui32 queueIndex,
    vhd_request_queue* queue,
    TSimpleStats& queueStats)
{
    Y_UNUSED(queueIndex);
    vhd_request req{};
    while (vhd_dequeue_request(queue, &req)) {
        const TCpuCycles started =
            LatencyTracker.IsEnabled() ? GetCycleCount() : 0;
        vhd_bdev_io* bio = vhd_get_bdev_io(req.io);
        STORAGE_DEBUG(
            "%s Index=%lu, BlocksCount=%lu, BlockSize=%u",
            bio->type == VHD_BDEV_READ ? "READ" : "WRITE",
            bio->first_sector,
            bio->total_sectors,
            BlockSize);

        if (LatencyTracker.IsEnabled()) {
            LatencyTracker.Record(
                queueStats,
                bio->type,
                bio->total_sectors * VHD_SECTOR_SIZE,
                GetCycleCount() - started,
                ELatencyCompletion::Success);
        }

        vhd_complete_bio(req.io, VHD_BDEV_SUCCESS);
    }
}

std::optional<TSimpleStats> TNullBackend::GetCompletionStats(TDuration timeout)
{
    Y_UNUSED(timeout);
    return LatencyTracker.IsEnabled()
        ? std::optional<TSimpleStats>(TSimpleStats{})
        : std::nullopt;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateNullBackend(ILoggingServicePtr logging)
{
    return std::make_shared<TNullBackend>(std::move(logging));
}

}   // namespace NCloud::NBlockStore::NVHostServer
