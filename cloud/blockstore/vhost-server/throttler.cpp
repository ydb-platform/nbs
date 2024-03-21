#include "throttler.h"

#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottler final: public IThrottler
{
private:
    ITimerPtr Timer;
    TLeakyBucket Bucket;
    ui32 MaxReadBandwidth;
    ui32 MaxReadIops;
    ui32 MaxWriteBandwidth;
    ui32 MaxWriteIops;
    TDeque<vhd_io*> PostponedIos;
    bool IsStopped = false;

public:
    TThrottler(
        ITimerPtr timer,
        ui32 maxReadBandwidth,
        ui32 maxReadIops,
        ui32 maxWriteBandwidth,
        ui32 maxWriteIops);
    ~TThrottler() override;
    bool HasThrottledIos() const override;
    vhd_io* ResumeNextThrottledIo() override;
    bool ThrottleIo(struct vhd_io* io) override;
    void Stop() override;

private:
    bool IsIoThrottled(struct vhd_io* io);
};

////////////////////////////////////////////////////////////////////////////////

TThrottler::TThrottler(
    ITimerPtr timer,
    ui32 maxReadBandwidth,
    ui32 maxReadIops,
    ui32 maxWriteBandwidth,
    ui32 maxWriteIops)
    : Timer(std::move(timer))
    , Bucket(1.0, 1.0, 1.0)
    , MaxReadBandwidth(maxReadBandwidth)
    , MaxReadIops(maxReadIops)
    , MaxWriteBandwidth(maxWriteBandwidth)
    , MaxWriteIops(maxWriteIops)

{}

TThrottler::~TThrottler()
{}

bool TThrottler::IsIoThrottled(struct vhd_io* io)
{
    if (IsStopped) {
        return false;
    }

    auto* bio = vhd_get_bdev_io(io);
    ui32 maxBandwidth =
        bio->type == VHD_BDEV_WRITE ? MaxWriteBandwidth : MaxReadBandwidth;
    ui32 maxIops = bio->type == VHD_BDEV_WRITE ? MaxWriteIops : MaxReadIops;
    ui64 bytesCount = bio->total_sectors * VHD_SECTOR_SIZE;

    auto seconds = Bucket.Register(
        Timer->Now(),
        CostPerIO(
            CalculateThrottlerC1(maxIops, maxBandwidth),
            CalculateThrottlerC2(maxIops, maxBandwidth),
            bytesCount).MicroSeconds() / 1e6);
    if (seconds == 0) {
        return false;
    }

    return true;
}

bool TThrottler::HasThrottledIos() const
{
    return !PostponedIos.empty();
}

vhd_io* TThrottler::ResumeNextThrottledIo()
{
    auto io = PostponedIos.front();
    if (!IsIoThrottled(io)) {
        PostponedIos.pop_front();
        return io;
    }

    return nullptr;
}

bool TThrottler::ThrottleIo(struct vhd_io* io)
{
    if (IsIoThrottled(io)) {
        PostponedIos.push_back(io);
        return true;
    }

    return false;
}

void TThrottler::Stop()
{
    IsStopped = true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IThrottler> CreateThrottler(
    ITimerPtr timer,
    ui32 maxReadBandwidth,
    ui32 maxReadIops,
    ui32 maxWriteBandwidth,
    ui32 maxWriteIops)

{
    return std::make_shared<TThrottler>(
        std::move(timer),
        maxReadBandwidth,
        maxReadIops,
        maxWriteBandwidth,
        maxWriteIops);
}

}   // namespace NCloud::NBlockStore::NVHostServer
