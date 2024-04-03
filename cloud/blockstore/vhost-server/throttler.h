#pragma once

#include <cloud/contrib/vhost/include/vhost/blockdev.h>
#include <cloud/storage/core/libs/common/timer.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct IThrottler
{
    virtual ~IThrottler() = default;

    virtual bool HasThrottledIos() const = 0;
    virtual vhd_io* ResumeNextThrottledIo() = 0;
    virtual bool ThrottleIo(struct vhd_io* io) = 0;
    virtual void Stop() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IThrottler> CreateThrottler(
    ITimerPtr timer,
    ui32 maxReadBandwidth,
    ui32 maxReadIops,
    ui32 maxWriteBandwidth,
    ui32 maxWriteIops);

}   // namespace NCloud::NBlockStore::NVHostServer
