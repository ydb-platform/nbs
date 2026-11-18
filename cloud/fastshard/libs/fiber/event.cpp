#include "event.h"

#include <silk/fibers/future.h>

#include <cerrno>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

bool WaitFor(silk::FiberEvent& event, TDuration timeout)
{
    if (event.isSet()) {
        return true;
    }

    silk::FiberEvent::Future set;
    event.wait(&set);
    int r = silk::FiberFuture::waitWithTimeout(&set, timeout.NanoSeconds());
    if (r != ETIMEDOUT) {
        return true;
    }

    set.cancel();
    return set.wait() == 0;
}

}   // namespace NCloud::NFastShard
