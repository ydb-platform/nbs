#pragma once

#include <cloud/fastshard/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <util/system/yassert.h>

#include <utility>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

// Starts a fiber on @p fiberMain with @p params; aborts if it cannot.
template <typename TParams>
void Spawn(
    int (*fiberMain)(TParams*) noexcept,
    TParams params,
    silk::FiberFuture* future)
{
    int r = silk::FiberScheduler::run(
        fiberMain,
        std::move(params),
        future);

    Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", SystemErrorText(r).c_str());
}

}   // namespace NCloud::NFastShard
