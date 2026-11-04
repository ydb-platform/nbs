#pragma once

#include <silk/fibers/event.h>

#include <util/datetime/base.h>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

// Sleeps until @p event is set or @p timeout passes; true if it was set.
bool WaitFor(silk::FiberEvent& event, TDuration timeout);

}   // namespace NCloud::NFastShard
