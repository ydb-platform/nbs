#pragma once

#include "public.h"

#include <atomic>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

// State shared by the app and the tests it runs. The app sets the fields
// (also from a signal handler); the tests only read them.
struct TAppContext
{
    // Set when the run must end: the tests finish as soon as they can.
    std::atomic<bool> ShouldStop{false};
    std::atomic<int> ExitCode{0};
};

}   // namespace NCloud::NFastShard::NLoadTest
