#pragma once

#include "private.h"

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

// Runs the load test and writes its results. Returns the exit code of the
// process.
int AppMain(TBootstrap& bootstrap);

// Makes the running test finish. Safe to call from a signal handler or
// another thread.
void AppStop(int exitCode);

}   // namespace NCloud::NFastShard::NLoadTest
