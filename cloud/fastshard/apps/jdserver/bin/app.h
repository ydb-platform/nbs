#pragma once

#include "private.h"

namespace NCloud::NFastShard::NJdServer {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

/**
 * Serves until SIGINT or SIGTERM arrives. The bootstrap must be started.
 *
 * @return - Process exit code.
 */
int AppMain(TBootstrap& bootstrap);

}   // namespace NCloud::NFastShard::NJdServer
