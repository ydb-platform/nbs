#pragma once

namespace NCloud::NFastShard::NJdServer {

////////////////////////////////////////////////////////////////////////////////

/**
 * Parses the options, starts the journalled device server over the file
 * device and serves until SIGINT or SIGTERM arrives.
 *
 * @return - Process exit code: 0 if the server was started and stopped
 *           cleanly, 1 otherwise.
 */
int Run(int argc, const char* argv[]);

void ConfigureSignals();

}   // namespace NCloud::NFastShard::NJdServer
