#pragma once

#include "loadtest.h"

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    ILoadTestPtr LoadTest;

public:
    static TApp& Instance();

    /**
     * Parses the options, brings the silk runtime up, runs the load,
     * prints the results and tears the runtime down.
     *
     * @return - Process exit code: 0 if every request succeeded, 1
     *           otherwise.
     */
    int Run(int argc, const char* argv[]);

    // Asks the running load to stop; called from the signal handler.
    void Shutdown();
};

void Shutdown(int signum);
void ConfigureSignals();

}   // namespace NCloud::NFastShard::NLoadTest
