#pragma once

#include "command.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TCommandPtr Command;

public:
    static TApp& Instance();

    /**
     * Picks the command named by argv[1], brings the silk runtime up,
     * runs the command and tears the runtime down.
     *
     * @return - Process exit code: 0 on success, 1 on any failure.
     */
    int Run(int argc, const char* argv[]);

    // Asks the running command to stop; called from the signal handler.
    void Shutdown();
};

void Shutdown(int signum);
void ConfigureSignals();

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
