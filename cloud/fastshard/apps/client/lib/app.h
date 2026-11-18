#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TCommandPtr Command;

public:
    static TApp& Instance();

    int Run(int argc, const char* argv[]);
    void Shutdown();
};

void Shutdown(int signum);
void ConfigureSignals();

}   // namespace NCloud::NFastShard::NClient
