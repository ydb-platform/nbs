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

    int Run(int argc, const char* argv[]);
    void Shutdown();
};

void Shutdown(int signum);
void ConfigureSignals();

}   // namespace NCloud::NFastShard::NLoadTest
