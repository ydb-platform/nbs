#pragma once

#include "public.h"
#include "options.h"

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/actors/util/should_continue.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    TOptions Options;
    ISchedulerPtr Scheduler;
    IEndpointProxyServerPtr Server;
    TProgramShouldContinue ShouldContinue;

public:
    void ParseOptions(int argc, char** argv);
    void Init();

    void Start();
    void Stop();

    TProgramShouldContinue& GetShouldContinue()
    {
        return ShouldContinue;
    }
};

}   // namespace NCloud::NBlockStore::NServer
