#pragma once

#include "public.h"
#include "options.h"

#include <contrib/ydb/library/actors/util/should_continue.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    TOptions Options;
    IServerPtr Server;
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
