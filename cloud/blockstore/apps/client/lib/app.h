#pragma once

#include "factory.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TCommandPtr Handler = nullptr;

public:
    static TApp& Instance();
    void Shutdown();
    int Run(
        std::shared_ptr<TClientFactories> clientFactories,
        int argc,
        const char* argv[]);
};

void Shutdown(int signum);
void ConfigureSignals();

} // namespace
