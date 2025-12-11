#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

int AppMain(TBootstrap& bootstrap);
void AppStop(int exitCode);

int DoMain(
    int argc,
    char** argv,
    std::shared_ptr<TModuleFactories> moduleFactories);

}   // namespace NCloud::NBlockStore::NLoadTest
