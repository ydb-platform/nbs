#pragma once

#include "private.h"

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

int AppMain(TBootstrap& bootstrap);
void AppStop(int exitCode);

}   // namespace NCloud::NBlockStore::NLoadTest
