#pragma once

#include "options.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

int AppMain(TOptions options);
void AppStop();

}   // namespace NCloud::NBlockStore
