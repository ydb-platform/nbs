#pragma once

#include "options.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

int AppMain(const TOptions& options);
void AppStop();

}   // namespace NCloud::NBlockStore
