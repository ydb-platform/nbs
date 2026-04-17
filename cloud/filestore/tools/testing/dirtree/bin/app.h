#pragma once

#include "options.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

int AppMain(const TOptions& options);
void AppStop();

}   // namespace NCloud::NFileStore
