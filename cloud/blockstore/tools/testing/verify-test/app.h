#pragma once

#include "private.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

int AppMain(TOptionsPtr options);
void AppStop(int exitCode);

}   // namespace NCloud::NBlockStore
