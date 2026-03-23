#pragma once

#include "mpi.h"
#include "options.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals();

int AppMain(const TOptions& options, const TMpiContext& mpi);
void AppStop();

}   // namespace NCloud::NFileStore
