#pragma once

#include "public.h"

#include <cloud/filestore/libs/spdk/iface/env.h>

namespace NCloud::NFileStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

void InitLogging(const TLog& log);

ISpdkEnvPtr CreateEnv(TSpdkEnvConfigPtr config);

}   // namespace NCloud::NFileStore::NSpdk
