#pragma once

#include "public.h"

#include <cloud/blockstore/libs/spdk/iface/env.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

void InitLogging(const TLog& log);

ISpdkEnvPtr CreateEnv(TSpdkEnvConfigPtr config);

}   // namespace NCloud::NBlockStore::NSpdk
