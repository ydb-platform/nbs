#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

ISpdkEnvPtr CreateEnvStub();
ISpdkDevicePtr CreateDeviceStub();
ISpdkTargetPtr CreateTargetStub();

}   // namespace NCloud::NBlockStore::NSpdk
