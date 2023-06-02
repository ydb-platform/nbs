#pragma once

#include "public.h"

#include <cloud/blockstore/libs/vhost/public.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

NVhost::TVhostCallbacks VhostCallbacks();

}   // namespace NCloud::NBlockStore::NSpdk
