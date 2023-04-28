#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

int RegisterDeviceProxy(spdk_bdev* bdev, const TString& name);

}   // namespace NCloud::NBlockStore::NSpdk
