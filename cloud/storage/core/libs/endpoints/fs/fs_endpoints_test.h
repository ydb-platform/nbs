#pragma once

#include <cloud/storage/core/libs/endpoints/iface/endpoints_test.h>
#include <cloud/storage/core/libs/endpoints/iface/public.h>

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

IMutableEndpointStoragePtr CreateFileMutableEndpointStorage(TString dirPath);

}   // namespace NCloud
