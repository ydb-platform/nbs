#pragma once

#include <cloud/storage/core/libs/endpoints/iface/endpoints.h>
#include <cloud/storage/core/libs/endpoints/iface/public.h>

#include <util/generic/string.h>

namespace NCloud {

IEndpointStoragePtr CreateFileEndpointStorage(TString dirPath);

}   // namespace NCloud
