#pragma once

#include <cloud/storage/core/libs/endpoints/iface/endpoints_test.h>
#include <cloud/storage/core/libs/endpoints/iface/public.h>

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

NProto::TError CreateEndpointsDirectory(const TString& dirPath);

NProto::TError CleanUpEndpointsDirectory(const TString& dirPath);

}   // namespace NCloud
