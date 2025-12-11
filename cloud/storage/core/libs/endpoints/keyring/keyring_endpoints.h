#pragma once

#include <cloud/storage/core/libs/endpoints/iface/endpoints.h>

#include <util/generic/string.h>

namespace NCloud {

IEndpointStoragePtr CreateKeyringEndpointStorage(
    TString rootKeyringDesc,
    TString endpointsKeyringDesc,
    bool notImplementedErrorIsFatal);

}   // namespace NCloud
