#pragma once

#include "public.h"

#include "request.h"
#include "request_helpers.h"

#include <cloud/storage/core/libs/auth/auth_scheme.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TPermissionList GetRequestPermissions(EBlockStoreRequest requestType);

template <typename T>
TPermissionList GetRequestPermissions(const T& request)
{
    Y_UNUSED(request);
    return GetRequestPermissions(GetBlockStoreRequest<T>());
}

TPermissionList GetRequestPermissions(
    const NProto::TMountVolumeRequest& request);

TPermissionList GetRequestPermissions(
    const NProto::TExecuteActionRequest& request);

}   // namespace NCloud::NBlockStore
