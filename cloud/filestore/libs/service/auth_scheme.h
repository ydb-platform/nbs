#pragma once

#include "public.h"

#include "request.h"

#include <cloud/storage/core/libs/auth/auth_scheme.h>


namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TPermissionList GetRequestPermissions(EFileStoreRequest requestType);

template <typename T>
TPermissionList GetRequestPermissions(const T& request)
{
    Y_UNUSED(request);
    return GetRequestPermissions(GetFileStoreServiceRequest<T>());
}

TPermissionList GetRequestPermissions(
    const NProto::TExecuteActionRequest& request);

}   // namespace NCloud::NFileStore
