#pragma once

#include "public.h"

#include "request.h"

#include <cloud/storage/core/libs/auth/auth_scheme.h>


namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TPermissionList GetRequestPermissions(EFileStoreRequest requestType);

template <typename T>
TPermissionList GetRequestPermissions(
    const T& request,
    const TVector<TString>& actionsNoAuth)
{
    Y_UNUSED(request, actionsNoAuth);
    return GetRequestPermissions(GetFileStoreRequest<T>());
}

TPermissionList GetRequestPermissions(
    const NProto::TExecuteActionRequest& request, const TVector<TString>& actionsNoAuth);

}   // namespace NCloud::NFileStore
