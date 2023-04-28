#pragma once

#include "public.h"

#include "request.h"
#include "request_helpers.h"

#include <util/generic/bitmap.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class EPermission
{
    Get = 0,
    List,
    Create,
    Update,
    Delete,
    Read,
    Write,

    MAX
};

using TPermissionList = TBitMap<static_cast<size_t>(EPermission::MAX)>;

TPermissionList CreatePermissionList(
    std::initializer_list<EPermission> permissions);

TVector<TString> GetPermissionStrings(const TPermissionList& permissions);

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
