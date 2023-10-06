#pragma once

#include <util/generic/bitmap.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud {

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

}   // namespace NCloud
