#include "auth_scheme.h"

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ToString(EPermission permission)
{
    switch (permission) {
        case EPermission::Get:
            return "nbsInternal.disks.get";
        case EPermission::List:
            return "nbsInternal.disks.list";
        case EPermission::Create:
            return "nbsInternal.disks.create";
        case EPermission::Update:
            return "nbsInternal.disks.update";
        case EPermission::Delete:
            return "nbsInternal.disks.delete";
        case EPermission::Read:
            return "nbsInternal.disks.read";
        case EPermission::Write:
            return "nbsInternal.disks.write";
        case EPermission::MAX:
            Y_ABORT("EPermission::MAX is invalid");
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPermissionList CreatePermissionList(
    std::initializer_list<EPermission> permissions)
{
    TPermissionList result;
    for (EPermission permission: permissions) {
        result.Set(static_cast<size_t>(permission));
    }
    return result;
}

TVector<TString> GetPermissionStrings(const TPermissionList& permissions)
{
    TVector<TString> result;
    Y_FOR_EACH_BIT(permission, permissions)
    {
        result.push_back(ToString(static_cast<EPermission>(permission)));
    }
    return result;
}

}   // namespace NCloud
