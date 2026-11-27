#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <cloud/storage/core/libs/common/error.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EXattrAclType {
    Undefined,
    Extended,
    Minimal,
    // A header-only value is equivalent to the absence of an ACL.
    HeaderOnly
};

struct TNodeXattrAcl {
    TString Acl;
    ui32 Mode = 0;
    EXattrAclType Type = EXattrAclType::Undefined;
};

TResultOrError<TNodeXattrAcl> ReconcilePosixAclWithMode(
    const TString& xattrAcl,
    const ui32& srcMode,
    const bool overwriteAclPermsFromMode,
    const bool dontCalculateXattrAcl);

inline TResultOrError<TNodeXattrAcl> GetChildXattrAcl(
    const TString& xattrAcl,
    const ui32 mode)
{
    return ReconcilePosixAclWithMode(
        xattrAcl,
        mode,
        false /* overwriteAclPermsFromMode */,
        false /* dontCalculateXattrAcl */);
}

inline TResultOrError<TNodeXattrAcl> RewriteXattrAclPermByMode(
    const TString& xattrAcl, const ui32 mode)
{
    return ReconcilePosixAclWithMode(
        xattrAcl,
        mode,
        true /* overwriteAclPermsFromMode */,
        false /* dontCalculateXattrAcl */);
}

inline TResultOrError<TNodeXattrAcl> GetModeByXattrAcl(const TString& xattrAcl)
{
    return ReconcilePosixAclWithMode(
        xattrAcl,
        S_IRWXU | S_IRWXG | S_IRWXO,
        false /* overwriteAclPermsFromMode */,
        true /* dontCalculateXattrAcl */);
}

constexpr TStringBuf PosixAclAccessXAttr = "system.posix_acl_access";
constexpr TStringBuf PosixAclDefaultXAttr = "system.posix_acl_default";

}   // namespace NCloud::NFileStore::NStorage
