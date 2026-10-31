#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EXattrAclType {
    Undefined,
    Extended,
    Minimal,
    // A header-only value is equivalent to the absence of an ACL.
    HeaderOnly
};

struct TChildXattrAcl {
    TString XAttraAcl;
    ui32 Mode = 0;
    EXattrAclType Type = EXattrAclType::Undefined;
};

TResultOrError<TChildXattrAcl> GetChildXattrAcl(
    const TString& xattrAcl,
    const ui32& mode);

constexpr TStringBuf PosixAclAccessXAttr = "system.posix_acl_access";
constexpr TStringBuf PosixAclDefaultXAttr = "system.posix_acl_default";

}   // namespace NCloud::NFileStore::NStorage
