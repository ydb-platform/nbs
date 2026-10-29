#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EXattrAclType {
    Extended,
    Minimal,
    // A header-only value is equivalent to the absence of an ACL.
    HeaderOnly,
    Malformed,
};

NProto::TError GetChildXattrAcl(
    TString& xattrAcl,
    ui32& mode,
    EXattrAclType& xattrAclType
);

constexpr TStringBuf PosixAclAccessXAttr = "system.posix_acl_access";
constexpr TStringBuf PosixAclDefaultXAttr = "system.posix_acl_default";

}   // namespace NCloud::NFileStore::NStorage
