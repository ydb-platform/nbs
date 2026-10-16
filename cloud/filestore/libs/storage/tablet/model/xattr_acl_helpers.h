#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NProto::TError GetChildXattrAcl(TString& xattrAcl, ui32& mode);

constexpr TStringBuf PosixAclAccessXAttr = "system.posix_acl_access";
constexpr TStringBuf PosixAclDefaultXAttr = "system.posix_acl_default";

}   // namespace NCloud::NFileStore::NStorage
