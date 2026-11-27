#pragma once

#include <contrib/libs/linux-headers/linux/posix_acl.h>
#include <contrib/libs/linux-headers/linux/posix_acl_xattr.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NAclTest {

extern const TString AccessAcl;
extern const TString DefaultAcl;

using TPosixAcl = TVector<posix_acl_xattr_entry>;

ui32 Permissions(TStringBuf value);

posix_acl_xattr_entry MakeAclEntry(
    ui16 tag,
    ui16 permissions,
    ui32 id = static_cast<ui32>(ACL_UNDEFINED_ID));

posix_acl_xattr_entry MakeAclEntry(
    ui16 tag,
    TStringBuf permissions,
    ui32 id = static_cast<ui32>(ACL_UNDEFINED_ID));

TString SerializeAcl(
    const TPosixAcl& entries,
    ui32 version = POSIX_ACL_XATTR_VERSION);

TPosixAcl DeserializeAcl(const TString& value);

}   // namespace NCloud::NFileStore::NStorage::NAclTest
