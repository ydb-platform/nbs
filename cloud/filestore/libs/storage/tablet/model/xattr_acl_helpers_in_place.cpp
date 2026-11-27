
// Convert a parent's system.posix_acl_default xattr value into the
// system.posix_acl_access value inherited by a newly created child.
// This is a userspace adaptation of posix_acl_from_xattr() and
// posix_acl_create_masq() from fs/posix_acl.c.
// The xattr representation and the host are assumed to be little-endian.

#include "xattr_acl_helpers.h"

#include <contrib/libs/linux-headers/linux/posix_acl.h>
#include <contrib/libs/linux-headers/linux/posix_acl_xattr.h>

#include <util/string/builder.h>

#include <util/system/types.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage {

namespace {

NProto::TError ReportMalformedPosixAcl(const TString& xattrAcl)
{
    return MakeError(
        E_FS_INVAL,
        TStringBuilder() << "Malformed POSIX ACL xattr: " << xattrAcl.Quote());
}

NProto::TError
PosixAclXattrCount(const char* xattrAcl, size_t size, size_t& count)
{
    const posix_acl_xattr_header* header =
        reinterpret_cast<const posix_acl_xattr_header*>(xattrAcl);

    if (size < sizeof(posix_acl_xattr_header)) {
        return ReportMalformedPosixAcl(TString(xattrAcl, size));
    }

    if (header->a_version != POSIX_ACL_XATTR_VERSION) {
        return MakeError(
            E_FS_NOTSUPP,
            TStringBuilder() << "Unsupported version of POSIX ACL xattr: "
                             << header->a_version);
    }

    size -= sizeof(posix_acl_xattr_header);
    if (size % sizeof(posix_acl_xattr_entry)) {
        return ReportMalformedPosixAcl(TString(xattrAcl, size));
    }
    count = size / sizeof(posix_acl_xattr_entry);
    return {};
}

NProto::TError PosixAclFromXattr(
    TString& valueStr,
    posix_acl_xattr_entry*& acls,
    size_t& count)
{
    // As we are going to change xattr value in place, we need to guarantee
    // that the string buffer is unique.
    char* value = valueStr.Detach();

    acls = nullptr;
    count = 0;
    NProto::TError error = PosixAclXattrCount(value, valueStr.size(), count);
    if (HasError(error)) {
        return error;
    } else if (count == 0) {
        return {};
    }

    posix_acl_xattr_header* header =
        reinterpret_cast<posix_acl_xattr_header*>(value);
    acls = reinterpret_cast<posix_acl_xattr_entry*>(header + 1);

    return {};
}

NProto::TError PosixAclCreateMasq(
    posix_acl_xattr_entry* acls,
    const size_t count,
    ui32& srcMode)
{
    posix_acl_xattr_entry* group_obj = nullptr;
    posix_acl_xattr_entry* mask_obj = nullptr;
    ui32 mode = srcMode;

    constexpr __le16 s_irwxo_16 = S_IRWXO;
    constexpr ui32 s_irwxo_32 = S_IRWXO;
    constexpr ui32 s_irwxu_32 = S_IRWXU;
    constexpr ui32 s_irwxg_32 = S_IRWXG;

    for (size_t i = 0; i < count; ++i) {
        posix_acl_xattr_entry& acl = acls[i];
        switch (acl.e_tag) {
            case ACL_USER_OBJ:
                acl.e_perm &= static_cast<__le16>(mode >> 6) | ~s_irwxo_16;
                mode &= static_cast<ui32>(acl.e_perm << 6) | ~s_irwxu_32;
                break;
            case ACL_USER:
            case ACL_GROUP:
                break;
            case ACL_GROUP_OBJ:
                group_obj = &acl;
                break;
            case ACL_OTHER:
                acl.e_perm &= static_cast<__le16>(mode) | ~s_irwxo_16;
                mode &= static_cast<ui32>(acl.e_perm) | ~s_irwxo_32;
                break;
            case ACL_MASK:
                mask_obj = &acl;
                break;
            default:
                return MakeError(E_FS_IO, "Unknown ACL tag");
        }
    }

    if (mask_obj) {
        // Extended ACL: group mode bits come from ACL_MASK.
        mask_obj->e_perm &= static_cast<__le16>(mode >> 3) | ~s_irwxo_16;
        mode &= static_cast<ui32>(mask_obj->e_perm << 3) | ~s_irwxg_32;
    } else {
        // Minimal ACL: group mode bits come from ACL_GROUP_OBJ.
        if (!group_obj) {
            return MakeError(E_FS_IO, "Minimal ACL requires ACL_GROUP_OBJ");
        }
        group_obj->e_perm &= static_cast<__le16>(mode >> 3) | ~s_irwxo_16;
        mode &= static_cast<ui32>(group_obj->e_perm << 3) | ~s_irwxg_32;
    }

    srcMode = (srcMode & ~(s_irwxo_32 | s_irwxu_32 | s_irwxg_32)) | mode;
    return {};
}

}   // namespace

NProto::TError GetChildXattrAcl(TString& xattrAcl, ui32& mode)
{
    posix_acl_xattr_entry* acls = nullptr;
    size_t count = 0;
    NProto::TError error = PosixAclFromXattr(xattrAcl, acls, count);
    if (HasError(error)) {
        return error;
    }

    error = PosixAclCreateMasq(acls, count, mode);
    if (HasError(error)) {
        return error;
    }

    return {};
}

}   // namespace NCloud::NFileStore::NStorage
