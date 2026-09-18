/*
 * Convert a parent's system.posix_acl_default xattr value into the
 * system.posix_acl_access value inherited by a newly created child.
 *
 * This is a userspace adaptation of posix_acl_from_xattr(),
 * posix_acl_create_masq(), and posix_acl_to_xattr() from fs/posix_acl.c.
 * The xattr representation and the host are assumed to be little-endian.
 */

#include "xattr_acl_helpers.h"

#include <contrib/libs/linux-headers/linux/posix_acl.h>
#include <contrib/libs/linux-headers/linux/posix_acl_xattr.h>

#include <util/string/builder.h>

#include <util/system/types.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage {

namespace {

#define S_IRWXUGO (S_IRWXU|S_IRWXG|S_IRWXO)

NProto::TError ReportMalformedPosixAcl(const TString& xattrAcl)
{
    return MakeError(
        E_FAIL,
        TStringBuilder() << "Malformed POSIX ACL xattr: " << xattrAcl.Quote());
}

NProto::TError PosixAclXattrCount(const TString& xattrAcl, size_t& count)
{
    const posix_acl_xattr_header* header =
        reinterpret_cast<const posix_acl_xattr_header*>(xattrAcl.data());

    size_t size = xattrAcl.size();

    if (size < sizeof(posix_acl_xattr_header)) {
        return ReportMalformedPosixAcl(xattrAcl);
    }

    if (header->a_version != POSIX_ACL_XATTR_VERSION) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Unsupported version of POSIX ACL xattr: "
                             << header->a_version);
    }

    size -= sizeof(posix_acl_xattr_header);
    if (size % sizeof(posix_acl_xattr_entry)) {
        return ReportMalformedPosixAcl(xattrAcl);
    }
    count = size / sizeof(posix_acl_xattr_entry);
    return {};
}

NProto::TError PosixAclFromXattr(
    const TString& value,
    TVector<posix_acl_xattr_entry>& acls)
{
    acls.clear();
    size_t count = 0;
    NProto::TError error = PosixAclXattrCount(value, count);
    if (HasError(error)) {
        return error;
    } else if (count == 0) {
        return {};
    }

    const posix_acl_xattr_header* header =
        reinterpret_cast<const posix_acl_xattr_header*>(value.data());
    const posix_acl_xattr_entry* entry =
        reinterpret_cast<const posix_acl_xattr_entry*>(header + 1);

    acls.resize(count);
    for (size_t i = 0; i < acls.size(); ++i, ++entry) {
        acls[i] = *entry;
    }

    return {};
}

void PosixAclToXattr(const TVector<posix_acl_xattr_entry>& acls, TString& value)
{
    value.resize(
        sizeof(posix_acl_xattr_header) +
        acls.size() * sizeof(posix_acl_xattr_entry));

    posix_acl_xattr_header* header =
        reinterpret_cast<posix_acl_xattr_header*>(value.Detach());
    posix_acl_xattr_entry* entry =
        reinterpret_cast<posix_acl_xattr_entry*>(header + 1);

    *header = posix_acl_xattr_header{POSIX_ACL_XATTR_VERSION};

    for (size_t i = 0; i < acls.size(); ++i, ++entry) {
        *entry = acls[i];
    }
}

NProto::TError PosixAclCreateMasq(
    TVector<posix_acl_xattr_entry>& acls,
    ui32& srcMode)
{
    posix_acl_xattr_entry* group_obj = nullptr;
    posix_acl_xattr_entry* mask_obj = nullptr;
    ui32 mode = srcMode;

    constexpr __le16 s_irwxo_16 = S_IRWXO;
    constexpr ui32 s_irwxo_32 = S_IRWXO;
    constexpr ui32 s_irwxu_32 = S_IRWXU;
    constexpr ui32 s_irwxg_32 = S_IRWXG;
    constexpr ui32 s_irwxugo_32 =  S_IRWXUGO;

    for (auto& acl: acls) {
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
                return MakeError(E_FAIL, "Unknown ACL tag");
        }
    }

    if (mask_obj) {
        mask_obj->e_perm &= static_cast<__le16>(mode >> 3) | ~s_irwxo_16;
        mode &= static_cast<ui32>(mask_obj->e_perm << 3) | ~s_irwxg_32;
    } else {
        if (!group_obj) {
            return MakeError(E_FAIL);
        }
        group_obj->e_perm &= static_cast<__le16>(mode >> 3) | ~s_irwxo_16;
        mode &= static_cast<ui32>(group_obj->e_perm << 3) | ~s_irwxg_32;
    }

    srcMode = (srcMode & ~s_irwxugo_32) | mode;
    return {};
}

}   // namespace

NProto::TError GetChildXattrAcl(TString& xattrAcl, ui32& mode)
{
    TVector<posix_acl_xattr_entry> acls;

    NProto::TError error = PosixAclFromXattr(xattrAcl, acls);
    if (HasError(error)) {
        return error;
    }

    error = PosixAclCreateMasq(acls, mode);
    if (HasError(error)) {
        return error;
    }

    PosixAclToXattr(acls, xattrAcl);
    return {};
}

}   // namespace NCloud::NFileStore::NStorage
