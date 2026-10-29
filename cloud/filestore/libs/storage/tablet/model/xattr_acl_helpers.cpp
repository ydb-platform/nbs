//
// Convert a parent's system.posix_acl_default xattr value into the
// system.posix_acl_access value inherited by a newly created child.
//
// This is a userspace adaptation of posix_acl_from_xattr(),
// posix_acl_create_masq(), and posix_acl_to_xattr() from fs/posix_acl.c.
// The xattr representation and the host are assumed to be little-endian.
//

#include "xattr_acl_helpers.h"

#include <contrib/libs/linux-headers/linux/posix_acl.h>
#include <contrib/libs/linux-headers/linux/posix_acl_xattr.h>

#include <util/string/builder.h>
#include <util/system/types.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage {

namespace {

NProto::TError ReportMalformedPosixAcl(
    const TString& xattrAcl,
    const TString& message = "")
{
    TStringBuilder errorMessage;
    errorMessage << "Malformed POSIX ACL xattr: " << xattrAcl.Quote();
    if (message) {
        errorMessage << ", " << message;
    }
    return MakeError(E_FS_INVAL, errorMessage);
}

NProto::TError PosixAclXattrCount(const TString& xattrAcl, size_t& count)
{
    size_t size = xattrAcl.size();

    if (size < sizeof(posix_acl_xattr_header)) {
        return ReportMalformedPosixAcl(xattrAcl);
    }

    posix_acl_xattr_header header;
    memcpy(&header, xattrAcl.data(), sizeof(posix_acl_xattr_header));

    if (header.a_version != POSIX_ACL_XATTR_VERSION) {
        return MakeError(
            E_FS_NOTSUPP,
            TStringBuilder() << "Unsupported version of POSIX ACL xattr: "
                             << header.a_version);
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
    size_t count = 0;
    acls.resize(count);

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
        memcpy(acls.data() + i, entry, sizeof(posix_acl_xattr_entry));
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

    posix_acl_xattr_header srcHeader{POSIX_ACL_XATTR_VERSION};
    memcpy(header, &srcHeader, sizeof(posix_acl_xattr_header));

    for (size_t i = 0; i < acls.size(); ++i, ++entry) {
        memcpy(entry, acls.data() + i, sizeof(posix_acl_xattr_entry));
    }
}

NProto::TError PosixAclCreateMasq(
    const TString& xattrAcl,
    TVector<posix_acl_xattr_entry>& acls,
    ui32& srcMode,
    EXattrAclType& xattrAclType)
{
    posix_acl_xattr_entry* groupObj = nullptr;
    posix_acl_xattr_entry* maskObj = nullptr;
    ui32 mode = srcMode;

    constexpr __le16 s_irwxo_16 = S_IRWXO;
    constexpr ui32 s_irwxo_32 = S_IRWXO;
    constexpr ui32 s_irwxu_32 = S_IRWXU;
    constexpr ui32 s_irwxg_32 = S_IRWXG;

    xattrAclType = EXattrAclType::Malformed;

    __le16 state = ACL_USER_OBJ;
    bool needsMask = false;

    for (auto& acl: acls) {
        if (acl.e_perm & ~(ACL_READ | ACL_WRITE | ACL_EXECUTE)) {
            return ReportMalformedPosixAcl(xattrAcl);
        }
        switch (acl.e_tag) {
            case ACL_USER_OBJ:
                if (state != ACL_USER_OBJ) {
                    return ReportMalformedPosixAcl(xattrAcl);
                }
                state = ACL_USER;

                acl.e_perm &= static_cast<__le16>(mode >> 6) | ~s_irwxo_16;
                mode &= static_cast<ui32>(acl.e_perm << 6) | ~s_irwxu_32;
                break;
            case ACL_USER:
                if (state != ACL_USER) {
                    return ReportMalformedPosixAcl(xattrAcl);
                }
                if (acl.e_id == static_cast<__le32>(ACL_UNDEFINED_ID)) {
                    return ReportMalformedPosixAcl(
                        xattrAcl,
                        "Invalid ACL user ID");
                }
                needsMask = true;
                break;
            case ACL_GROUP_OBJ:
                if (state != ACL_USER) {
                    return ReportMalformedPosixAcl(xattrAcl);
                }
                state = ACL_GROUP;

                groupObj = &acl;
                break;
            case ACL_GROUP:
                if (state != ACL_GROUP) {
                    return ReportMalformedPosixAcl(xattrAcl);
                }
                if (acl.e_id == static_cast<__le32>(ACL_UNDEFINED_ID)) {
                    return ReportMalformedPosixAcl(
                        xattrAcl,
                        "Invalid ACL group ID");
                }
                state = ACL_GROUP;
                needsMask = true;
                break;
            case ACL_MASK:
                if (state != ACL_GROUP) {
                    return ReportMalformedPosixAcl(xattrAcl);
                }
                state = ACL_OTHER;

                maskObj = &acl;
                break;
            case ACL_OTHER:
                if (state != ACL_OTHER && (state != ACL_GROUP || needsMask)) {
                    return ReportMalformedPosixAcl(xattrAcl);
                }
                state = 0;

                acl.e_perm &= static_cast<__le16>(mode) | ~s_irwxo_16;
                mode &= static_cast<ui32>(acl.e_perm) | ~s_irwxo_32;
                break;
            default:
                return ReportMalformedPosixAcl(xattrAcl, "Unknown ACL tag");
        }
    }

    if (state != 0) {
        return ReportMalformedPosixAcl(xattrAcl);
    }

    if (maskObj) {
        // Extended ACL: group mode bits come from ACL_MASK.
        xattrAclType = EXattrAclType::Extended;
        maskObj->e_perm &= static_cast<__le16>(mode >> 3) | ~s_irwxo_16;
        mode &= static_cast<ui32>(maskObj->e_perm << 3) | ~s_irwxg_32;
    } else {
        // Minimal ACL: group mode bits come from ACL_GROUP_OBJ.
        xattrAclType = EXattrAclType::Minimal;
        if (!groupObj) {
            return ReportMalformedPosixAcl(
                xattrAcl,
                "Minimal ACL requires ACL_GROUP_OBJ");
        }
        groupObj->e_perm &= static_cast<__le16>(mode >> 3) | ~s_irwxo_16;
        mode &= static_cast<ui32>(groupObj->e_perm << 3) | ~s_irwxg_32;
    }

    srcMode = (srcMode & ~(s_irwxo_32 | s_irwxu_32 | s_irwxg_32)) | mode;
    return {};
}

}   // namespace

NProto::TError GetChildXattrAcl(
    TString& xattrAcl,
    ui32& mode,
    EXattrAclType& xattrAclType)
{
    TVector<posix_acl_xattr_entry> acls;

    NProto::TError error = PosixAclFromXattr(xattrAcl, acls);
    if (HasError(error)) {
        xattrAclType = EXattrAclType::Malformed;
        return error;
    }

    // In case only header is passed to GetChildXattrAcl,
    // the kernel behaves as if no xattrAcl is present.
    // We return an empty string, which means that there is nothing to inherit.
    if (!acls) {
        xattrAcl.clear();
        xattrAclType = EXattrAclType::HeaderOnly;
        return {};
    }

    error = PosixAclCreateMasq(xattrAcl, acls, mode, xattrAclType);
    if (HasError(error)) {
        return error;
    }

    if (xattrAclType == EXattrAclType::Extended) {
        PosixAclToXattr(acls, xattrAcl);
    } else {
        xattrAcl.clear();
    }

    return {};
}

}   // namespace NCloud::NFileStore::NStorage
