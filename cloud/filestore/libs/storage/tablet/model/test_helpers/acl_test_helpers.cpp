#include "acl_test_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <cstring>

namespace NCloud::NFileStore::NStorage::NAclTest {

const TString AccessAcl = "system.posix_acl_access";
const TString DefaultAcl = "system.posix_acl_default";

ui32 Permissions(TStringBuf value)
{
    UNIT_ASSERT_C(
        value.size() == 3 || value.size() == 9,
        "Permission string must describe one ACL entry or a complete mode");

    ui32 result = 0;
    constexpr TStringBuf permissionChars = "rwx";
    for (size_t i = 0; i < value.size(); ++i) {
        UNIT_ASSERT_C(
            value[i] == '-' || value[i] == permissionChars[i % 3],
            "Invalid permission string: " << value);
        if (value[i] != '-') {
            result |= 1U << (value.size() - i - 1);
        }
    }
    return result;
}

posix_acl_xattr_entry MakeAclEntry(ui16 tag, ui16 permissions, ui32 id)
{
    return {.e_tag = tag, .e_perm = permissions, .e_id = id};
}

posix_acl_xattr_entry MakeAclEntry(ui16 tag, TStringBuf permissions, ui32 id)
{
    return MakeAclEntry(tag, static_cast<ui16>(Permissions(permissions)), id);
}

TString SerializeAcl(const TPosixAcl& entries, ui32 version)
{
    const posix_acl_xattr_header header{version};
    TString value(reinterpret_cast<const char*>(&header), sizeof(header));
    if (!entries.empty()) {
        value.append(
            reinterpret_cast<const char*>(entries.data()),
            entries.size() * sizeof(posix_acl_xattr_entry));
    }
    return value;
}

TPosixAcl DeserializeAcl(const TString& value)
{
    posix_acl_xattr_header header;
    UNIT_ASSERT_GE(value.size(), sizeof(header));
    std::memcpy(&header, value.data(), sizeof(header));
    UNIT_ASSERT_VALUES_EQUAL(POSIX_ACL_XATTR_VERSION, header.a_version);

    const size_t entriesSize = value.size() - sizeof(header);
    UNIT_ASSERT_VALUES_EQUAL(0, entriesSize % sizeof(posix_acl_xattr_entry));

    TPosixAcl entries(entriesSize / sizeof(posix_acl_xattr_entry));
    if (!entries.empty()) {
        std::memcpy(entries.data(), value.data() + sizeof(header), entriesSize);
    }
    return entries;
}

}   // namespace NCloud::NFileStore::NStorage::NAclTest
