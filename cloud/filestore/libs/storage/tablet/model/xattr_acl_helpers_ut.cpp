#include "xattr_acl_helpers.h"

#include <contrib/libs/linux-headers/linux/posix_acl.h>
#include <contrib/libs/linux-headers/linux/posix_acl_xattr.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <cstring>
#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

posix_acl_xattr_entry MakeEntry(ui16 tag, ui16 perm, ui32 id = ACL_UNDEFINED_ID)
{
    return {
        .e_tag = tag,
        .e_perm = perm,
        .e_id = id,
    };
}

TString SerializeAcl(
    const TVector<posix_acl_xattr_entry>& entries,
    ui32 version = POSIX_ACL_XATTR_VERSION)
{
    posix_acl_xattr_header header = {version};

    TString result;
    result.resize(sizeof(header) + entries.size() * sizeof(entries.front()));
    std::memcpy(result.Detach(), &header, sizeof(header));
    std::memcpy(
        result.Detach() + sizeof(header),
        entries.data(),
        entries.size() * sizeof(entries.front()));
    return result;
}

TVector<posix_acl_xattr_entry> DeserializeAcl(const TString& value)
{
    const size_t entryCount =
        (value.size() - sizeof(posix_acl_xattr_header)) /
        sizeof(posix_acl_xattr_entry);

    TVector<posix_acl_xattr_entry> entries(entryCount);
    std::memcpy(
        entries.data(),
        value.data() + sizeof(posix_acl_xattr_header),
        entries.size() * sizeof(entries.front()));
    return entries;
}

void AssertEntry(
    const posix_acl_xattr_entry& entry,
    ui16 tag,
    ui16 perm,
    ui32 id = ACL_UNDEFINED_ID)
{
    UNIT_ASSERT_VALUES_EQUAL(tag, entry.e_tag);
    UNIT_ASSERT_VALUES_EQUAL(perm, entry.e_perm);
    UNIT_ASSERT_VALUES_EQUAL(id, entry.e_id);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

Y_UNIT_TEST_SUITE(TGetChildXattrAclTest)
{
    Y_UNIT_TEST(ShouldRejectMalformedAcl)
    {
        // A POSIX ACL xattr must contain at least a complete version header.
        TString acl = "bad";
        ui32 mode = S_IFREG | 0666;

        const auto error = GetChildXattrAcl(acl, mode);

        UNIT_ASSERT_VALUES_EQUAL(E_FS_INVAL, error.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            error.GetMessage(),
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedAclVersion)
    {
        TString acl = SerializeAcl(
            {MakeEntry(ACL_USER_OBJ, 7)},
            POSIX_ACL_XATTR_VERSION + 1);
        ui32 mode = S_IFREG | 0666;

        const auto error = GetChildXattrAcl(acl, mode);

        UNIT_ASSERT_VALUES_EQUAL(E_FS_NOTSUPP, error.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            error.GetMessage(),
            "Unsupported version of POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectUnknownAclTag)
    {
        constexpr ui16 invalidAclTag = 0xffff;
        TString acl = SerializeAcl({MakeEntry(invalidAclTag, 7)});
        ui32 mode = S_IFREG | 0666;

        const auto error = GetChildXattrAcl(acl, mode);

        UNIT_ASSERT_VALUES_EQUAL(E_FS_IO, error.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "Unknown ACL tag");
    }

    Y_UNIT_TEST(ShouldAdjustAllModeClassesUsingMask)
    {
        TString acl = SerializeAcl({
            MakeEntry(ACL_USER_OBJ, 6),
            MakeEntry(ACL_USER, 7, 1000),
            MakeEntry(ACL_GROUP_OBJ, 2),
            MakeEntry(ACL_GROUP, 3, 2000),
            MakeEntry(ACL_MASK, 6),
            MakeEntry(ACL_OTHER, 6),
        });
        ui32 mode = S_IFREG | S_ISUID | S_ISGID | S_ISVTX | 0555;

        const auto error = GetChildXattrAcl(acl, mode);

        UNIT_ASSERT_C(!NCloud::HasError(error), error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(S_IFREG | S_ISUID | S_ISGID | S_ISVTX | 0444),
            mode);

        const auto entries = DeserializeAcl(acl);
        UNIT_ASSERT_VALUES_EQUAL(6, entries.size());
        AssertEntry(entries[0], ACL_USER_OBJ, 4);
        AssertEntry(entries[1], ACL_USER, 7, 1000);
        AssertEntry(entries[2], ACL_GROUP_OBJ, 2);
        AssertEntry(entries[3], ACL_GROUP, 3, 2000);
        AssertEntry(entries[4], ACL_MASK, 4);
        AssertEntry(entries[5], ACL_OTHER, 4);
    }

    Y_UNIT_TEST(ShouldAdjustGroupModeUsingGroupObjectWithoutMask)
    {
        TString acl = SerializeAcl({
            MakeEntry(ACL_USER_OBJ, 7),
            MakeEntry(ACL_GROUP_OBJ, 6),
            MakeEntry(ACL_OTHER, 0),
        });
        ui32 mode = S_IFDIR | 0750;

        const auto error = GetChildXattrAcl(acl, mode);

        UNIT_ASSERT_C(!NCloud::HasError(error), error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(S_IFDIR | 0740), mode);

        const auto entries = DeserializeAcl(acl);
        UNIT_ASSERT_VALUES_EQUAL(3, entries.size());
        AssertEntry(entries[0], ACL_USER_OBJ, 7);
        AssertEntry(entries[1], ACL_GROUP_OBJ, 4);
        AssertEntry(entries[2], ACL_OTHER, 0);
    }
}

}   // namespace NCloud::NFileStore::NStorage
