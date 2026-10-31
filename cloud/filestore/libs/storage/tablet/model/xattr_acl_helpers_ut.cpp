#include "xattr_acl_helpers.h"

#include <contrib/libs/linux-headers/linux/posix_acl.h>
#include <contrib/libs/linux-headers/linux/posix_acl_xattr.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <sys/stat.h>

#include <cstring>

namespace NCloud::NFileStore::NStorage {

namespace {

posix_acl_xattr_entry MakeEntry(ui16 tag, ui16 perm, ui32 id = ACL_UNDEFINED_ID)
{
    return {.e_tag = tag, .e_perm = perm, .e_id = id};
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
    const size_t entryCount = (value.size() - sizeof(posix_acl_xattr_header)) /
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

void AssertError(
    const TResultOrError<TChildXattrAcl>& resultOrError,
    ui32 code,
    TStringBuf message)
{
    UNIT_ASSERT(NCloud::HasError(resultOrError));
    UNIT_ASSERT_VALUES_EQUAL(code, resultOrError.GetError().GetCode());
    UNIT_ASSERT_STRING_CONTAINS(
        resultOrError.GetError().GetMessage(),
        message);
}

}   // namespace

Y_UNIT_TEST_SUITE(TGetChildXattrAclTest)
{
    Y_UNIT_TEST(ShouldIgnoreHeaderOnlyAcl)
    {
        auto resultOrError =
            GetChildXattrAcl(SerializeAcl({}), S_IFREG | 0666);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::HeaderOnly);
        UNIT_ASSERT(result.XAttraAcl.empty());
        UNIT_ASSERT_VALUES_EQUAL(0, result.Mode);
    }

    Y_UNIT_TEST(ShouldRejectMalformedAcl)
    {
        AssertError(
            GetChildXattrAcl("bad", S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectAclWithPartialEntry)
    {
        TString acl = SerializeAcl({});
        acl += 'x';

        AssertError(
            GetChildXattrAcl(acl, S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedAclVersion)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl(
                    {MakeEntry(ACL_USER_OBJ, 7)},
                    POSIX_ACL_XATTR_VERSION + 1),
                S_IFREG | 0666),
            E_FS_NOTSUPP,
            "Unsupported version of POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectUnknownAclTag)
    {
        constexpr ui16 invalidAclTag = 0xffff;
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({MakeEntry(invalidAclTag, 7)}),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Unknown ACL tag");
    }

    Y_UNIT_TEST(ShouldRejectPermissionBitsOutsideRwx)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeEntry(ACL_USER_OBJ, 8),
                    MakeEntry(ACL_GROUP_OBJ, 7),
                    MakeEntry(ACL_OTHER, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectUndefinedUserId)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeEntry(ACL_USER_OBJ, 7),
                    MakeEntry(ACL_USER, 7),
                    MakeEntry(ACL_GROUP_OBJ, 7),
                    MakeEntry(ACL_MASK, 7),
                    MakeEntry(ACL_OTHER, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Invalid ACL user ID");
    }

    Y_UNIT_TEST(ShouldRejectUndefinedGroupId)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeEntry(ACL_USER_OBJ, 7),
                    MakeEntry(ACL_GROUP_OBJ, 7),
                    MakeEntry(ACL_GROUP, 7),
                    MakeEntry(ACL_MASK, 7),
                    MakeEntry(ACL_OTHER, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Invalid ACL group ID");
    }

    Y_UNIT_TEST(ShouldRejectDuplicateAclTag)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeEntry(ACL_USER_OBJ, 7),
                    MakeEntry(ACL_USER_OBJ, 7),
                    MakeEntry(ACL_GROUP_OBJ, 7),
                    MakeEntry(ACL_OTHER, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectInvalidAclTagOrder)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeEntry(ACL_USER_OBJ, 7),
                    MakeEntry(ACL_GROUP_OBJ, 7),
                    MakeEntry(ACL_USER, 7, 1000),
                    MakeEntry(ACL_MASK, 7),
                    MakeEntry(ACL_OTHER, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectAclWithoutOtherEntry)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeEntry(ACL_USER_OBJ, 7),
                    MakeEntry(ACL_GROUP_OBJ, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectNamedUserWithoutMask)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeEntry(ACL_USER_OBJ, 7),
                    MakeEntry(ACL_USER, 7, 1000),
                    MakeEntry(ACL_GROUP_OBJ, 7),
                    MakeEntry(ACL_OTHER, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldRejectNamedGroupWithoutMask)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeEntry(ACL_USER_OBJ, 7),
                    MakeEntry(ACL_GROUP_OBJ, 7),
                    MakeEntry(ACL_GROUP, 7, 2000),
                    MakeEntry(ACL_OTHER, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldTreatMaskWithoutNamedEntriesAsExtended)
    {
        auto resultOrError = GetChildXattrAcl(
            SerializeAcl({
                MakeEntry(ACL_USER_OBJ, 7),
                MakeEntry(ACL_GROUP_OBJ, 5),
                MakeEntry(ACL_MASK, 6),
                MakeEntry(ACL_OTHER, 4),
            }),
            S_IFREG | 0777);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::Extended);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(S_IFREG | 0764),
            result.Mode);

        const auto entries = DeserializeAcl(result.XAttraAcl);
        UNIT_ASSERT_VALUES_EQUAL(4, entries.size());
        AssertEntry(entries[0], ACL_USER_OBJ, 7);
        AssertEntry(entries[1], ACL_GROUP_OBJ, 5);
        AssertEntry(entries[2], ACL_MASK, 6);
        AssertEntry(entries[3], ACL_OTHER, 4);
    }

    Y_UNIT_TEST(ShouldPreserveDirectoryTypeForExtendedAcl)
    {
        auto resultOrError = GetChildXattrAcl(
            SerializeAcl({
                MakeEntry(ACL_USER_OBJ, 6),
                MakeEntry(ACL_USER, 7, 1000),
                MakeEntry(ACL_GROUP_OBJ, 7),
                MakeEntry(ACL_MASK, 5),
                MakeEntry(ACL_OTHER, 7),
            }),
            S_IFDIR | 0750);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::Extended);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(S_IFDIR | 0650),
            result.Mode);

        const auto entries = DeserializeAcl(result.XAttraAcl);
        UNIT_ASSERT_VALUES_EQUAL(5, entries.size());
        AssertEntry(entries[0], ACL_USER_OBJ, 6);
        AssertEntry(entries[1], ACL_USER, 7, 1000);
        AssertEntry(entries[2], ACL_GROUP_OBJ, 7);
        AssertEntry(entries[3], ACL_MASK, 5);
        AssertEntry(entries[4], ACL_OTHER, 0);
    }

    Y_UNIT_TEST(ShouldAdjustAllModeClassesUsingMask)
    {
        auto resultOrError = GetChildXattrAcl(
            SerializeAcl({
                MakeEntry(ACL_USER_OBJ, 6),
                MakeEntry(ACL_USER, 7, 1000),
                MakeEntry(ACL_GROUP_OBJ, 2),
                MakeEntry(ACL_GROUP, 3, 2000),
                MakeEntry(ACL_MASK, 6),
                MakeEntry(ACL_OTHER, 6),
            }),
            S_IFREG | S_ISUID | S_ISGID | S_ISVTX | 0555);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::Extended);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(S_IFREG | S_ISUID | S_ISGID | S_ISVTX | 0444),
            result.Mode);

        const auto entries = DeserializeAcl(result.XAttraAcl);
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
        auto resultOrError = GetChildXattrAcl(
            SerializeAcl({
                MakeEntry(ACL_USER_OBJ, 7),
                MakeEntry(ACL_GROUP_OBJ, 6),
                MakeEntry(ACL_OTHER, 0),
            }),
            S_IFDIR | 0750);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::Minimal);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(S_IFDIR | 0740),
            result.Mode);
        UNIT_ASSERT(result.XAttraAcl.empty());
    }
}

}   // namespace NCloud::NFileStore::NStorage
