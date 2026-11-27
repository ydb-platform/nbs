#include "xattr_acl_helpers.h"

#include "test_helpers/acl_test_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage {

using namespace NAclTest;

namespace {

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
    const TResultOrError<TNodeXattrAcl>& resultOrError,
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
        const ui32 mode = S_IFREG | 0666;
        auto resultOrError =
            GetChildXattrAcl(SerializeAcl({}), mode);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::HeaderOnly);
        UNIT_ASSERT(result.Acl.empty());
        UNIT_ASSERT_VALUES_EQUAL(mode, result.Mode);
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
                    {MakeAclEntry(ACL_USER_OBJ, 7)},
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
                SerializeAcl({MakeAclEntry(invalidAclTag, 7)}),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Unknown ACL tag");
    }

    Y_UNIT_TEST(ShouldRejectPermissionBitsOutsideRwx)
    {
        AssertError(
            GetChildXattrAcl(
                SerializeAcl({
                    MakeAclEntry(ACL_USER_OBJ, 8),
                    MakeAclEntry(ACL_GROUP_OBJ, 7),
                    MakeAclEntry(ACL_OTHER, 7),
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
                    MakeAclEntry(ACL_USER_OBJ, 7),
                    MakeAclEntry(ACL_USER, 7),
                    MakeAclEntry(ACL_GROUP_OBJ, 7),
                    MakeAclEntry(ACL_MASK, 7),
                    MakeAclEntry(ACL_OTHER, 7),
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
                    MakeAclEntry(ACL_USER_OBJ, 7),
                    MakeAclEntry(ACL_GROUP_OBJ, 7),
                    MakeAclEntry(ACL_GROUP, 7),
                    MakeAclEntry(ACL_MASK, 7),
                    MakeAclEntry(ACL_OTHER, 7),
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
                    MakeAclEntry(ACL_USER_OBJ, 7),
                    MakeAclEntry(ACL_USER_OBJ, 7),
                    MakeAclEntry(ACL_GROUP_OBJ, 7),
                    MakeAclEntry(ACL_OTHER, 7),
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
                    MakeAclEntry(ACL_USER_OBJ, 7),
                    MakeAclEntry(ACL_GROUP_OBJ, 7),
                    MakeAclEntry(ACL_USER, 7, 1000),
                    MakeAclEntry(ACL_MASK, 7),
                    MakeAclEntry(ACL_OTHER, 7),
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
                    MakeAclEntry(ACL_USER_OBJ, 7),
                    MakeAclEntry(ACL_GROUP_OBJ, 7),
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
                    MakeAclEntry(ACL_USER_OBJ, 7),
                    MakeAclEntry(ACL_USER, 7, 1000),
                    MakeAclEntry(ACL_GROUP_OBJ, 7),
                    MakeAclEntry(ACL_OTHER, 7),
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
                    MakeAclEntry(ACL_USER_OBJ, 7),
                    MakeAclEntry(ACL_GROUP_OBJ, 7),
                    MakeAclEntry(ACL_GROUP, 7, 2000),
                    MakeAclEntry(ACL_OTHER, 7),
                }),
                S_IFREG | 0666),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }

    Y_UNIT_TEST(ShouldTreatMaskWithoutNamedEntriesAsExtended)
    {
        auto resultOrError = GetChildXattrAcl(
            SerializeAcl({
                MakeAclEntry(ACL_USER_OBJ, 7),
                MakeAclEntry(ACL_GROUP_OBJ, 5),
                MakeAclEntry(ACL_MASK, 6),
                MakeAclEntry(ACL_OTHER, 4),
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

        const auto entries = DeserializeAcl(result.Acl);
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
                MakeAclEntry(ACL_USER_OBJ, 6),
                MakeAclEntry(ACL_USER, 7, 1000),
                MakeAclEntry(ACL_GROUP_OBJ, 7),
                MakeAclEntry(ACL_MASK, 5),
                MakeAclEntry(ACL_OTHER, 7),
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

        const auto entries = DeserializeAcl(result.Acl);
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
                MakeAclEntry(ACL_USER_OBJ, 6),
                MakeAclEntry(ACL_USER, 7, 1000),
                MakeAclEntry(ACL_GROUP_OBJ, 2),
                MakeAclEntry(ACL_GROUP, 3, 2000),
                MakeAclEntry(ACL_MASK, 6),
                MakeAclEntry(ACL_OTHER, 6),
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

        const auto entries = DeserializeAcl(result.Acl);
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
                MakeAclEntry(ACL_USER_OBJ, 7),
                MakeAclEntry(ACL_GROUP_OBJ, 6),
                MakeAclEntry(ACL_OTHER, 0),
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
        UNIT_ASSERT(result.Acl.empty());
    }
}

Y_UNIT_TEST_SUITE(TRewriteXattrAclPermByModeTest)
{
    Y_UNIT_TEST(ShouldRewriteExtendedAclUsingMask)
    {
        const ui32 mode = S_IFREG | S_ISUID | S_ISGID | S_ISVTX | 0651;
        const auto resultOrError = RewriteXattrAclPermByMode(
            SerializeAcl({
                MakeAclEntry(ACL_USER_OBJ, 4),
                MakeAclEntry(ACL_USER, 7, 1000),
                MakeAclEntry(ACL_GROUP_OBJ, 2),
                MakeAclEntry(ACL_GROUP, 3, 2000),
                MakeAclEntry(ACL_MASK, 2),
                MakeAclEntry(ACL_OTHER, 6),
            }),
            mode);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::Extended);
        UNIT_ASSERT_VALUES_EQUAL(mode, result.Mode);
        UNIT_ASSERT_VALUES_EQUAL(
            SerializeAcl({
                MakeAclEntry(ACL_USER_OBJ, 6),
                MakeAclEntry(ACL_USER, 7, 1000),
                MakeAclEntry(ACL_GROUP_OBJ, 2),
                MakeAclEntry(ACL_GROUP, 3, 2000),
                MakeAclEntry(ACL_MASK, 5),
                MakeAclEntry(ACL_OTHER, 1),
            }),
            result.Acl);
    }

    Y_UNIT_TEST(ShouldRewriteAndSerializeMinimalAcl)
    {
        const ui32 mode = S_IFDIR | 0750;
        const auto resultOrError = RewriteXattrAclPermByMode(
            SerializeAcl({
                MakeAclEntry(ACL_USER_OBJ, 6),
                MakeAclEntry(ACL_GROUP_OBJ, 2),
                MakeAclEntry(ACL_OTHER, 7),
            }),
            mode);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::Minimal);
        UNIT_ASSERT_VALUES_EQUAL(mode, result.Mode);
        UNIT_ASSERT_VALUES_EQUAL(
            SerializeAcl({
                MakeAclEntry(ACL_USER_OBJ, 7),
                MakeAclEntry(ACL_GROUP_OBJ, 5),
                MakeAclEntry(ACL_OTHER, 0),
            }),
            result.Acl);
    }

    Y_UNIT_TEST(ShouldIgnoreHeaderOnlyAcl)
    {
        const ui32 mode = S_IFREG | 0640;
        const auto resultOrError =
            RewriteXattrAclPermByMode(SerializeAcl({}), mode);

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::HeaderOnly);
        UNIT_ASSERT_VALUES_EQUAL(mode, result.Mode);
        UNIT_ASSERT(result.Acl.empty());
    }

    Y_UNIT_TEST(ShouldRejectInvalidPermissionsBeforeRewriting)
    {
        AssertError(
            RewriteXattrAclPermByMode(
                SerializeAcl({
                    MakeAclEntry(ACL_USER_OBJ, 8),
                    MakeAclEntry(ACL_GROUP_OBJ, 4),
                    MakeAclEntry(ACL_OTHER, 0),
                }),
                0640),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }
}

Y_UNIT_TEST_SUITE(TGetModeByXattrAclTest)
{
    Y_UNIT_TEST(ShouldDeriveGroupModeFromMaskForExtendedAcl)
    {
        const auto resultOrError = GetModeByXattrAcl(SerializeAcl({
            MakeAclEntry(ACL_USER_OBJ, 6),
            MakeAclEntry(ACL_USER, 7, 1000),
            MakeAclEntry(ACL_GROUP_OBJ, 2),
            MakeAclEntry(ACL_MASK, 5),
            MakeAclEntry(ACL_OTHER, 1),
        }));

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::Extended);
        UNIT_ASSERT_VALUES_EQUAL(0651u, result.Mode);
        UNIT_ASSERT(result.Acl.empty());
    }

    Y_UNIT_TEST(ShouldDeriveGroupModeFromGroupObjectForMinimalAcl)
    {
        const auto resultOrError = GetModeByXattrAcl(SerializeAcl({
            MakeAclEntry(ACL_USER_OBJ, 7),
            MakeAclEntry(ACL_GROUP_OBJ, 4),
            MakeAclEntry(ACL_OTHER, 0),
        }));

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::Minimal);
        UNIT_ASSERT_VALUES_EQUAL(0740u, result.Mode);
        UNIT_ASSERT(result.Acl.empty());
    }

    Y_UNIT_TEST(ShouldIgnoreHeaderOnlyAcl)
    {
        const auto resultOrError = GetModeByXattrAcl(SerializeAcl({}));

        UNIT_ASSERT_C(
            !NCloud::HasError(resultOrError),
            resultOrError.GetError());
        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT(result.Type == EXattrAclType::HeaderOnly);
        UNIT_ASSERT_VALUES_EQUAL(0777u, result.Mode);
        UNIT_ASSERT(result.Acl.empty());
    }

    Y_UNIT_TEST(ShouldRejectNamedUserWithoutMask)
    {
        AssertError(
            GetModeByXattrAcl(SerializeAcl({
                MakeAclEntry(ACL_USER_OBJ, 7),
                MakeAclEntry(ACL_USER, 7, 1000),
                MakeAclEntry(ACL_GROUP_OBJ, 4),
                MakeAclEntry(ACL_OTHER, 0),
            })),
            E_FS_INVAL,
            "Malformed POSIX ACL xattr");
    }
}

}   // namespace NCloud::NFileStore::NStorage
