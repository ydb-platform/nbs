#include <cloud/filestore/libs/service/mediakind.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMediaKindTest)
{
    Y_UNIT_TEST(ShouldParseFileStoreMediaKind)
    {
        using namespace NCloud::NProto;

        const std::pair<TString, EStorageMediaKind> kinds[] = {
            {"ssd", STORAGE_MEDIA_SSD},
            {"hdd", STORAGE_MEDIA_HDD},
            {"hybrid", STORAGE_MEDIA_HDD},
            {"default", STORAGE_MEDIA_HDD},
        };

        for (const auto& [name, expected]: kinds) {
            const auto kind = ParseFileStoreMediaKind(name);
            UNIT_ASSERT_C(!HasError(kind), FormatError(kind.GetError()));
            UNIT_ASSERT_EQUAL(expected, kind.GetResult());
        }
    }

    Y_UNIT_TEST(ShouldNotParseUnknownFileStoreMediaKind)
    {
        const auto kind = ParseFileStoreMediaKind("ssd_mirror3");
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, kind.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldParseFastShardStorageKind)
    {
        using namespace NProto;

        auto kind = ParseFastShardStorageKind("mirror");
        UNIT_ASSERT_C(!HasError(kind), FormatError(kind.GetError()));
        UNIT_ASSERT_EQUAL(FAST_SHARD_STORAGE_MIRROR, kind.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(
            "mirror",
            FastShardStorageKindToString(kind.GetResult()));

        kind = ParseFastShardStorageKind("quorum_mirror");
        UNIT_ASSERT_C(!HasError(kind), FormatError(kind.GetError()));
        UNIT_ASSERT_EQUAL(FAST_SHARD_STORAGE_QUORUM_MIRROR, kind.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(
            "quorum_mirror",
            FastShardStorageKindToString(kind.GetResult()));
    }

    Y_UNIT_TEST(ShouldNotParseUnknownFastShardStorageKind)
    {
        const auto kind = ParseFastShardStorageKind("unknown");
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, kind.GetError().GetCode());
    }
}

}   // namespace NCloud::NFileStore
