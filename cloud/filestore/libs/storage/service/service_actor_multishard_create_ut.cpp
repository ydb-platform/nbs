#include "service_actor_multishard_create.h"

#include <cloud/storage/core/libs/common/compressed_bitmap.h>
#include <cloud/storage/core/libs/common/compressed_bitmap_serialization.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMultiShardCreateTest)
{
    Y_UNIT_TEST(ShouldContinueUnfinishedShardCreationOfSameShardsOnly)
    {
        // A filesystem with two shards gets two more.
        TMultiShardCreateConfig config;
        config.FileSystemId = "test";
        config.ExistingShardCount = 2;
        for (const auto* shardId: {"test_s3", "test_s4"}) {
            auto& shard = config.ShardsToCreate.emplace_back();
            shard.Config.SetFileSystemId(shardId);
            shard.Config.SetBlocksCount(1024);
            shard.Type = NProtoPrivate::TShardTopology::E_ORDINARY;
        }

        // The first attempt created one shard of the two.
        auto saved = InitShardCreationState({}, config).GetResult();
        TCompressedBitmap created(4);
        created.Set(2, 3);
        SaveCompressedBitmap(created, 4, *saved.MutableCreatedShardBitmap());

        // A retry differs only in the fields that change between attempts.
        for (auto& shard: config.ShardsToCreate) {
            shard.Config.SetVersion(5);
            shard.Config.SetCreationTs(100);
            shard.Config.SetAlterTs(200);
        }

        // The retry should continue the saved creation.
        const auto state = InitShardCreationState(saved, config);
        UNIT_ASSERT_C(!HasError(state), FormatError(state.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            saved.ShortDebugString(),
            state.GetResult().ShortDebugString());

        // A request for other shards should be rejected.
        config.ShardsToCreate[1].Config.SetBlocksCount(2048);
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InitShardCreationState(saved, config).GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldStartNewShardCreationAfterFinishedOne)
    {
        // A filesystem with two shards got two more.
        TMultiShardCreateConfig config;
        config.FileSystemId = "test";
        config.ExistingShardCount = 2;
        for (const auto* shardId: {"test_s3", "test_s4"}) {
            auto& shard = config.ShardsToCreate.emplace_back();
            shard.Config.SetFileSystemId(shardId);
            shard.Type = NProtoPrivate::TShardTopology::E_ORDINARY;
        }

        auto saved = InitShardCreationState({}, config).GetResult();
        saved.SetVersion(7);
        TCompressedBitmap created(4);
        created.Set(2, 4);
        SaveCompressedBitmap(created, 4, *saved.MutableCreatedShardBitmap());

        // The filesystem uses the four shards and gets one more.
        config.ExistingShardCount = 4;
        config.ShardsToCreate.resize(1);
        config.ShardsToCreate[0].Config.SetFileSystemId("test_s5");

        // The saved state should be replaced with a new creation.
        auto expected = InitShardCreationState({}, config).GetResult();
        expected.SetVersion(7);

        const auto state = InitShardCreationState(saved, config);
        UNIT_ASSERT_C(!HasError(state), FormatError(state.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            expected.ShortDebugString(),
            state.GetResult().ShortDebugString());
    }
}

}   // namespace NCloud::NFileStore::NStorage
