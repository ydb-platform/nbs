#include "shard_balancer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TShardBalancerTest)
{
#define ASSERT_NO_SB_ERROR(fileSize, expectedShardId) {                        \
    TString shardId;                                                           \
    const auto error = balancer.SelectShard(fileSize, &shardId);               \
    UNIT_ASSERT_VALUES_EQUAL_C(                                                \
        S_OK,                                                                  \
        error.GetCode(),                                                       \
        error.GetMessage());                                                   \
    UNIT_ASSERT_VALUES_EQUAL(expectedShardId, shardId);                        \
}                                                                              \
// ASSERT_NO_ERROR

#define ASSERT_SB_ERROR(fileSize, expectedCode) {                              \
    TString shardId;                                                           \
    const auto error = balancer.SelectShard(fileSize, &shardId);               \
    UNIT_ASSERT_VALUES_EQUAL_C(                                                \
        expectedCode,                                                          \
        error.GetCode(),                                                       \
        error.GetMessage());                                                   \
}                                                                              \
// ASSERT_ERROR

    Y_UNIT_TEST(ShouldBalanceShardsRoundRobin)
    {
        TShardBalancerRoundRobin balancer(
            4_KB,
            1_TB,
            1_MB,
            {"s1", "s2", "s3", "s4", "s5"});
        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s2");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s4");
        ASSERT_NO_SB_ERROR(0, "s5");
        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s2");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s4");
        ASSERT_NO_SB_ERROR(0, "s5");

        balancer.UpdateShardStats({
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 3_TB / 4_KB, 0, 0},
        });

        // order changed: s1, s3, s4, s2, s5

        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s4");
        ASSERT_NO_SB_ERROR(0, "s2");
        ASSERT_NO_SB_ERROR(0, "s5");
        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s4");
        ASSERT_NO_SB_ERROR(0, "s2");
        ASSERT_NO_SB_ERROR(0, "s5");

        // order changed: s1, s2, s3, s4, s5

        balancer.UpdateShardStats({
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 4_TB / 4_KB, 0, 0},
        });

        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s2");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s4");
        ASSERT_NO_SB_ERROR(0, "s5");
        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s2");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s4");
        ASSERT_NO_SB_ERROR(0, "s5");

        // one of the shard has less than desired free space
        // order changed: s1, s2, s3, s4

        balancer.UpdateShardStats({
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, (4_TB + 500_GB) / 4_KB, 0, 0},
        });

        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s2");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s4");
        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s2");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s4");

        // more full / close to full shards
        // order changed: s1, s4
        // tier 2: s3, s5

        balancer.UpdateShardStats({
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, (4_TB + 300_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, (4_TB + 500_GB) / 4_KB, 0, 0},
        });

        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s4");
        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s4");

        // 3 close to full shards, 2 full shards
        // order changed: s3, s1, s5

        balancer.UpdateShardStats({
            {5_TB / 4_KB, (4_TB + 400_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB + 100_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (4_TB + 300_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, (4_TB + 500_GB) / 4_KB, 0, 0},
        });

        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s5");
        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s1");
        ASSERT_NO_SB_ERROR(0, "s5");

        // 1 close to full shard left: s3

        balancer.UpdateShardStats({
            {5_TB / 4_KB, (5_TB - 512_KB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB + 100_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (4_TB + 300_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
        });

        ASSERT_NO_SB_ERROR(0, "s3");
        ASSERT_NO_SB_ERROR(0, "s3");

        // out of space

        balancer.UpdateShardStats({
            {5_TB / 4_KB, (5_TB - 512_KB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB + 100_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB + 300_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
        });

        ASSERT_SB_ERROR(0, E_FS_NOSPC);
        ASSERT_SB_ERROR(0, E_FS_NOSPC);
        ASSERT_SB_ERROR(0, E_FS_NOSPC);
    }

    Y_UNIT_TEST(ShouldBalanceShardsWithFileSizeRoundRobin)
    {
        TShardBalancerRoundRobin balancer(
            4_KB,
            1_TB,
            1_MB,
            {"s1", "s2", "s3", "s4", "s5"});

        balancer.UpdateShardStats({
            {5_TB / 4_KB, 512_GB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 3_TB / 4_KB, 0, 0},
        });

        // 1 TiB can fit in any shard

        ASSERT_NO_SB_ERROR(1_TB, "s1");
        ASSERT_NO_SB_ERROR(1_TB, "s3");
        ASSERT_NO_SB_ERROR(1_TB, "s4");
        ASSERT_NO_SB_ERROR(1_TB, "s2");
        ASSERT_NO_SB_ERROR(1_TB, "s5");
        ASSERT_NO_SB_ERROR(1_TB, "s1");
        ASSERT_NO_SB_ERROR(1_TB, "s3");
        ASSERT_NO_SB_ERROR(1_TB, "s4");
        ASSERT_NO_SB_ERROR(1_TB, "s2");
        ASSERT_NO_SB_ERROR(1_TB, "s5");

        // 3 TiB can fit in s1, s3, s4
        // but s1 is selected because it has >= 1 TiB reserve

        ASSERT_NO_SB_ERROR(3_TB, "s1");

        // 3 TiB + 600 GiB can't fit with a 1 TiB reserve but can physically
        // fit in s1, s3, s4

        ASSERT_NO_SB_ERROR(3_TB + 600_GB, "s3");
        ASSERT_NO_SB_ERROR(3_TB + 600_GB, "s4");
        ASSERT_NO_SB_ERROR(3_TB + 600_GB, "s1");

        // 4 TiB + 512 GiB can't fit at all

        ASSERT_SB_ERROR(4_TB + 512_GB, E_FS_NOSPC);
        ASSERT_SB_ERROR(4_TB + 512_GB, E_FS_NOSPC);
        ASSERT_SB_ERROR(4_TB + 512_GB, E_FS_NOSPC);
    }

    Y_UNIT_TEST(ShouldBalanceShardsRandom)
    {
        TShardBalancerRandom balancer(
            4_KB,
            1_TB,
            1_MB,
            {"s1", "s2", "s3", "s4", "s5"});
        const ui64 shardCount = 5;

        // 1 TiB can fit in any shard

        const ui64 iterations = 5000;
        // After 5000 random samples we expect each of five shards to be
        // encountered from 900 to 1100 times.
        const ui64 rangeToleration = 100;

        THashMap<TString, ui64> hitCount;
        for (ui64 i = 0; i < iterations; ++i) {
            TString shardId;
            const auto error = balancer.SelectShard(1_TB, &shardId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            hitCount[shardId];
            ++hitCount[shardId];
        }
        UNIT_ASSERT_VALUES_EQUAL(shardCount, hitCount.size());
        for (const auto& [shardId, count]: hitCount) {
            UNIT_ASSERT_GE(count, iterations / shardCount - rangeToleration);
            UNIT_ASSERT_LE(count, iterations / shardCount + rangeToleration);
        }

        // Now let's fill up last 3 shards to their limits
        balancer.UpdateShardStats({
            {5_TB / 4_KB, 0, 0, 0},
            {5_TB / 4_KB, 0, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
        });

        // 1 TiB can now fit only in s1 or s2
        hitCount.clear();
        for (ui64 i = 0; i < iterations; ++i) {
            TString shardId;
            const auto error = balancer.SelectShard(1_TB, &shardId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            hitCount[shardId];
            ++hitCount[shardId];
        }
        UNIT_ASSERT_VALUES_EQUAL(2, hitCount.size());
        for (const auto& [shardId, count]: hitCount) {
            UNIT_ASSERT_GE(count, iterations / 2 - rangeToleration);
            UNIT_ASSERT_LE(count, iterations / 2 + rangeToleration);
        }
    }

    Y_UNIT_TEST(ShouldBalanceShardsWeightedRandom)
    {
        TShardBalancerWeightedRandom balancer(
            4_KB,
            1_TB,
            0,
            {"s1", "s2", "s3", "s4", "s5"});
        const ui64 shardCount = 5;

        // 1 TiB can fit in any shard

        const ui64 iterations = 5000;
        // After 5000 random samples we expect each of five shards to be
        // encountered from 900 to 1100 times.
        const ui64 rangeToleration = 100;

        THashMap<TString, ui64> hitCount;
        for (ui64 i = 0; i < iterations; ++i) {
            TString shardId;
            const auto error = balancer.SelectShard(1_TB, &shardId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            hitCount[shardId];
            ++hitCount[shardId];
        }
        UNIT_ASSERT_VALUES_EQUAL(shardCount, hitCount.size());
        for (const auto& [shardId, count]: hitCount) {
            UNIT_ASSERT_GE(count, iterations / shardCount - rangeToleration);
            UNIT_ASSERT_LE(count, iterations / shardCount + rangeToleration);
        }

        // Now let's fill up last 2 shards to their limits and leave first 3
        // shards 1:2:3 free space
        balancer.UpdateShardStats({
            {5_TB / 4_KB, (5_TB - 1_TB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB - 2_TB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB - 3_TB) / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 5_TB / 4_KB, 0, 0},
        });

        // It is expected that s1, s2, s3 will be selected with 1:2:3 ratio
        hitCount.clear();
        for (ui64 i = 0; i < iterations; ++i) {
            TString shardId;
            const auto error = balancer.SelectShard(0, &shardId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            hitCount[shardId];
            ++hitCount[shardId];
        }
        UNIT_ASSERT_VALUES_EQUAL(3, hitCount.size());
        for (const auto& [shardId, count]: hitCount) {
            if (shardId == "s1") {
                UNIT_ASSERT_GE(count, iterations / 6 - rangeToleration);
                UNIT_ASSERT_LE(count, iterations / 6 + rangeToleration);
            } else if (shardId == "s2") {
                UNIT_ASSERT_GE(count, iterations / 3 - rangeToleration);
                UNIT_ASSERT_LE(count, iterations / 3 + rangeToleration);
            } else if (shardId == "s3") {
                UNIT_ASSERT_GE(count, iterations / 2 - rangeToleration);
                UNIT_ASSERT_LE(count, iterations / 2 + rangeToleration);
            }
        }

        // If we fill up all the shards with less than 1 TiB left it should not
        // be possible to select any shard
        balancer.UpdateShardStats(
            TVector<TShardStats>(
                shardCount,
                TShardStats{
                    .TotalBlocksCount = 5_TB / 4_KB,
                    .UsedBlocksCount = (5_TB - 500_GB) / 4_KB,
                    .CurrentLoad = 0,
                    .Suffer = 0}));
        for (ui64 i = 0; i < iterations; ++i) {
            ASSERT_SB_ERROR(1_TB, E_FS_NOSPC);
        }

        // For 500 GiB file though it should be possible to select any shard
        TString shardId;
        const auto error = balancer.SelectShard(500_GB, &shardId);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // For a situation where in every shard there is less than 1 TiB left,
        // we should disregard additional 1 TiB reserve
        balancer.UpdateShardStats({
            {5_TB / 4_KB, (5_TB - 1_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB - 2_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB - 3_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB - 4_GB) / 4_KB, 0, 0},
            {5_TB / 4_KB, (5_TB - 5_GB) / 4_KB, 0, 0},
        });
        // Trying to allocate 3 GiB should result in s3, s4, s5 being selected
        // in a 3:4:5 ratio
        hitCount.clear();
        for (ui64 i = 0; i < iterations; ++i) {
            TString shardId;
            const auto error = balancer.SelectShard(3_GB, &shardId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            hitCount[shardId];
            ++hitCount[shardId];
        }
        UNIT_ASSERT_VALUES_EQUAL(3, hitCount.size());
        for (const auto& [shardId, count]: hitCount) {
            if (shardId == "s3") {
                UNIT_ASSERT_GE(count, (iterations * 3) / 12 - rangeToleration);
                UNIT_ASSERT_LE(count, (iterations * 3) / 12 + rangeToleration);
            } else if (shardId == "s4") {
                UNIT_ASSERT_GE(count, (iterations * 4) / 12 - rangeToleration);
                UNIT_ASSERT_LE(count, (iterations * 4) / 12 + rangeToleration);
            } else if (shardId == "s5") {
                UNIT_ASSERT_GE(count, (iterations * 5) / 12 - rangeToleration);
                UNIT_ASSERT_LE(count, (iterations * 5) / 12 + rangeToleration);
            }
        }
        // For 5 GiB file though it should be possible to select only s5
        shardId.clear();
        for (ui64 i = 0; i < iterations; ++i) {
            const auto error = balancer.SelectShard(5_GB, &shardId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL("s5", shardId);
        }

        // For 6 GiB file it should not be possible to select any shard
        for (ui64 i = 0; i < iterations; ++i) {
            const auto error = balancer.SelectShard(6_GB, &shardId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOSPC,
                error.GetCode(),
                error.GetMessage());
        }
    }
}

#undef ASSERT_NO_SB_ERROR
#undef ASSERT_SB_ERROR

}   // namespace NCloud::NFileStore::NStorage
