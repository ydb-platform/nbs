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

    Y_UNIT_TEST(ShouldBalanceShards)
    {
        TShardBalancer balancer;
        balancer.SetParameters(4_KB, 1_TB, 1_MB);
        balancer.UpdateShards({"s1", "s2", "s3", "s4", "s5"});
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

    Y_UNIT_TEST(ShouldBalanceShardsWithFileSize)
    {
        TShardBalancer balancer;
        balancer.SetParameters(4_KB, 1_TB, 1_MB);
        balancer.UpdateShards({"s1", "s2", "s3", "s4", "s5"});

        balancer.UpdateShardStats({
            {5_TB / 4_KB, 512_GB / 4_KB, 0, 0},
            {5_TB / 4_KB, 2_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 1_TB / 4_KB, 0, 0},
            {5_TB / 4_KB, 3_TB / 4_KB, 0, 0},
        });

        // 1_TB can fit in any shard

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

        // 3_TB can fit in s1, s3, s4
        // but s1 is selected because it has >= 1_TB reserve

        ASSERT_NO_SB_ERROR(3_TB, "s1");

        // 3_TB + 600_GB can't fit with a 1_TB reserve but can physically
        // fit in s1, s3, s4

        ASSERT_NO_SB_ERROR(3_TB + 600_GB, "s3");
        ASSERT_NO_SB_ERROR(3_TB + 600_GB, "s4");
        ASSERT_NO_SB_ERROR(3_TB + 600_GB, "s1");

        // 4_TB + 512_GB can't fit at all

        ASSERT_SB_ERROR(4_TB + 512_GB, E_FS_NOSPC);
        ASSERT_SB_ERROR(4_TB + 512_GB, E_FS_NOSPC);
        ASSERT_SB_ERROR(4_TB + 512_GB, E_FS_NOSPC);
    }
}

#undef ASSERT_NO_SB_ERROR
#undef ASSERT_SB_ERROR

}   // namespace NCloud::NFileStore::NStorage
