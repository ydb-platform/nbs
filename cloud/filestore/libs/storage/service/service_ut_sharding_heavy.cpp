#include "service.h"
#include "service_private.h"
#include "service_ut_helpers.h"

#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/tablet/events/tablet_private.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>


#include <library/cpp/testing/unittest/registar.h>


namespace NCloud::NFileStore::NStorage {

using namespace NActors;

Y_UNIT_TEST_SUITE(TStorageServiceShardingHeavyTest)
{

    Y_UNIT_TEST(ShouldCreateALotOfShards)
    {
        const ui64 blockSize = 4_KB;
        const ui64 shardBlockCount = 1024;
        const ui64 shardAllocationUnit = shardBlockCount * blockSize;
        const ui64 shardCount = 288;
        const ui64 fsSize =
            shardBlockCount * (shardCount - 1) + shardBlockCount / 2;

        NProto::TStorageConfig config;
        config.SetStrictFileSystemSizeEnforcementEnabled(true);
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(shardAllocationUnit);
        config.SetMaxShardCount(1024);
        config.SetMaxShardManagementRequestsInFlight(0);

        const TString fsId = "test";

        TTestEnv env({}, config);

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);
        {
            TShardRequestCounter counters(env.GetRuntime(), fsId);

            service.CreateFileStore(fsId, fsSize);

            UNIT_ASSERT_VALUES_EQUAL(shardCount + 1, counters.CreateRequests);
            UNIT_ASSERT_VALUES_EQUAL(shardCount + 1, counters.CreateResponses);
            UNIT_ASSERT_VALUES_EQUAL(shardCount, counters.CreateMaxInFlight);
        }

        WaitForTabletStart(service);

        auto headers = service.InitSession(fsId, "client");

        // Check that the main fs and all the shards have the same size
        const auto stats = GetStorageStats(service, fsId).GetStats();
        const auto& shardStats = stats.GetShardStats();
        UNIT_ASSERT_EQUAL(shardCount, shardStats.size());
        UNIT_ASSERT_EQUAL(fsSize, stats.GetTotalBlocksCount());
        for (const auto& shardStat: shardStats) {
            UNIT_ASSERT_EQUAL(fsSize, shardStat.GetTotalBlocksCount());
        }

        const ui64 filesCount = shardCount * 2;
        ui64 shardNo = 1;
        ui64 sevenBytesHandlesCount = 0;
        for (ui64 i = 0; i < filesCount; ++i) {
            auto createNodeResponse =
                service.CreateNode(
                        headers,
                        TCreateNodeArgs::File(
                            RootNodeId,
                            TStringBuilder() << "file" << i))
                    ->Record;
            const ui64 nodeId = createNodeResponse.GetNode().GetId();

            sevenBytesHandlesCount += IsSeventhByteUsed(nodeId);
            UNIT_ASSERT_VALUES_EQUAL(
                shardNo > MaxOneByteShardCount,
                IsSeventhByteUsed(nodeId));
            UNIT_ASSERT_VALUES_EQUAL(shardNo, ExtractShardNo(nodeId));

            const ui64 handle = service.CreateHandle(
                headers,
                fsId,
                nodeId,
                "",
                TCreateHandleArgs::RDWR)->Record.GetHandle();

            UNIT_ASSERT_VALUES_EQUAL(
                shardNo > MaxOneByteShardCount,
                IsSeventhByteUsed(handle));
            UNIT_ASSERT_VALUES_EQUAL(shardNo, ExtractShardNo(handle));

            shardNo++;
            if (shardNo > shardCount) {
                shardNo = 1;
            }
        }

        // We created 2 files in each shard. Files in the shards with
        // shardNo > 255 have handles that use 7th byte.
        UNIT_ASSERT_VALUES_EQUAL(
            (shardCount - MaxOneByteShardCount) * 2,
            sevenBytesHandlesCount);

        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(15));

        // Update counters in all the shards.
        TDispatchOptions options;
        options.FinalEvents = {TDispatchOptions::TFinalEventCondition(
            TEvIndexTabletPrivate::EvAggregateStatsCompleted,
            shardCount + 1)};
        service.AccessRuntime().DispatchEvents(options);

        const auto mainStats = GetStorageStats(service, fsId);
        UNIT_ASSERT_VALUES_EQUAL(
            sevenBytesHandlesCount,
            mainStats.GetStats().GetSevenBytesHandlesCount());
    }

    Y_UNIT_TEST(ShouldCreateALotOfShardsThrottled)
    {
        const ui64 blockSize = 4_KB;
        const ui64 shardBlockCount = 1024;
        const ui64 shardAllocationUnit = shardBlockCount * blockSize;
        const ui64 shardCount = 288;
        const ui64 fsSize =
            shardBlockCount * (shardCount - 1) + shardBlockCount / 2;
        const ui32 requestsLimit = 32;

        NProto::TStorageConfig config;
        config.SetStrictFileSystemSizeEnforcementEnabled(true);
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(shardAllocationUnit);
        config.SetMaxShardCount(1024);
        config.SetMaxShardManagementRequestsInFlight(requestsLimit);

        const TString fsId = "test";

        TTestEnv env({}, config);

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);
        {
            TShardRequestCounter counters(env.GetRuntime(), fsId);

            service.CreateFileStore(fsId, fsSize);

            UNIT_ASSERT_VALUES_EQUAL(shardCount + 1, counters.CreateRequests);
            UNIT_ASSERT_VALUES_EQUAL(shardCount + 1, counters.CreateResponses);
            UNIT_ASSERT_VALUES_EQUAL(requestsLimit, counters.CreateMaxInFlight);
        }

        WaitForTabletStart(service);

        auto headers = service.InitSession(fsId, "client");

        // Check that the main fs and all the shards have the same size
        const auto stats = GetStorageStats(service, fsId).GetStats();
        const auto& shardStats = stats.GetShardStats();
        UNIT_ASSERT_EQUAL(shardCount, shardStats.size());
        UNIT_ASSERT_EQUAL(fsSize, stats.GetTotalBlocksCount());
        for (const auto& shardStat: shardStats) {
            UNIT_ASSERT_EQUAL(fsSize, shardStat.GetTotalBlocksCount());
        }
    }

    Y_UNIT_TEST(ShouldBalanceShardsByWeightedDeterministic)
    {
        constexpr ui64 blockSize = 4_KB;
        ui64 shardCount = 8;
        ui64 fsSize = 9_MB / 2 + 100_KB;
        const ui64 shardAllocationUnit = fsSize / shardCount;

        NProto::TStorageConfig config;
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(shardAllocationUnit);
        config.SetStrictFileSystemSizeEnforcementEnabled(true);
        config.SetShardBalancerPrecisionBytes(16_KB);
        config.SetShardBalancerPolicy(NProto::SBP_WEIGHTED_DETERMINISTIC);

        TTestEnv env({}, config);
        const ui32 nodeIdx = env.AddDynamicNode();

        const TString fsId = "test";
        TServiceClient service(env.GetRuntime(), nodeIdx);

        TMap<TString, TActorId> fsToActor;

        CreateOrResizeFilesystem(
            service,
            fsId,
            fsSize / blockSize,
            false,
            fsToActor);

        auto headers = service.InitSession(fsId, "client");

        auto updateCounters = [&]() {
            for (const auto& item: fsToActor) {
                UpdateCounters(env, service, nodeIdx, item.second);
            }
        };

        updateCounters();

        ui64 totalFilesSize = 0;
        ui64 filesCount = 0;
        auto createFiles = [&] (ui64 totalSizeLimit) {
            while (totalFilesSize < totalSizeLimit) {
                for (ui64 fileSize: {40_KB, 80_KB}) {
                    const auto response = service.CreateHandle(
                        headers,
                        fsId,
                        RootNodeId,
                        TStringBuilder() << "file" << filesCount++,
                        TCreateHandleArgs::CREATE)->Record;
                    service.AllocateData(
                        headers,
                        fsId,
                        response.GetNodeAttr().GetId(),
                        response.GetHandle(),
                        0,
                        fileSize);

                    totalFilesSize += fileSize;

                    if (filesCount % 40 == 0) {
                        updateCounters();
                    }
                }
            }
        };

        createFiles(4_MB);

        fsSize *= 2;
        shardCount *= 2;
        CreateOrResizeFilesystem(
            service,
            fsId,
            fsSize / blockSize,
            true,
            fsToActor);

        headers = service.InitSession(fsId, "client");

        updateCounters();

        env.GetRuntime().ResetScheduledCount();
        createFiles(9_MB);

        auto stats = GetStorageStats(service, fsId).GetStats();
        UNIT_ASSERT_VALUES_EQUAL(shardCount, stats.GetShardStats().size());

        ui64 minOldShardSize = Max<ui64>();
        ui64 maxOldShardSize = 0;
        ui64 totalOld = 0;
        ui64 minNewShardSize = Max<ui64>();
        ui64 maxNewShardSize = 0;
        ui64 totalNew = 0;
        stats = GetStorageStats(service, fsId).GetStats();
        UNIT_ASSERT_VALUES_EQUAL(shardCount, stats.GetShardStats().size());
        for (ui64 i = 0; i < shardCount / 2; ++i) {
            const auto& shardStats = stats.GetShardStats(i);
            const ui64 shardSize = shardStats.GetUsedBlocksCount() * blockSize;
            minOldShardSize = Min<ui64>(minOldShardSize, shardSize);
            maxOldShardSize = Max<ui64>(maxOldShardSize, shardSize);
            totalOld += shardSize;
        }
        for (ui64 i = shardCount / 2; i < shardCount; ++i) {
            const auto& shardStats = stats.GetShardStats(i);
            const ui64 shardSize = shardStats.GetUsedBlocksCount() * blockSize;
            minNewShardSize = Min<ui64>(minNewShardSize, shardSize);
            maxNewShardSize = Max<ui64>(maxNewShardSize, shardSize);
            totalNew += shardSize;
        }

        // We actually need to confirm that the balancer really balances shards.
        // Balancing in this test is imperfect because it creates relatively few
        // files, and those files are large relative to the filesystem.
        UNIT_ASSERT_LE(
            static_cast<double>(maxOldShardSize) / minOldShardSize,
            1.4);
        UNIT_ASSERT_LE(
            static_cast<double>(maxNewShardSize) / minNewShardSize,
            1.5);
        UNIT_ASSERT_LE(static_cast<double>(totalOld) / totalNew, 1.4);
    }

    Y_UNIT_TEST(
        ShouldBalanceShardsWithDirectoryRestrictionByWeightedDeterministic)
    {
        constexpr ui64 blockSize = 4_KB;
        constexpr ui64 shardCount = 8;
        constexpr ui64 fsSize = 4_MB + 100_KB;
        const ui64 shardAllocationUnit = fsSize / shardCount;

        NProto::TStorageConfig config;
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(shardAllocationUnit);
        config.SetStrictFileSystemSizeEnforcementEnabled(true);
        config.SetShardBalancerPrecisionBytes(16_KB);
        config.SetShardBalancerPolicy(NProto::SBP_WEIGHTED_DETERMINISTIC);
        config.SetDirectoryCreationInShardsEnabled(true);
        config.SetShardsPerDirectoryCount(4);

        TTestEnv env({}, config);
        const ui32 nodeIdx = env.AddDynamicNode();

        const TString fsId = "test";
        TServiceClient service(env.GetRuntime(), nodeIdx);

        TMap<TString, TActorId> fsToActor;

        CreateOrResizeFilesystem(
            service,
            fsId,
            fsSize / blockSize,
            false,
            fsToActor);

        auto headers = service.InitSession(fsId, "client");

        auto updateCounters = [&]() {
            for (const auto& item: fsToActor) {
                UpdateCounters(env, service, nodeIdx, item.second);
            }
        };

        updateCounters();

        TMap<ui32, ui64> dirByShard;
        const TVector<ui32> expectedDirectoryShards =
            {1, 2, 3, 4, 5, 6, 7, 8};
        ui64 directoryNo = 0;
        ui64 parentId = RootNodeId;
        for (ui64 i = 0; i < shardCount; ++i) {
            const auto response = service.CreateNode(
                headers,
                TCreateNodeArgs::Directory(
                    parentId,
                    TStringBuilder() << "dir" << directoryNo))->Record;

            const ui64 directoryId = response.GetNode().GetId();
            const ui32 shardNo = ExtractShardNo(directoryId);
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDirectoryShards[directoryNo],
                shardNo);
            UNIT_ASSERT(
                dirByShard.emplace(shardNo, directoryId).second);

            parentId = directoryId;
            ++directoryNo;
        }
        UNIT_ASSERT_VALUES_EQUAL(shardCount, dirByShard.size());

        ui64 filesCount = 0;

        auto createFiles = [&](ui64 directoryId,
                               ui64 bytesToCreate,
                               const TSet<ui32>& expectedShards)
        {
            ui64 totalSize = 0;
            TSet<ui32> usedShards;
            while (totalSize < bytesToCreate) {
                for (ui64 fileSize: {40_KB, 80_KB}) {
                    const auto response = service.CreateHandle(
                        headers,
                        fsId,
                        directoryId,
                        TStringBuilder() << "file" << filesCount++,
                        TCreateHandleArgs::CREATE)->Record;

                    const ui32 shardNo =
                        ExtractShardNo(response.GetNodeAttr().GetId());
                    UNIT_ASSERT_C(
                        expectedShards.contains(shardNo),
                        TStringBuilder()
                            << "unexpected shard " << shardNo
                            << " for directory in shard "
                            << ExtractShardNo(directoryId));
                    usedShards.insert(shardNo);

                    service.AllocateData(
                        headers,
                        fsId,
                        response.GetNodeAttr().GetId(),
                        response.GetHandle(),
                        0,
                        fileSize);
                    totalSize += fileSize;

                    if (filesCount % 32 == 0) {
                        updateCounters();
                    }
                }
            }
        };

        const TSet<ui32> dir2Shards = {3, 4, 5, 6};
        const TSet<ui32> dir4Shards = {5, 6, 7, 8};
        for (ui32 i = 0; i < 8; ++i) {
            createFiles(dirByShard[2], 128_KB, dir2Shards);
            createFiles(dirByShard[4], 128_KB, dir4Shards);
            env.GetRuntime().ResetScheduledCount();
        }

        auto stats = GetStorageStats(service, fsId).GetStats();
        UNIT_ASSERT_VALUES_EQUAL(shardCount, stats.GetShardStats().size());

        ui64 minShardSize = Max<ui64>();
        ui64 maxShardSize = 0;
        for (ui64 i = 0; i < shardCount; ++i) {
            const auto& shardStats = stats.GetShardStats(i);
            const ui64 shardSize = shardStats.GetUsedBlocksCount() * blockSize;
            if (dir2Shards.contains(i + 1) || dir4Shards.contains(i + 1)) {
                minShardSize = Min<ui64>(minShardSize, shardSize);
                maxShardSize = Max<ui64>(maxShardSize, shardSize);
            } else {
                UNIT_ASSERT_EQUAL(0, shardSize);
            }
        }

        UNIT_ASSERT_LE(static_cast<double>(maxShardSize) / minShardSize, 1.6);
    }

}

}   // namespace NCloud::NFileStore::NStorage
