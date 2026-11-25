#include "part_ut.h"

#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/storage/partition_common/model/fresh_blob_test.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NCloud::NStorage;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionFreshBlocksTest)
{
    Y_UNIT_TEST(ShouldRecoverBlocksOnRebootFromFreshChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshChannelZeroRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.ZeroBlocks(4);
        partition.WriteBlocks(2, 2);
        partition.ZeroBlocks(5);
        partition.WriteBlocks(3, 3);
        partition.ZeroBlocks(6);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(6, stats.GetFreshBlocksCount());

        auto partitionInfo = partition.GetPartitionInfo();
        auto value =
            NJson::ReadJsonFastTree(partitionInfo->Record.GetPayload());
        auto stateJson = value["State"];
        UNIT_ASSERT_VALUES_EQUAL(
            6,
            stateJson["FreshBlocksFromChannel"].GetInteger());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            stateJson["FreshBlocksFromDb"].GetInteger());

        partition.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3)));

        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(4)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(6)));
    }

    Y_UNIT_TEST(ShouldRecoverFreshBlobTimestampsOnReboot)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshChannelZeroRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto getLowestCommitIdTimestamp = [&]()
        {
            auto partitionInfo = partition.GetPartitionInfo();
            auto value =
                NJson::ReadJsonFastTree(partitionInfo->Record.GetPayload());
            const auto& state = value["State"];
            if (!state.Has("FirstUnflushedFreshBlobTimestamp")) {
                return TInstant::Max();
            }
            return TInstant::MicroSeconds(
                state["FirstUnflushedFreshBlobTimestamp"].GetUInteger());
        };

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(), getLowestCommitIdTimestamp());

        const auto zeroTime = runtime->GetCurrentTime();
        partition.ZeroBlocks(1);

        runtime->AdvanceCurrentTime(TDuration::Seconds(5));
        const auto writeTime = runtime->GetCurrentTime();
        partition.WriteBlocks(2, 2);

        const auto lowestCommitIdTimestamp = getLowestCommitIdTimestamp();
        UNIT_ASSERT_GE(lowestCommitIdTimestamp, zeroTime);
        UNIT_ASSERT_LT(lowestCommitIdTimestamp, writeTime);

        // Prevent flush after reboot to keep fresh blobs in the partition.
        ui32 droppedFlushRequests = 0;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvFlushRequest) {
                    ++droppedFlushRequests;
                    return true;
                }
                return false;
            });

        runtime->AdvanceCurrentTime(TDuration::Seconds(10));
        partition.RebootTablet();
        partition.WaitReady();
        UNIT_ASSERT_GT(droppedFlushRequests, 0);

        UNIT_ASSERT_VALUES_EQUAL(
            lowestCommitIdTimestamp,
            getLowestCommitIdTimestamp());

        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldRecoverBlocksOnRebootFromFreshChannelWithLongHistory)
    {
        const ui32 blockCount = 1024;
        const ui32 channelCount = DataChannelOffset + 2;
        const ui32 freshChannelNo = channelCount - 1;
        const ui32 freshChannelHistorySize = 3;
        const auto groupCount =
            channelCount - DataChannelOffset + freshChannelHistorySize - 1;
        const ui32 nodeIdx = 0;

        // initializing test runtime

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();
        InitLogSettings(runtime);

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        {
            env.CreateSubDomain("nbs");
            auto storageConfig = CreateTestStorageConfig(config);
            env.CreateBlockStoreNode(
                "nbs",
                storageConfig,
                CreateTestDiagnosticsConfig());
        }

        const auto tabletId = NKikimr::MakeTabletID(1, HiveId, 1);
        std::unique_ptr<TTabletStorageInfo> x(new TTabletStorageInfo());

        x->TabletID = tabletId;
        x->TabletType = TTabletTypes::BlockStorePartition;
        auto& channels = x->Channels;
        channels.resize(channelCount);

        for (ui64 channel = 0; channel < channels.size(); ++channel) {
            channels[channel].Channel = channel;
            channels[channel].Type = TBlobStorageGroupType(BootGroupErasure);
            channels[channel].History.resize(1);
            channels[channel].History[0].FromGeneration = 0;
            const auto gidx =
                channel > DataChannelOffset ? channel - DataChannelOffset : 0;
            channels[channel].History[0].GroupID = env.GetGroupIds()[gidx];
        }

        // filling fresh channel history with 3 items

        auto& freshChannel = channels[freshChannelNo];
        freshChannel.Channel = freshChannelNo;
        freshChannel.Type = TBlobStorageGroupType(BootGroupErasure);
        auto& history = freshChannel.History;
        history.resize(freshChannelHistorySize);
        auto setHistoryItem = [&](ui32 i, ui32 gen)
        {
            history[i].FromGeneration = gen;
            const auto gidx = freshChannelNo - DataChannelOffset + i;
            history[i].GroupID = env.GetGroupIds()[gidx];
        };
        setHistoryItem(0, 1);
        setHistoryItem(1, 2);
        setHistoryItem(2, 3);   // this one is a bit fake - it's from the future

        InitTestActorRuntime(
            runtime,
            config,
            blockCount,
            channelCount,
            std::move(x));

        // fresh blob builder func

        auto makeResponse = [&](ui32 gen, ui32 step, ui32 block, TString data)
        {
            TVector<TVector<TString>> buffers{{std::move(data)}};
            const auto holders = GetHolders(buffers);
            auto blob = BuildWriteFreshBlocksBlobContent(
                {TBlockRange32::MakeOneBlock(block)},
                holders,
                TInstant::Zero());

            TPartialBlobId blobId(
                gen,
                step,
                freshChannelNo,
                static_cast<ui32>(blob.size()),
                0,   // cookie
                0    // partId
            );

            return TEvBlobStorage::TEvRangeResult::TResponse(
                MakeBlobId(tabletId, blobId),
                std::move(blob));
        };

        // setting up event observer (filter) to replace real EvRangeResult
        // with a fake one

        TVector<ui32> groupsRequested;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvRangeResult: {
                        using TEvent = TEvBlobStorage::TEvRangeResult;
                        auto* msg = event->Get<TEvent>();

                        if (msg->From.Channel() != freshChannelNo) {
                            return false;
                        }

                        auto response = std::make_unique<TEvent>(
                            msg->Status,
                            msg->From,
                            msg->To,
                            msg->GroupId);

                        groupsRequested.push_back(msg->GroupId);

                        if (msg->GroupId == history[0].GroupID) {
                            response->Responses.push_back(makeResponse(
                                0,
                                1,
                                0,
                                TString(DefaultBlockSize, 'A')));
                        } else if (msg->GroupId == history[1].GroupID) {
                            response->Responses.push_back(makeResponse(
                                1,
                                1,
                                1,
                                TString(DefaultBlockSize, 'B')));
                        } else if (msg->GroupId == history[2].GroupID) {
                            response->Responses.push_back(makeResponse(
                                2,
                                1,
                                2,
                                TString(DefaultBlockSize, 'C')));
                        } else {
                            UNIT_ASSERT_C(
                                false,
                                TStringBuilder()
                                    << "unexpected group: " << msg->GroupId);
                        }

                        auto* handle = new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0,
                            event->Cookie);

                        runtime.Send(handle, 0);

                        return true;
                    }

                    default:
                        return false;
                }
            });

        TPartitionClient partition(runtime, nodeIdx, tabletId);
        partition.WaitReady();

        // all groups should've been requested upon first tablet launch

        Sort(groupsRequested);
        UNIT_ASSERT_VALUES_EQUAL(3, groupsRequested.size());
        UNIT_ASSERT_VALUES_EQUAL(history[0].GroupID, groupsRequested[0]);
        UNIT_ASSERT_VALUES_EQUAL(history[1].GroupID, groupsRequested[1]);
        UNIT_ASSERT_VALUES_EQUAL(history[2].GroupID, groupsRequested[2]);
        groupsRequested.clear();

        // checking that our data wasn't corrupted

        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'A'),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'B'),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'C'),
            GetBlockContent(partition.ReadBlocks(2)));

        // triggering Trim to initialize TrimFreshLogToCommitId

        partition.TrimFreshLog();

        // all fresh blocks loaded upon startup acquire barriers in
        // TrimFreshLogBarriers, that's why we need to cause another Flush
        // to move our TrimFreshLogToCommitId in meta past some of those blocks

        partition.WriteBlocks(3, TString(DefaultBlockSize, 'D'));
        partition.Flush();

        partition.RebootTablet();
        partition.WaitReady();

        // after reboot our tablet shouldn't load fresh blobs from the groups
        // that certainly contain only the data that was generated before
        // TrimFreshLogToCommitId

        Sort(groupsRequested);
        UNIT_ASSERT_VALUES_EQUAL(2, groupsRequested.size());
        UNIT_ASSERT_VALUES_EQUAL(history[1].GroupID, groupsRequested[0]);
        UNIT_ASSERT_VALUES_EQUAL(history[2].GroupID, groupsRequested[1]);
        groupsRequested.clear();

        // checking that our data wasn't corrupted

        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'A'),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'B'),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'C'),
            GetBlockContent(partition.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'D'),
            GetBlockContent(partition.ReadBlocks(3)));
    }

    Y_UNIT_TEST(ShouldFlushAsNewBlob)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        partition.ZeroBlocks(4);
        partition.ZeroBlocks(5);
        partition.ZeroBlocks(6);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());

            auto partitionInfo = partition.GetPartitionInfo();
            auto value =
                NJson::ReadJsonFastTree(partitionInfo->Record.GetPayload());
            auto stateJson = value["State"];
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                stateJson["FreshBlocksFromChannel"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(
                6,
                stateJson["FreshBlocksFromDb"].GetInteger());
        }

        partition.Flush();
        // Fresh Zero blocks from the database are flushed only if the 4MB
        // threshold is reached or if there are no blocks with content.
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());

            UNIT_ASSERT(stats.GetSysWriteCounters().GetExecTime() != 0);
        }

        for (auto i: {1, 2, 3}) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(i),
                GetBlockContent(partition.ReadBlocks(i)));
        }

        for (auto i: {4, 5, 6}) {
            UNIT_ASSERT_VALUES_EQUAL(
                TString(),
                GetBlockContent(partition.ReadBlocks(i)));
        }
    }

    Y_UNIT_TEST(ShouldFlushBlocksFromFreshChannelAsNewBlob)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshChannelZeroRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        partition.ZeroBlocks(4);
        partition.ZeroBlocks(5);
        partition.ZeroBlocks(6);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());

            auto partitionInfo = partition.GetPartitionInfo();
            auto value =
                NJson::ReadJsonFastTree(partitionInfo->Record.GetPayload());
            auto stateJson = value["State"];
            UNIT_ASSERT_VALUES_EQUAL(
                6,
                stateJson["FreshBlocksFromChannel"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                stateJson["FreshBlocksFromDb"].GetInteger());
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());

            UNIT_ASSERT(stats.GetSysWriteCounters().GetExecTime() != 0);
        }

        for (auto i: {1, 2, 3}) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(i),
                GetBlockContent(partition.ReadBlocks(i)));
        }

        for (auto i: {4, 5, 6}) {
            UNIT_ASSERT_VALUES_EQUAL(
                TString{},
                GetBlockContent(partition.ReadBlocks(i)));
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyFlush)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.WriteBlocks(MaxBlocksCount - 1, 0);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyFlushBlocksFromFreshChannel)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.WriteBlocks(MaxBlocksCount - 1, 0);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldScaleFlushThresholdByPartitionSize)
    {
        struct TTestCase
        {
            ui32 FlushThresholdPerAllocationUnit = 0;
            ui32 MaxFlushThreshold = 0;
            ui64 PartitionByteCount = 0;
            ui32 ExpectedFlushThresholdBlockCount = 0;
        };

        // Allocation unit is 1 GB, the fixed flush threshold is 40 KB.
        const TTestCase testCases[] = {
            {.FlushThresholdPerAllocationUnit = 0,
             .MaxFlushThreshold = 16_KB,
             .PartitionByteCount = 4_GB,
             .ExpectedFlushThresholdBlockCount = 10},
            {.FlushThresholdPerAllocationUnit = 32_KB,
             .MaxFlushThreshold = 0,
             .PartitionByteCount = 4_GB,
             .ExpectedFlushThresholdBlockCount = 10},
            {.FlushThresholdPerAllocationUnit = 32_KB,
             .MaxFlushThreshold = 1_MB,
             .PartitionByteCount = 512_MB,
             .ExpectedFlushThresholdBlockCount = 8},
            {.FlushThresholdPerAllocationUnit = 32_KB,
             .MaxFlushThreshold = 1_MB,
             .PartitionByteCount = 1536_MB,
             .ExpectedFlushThresholdBlockCount = 12},
            {.FlushThresholdPerAllocationUnit = 32_KB,
             .MaxFlushThreshold = 64_KB,
             .PartitionByteCount = 4_GB,
             .ExpectedFlushThresholdBlockCount = 16},
        };

        for (const auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_SSD,
              NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_HYBRID})
        {
            for (const auto& testCase: testCases) {
                const bool isSSD =
                    mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD;
                // The other media kind's settings would flush on every write.
                const auto pick = [isSSD](ui32 ssdValue, ui32 hddValue)
                {
                    return isSSD ? ssdValue : hddValue;
                };
                const ui32 perUnit = testCase.FlushThresholdPerAllocationUnit;
                const ui32 maxThreshold = testCase.MaxFlushThreshold;

                auto config = DefaultConfig();
                config.SetWriteBlobThreshold(4_MB);
                config.SetWriteBlobThresholdSSD(4_MB);
                config.SetAllocationUnitHDD(1);
                config.SetAllocationUnitSSD(1);
                config.SetFlushThresholdSSD(pick(40_KB, 4_KB));
                config.SetFlushThresholdHDD(pick(4_KB, 40_KB));
                config.SetFlushThresholdPerAllocationUnitSSD(
                    pick(perUnit, 4_KB));
                config.SetFlushThresholdPerAllocationUnitHDD(
                    pick(4_KB, perUnit));
                config.SetMaxFlushThresholdSSD(pick(maxThreshold, 4_KB));
                config.SetMaxFlushThresholdHDD(pick(4_KB, maxThreshold));

                const TString caseDescription =
                    TStringBuilder()
                    << NCloud::NProto::EStorageMediaKind_Name(mediaKind)
                    << ", per unit: " << perUnit << ", max: " << maxThreshold
                    << ", partition bytes: " << testCase.PartitionByteCount;

                auto runtime = PrepareTestActorRuntime(
                    config,
                    testCase.PartitionByteCount / DefaultBlockSize,
                    {},
                    {.MediaKind = mediaKind});
                TPartitionClient partition(*runtime);
                partition.WaitReady();

                const ui32 thresholdBlockCount =
                    testCase.ExpectedFlushThresholdBlockCount;
                partition.WriteBlocks(
                    TBlockRange32::WithLength(0, thresholdBlockCount - 1));
                runtime->DispatchEvents({}, TDuration::Seconds(1));
                UNIT_ASSERT_VALUES_EQUAL_C(
                    thresholdBlockCount - 1,
                    partition.StatPartition()
                        ->Record.GetStats()
                        .GetFreshBlocksCount(),
                    caseDescription);

                partition.WriteBlocks(thresholdBlockCount - 1, 1);
                runtime->DispatchEvents({}, TDuration::Seconds(1));
                UNIT_ASSERT_VALUES_EQUAL_C(
                    0,
                    partition.StatPartition()
                        ->Record.GetStats()
                        .GetFreshBlocksCount(),
                    caseDescription);
            }
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyFlushZerosFromFreshChannelBeforeHardLimit)
    {
        for (const auto mediaKind:
             {NProto::STORAGE_MEDIA_SSD, NProto::STORAGE_MEDIA_HDD})
        {
            auto config = DefaultConfig();
            config.SetFreshBlocksWriterEnabled(false);
            config.SetFreshChannelZeroRequestsEnabled(true);
            config.SetFlushThresholdSSD(8_KB);
            config.SetFlushThresholdHDD(8_KB);
            config.SetFreshLogicalBlocksByteCountHardLimit(16_KB);

            auto runtime = PrepareTestActorRuntime(
                config,
                1024,
                {},
                {.MediaKind = mediaKind});
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            for (ui32 batch = 0; batch < 3; ++batch) {
                partition.ZeroBlocks(2 * batch);
                UNIT_ASSERT_VALUES_EQUAL(
                    1,
                    partition.StatPartition()
                        ->Record.GetStats()
                        .GetFreshBlocksCount());

                partition.ZeroBlocks(2 * batch + 1);
                runtime->DispatchEvents({}, TDuration::Seconds(1));

                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
            }
        }
    }

    Y_UNIT_TEST(
        ShouldAutomaticallyFlushBlocksWhenFreshBlobCountThresholdIsReached)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);
        config.SetFreshBlobCountFlushThresholdHDD(4);
        config.SetFreshBlobByteCountFlushThresholdHDD(999999999);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));
        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));
        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.WriteBlocks(0, 0);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(
        ShouldAutomaticallyFlushBlocksWhenFreshBlobByteCountThresholdIsReached)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);
        config.SetFreshBlobCountFlushThresholdHDD(999999);
        config.SetFreshBlobByteCountFlushThresholdHDD(15_MB);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));
        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));
        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldFlushIdleFreshBlobsByAgeOnlyWhenEnabled)
    {
        for (const auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_SSD,
              NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_HYBRID})
        {
            for (bool enabled: {false, true}) {
                auto config = DefaultConfig();
                config.SetFreshChannelWriteRequestsEnabled(true);
                config.SetFreshChannelZeroRequestsEnabled(true);
                const auto threshold = enabled ? 60000 : 0;
                const bool isSSD =
                    mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD;
                config.SetFreshBlobAgeFlushThresholdSSD(isSSD ? threshold : 1);
                config.SetFreshBlobAgeFlushThresholdHDD(isSSD ? 1 : threshold);

                auto runtime = PrepareTestActorRuntime(
                    config,
                    1024,
                    {},
                    {.MediaKind = mediaKind});
                TPartitionClient partition(*runtime);
                partition.WaitReady();

                partition.WriteBlocks(1, 1);
                partition.ZeroBlocks(2);

                runtime->AdvanceCurrentTime(TDuration::Seconds(30));
                partition.SendToPipe(
                    std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
                UNIT_ASSERT_VALUES_EQUAL(
                    2,
                    partition.StatPartition()
                        ->Record.GetStats()
                        .GetFreshBlocksCount());

                runtime->AdvanceCurrentTime(TDuration::Seconds(31));
                partition.SendToPipe(
                    std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
                runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? 0 : 2,
                    partition.StatPartition()
                        ->Record.GetStats()
                        .GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    GetBlockContent(1),
                    GetBlockContent(partition.ReadBlocks(1)));
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(),
                    GetBlockContent(partition.ReadBlocks(2)));
            }
        }
    }

    Y_UNIT_TEST(ShouldFlushFreshBlobsWithUnknownTimestampsByAge)
    {
        for (bool enabled: {false, true}) {
            auto config = DefaultConfig();
            config.SetFreshChannelWriteRequestsEnabled(true);
            config.SetFreshBlobAgeFlushThresholdHDD(enabled ? 60000 : 0);

            auto runtime = PrepareTestActorRuntime(config);
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvAddFreshBlocksRequest)
                    {
                        auto* msg = event->Get<TEvPartitionCommonPrivate::
                                                   TEvAddFreshBlocksRequest>();
                        msg->Timestamp = TInstant::Zero();
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });

            // The test clock starts at the epoch; move it past the threshold
            // so that a zero timestamp is older than any real one.
            runtime->AdvanceCurrentTime(TDuration::Hours(1));

            // Blobs without timestamps count as old, so no time advance is
            // needed after the write.
            partition.WriteBlocks(1, 1);
            partition.SendToPipe(
                std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
            runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(
                enabled ? 0 : 1,
                partition.StatPartition()
                    ->Record.GetStats()
                    .GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(1),
                GetBlockContent(partition.ReadBlocks(1)));
        }
    }

    Y_UNIT_TEST(ShouldWaitForFutureFreshBlobTimestampsInAgeFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshBlobAgeFlushThresholdHDD(60000);

        auto runtime = PrepareTestActorRuntime(config);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvAddFreshBlocksRequest)
                {
                    auto* msg = event->Get<
                        TEvPartitionCommonPrivate::TEvAddFreshBlocksRequest>();
                    msg->Timestamp =
                        runtime->GetCurrentTime() + TDuration::Seconds(120);
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.WriteBlocks(1, 1);
        runtime->AdvanceCurrentTime(TDuration::Seconds(61));
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            partition.StatPartition()->Record.GetStats().GetFreshBlocksCount());

        runtime->AdvanceCurrentTime(TDuration::Seconds(120));
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            partition.StatPartition()->Record.GetStats().GetFreshBlocksCount());
    }

    Y_UNIT_TEST(ShouldExcludeObsoleteBlobsBeforeTrimBoundaryFromAgeFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshBlobAgeFlushThresholdHDD(60000);

        auto runtime = PrepareTestActorRuntime(config);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        runtime->AdvanceCurrentTime(TDuration::Seconds(61));
        partition.WriteBlocks(1, 2);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            partition.StatPartition()->Record.GetStats().GetFreshBlocksCount());

        runtime->AdvanceCurrentTime(TDuration::Seconds(61));
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            partition.StatPartition()->Record.GetStats().GetFreshBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldExcludeFlushedBlobsAwaitingTrimFromAgeFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshBlobAgeFlushThresholdHDD(60000);

        auto runtime = PrepareTestActorRuntime(config);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool trimRequested = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvTrimFreshLogRequest)
                {
                    trimRequested = true;
                    return true;
                }
                return false;
            });

        partition.WriteBlocks(1, 1);
        partition.Flush();
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
        UNIT_ASSERT(trimRequested);

        runtime->AdvanceCurrentTime(TDuration::Seconds(120));
        partition.WriteBlocks(2, 2);
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            partition.StatPartition()->Record.GetStats().GetFreshBlocksCount());
    }

    Y_UNIT_TEST(ShouldExcludeFlushedBlobsAboveTrimBoundaryFromAgeFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshBlobAgeFlushThresholdHDD(60000);

        auto runtime = PrepareTestActorRuntime(config);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool intercept = true;
        std::unique_ptr<IEventHandle> addFreshBlocks;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvAddFreshBlocksRequest &&
                    intercept)
                {
                    intercept = false;
                    addFreshBlocks.reset(event.Release());
                    return true;
                }
                return false;
            });

        // The in-flight write holds the trim boundary below the next blobs.
        partition.SendWriteBlocksRequest(1, 1);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
        UNIT_ASSERT(addFreshBlocks);

        partition.WriteBlocks(2, 2);
        partition.Flush();

        runtime->AdvanceCurrentTime(TDuration::Seconds(61));
        partition.WriteBlocks(3, 3);
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            partition.StatPartition()->Record.GetStats().GetFreshBlocksCount());

        // The completed write is the oldest unflushed blob.
        runtime->Send(addFreshBlocks.release());
        partition.RecvWriteBlocksResponse();
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            partition.StatPartition()->Record.GetStats().GetFreshBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldNotAddSmallNonDeletionBlobsDuringFlush)
    {
        auto config = DefaultConfig(4_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        constexpr ui32 extraBlocks = 4;
        partition.WriteBlocks(
            TBlockRange32::WithLength(MaxBlocksCount - 1, extraBlocks + 1));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(extraBlocks, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldAddSmallBlobsDuringFlushIfThereAreAnyBlobsInFreshChannel)
    {
        auto config = DefaultConfig(4_MB);
        config.SetWriteBlobThreshold(4_MB);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        constexpr ui32 extraBlocks = 4;
        partition.WriteBlocks(
            TBlockRange32::WithLength(MaxBlocksCount - 1, extraBlocks + 1));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount + extraBlocks,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(
        ShouldCorrectlyMarkFirstBlockInBlobIfItIsTheSameAsLastBlockInPreviousBlob)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetCollectGarbageThreshold(1);
        config.SetFlushThresholdHDD(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1023), 1);
        for (ui32 i = 0; i < 1024; ++i) {
            partition.WriteBlocks(1023, 1);
        }

        // here we expect that fresh contains 2048 blocks
        // flush will write two blobs but latest blob
        // will be immediately gc as it is completely overwritten

        partition.Flush();

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleZeroAndNonZeroFreshBlobsDuringFlush)
    {
        auto config = DefaultConfig();
        config.SetFlushThresholdHDD(12_KB);
        config.SetFlushBlobSizeThreshold(8_KB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.ZeroBlocks(0);
        partition.WriteBlocks(0, 1);

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldWaitForFreshWritesBeforeFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetWaitForFreshWritesBeforeFlushEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, '0');

        std::unique_ptr<IEventHandle> addFreshBlocksRequest;
        bool interceptAddFreshBlocksRequest = true;
        bool flushCompleted = false;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvAddFreshBlocksRequest &&
                    interceptAddFreshBlocksRequest)
                {
                    addFreshBlocksRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                flushCompleted |= event->GetTypeRewrite() ==
                                  TEvPartitionPrivate::EvFlushCompleted;

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendWriteBlocksRequest(1, '1');
        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT(addFreshBlocksRequest);
        interceptAddFreshBlocksRequest = false;

        partition.SendFlushRequest();
        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT(!flushCompleted);

        runtime->SendAsync(addFreshBlocksRequest.release());
        partition.RecvFlushResponse();
        UNIT_ASSERT(flushCompleted);
    }

    Y_UNIT_TEST(
        ShouldHandleFlushCorrectlyWhileBlockFromFreshChannelIsBeingDeleted)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        TAutoPtr<IEventHandle> addBlobsRequest;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    addBlobsRequest = event.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendFlushRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        UNIT_ASSERT(addBlobsRequest);
        runtime->Send(addBlobsRequest.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldHandleFlushCorrectlyWhileBlockFromDbIsBeingDeleted)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCountSSD(0);
        config.SetFreshChannelWriteRequestsEnabled(false);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        TAutoPtr<IEventHandle> addBlobsRequest;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    addBlobsRequest = event.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendFlushRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        UNIT_ASSERT(addBlobsRequest);
        runtime->Send(addBlobsRequest.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldHandleBSErrorsOnInitFreshBlocksFromChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvRangeResult: {
                        using TEvent = TEvBlobStorage::TEvRangeResult;
                        auto* msg = event->Get<TEvent>();

                        if (msg->From.Channel() != 4 || evRangeResultSeen) {
                            return false;
                        }

                        evRangeResultSeen = true;

                        auto response = std::make_unique<TEvent>(
                            NKikimrProto::ERROR,
                            TLogoBlobID(),   // doesn't matter
                            TLogoBlobID(),   // doesn't matter
                            0);              // doesn't matter

                        auto* handle = new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0,
                            event->Cookie);

                        runtime.Send(handle, 0);

                        return true;
                    }
                    case TEvPartitionCommonPrivate::EvLoadFreshBlobsCompleted: {
                        ++evLoadFreshBlobsCompletedCount;
                        return false;
                    }
                    default: {
                        return false;
                    }
                }
            });

        partition.RebootTablet();
        partition.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldHandleCorruptedFreshBlobOnInitFreshBlocks)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvRangeResult: {
                        using TEvent = TEvBlobStorage::TEvRangeResult;
                        auto* msg = event->Get<TEvent>();

                        if (msg->From.Channel() != 4 || evRangeResultSeen) {
                            return false;
                        }

                        evRangeResultSeen = true;

                        auto response = std::make_unique<TEvent>(
                            msg->Status,
                            msg->From,
                            msg->To,
                            msg->GroupId);

                        response->Responses = std::move(msg->Responses);

                        // corrupt
                        auto& buffer = response->Responses[0].Buffer;
                        std::memset(buffer.Detach(), 0, 4);

                        auto* handle = new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0,
                            event->Cookie);

                        runtime.Send(handle, 0);

                        return true;
                    }
                    case TEvPartitionCommonPrivate::EvLoadFreshBlobsCompleted: {
                        ++evLoadFreshBlobsCompletedCount;
                        return false;
                    }
                    default: {
                        return false;
                    }
                }
            });

        partition.RebootTablet();
        partition.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldUpdateUsedBlocksMapWhenFlushingBlocksFromFreshChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1);
        partition.WriteBlocks(2);
        partition.WriteBlocks(3);

        partition.ZeroBlocks(2);

        {
            auto response = partition.StatPartition();
            auto stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedBlocksCount());
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            auto stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetUsedBlocksCount());
        }
    }

    Y_UNIT_TEST(ShouldAbortFlushIfWriteBlobFailsWithDeadlineExceeded)
    {
        NProto::TStorageServiceConfig config;
        config.SetBlobStorageAsyncRequestTimeoutHDD(
            TDuration::Seconds(1).MilliSeconds());
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(3, 33);

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
            {
                Y_UNUSED(runtime);

                if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                    const auto* msg = ev->Get<TEvBlobStorage::TEvVPut>();
                    if (msg->Record.GetHandleClass() ==
                            NKikimrBlobStorage::AsyncBlob &&
                        msg->Record.GetMsgQoS().HasDeadlineSeconds())
                    {
                        return true;
                    }
                }
                return false;
            });

        partition.SendFlushRequest();
        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
    }

    Y_UNIT_TEST(
        ShouldReportAddFreshBlocksResultedInErrorOnlyForNonRetriableErrors)
    {
        NProto::TStorageServiceConfig config;
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        NProto::TError error;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvAddFreshBlocksResponse)
                {
                    auto* handle = new IEventHandle(
                        event->Recipient,
                        event->Sender,
                        std::make_unique<TEvPartitionCommonPrivate::
                                             TEvAddFreshBlocksResponse>(error)
                            .release(),
                        0,
                        event->Cookie);

                    runtime->Send(handle, 0);
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto freshBlocksResultedInErrorCounter = counters->GetCounter(
            "AppCriticalEvents/AddFreshBlocksResultedInError",
            true);

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(0, freshBlocksResultedInErrorCounter->Val());

        error = MakeError(E_REJECTED);

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(0, freshBlocksResultedInErrorCounter->Val());

        error = MakeError(E_INVALID_STATE);

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(1, freshBlocksResultedInErrorCounter->Val());
    }

    Y_UNIT_TEST(ShouldSplitFlushBlobsByCompactionRangeBoundaries)
    {
        constexpr ui32 rangeSize = 1024;
        constexpr ui32 blockCount = rangeSize * 3;

        auto config = DefaultConfig();
        config.SetReadBlockMaskOnCompactionOptimizationEnabled(true);
        config.SetSplitByCompactionRangeMaxBlobCount(3);

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 i = 1022; i < 1026; ++i) {
            partition.WriteBlocks(i, 1);
        }
        for (ui32 i = 1500; i < 1505; ++i) {
            partition.WriteBlocks(i, 2);
        }
        for (ui32 i = 2045; i < 2050; ++i) {
            partition.WriteBlocks(i, 3);
        }

        ui32 mixedBlobsCount = 0;
        TVector<TVector<ui32>> blobIndexes;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                            mixedBlobsCount = msg->FreshBlobs.size();

                            for (const auto& blob: msg->FreshBlobs) {
                                blobIndexes.emplace_back();
                                for (const auto& block: blob.Blocks) {
                                    blobIndexes.back().push_back(
                                        block.BlockIndex);
                                }
                            }
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Flush();

        // Expect 3 blobs: [1022-1023], [1024-1025] + [1500-1504] + [2045-2047],
        // [2048-2049]
        UNIT_ASSERT_VALUES_EQUAL(3, mixedBlobsCount);

        for (const auto& blobBlocks: blobIndexes) {
            ui32 rangeIndex = blobBlocks.front() / rangeSize;
            for (auto blockIdx: blobBlocks) {
                UNIT_ASSERT_VALUES_EQUAL(rangeIndex, blockIdx / rangeSize);
            }
        }

        partition.Compaction();
        partition.Compaction();
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                stats.GetBlobsProcessedDuringCompaction());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                stats.GetBlockMaskReadDuringCompaction());
        }
    }

    Y_UNIT_TEST(ShouldSplitFlushZeroBlobsByCompactionRangeBoundaries)
    {
        constexpr ui32 rangeSize = 1024;
        constexpr ui32 blockCount = rangeSize * 3;

        auto config = DefaultConfig();
        config.SetReadBlockMaskOnCompactionOptimizationEnabled(true);
        config.SetSplitByCompactionRangeMaxBlobCount(3);

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 'A');
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 'B');
        partition.WriteBlocks(TBlockRange32::WithLength(2048, 1024), 'C');

        for (ui32 i = 1022; i < 1026; ++i) {
            partition.ZeroBlocks(i);
        }
        for (ui32 i = 1500; i < 1505; ++i) {
            partition.ZeroBlocks(i);
        }
        for (ui32 i = 2045; i < 2050; ++i) {
            partition.ZeroBlocks(i);
        }

        ui32 mixedBlobsCount = 0;
        TVector<TVector<ui32>> blobIndexes;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                            mixedBlobsCount = msg->FreshBlobs.size();

                            for (const auto& blob: msg->FreshBlobs) {
                                blobIndexes.emplace_back();
                                for (const auto& block: blob.Blocks) {
                                    blobIndexes.back().push_back(
                                        block.BlockIndex);
                                }
                            }
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(3, mixedBlobsCount);

        for (const auto& blobBlocks: blobIndexes) {
            ui32 rangeIndex = blobBlocks.front() / rangeSize;
            for (auto blockIdx: blobBlocks) {
                UNIT_ASSERT_VALUES_EQUAL(rangeIndex, blockIdx / rangeSize);
            }
        }

        for (ui32 i = 1022; i < 1026; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                "",
                GetBlockContent(partition.ReadBlocks(i)));
        }
        for (ui32 i = 1500; i < 1505; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                "",
                GetBlockContent(partition.ReadBlocks(i)));
        }
        for (ui32 i = 2045; i < 2050; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                "",
                GetBlockContent(partition.ReadBlocks(i)));
        }

        partition.Compaction();
        partition.Compaction();
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                6,
                stats.GetBlobsProcessedDuringCompaction());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                stats.GetBlockMaskReadDuringCompaction());
        }
    }

    Y_UNIT_TEST(ShouldSkipCompactionRangeSplitInFlushWhenRunCountExceedsLimit)
    {
        constexpr ui32 rangeSize = 1024;
        constexpr ui32 blockCount = rangeSize * 3;

        auto config = DefaultConfig();
        config.SetReadBlockMaskOnCompactionOptimizationEnabled(true);
        config.SetSplitByCompactionRangeMaxBlobCount(2);

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 i = 1022; i < 1026; ++i) {
            partition.WriteBlocks(i, 1);
        }
        for (ui32 i = 1500; i < 1505; ++i) {
            partition.WriteBlocks(i, 2);
        }
        for (ui32 i = 2045; i < 2050; ++i) {
            partition.WriteBlocks(i, 3);
        }

        ui32 mixedBlobsCount = 0;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                            mixedBlobsCount = msg->FreshBlobs.size();
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }

        for (ui32 i = 1022; i < 1024; ++i) {
            partition.WriteBlocks(i, 1);
        }

        for (ui32 i = 2048; i < 2050; ++i) {
            partition.WriteBlocks(i, 3);
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlobsCount());
        }

        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                stats.GetBlobsProcessedDuringCompaction());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.GetBlockMaskReadDuringCompaction());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
