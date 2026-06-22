#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"

#include "part_ut_helpers.h"

Y_UNIT_TEST_SUITE(TPartitionTestSplit3)
{
void DoShouldAutomaticallyRunGarbageOrIgnoringZeroedCompaction(
        bool ignoringZeroedCompactionEnabled)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetIgnoringZeroedCompactionEnabled(
            ignoringZeroedCompactionEnabled);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByGarbageBlocksPerRange = 0;
        ui64 compactionByGarbageBlocksPerDisk = 0;
        ui64 compactionByIgnoringZeroedPerRange = 0;
        ui64 compactionByIgnoringZeroedPerDisk = 0;
        bool garbageCompactionRequestObserved = false;
        bool ignoringZeroedCompactionRequestObserved = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::GarbageCompaction)
                        {
                            garbageCompactionRequestObserved = true;
                        } else if (
                            msg->Mode ==
                            TEvPartitionPrivate::IgnoringZeroedCompaction)
                        {
                            ignoringZeroedCompactionRequestObserved =
                                true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByIgnoringZeroedPerRange =
                            cc.CompactionByIgnoringZeroedPerRange.Value;
                        compactionByIgnoringZeroedPerDisk =
                            cc.CompactionByIgnoringZeroedPerDisk.Value;
                        compactionByGarbageBlocksPerRange =
                            cc.CompactionByGarbageBlocksPerRange.Value;
                        compactionByGarbageBlocksPerDisk =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                        break;
                    }
                }
                return false;
            });

        auto checkCompactionRequests = [&](bool shouldObserveRequest)
        {
            if (!shouldObserveRequest) {
                UNIT_ASSERT(!garbageCompactionRequestObserved);
                UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
            } else if (ignoringZeroedCompactionEnabled) {
                UNIT_ASSERT(!garbageCompactionRequestObserved);
                UNIT_ASSERT(ignoringZeroedCompactionRequestObserved);
            } else {
                UNIT_ASSERT(garbageCompactionRequestObserved);
                UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
            }
            garbageCompactionRequestObserved = false;
            ignoringZeroedCompactionRequestObserved = false;
        };

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 301));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        checkCompactionRequests(true);

        // marking range 0 as non-compacted
        partition.WriteBlocks(0);
        partition.Flush();

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1024, 1400));
        // 50% garbage
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1024, 1400));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        checkCompactionRequests(true);

        partition.CreateCheckpoint("c1");

        // writing lots of blocks into range 1
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // there is a checkpoint => zeroed compaction should not run
        checkCompactionRequests(false);

        partition.DeleteCheckpoint("c1");

        // triggering compaction attempt, block index does not matter
        partition.WriteBlocks(0);
        partition.Flush();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        checkCompactionRequests(true);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerRange);
        UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerRange);

        if (ignoringZeroedCompactionEnabled) {
            UNIT_ASSERT(compactionByIgnoringZeroedPerDisk > 0);
            UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerDisk);
        } else {
            UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerDisk);
            UNIT_ASSERT(compactionByGarbageBlocksPerDisk > 0);
        }
    }

void DoShouldAutomaticallyRunGarbageCompactionForSuperDirtyRanges(
        bool ignoringZeroedCompactionEnabled)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetIgnoringZeroedCompactionEnabled(
            ignoringZeroedCompactionEnabled);
        config.SetCompactionGarbageThreshold(999999);
        config.SetCompactionRangeGarbageThreshold(200);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByGarbageBlocksPerRange = 0;
        ui64 compactionByGarbageBlocksPerDisk = 0;
        ui64 compactionByIgnoringZeroedPerRange = 0;
        ui64 compactionByIgnoringZeroedPerDisk = 0;
        bool garbageCompactionRequestObserved = false;
        bool ignoringZeroedCompactionRequestObserved = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::GarbageCompaction)
                        {
                            garbageCompactionRequestObserved = true;
                        } else if (
                            msg->Mode ==
                            TEvPartitionPrivate::IgnoringZeroedCompaction)
                        {
                            ignoringZeroedCompactionRequestObserved =
                                true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByIgnoringZeroedPerRange =
                            cc.CompactionByIgnoringZeroedPerRange.Value;
                        compactionByIgnoringZeroedPerDisk =
                            cc.CompactionByIgnoringZeroedPerDisk.Value;
                        compactionByGarbageBlocksPerRange =
                            cc.CompactionByGarbageBlocksPerRange.Value;
                        compactionByGarbageBlocksPerDisk =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                        break;
                    }
                }
                return false;
            });

        auto checkCompactionRequests = [&](bool shouldObserveRequest)
        {
            if (!shouldObserveRequest) {
                UNIT_ASSERT(!garbageCompactionRequestObserved);
                UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
            } else if (ignoringZeroedCompactionEnabled) {
                UNIT_ASSERT(!garbageCompactionRequestObserved);
                UNIT_ASSERT(ignoringZeroedCompactionRequestObserved);
            } else {
                UNIT_ASSERT(garbageCompactionRequestObserved);
                UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
            }
            garbageCompactionRequestObserved = false;
            ignoringZeroedCompactionRequestObserved = false;
        };

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used x2 => compaction
        checkCompactionRequests(true);

        garbageCompactionRequestObserved = false;

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used x1 => no compaction
        checkCompactionRequests(false);

        partition.CreateCheckpoint("c1");

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1024, stats.GetCompactionGarbageScore());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used x2 => still no compaction because there is a
        // checkpoint
        checkCompactionRequests(false);

        partition.DeleteCheckpoint("c1");

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0));
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used x2 => compaction
        checkCompactionRequests(true);

        garbageCompactionRequestObserved = false;

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0));
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetCompactionGarbageScore());
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // block count for this range should've been reset
        checkCompactionRequests(false);

        // a range with no used blocks
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // only garbage => compaction
        UNIT_ASSERT(garbageCompactionRequestObserved);
        UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);
        garbageCompactionRequestObserved = false;

        partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // no garbage (99% of the blocks belong to a deletion marker) => no
        // compaction
        checkCompactionRequests(false);

        partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used => no compaction
        checkCompactionRequests(false);

        partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == 2 * used => compaction
        checkCompactionRequests(true);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerDisk);
        UNIT_ASSERT(compactionByGarbageBlocksPerRange > 0);
        UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerDisk);

        if (ignoringZeroedCompactionEnabled) {
            UNIT_ASSERT(compactionByIgnoringZeroedPerRange > 0);
        } else {
            UNIT_ASSERT_EQUAL(0, compactionByIgnoringZeroedPerRange);
        }
    }

auto BuildEvGetBreaker(ui32 blockCount, bool& broken) {
        return [blockCount, &broken] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvGetResult: {
                    auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                    ui32 totalSize = 0;
                    for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                        totalSize += msg->Responses[i].Buffer.size();
                    }
                    if (totalSize == blockCount * DefaultBlockSize) {
                        // it's our blob
                        msg->Responses[0].Status = NKikimrProto::NODATA;
                        broken = true;
                    }

                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };
    }

void DoTestFullCompaction(bool forced)
    {
        constexpr ui32 rangesCount = 5;
        auto storageConfig = DefaultConfig();
        storageConfig.SetWriteBlobThreshold(1_MB);
        auto runtime = PrepareTestActorRuntime(storageConfig, rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        auto response = partition.StatPartition();
        auto oldStats = response->Record.GetStats();

        TCompactionOptions options;
        options.set(ToBit(ECompactionOption::Full));
        if (forced) {
            options.set(ToBit(ECompactionOption::Forced));
        }
        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.Compaction(range * 1024, options);
        }

        response = partition.StatPartition();
        auto newStats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            oldStats.GetMixedBlobsCount() + oldStats.GetMergedBlobsCount() + rangesCount,
            newStats.GetMixedBlobsCount() + newStats.GetMergedBlobsCount()
        );
    }

void DoTestEmptyRangesFullCompaction(bool forced)
    {
        constexpr ui32 rangesCount = 5;
        constexpr ui32 emptyRange = 2;
        auto storageConfig = DefaultConfig();
        storageConfig.SetWriteBlobThreshold(1_MB);
        auto runtime = PrepareTestActorRuntime(storageConfig, rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            if (range != emptyRange) {
                partition.WriteBlocks(
                    TBlockRange32::WithLength(range * 1024, 900),
                    1);
            }
        }
        partition.Flush();

        auto response = partition.StatPartition();
        auto oldStats = response->Record.GetStats();

        TCompactionOptions options;
        options.set(ToBit(ECompactionOption::Full));
        if (forced) {
            options.set(ToBit(ECompactionOption::Forced));
        }

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.Compaction(range * 1024, options);
        }

        response = partition.StatPartition();
        auto newStats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            oldStats.GetMixedBlobsCount() + oldStats.GetMergedBlobsCount() + rangesCount - 1,
            newStats.GetMixedBlobsCount() + newStats.GetMergedBlobsCount()
        );
    }

void DoTestIncrementalCompaction(
        NProto::TStorageServiceConfig config,
        NProto::EStorageMediaKind mediaKind,
        bool incrementalCompactionExpected)
    {
        if (mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD) {
            config.SetWriteBlobThresholdSSD(1);   // disable FreshBlocks
            config.SetSSDMaxBlobsPerRange(4);
            config.SetMaxSkippedBlobsDuringCompaction(1);
        }
        else {
            config.SetWriteBlobThreshold(1);   // disable FreshBlocks
            config.SetHDDMaxBlobsPerRange(4);
            config.SetMaxSkippedBlobsDuringCompactionHDD(1);
        }
        config.SetIncrementalCompactionEnabled(true);
        config.SetTargetCompactionBytesPerOp(64_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = mediaKind});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        std::unique_ptr<IEventHandle> compactionRequest;
        bool intercept = true;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        if (intercept) {
                            auto request =
                                event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                            UNIT_ASSERT_VALUES_EQUAL(
                                incrementalCompactionExpected,
                                !request->CompactionOptions.test(
                                    ToBit(ECompactionOption::Full))
                            );

                            compactionRequest.reset(event.Release());
                            intercept = false;

                            return TTestActorRuntime::EEventAction::DROP;
                        }

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 2);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(10, 14), 3);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(20, 25), 4);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1040, stats.GetMergedBlocksCount());
        }

        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(compactionRequest);
        runtime->Send(compactionRequest.release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        partition.Cleanup();
        partition.CollectGarbage();

        {
            ui32 blobs = incrementalCompactionExpected ? 2 : 1;
            ui32 blocks = incrementalCompactionExpected ? 1040 : 1024;

            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(blobs, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(blocks, stats.GetMergedBlocksCount());
        }

        for (ui32 i = 0; i <= 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(2),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 5; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(1),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 10; i <= 14; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(3),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 15; i < 20; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(1),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 20; i <= 25; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(4),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 26; i < 1023; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(1),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }
    }

// NBS-3934
void DoTestCompression(
        ui32 writeBlobThreshold,
        ui32 uncompressedExpected,
        ui32 compressedExpected)
    {
        auto config = DefaultConfig();
        config.SetBlobCompressionRate(1);
        config.SetWriteBlobThreshold(writeBlobThreshold);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui32 uncompressed = 0;
        ui32 compressed = 0;

        auto obs =
            [&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite()
                        == TEvStatsService::EvVolumePartCounters)
                {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();

                    const auto& cc = msg->DiskCounters->Cumulative;
                    uncompressed = cc.UncompressedBytesWritten.Value;
                    compressed = cc.CompressedBytesWritten.Value;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            };

        runtime->SetObserverFunc(obs);

        partition.WriteBlocks(1, 1);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(uncompressedExpected, uncompressed);
        UNIT_ASSERT_VALUES_EQUAL(compressedExpected, compressed);
    }

class TStatsChecker
    {
    private:
        ui64 RealWriteBlocksCount = 0;
        ui64 RealReadBlocksCount = 0;
        ui64 WriteBlocksCount = 0;
        ui64 ReadBlocksCount = 0;

    public:
        TStatsChecker() = default;

        void CheckStats(
            const NProto::TVolumeStats& stats,
            const ui64 realReadBlocksCount,
            const ui64 realWriteBlocksCount,
            const ui64 readBlocksCount,
            const ui64 writeBlocksCount)
        {
            RealReadBlocksCount += realReadBlocksCount;
            RealWriteBlocksCount += realWriteBlocksCount;
            ReadBlocksCount += readBlocksCount;
            WriteBlocksCount += writeBlocksCount;
            UNIT_ASSERT_VALUES_EQUAL(
                ReadBlocksCount, stats.GetSysReadCounters().GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                WriteBlocksCount, stats.GetSysWriteCounters().GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                RealReadBlocksCount,
                stats.GetRealSysReadCounters().GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                RealWriteBlocksCount,
                stats.GetRealSysWriteCounters().GetBlocksCount());
        }
    };

void CheckIncrementAndDecrementCompactionPerRun(
        ui32 rangeCountPerRun,
        ui32 maxBlobsPerUnit,
        ui32 maxBlobsPerRange,
        ui32 diskGarbageThreshold,
        ui32 rangeGarbageThreshold,
        ui32 blobsCountAfterCompaction,
        ui32 increasingPercentageThreshold,
        ui32 decreasingPercentageThreshold,
        ui32 compactionRangeCountPerRun,
        ui32 maxCompactionRangeCountPerRun = 10)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBatchCompactionEnabled(true);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(diskGarbageThreshold);
        config.SetCompactionRangeGarbageThreshold(rangeGarbageThreshold);
        config.SetCompactionRangeCountPerRun(rangeCountPerRun);
        config.SetCompactionCountPerRunIncreasingThreshold(
            increasingPercentageThreshold);
        config.SetCompactionCountPerRunDecreasingThreshold(
            decreasingPercentageThreshold);
        config.SetHDDMaxBlobsPerUnit(maxBlobsPerUnit);
        config.SetSSDMaxBlobsPerUnit(maxBlobsPerUnit);
        config.SetMaxCompactionRangeCountPerRun(maxCompactionRangeCountPerRun);
        config.SetCompactionCountPerRunChangingPeriod(1);
        config.SetSSDMaxBlobsPerRange(maxBlobsPerRange);
        config.SetHDDMaxBlobsPerRange(maxBlobsPerRange);

        auto runtime = PrepareTestActorRuntime(
            config,
            4 * 1024 * 1024
        );
        runtime->AdvanceCurrentTime(TDuration::Seconds(5));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 = TBlockRange32::WithLength(2 * 1024 * 1024, 1024);

        bool compactionFilter = true;
        ui32 resultCompactionRangeCountPerRun = 0;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        if (compactionFilter) {
                            return true;
                        }
                        compactionFilter = true;
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Simple;
                        resultCompactionRangeCountPerRun =
                            cc.CompactionRangeCountPerRun.Value;
                    }
                }
                return false;
            }
        );

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange3, 7);

        partition.WriteBlocks(blockRange1, 2);
        partition.WriteBlocks(blockRange1, 3);
        partition.WriteBlocks(blockRange2, 5);
        partition.WriteBlocks(blockRange2, 6);
        partition.WriteBlocks(blockRange3, 8);
        partition.WriteBlocks(blockRange3, 9);
        partition.WriteBlocks(blockRange3, 10);
        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        compactionFilter = false;

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(blobsCountAfterCompaction,
                stats.GetMergedBlobsCount());
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(compactionRangeCountPerRun,
                resultCompactionRangeCountPerRun);
        }
    }

template <typename FSend, typename FReceive>
    void DoShouldReportLongRunningBlobOperations(
        FSend sendRequest,
        FReceive receiveResponse,
        TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation
            expectedOperation,
        bool killPartition)
    {
        auto config = DefaultConfig();

        TTestPartitionInfo testPartitionInfo;
        testPartitionInfo.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            testPartitionInfo,
            {},
            EStorageAccessMode::Default);

        // Enable Schedule for all actors!!!
        runtime->SetRegistrationObserverFunc(
            [](auto& runtime, const auto& parentId, const auto& actorId)
            {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        // Make partition client.
        TPartitionClient partition(*runtime);
        partition.WaitReady();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 255));

        // Make handler for stealing nested messages
        std::vector<std::unique_ptr<IEventHandle>> stolenRequests;
        auto requestThief = [&](TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvGetResult:
                case TEvBlobStorage::EvPutResult: {
                    stolenRequests.push_back(
                        std::unique_ptr<IEventHandle>{event.Release()});
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        // Make handler for intercepting EvLongRunningOperation message.
        // Attention! counters will be doubled, because we will intercept the
        // requests sent to TPartitionActor and TVolumeActor.
        ui32 longRunningBeginCount = 0;
        ui32 longRunningFinishCount = 0;
        ui32 longRunningPingCount = 0;
        ui32 longRunningCanceledCount = 0;

        auto takeCounters = [&](TAutoPtr<IEventHandle>& event)
        {
            using TEvLongRunningOperation =
                TEvPartitionCommonPrivate::TEvLongRunningOperation;
            using EReason = TEvLongRunningOperation::EReason;

            if (event->GetTypeRewrite() ==
                TEvPartitionCommonPrivate::EvLongRunningOperation)
            {
                auto* msg = event->Get<TEvLongRunningOperation>();

                UNIT_ASSERT_VALUES_EQUAL(expectedOperation, msg->Operation);

                switch (msg->Reason) {
                    case EReason::LongRunningDetected:
                        if (msg->FirstNotify) {
                            longRunningBeginCount++;
                        } else {
                            longRunningPingCount++;
                        }
                        break;
                    case EReason::FinishedOk:
                        longRunningFinishCount++;
                        break;
                    case EReason::Cancelled:
                        longRunningCanceledCount++;
                        break;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        // Ready to postpone request.
        runtime->SetObserverFunc(requestThief);

        // Starting the execution of the request. It won't
        // be finished since we stole it EvPutResult message.
        sendRequest(partition);
        runtime->DispatchEvents({}, TDuration());
        UNIT_ASSERT_VALUES_UNEQUAL(0, stolenRequests.size());

        // Ready to check counters.
        runtime->SetObserverFunc(takeCounters);

        // Wait for EvLongRunningOperation arrived.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvLongRunningOperation);
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        // Wait #1 for EvLongRunningOperation ping arrived.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvLongRunningOperation);
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        // Wait #2 for EvLongRunningOperation ping arrived.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvLongRunningOperation);
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        if (killPartition) {
            partition.KillTablet();
        } else {
            // Returning stolen requests to complete the execution of the
            // request.
            for (auto& request: stolenRequests) {
                runtime->Send(request.release());
            }
        }

        // Wait for EvLongRunningOperation (finish or cancel) arrival.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvLongRunningOperation);
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // Check request completed.
        {
            auto response = receiveResponse(partition);
            UNIT_ASSERT_VALUES_EQUAL_C(
                killPartition ? E_REJECTED : S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }
        // Wait for EvVolumePartCounters arrived.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);

            UNIT_ASSERT_VALUES_EQUAL(2, longRunningBeginCount);
            UNIT_ASSERT_VALUES_EQUAL(4, longRunningPingCount);
            UNIT_ASSERT_VALUES_EQUAL(
                killPartition ? 0 : 2,
                longRunningFinishCount);
            UNIT_ASSERT_VALUES_EQUAL(
                killPartition ? 2 : 0,
                longRunningCanceledCount);
        }

        if (!killPartition) {
            // smoke test for monpage
            auto channelsTab = partition.RemoteHttpInfo(
                BuildRemoteHttpQuery(TestTabletId, {}, "Channels"),
                HTTP_METHOD::HTTP_METHOD_GET);

            UNIT_ASSERT_C(channelsTab->Html.Contains("svg"), channelsTab->Html);
        }
    }

void EnableReadBlobCorruption(
        TTestActorRuntime& runtime,
        ui16 checkedOffset = InvalidBlobOffset)
    {
        TVector<ui16> blobOffsets;

        runtime.SetEventFilter([=] (
            TTestActorRuntimeBase& runtime,
            TAutoPtr<IEventHandle>& event) mutable
        {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvPartitionCommonPrivate::EvReadBlobRequest: {
                    using TEv =
                        TEvPartitionCommonPrivate::TEvReadBlobRequest;
                    auto* msg = event->Get<TEv>();
                    blobOffsets = msg->BlobOffsets;

                    break;
                }
                case TEvBlobStorage::EvGetResult: {
                    auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                    UNIT_ASSERT(!blobOffsets.empty());
                    if (checkedOffset == InvalidBlobOffset
                            || blobOffsets[0] == checkedOffset)
                    {
                        auto& rope = msg->Responses[0].Buffer;
                        auto dst = rope.begin();
                        char* to = const_cast<char*>(dst.ContiguousData());
                        memset(to, 0, dst.ContiguousSize());
                    }

                    break;
                }
            }

            return false;
        });
    }

void DoShouldAbortCompactionIfBlobStorageRequestFailsWithDeadlineExceeded(
        int requestType)
    {
        NProto::TStorageServiceConfig config;
        config.SetBlobStorageAsyncRequestTimeoutHDD(TDuration::Seconds(1).MilliSeconds());
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(3, 33);
        partition.Flush();

        ui32 failedReadBlob = 0;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
            {
                Y_UNUSED(runtime);

                if (requestType == TEvBlobStorage::EvVGet &&
                    ev->GetTypeRewrite() == TEvBlobStorage::EvVGet)
                {
                    const auto* msg = ev->Get<TEvBlobStorage::TEvVGet>();
                    if (msg->Record.GetHandleClass() ==
                            NKikimrBlobStorage::AsyncRead &&
                        msg->Record.GetMsgQoS().HasDeadlineSeconds())
                    {
                        return true;
                    }
                } else if (
                    requestType == TEvBlobStorage::EvVPut &&
                    ev->GetTypeRewrite() == TEvBlobStorage::EvVPut)
                {
                    const auto* msg = ev->Get<TEvBlobStorage::TEvVPut>();
                    if (msg->Record.GetHandleClass() ==
                            NKikimrBlobStorage::AsyncBlob &&
                        msg->Record.GetMsgQoS().HasDeadlineSeconds())
                    {
                        return true;
                    }
                } else if (
                    ev->GetTypeRewrite() ==
                    TEvStatsService::EvVolumePartCounters)
                {
                    auto* msg =
                        ev->Get<TEvStatsService::TEvVolumePartCounters>();
                    failedReadBlob =
                        msg->DiskCounters->Simple.ReadBlobDeadlineCount.Value;
                }
                return false;
            });

        partition.SendCompactionRequest();
        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());

        runtime->AdvanceCurrentTime(TDuration::Seconds(15));

        if (requestType != TEvBlobStorage::EvVGet) {
            return;
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, failedReadBlob);
    }

void TestForcedCompaction(ui32 rangesPerRun)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetForcedCompactionRangeCountPerRun(rangesPerRun);
        config.SetV1GarbageCompactionEnabled(false);
        config.SetWriteBlobThreshold(15_KB);
        config.SetIncrementalCompactionEnabled(true);

        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 1);
            partition.WriteBlocks(TBlockRange32::WithLength(1024, 5), 2);

            partition.WriteBlocks(TBlockRange32::WithLength(0, 1), 3);
            partition.WriteBlocks(TBlockRange32::WithLength(1028, 3), 4);
            partition.Flush();

            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(10, stats.GetMergedBlocksCount());
        }

        {
            const auto response = partition.CompactRange(0, 0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto statResponse = partition.StatPartition();
            const auto& stats = statResponse->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(22, stats.GetMergedBlocksCount());
        }

        {
            partition.Cleanup();

            const auto statResponse = partition.StatPartition();
            const auto& stats = statResponse->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(12, stats.GetMergedBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(3),
                GetBlockContent(partition.ReadBlocks(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(1),
                GetBlockContent(partition.ReadBlocks(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(2),
                GetBlockContent(partition.ReadBlocks(1024)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(4),
                GetBlockContent(partition.ReadBlocks(1028)));
        }

        {
            const auto response = partition.CompactRange(0, 0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            partition.Cleanup();
        }

        // write to the same blocks, we need several blobs in one compacted range
        {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 12);
            partition.WriteBlocks(TBlockRange32::WithLength(1024, 5), 22);

            partition.WriteBlocks(TBlockRange32::WithLength(0, 1), 32);
            partition.WriteBlocks(TBlockRange32::WithLength(1028, 3), 42);
            partition.Flush();

            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(22, stats.GetMergedBlocksCount());
        }

        {
            const auto response = partition.CompactRange(0, 0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            partition.Cleanup();

            const auto statResponse = partition.StatPartition();
            const auto& stats = statResponse->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(12, stats.GetMergedBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(32),
                GetBlockContent(partition.ReadBlocks(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(12),
                GetBlockContent(partition.ReadBlocks(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(22),
                GetBlockContent(partition.ReadBlocks(1024)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(42),
                GetBlockContent(partition.ReadBlocks(1028)));
        }
    }

    Y_UNIT_TEST(ShouldRecoverStateOnReboot)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.RebootTablet();

        partition.StatPartition();
    }



    Y_UNIT_TEST(ShouldAutomaticallyTrimFreshLogOnFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());
        }

        bool trimSeen = false;
        bool trimCompletedSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            trimSeen = true;
                        }
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                        auto* msg = event->Get<TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted>();
                        UNIT_ASSERT(SUCCEEDED(msg->GetStatus()));
                        trimCompletedSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(true, trimSeen);
        UNIT_ASSERT_VALUES_EQUAL(true, trimCompletedSeen);
    }



    Y_UNIT_TEST(ShouldRunGarbageCompactionAfterZeroBlocks)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetIgnoringZeroedCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByGarbageBlocksPerRange = 0;
        ui64 compactionByGarbageBlocksPerDisk = 0;

        bool garbageCompactionRequestObserved = false;
        bool ignoringZeroedCompactionRequestObserved = false;

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::GarbageCompaction)
                        {
                            garbageCompactionRequestObserved = true;
                        } else if (
                            msg->Mode ==
                            TEvPartitionPrivate::IgnoringZeroedCompaction)
                        {
                            ignoringZeroedCompactionRequestObserved =
                                true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event
                                ->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByGarbageBlocksPerRange =
                            cc.CompactionByGarbageBlocksPerRange.Value;
                        compactionByGarbageBlocksPerDisk =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                        break;
                    }
                }
                return false;
            });

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));

        // < 20% on the whole disk, not enough to trigger compaction.
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 300));
        partition.Flush();

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(!garbageCompactionRequestObserved);
        UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);

        // > 20% on the whole disk, not enough to trigger compaction.
        partition.ZeroBlocks(TBlockRange32::WithLength(1024, 300));
        partition.Flush();

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(garbageCompactionRequestObserved);
        UNIT_ASSERT(!ignoringZeroedCompactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerRange);
        UNIT_ASSERT(compactionByGarbageBlocksPerDisk > 0);
    }



    Y_UNIT_TEST(ShouldReturnErrorIfNODATABlocksAreDetectedInDefaultStorageAccessMode)
    {
        auto runtime = PrepareTestActorRuntime(
            DefaultConfig(),
            1024,
            {},
            TTestPartitionInfo(),
            {},
            EStorageAccessMode::Default
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool broken = false;
        runtime->SetObserverFunc(BuildEvGetBreaker(777, broken));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 777), 1);
        partition.SendReadBlocksRequest(TBlockRange32::WithLength(0, 777));
        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(broken);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
    }



    Y_UNIT_TEST(ShouldNotWriteBlobIfCompactionRangeWasOverwrittenWithZeroBlocks)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetSSDMaxBlobsPerRange(2);
        config.SetHDDMaxBlobsPerRange(2);
        config.SetCollectGarbageThreshold(1);
        config.SetFlushThreshold(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 write_step = 0; write_step < 2; ++write_step) {
            partition.WriteBlocks(TBlockRange32::WithLength(0,512));
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }

        for (ui32 zero_step = 0; zero_step < 2; ++zero_step) {
            partition.ZeroBlocks(TBlockRange32::WithLength(0,512));
        }

        bool writeBlobSeen = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvWriteBlobRequest: {
                        writeBlobSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        UNIT_ASSERT(!writeBlobSeen);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }
    }



    Y_UNIT_TEST(ShouldCompactIncrementallyHybrid)
    {
        auto config = DefaultConfig();
        config.SetCompactionGarbageThreshold(20);
        DoTestIncrementalCompaction(std::move(config), NProto::STORAGE_MEDIA_HYBRID, true);
    }



    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWithOneBlob)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range1 = TBlockRange32::WithLength(11, 11);
        partition.WriteBlocks(range1, char(1));
        const auto range2 = TBlockRange32::WithLength(33, 22);
        partition.WriteBlocks(range2, char(1));
        partition.Flush();

        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::WithLength(0, 101));
            const auto& message = response->Record;
            // Expect that all data is contained in one blob.
            UNIT_ASSERT_VALUES_EQUAL(1, message.BlobPiecesSize());

            const auto& blobPiece = message.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece.RangesSize());
            const auto& r1 = blobPiece.GetRanges(0);
            const auto& r2 = blobPiece.GetRanges(1);

            UNIT_ASSERT_VALUES_EQUAL(0, r1.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(range1.Start, r1.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(range1.Size(), r1.GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(range1.Size(), r2.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(range2.Start, r2.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(range2.Size(), r2.GetBlocksCount());

            TVector<ui16> blobOffsets;
            for (size_t i = 0; i < r1.GetBlocksCount(); ++i) {
                blobOffsets.push_back(r1.GetBlobOffset() + i);
            }
            for (size_t i = 0; i < r2.GetBlocksCount(); ++i) {
                blobOffsets.push_back(r2.GetBlobOffset() + i);
            }

            {
                TVector<TString> blocks;
                auto sglist = ResizeBlocks(
                    blocks,
                    range1.Size() + range2.Size(),
                    TString(DefaultBlockSize, char(0)));

                const auto blobId =
                    LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
                const auto group = blobPiece.GetBSGroupId();
                const auto response =
                    partition.ReadBlob(blobId, group, blobOffsets, sglist);

                for (size_t i = 0; i < sglist.size(); ++i) {
                    const auto block = sglist[i].AsStringBuf();
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        GetBlockContent(
                            char(1)),
                            block,
                            "during iteration #" << i);
                }
            }
        }
    }



    Y_UNIT_TEST(ShouldNotKillTabletWhenFailedToReadBlobFromBaseDisk)
    {
        TPartitionContent baseContent = { TBlob(1, 1) };

        const auto baseTabletId = TestTabletId2;

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, baseTabletId, baseContent);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        int pillCount = 0;

        const auto eventHandler =
            [&] (const TEvBlobStorage::TEvGet::TPtr& ev) {
                bool result = false;

                auto& msg = *ev->Get();

                auto response = std::make_unique<TEvBlobStorage::TEvGetResult>(
                    NKikimrProto::ERROR,
                    msg.QuerySize,
                    0);  // groupId

                for (ui32 i = 0; i < msg.QuerySize; ++i) {
                    const auto& q = msg.Queries[i];
                    const auto& blobId = q.Id;

                    if (blobId.TabletID() == baseTabletId) {
                        result = true;
                    }
                }

                if (result) {
                    runtime.Schedule(
                        new IEventHandle(
                            ev->Sender,
                            ev->Recipient,
                            response.release(),
                            0,
                            ev->Cookie),
                        TDuration());
                }

                return result;
            };

        runtime.SetEventFilter(
            [eventHandler] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                bool handled = false;

                const auto wrapped =
                    [&] (const auto& ev) {
                        handled = eventHandler(ev);
                    };

                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvGet, wrapped);
                }
                return handled;
           });

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvents::TSystem::PoisonPill: {
                        ++pillCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendReadBlocksRequest(0);

        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));

        UNIT_ASSERT_VALUES_EQUAL(0, pillCount);
    }



    Y_UNIT_TEST(ShouldForgetTooOldCompactRangeOperations)
    {
        constexpr TDuration CompactOpHistoryDuration = TDuration::Days(1);

        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TVector<TString> op;

        for (ui32 i = 0; i < 4; ++i)
        {
            partition.WriteBlocks(0, 100);
            partition.SendCompactRangeRequest(0, 100);

            {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(
                    TEvPartitionPrivate::EvForcedCompactionCompleted,
                    1);
                runtime->DispatchEvents(options);
            }

            auto response = partition.RecvCompactRangeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            op.push_back(response->Record.GetOperationId());
        }

        runtime->AdvanceCurrentTime(CompactOpHistoryDuration + TDuration::Seconds(1));

        for (ui32 i = 0; i < 4; ++i)
        {
            partition.SendGetCompactionStatusRequest(op[i]);
            auto response = partition.RecvGetCompactionStatusResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }
    }



    Y_UNIT_TEST(ShouldFailGetMetadataRebuildStatusIfNoOperationRunning)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);

        partition.SendGetRebuildMetadataStatusRequest();
        auto progress = partition.RecvGetRebuildMetadataStatusResponse();
        UNIT_ASSERT(FAILED(progress->GetStatus()));
    }



    Y_UNIT_TEST(ShouldSuccessfullyScanDiskIfDiskIsEmpty)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 blobsPerBatch = 10;
        const auto response = partition.ScanDisk(blobsPerBatch);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvScanDiskCompleted);
        runtime->DispatchEvents(options, TDuration::Seconds(1));

        const auto progress = partition.GetScanDiskStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetTotal());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetBrokenBlobs().size());
    }



    Y_UNIT_TEST(ShouldChangeBatchSizeDueToBlocksPerRangeCount)
    {
        // real blocks per last range: 1024 * 4
        // total blocks per disk: 1024
        // garbage percentage: 100 * (4 - 1) * 1024 / 1024 = 300
        // 100 * (300 - 280) / 280 = 7

        // 7 > 6, so should increment and compact 2 ranges
        CheckIncrementAndDecrementCompactionPerRun(
            1, 1000, 99999, 9999, 280, 5, 6, 4, 2);

        // 7 > 6, but should compact only 1 range due to maxRangeCountPerRun
        CheckIncrementAndDecrementCompactionPerRun(
            1, 1000, 99999, 9999, 280, 7, 6, 4, 1, 1);

        // 7 < 8, so should decrement and compact only 1 range
        CheckIncrementAndDecrementCompactionPerRun(
            2, 1000, 99999, 9999, 280, 7, 30, 8, 1);
    }



    Y_UNIT_TEST(ShouldAllowForcedCompactionRequestsInPresenseOfTabletCompaction)
    {
        constexpr ui32 rangesCount = 5;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        bool steal = true;
        runtime->SetEventFilter([&]
            (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() == TEvPartitionPrivate::EvCompactionCompleted &&
                steal)
            {
                steal = false;
                return true;
            }
            return false;
        });

        partition.SendCompactionRequest(
            0,
            TCompactionOptions());
        partition.Compaction(
            0,
            TCompactionOptions().
                set(ToBit(ECompactionOption::Forced)));
    }



    Y_UNIT_TEST(ShouldReturnBlobsIdsOfFailedBlobsDuringReadIfRequested)
    {
        constexpr ui32 blobCount = 4;
        constexpr ui32 blockCount = 512 * blobCount;

        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(100_KB);
        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();
        {
            ui32 current_offset = 0;
            for (ui32 i = 0; i < blobCount; ++i) {
                const auto blockRange =
                    TBlockRange32::WithLength(current_offset, 512);
                partition.WriteBlocks(blockRange, 1);
                current_offset += 512;
            }
        }

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvReadBlobRequest: {
                        auto response = std::make_unique<
                            TEvPartitionCommonPrivate::TEvReadBlobResponse>(
                            MakeError(E_IO, "Simulated blob read failure"));

                        runtime->Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0,   // flags
                                event->Cookie),
                            0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TGuardedBuffer<TString> Buffer = TGuardedBuffer(
            TString::Uninitialized(blockCount * DefaultBlockSize));
        auto sgList = Buffer.GetGuardedSgList();
        auto sgListOrError =
            SgListNormalize(sgList.Acquire().Get(), DefaultBlockSize);

        UNIT_ASSERT(!HasError(sgListOrError));

        auto request = partition.CreateReadBlocksLocalRequest(
            TBlockRange32::WithLength(0, blockCount),
            sgListOrError.ExtractResult());

        request->Record.ShouldReportFailedRangesOnFailure = true;

        partition.SendToPipe(std::move(request));

        auto response = partition.RecvReadBlocksLocalResponse();
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            blobCount,
            response->Record.FailInfo.FailedRanges.size());
    }



    Y_UNIT_TEST(ShouldNotLoseAnyBlocksWhileWaitingForCheckpointCreation)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool intercept = false;
        std::unique_ptr<IEventHandle> putEvent;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvBlobStorage::EvPut
                        && intercept)
                {
                    putEvent.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.WriteBlocks(0, '1');
        intercept = true;

        partition.SendWriteBlocksRequest(1, '2');

        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(putEvent);
        intercept = false;

        partition.SendCreateCheckpointRequest("c1");

        runtime->DispatchEvents({}, 10ms);

        partition.WriteBlocks(0, '3');

        runtime->SendAsync(putEvent.release());

        partition.RecvWriteBlocksResponse();
        partition.RecvCreateCheckpointResponse();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('1') + GetBlockContent('2'),
            GetBlocksContent(
                partition.ReadBlocks(TBlockRange32::WithLength(0, 2), "c1")));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('3') + GetBlockContent('2'),
            GetBlocksContent(
                partition.ReadBlocks(TBlockRange32::WithLength(0, 2))));

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('1') + GetBlockContent('2'),
            GetBlocksContent(
                partition.ReadBlocks(TBlockRange32::WithLength(0, 2), "c1")));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('3') + GetBlockContent('2'),
            GetBlocksContent(
                partition.ReadBlocks(TBlockRange32::WithLength(0, 2))));

        partition.DeleteCheckpoint("c1");

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('3') + GetBlockContent('2'),
            GetBlocksContent(
                partition.ReadBlocks(TBlockRange32::WithLength(0, 2))));
    }


}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
