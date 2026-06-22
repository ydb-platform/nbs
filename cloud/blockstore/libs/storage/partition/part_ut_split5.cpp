#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"

#include "part_ut_helpers.h"

Y_UNIT_TEST_SUITE(TPartitionTestSplit5)
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

    Y_UNIT_TEST(ShouldStoreBlocksInFreshChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);
    }



    Y_UNIT_TEST(ShouldTrimUpToTheFirstUnflushedBlockCommitId)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);

        TAutoPtr<IEventHandle> trim;

        ui32 collectGen = 0;
        ui32 collectStep = 0;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvTrimFreshLogRequest: {
                        UNIT_ASSERT(!trim);
                        trim = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            collectGen = msg->CollectGeneration;
                            collectStep = msg->CollectStep;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.Flush();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);
        partition.WriteBlocks(3, 4);

        UNIT_ASSERT(trim);
        runtime->Send(trim.Release());

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvTrimFreshLogCompleted);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, collectGen);
        UNIT_ASSERT_VALUES_EQUAL(4, collectStep);

        partition.Flush();
        UNIT_ASSERT(trim);
        runtime->Send(trim.Release());

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvTrimFreshLogCompleted);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, collectGen);
        UNIT_ASSERT_VALUES_EQUAL(6, collectStep);
    }



    Y_UNIT_TEST(ShouldAutomaticallyRunGarbageCompaction)
    {
        DoShouldAutomaticallyRunGarbageOrIgnoringZeroedCompaction(false);
    }



    Y_UNIT_TEST(ShouldRejectRequestForBlockOutOfRange)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            partition.WriteBlocks(TBlockRange32::MakeOneBlock(1023));
        }

        {
            partition.SendWriteBlocksRequest(TBlockRange32::WithLength(1023, 2));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        {
            partition.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }



    Y_UNIT_TEST(ShouldCorrectlyHandleZeroAndNonZeroFreshBlobsDuringFlush)
    {
        auto config = DefaultConfig();
        config.SetFlushThreshold(12_KB);
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



    Y_UNIT_TEST(ShouldNotCompactIncrementallyIfHybridDiskGarbageLevelIsTooHigh)
    {
        auto config = DefaultConfig();
        config.SetCompactionGarbageThreshold(1);
        DoTestIncrementalCompaction(std::move(config), NProto::STORAGE_MEDIA_HYBRID, false);
    }



    Y_UNIT_TEST(ShouldRespondToDescribeBlocksRequestWithSparseBlobs)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, char(1));
        partition.WriteBlocks(2, char(1));
        partition.Flush();
        partition.WriteBlocks(4, char(2));
        partition.WriteBlocks(6, char(2));
        partition.Flush();

        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::WithLength(0, 7));
            const auto& message = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, message.BlobPiecesSize());

            const auto& blobPiece1 = message.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece1.RangesSize());
            const auto& r0 = blobPiece1.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(0, r0.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(0, r0.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r0.GetBlocksCount());
            const auto& r1 = blobPiece1.GetRanges(1);
            UNIT_ASSERT_VALUES_EQUAL(1, r1.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(2, r1.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r1.GetBlocksCount());
            const auto blobId1 =
                LogoBlobIDFromLogoBlobID(blobPiece1.GetBlobId());
            const auto group1 = blobPiece1.GetBSGroupId();

            const auto& blobPiece2 = message.GetBlobPieces(1);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece2.RangesSize());
            const auto& r4 = blobPiece2.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(0, r4.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(4, r4.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r4.GetBlocksCount());
            const auto& r6 = blobPiece2.GetRanges(1);
            UNIT_ASSERT_VALUES_EQUAL(1, r6.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(6, r6.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r6.GetBlocksCount());
            const auto blobId2 =
                LogoBlobIDFromLogoBlobID(blobPiece2.GetBlobId());
            const auto group2 = blobPiece2.GetBSGroupId();

            for (ui16 i = 0; i < 1; ++i) {
                const TVector<ui16> offsets = {i};
                {
                    TVector<TString> blocks;
                    auto sglist = ResizeBlocks(blocks, 1, TString(DefaultBlockSize, char(0)));
                    const auto response =
                        partition.ReadBlob(blobId1, group1, offsets, sglist);
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetBlockContent(char(1)),
                        sglist[0].AsStringBuf()
                    );
                }

                {
                    TVector<TString> blocks;
                    auto sglist = ResizeBlocks(blocks, 1, TString(DefaultBlockSize, char(0)));
                    const auto response =
                        partition.ReadBlob(blobId2, group2, offsets, sglist);
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetBlockContent(char(2)),
                        sglist[0].AsStringBuf()
                    );
                }
            }
        }
    }



    Y_UNIT_TEST(ShouldSendBlocksCountToReadInDescribeBlocksRequest)
    {
        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, {});
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        partition.WriteBlocks(4, 1);
        partition.WriteBlocks(5, 1);

        int describeBlocksCount = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvDescribeBlocksRequest: {
                        auto* msg = event->Get<TEvVolume::TEvDescribeBlocksRequest>();
                        auto& record = msg->Record;
                        UNIT_ASSERT_VALUES_EQUAL(
                            7,
                            record.GetBlocksCountToRead()
                        );
                        ++describeBlocksCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.ReadBlocks(TBlockRange32::WithLength(0, 9));
        UNIT_ASSERT_VALUES_EQUAL(1, describeBlocksCount);
    }



    Y_UNIT_TEST(ShouldNotTrimInFlightBlocks)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);

        TAutoPtr<IEventHandle> addFreshBlocks;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvAddFreshBlocksRequest: {
                        if (!addFreshBlocks) {
                            addFreshBlocks = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendWriteBlocksRequest(2, 2);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(3, 3);

        partition.Flush();
        partition.TrimFreshLog();

        UNIT_ASSERT(addFreshBlocks);
        runtime->Send(addFreshBlocks.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.RebootTablet();

        auto response = partition.ReadBlocks(1);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)),
            GetBlocksContent(response)
        );

        response = partition.ReadBlocks(2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(2)),
            GetBlocksContent(response)
        );

        response = partition.ReadBlocks(3);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(3)),
            GetBlocksContent(response)
        );
    }



    Y_UNIT_TEST(ShouldReturnRebuildMetadataProgressDuringExecution)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024 * 10), 1);
        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 10, 1024 * 10),
            1);

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvMetadataRebuildBlockCountResponse: {
                        const auto* msg =
                            event->Get<TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse>();
                        UNIT_ASSERT_VALUES_UNEQUAL(
                            0,
                            msg->RebuildState.MixedBlocks + msg->RebuildState.MergedBlocks);
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        auto response = partition.RebuildMetadata(NProto::ERebuildMetadataType::BLOCK_COUNT, 10);

        auto progress = partition.GetRebuildMetadataStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            progress->Record.GetProgress().GetProcessed()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted()
        );
    }



    Y_UNIT_TEST(ShouldFailScanDiskIfAlreadyRunning)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

        ui32 cnt = 0;
        TAutoPtr<IEventHandle> savedEvent;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvScanDiskBatchRequest: {
                        if (++cnt == 1) {
                            savedEvent = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        const auto responseFirst = partition.ScanDisk(10);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            responseFirst->Record.GetError().GetCode());

        const auto responseDuplicate = partition.ScanDisk(10);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            responseDuplicate->Record.GetError().GetCode());

        runtime->Send(savedEvent.Release());

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvScanDiskCompleted);
        runtime->DispatchEvents(options, TDuration::Seconds(1));

        const auto progress = partition.GetScanDiskStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(
            progress->Record.GetProgress().GetTotal(),
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetBrokenBlobs().size());
    }



    Y_UNIT_TEST(ShouldRespectCompactionCountPerRunChangingPeriod)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBatchCompactionEnabled(true);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(99999);
        config.SetCompactionRangeGarbageThreshold(280);
        config.SetCompactionRangeCountPerRun(1);
        config.SetCompactionCountPerRunIncreasingThreshold(6);
        config.SetCompactionCountPerRunDecreasingThreshold(4);
        config.SetHDDMaxBlobsPerUnit(1000);
        config.SetSSDMaxBlobsPerUnit(1000);
        config.SetMaxCompactionRangeCountPerRun(10);
        config.SetCompactionCountPerRunChangingPeriod(100000);

        auto runtime = PrepareTestActorRuntime(
            config,
            4 * 1024 * 1024
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange = TBlockRange32::WithLength(0, 1024);

        ui32 resultCompactionRangeCountPerRun = 0;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Simple;
                        resultCompactionRangeCountPerRun =
                            cc.CompactionRangeCountPerRun.Value;
                        break;
                    }
                }
                return false;
            }
        );

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(1, resultCompactionRangeCountPerRun);
        }

        // CompactionRangeCountPerRun was updated more then period seconds ago
        // So it should be changed
        runtime->AdvanceCurrentTime(TDuration::Seconds(102));

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(2, resultCompactionRangeCountPerRun);
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(50));

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            // Shouldn't increase compactionRangeCountPerRun due to period
            UNIT_ASSERT_VALUES_EQUAL(2, resultCompactionRangeCountPerRun);
        }
    }



    Y_UNIT_TEST(ShouldProcessMultipleRangesUponGarbageCompaction)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetGarbageCompactionRangeCountPerRun(3);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999);

        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        TAutoPtr<IEventHandle> compactionRequest;
        const auto interceptCompactionRequest =
            [&compactionRequest](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvCompactionRequest)
            {
                auto* msg =
                    event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
                    compactionRequest = event.Release();
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(interceptCompactionRequest);

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 =
            TBlockRange32::WithLength(2 * 1024 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange1, 2);

        partition.WriteBlocks(blockRange2, 3);
        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange2, 5);

        partition.WriteBlocks(blockRange3, 6);
        partition.WriteBlocks(blockRange3, 7);
        partition.WriteBlocks(blockRange3, 8);

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8, stats.GetMergedBlobsCount());
        }

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(compactionRequest);
        runtime->Send(compactionRequest.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Cleanup();

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(blockRange1.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(blockRange1.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(5),
            GetBlockContent(partition.ReadBlocks(blockRange2.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(5),
            GetBlockContent(partition.ReadBlocks(blockRange2.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(8),
            GetBlockContent(partition.ReadBlocks(blockRange3.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(8),
            GetBlockContent(partition.ReadBlocks(blockRange3.End)));

        // checking that we now have 1 blob in each of the ranges
        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
        }
    }



    Y_UNIT_TEST(ShouldAddRequestToLoadCompactionMapRangeIfNotLoaded)
    {
        const ui32 rangeCount = 100;

        auto config = DefaultConfig();
        config.SetMaxCompactionRangesLoadingPerTx(5);

        auto runtime = PrepareTestActorRuntime(config, rangeCount * 1024);

        TPartitionClient partition(*runtime);

        for (ui32 i = 0; i < rangeCount; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 256), i);
        }

        std::vector<ui32> waitOutOfOrderRangeIdx;
        TAutoPtr<IEventHandle> delayEvent;
        ui32 rangesLoaded = 0;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvLoadCompactionMapChunkRequest: {
                        auto* msg =
                            event->Get<TEvPartitionPrivate::
                                           TEvLoadCompactionMapChunkRequest>();
                        rangesLoaded += msg->Range.Size();
                        if (!waitOutOfOrderRangeIdx.empty()) {
                            UNIT_ASSERT_EQUAL(
                                waitOutOfOrderRangeIdx.back(),
                                msg->Range.Start);
                            waitOutOfOrderRangeIdx.pop_back();
                            return false;
                        }
                        if (msg->Range.Start == 40 ||
                            msg->Range.Start == 50) {
                            UNIT_ASSERT(!delayEvent);
                            delayEvent = event.Release();
                            return true;
                        }
                        return false;
                    }
                }
                return false;
            });

        partition.RebootTablet();

        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(42 * 1024, 256),
                1);
            const auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(66 * 1024, 256),
                1);
            const auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(83 * 1024, 256),
                1);
            const auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        waitOutOfOrderRangeIdx = { 80, 65 };

        UNIT_ASSERT(delayEvent);
        runtime->Send(delayEvent.Release());

        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT(waitOutOfOrderRangeIdx.empty());

        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(42 * 1024, 256),
                1);
            const auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }
        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(66 * 1024, 256),
                1);
            const auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }
        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(83 * 1024, 256),
                1);
            const auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        UNIT_ASSERT(delayEvent);
        runtime->Send(delayEvent.Release());

        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_GE(rangesLoaded, rangeCount);
    }



    Y_UNIT_TEST(
        ShouldNotLoseAnyMixedMergedBlocksWhileWaitingForCheckpointCreation)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, '1');
        partition.Flush();

        bool interceptTransactions = true;

        std::unique_ptr<IEventHandle> executeTransactionsEvent;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvExecuteTransactions &&
                    interceptTransactions)
                {
                    executeTransactionsEvent.reset(event.Release());
                    interceptTransactions = false;

                    return true;
                }
                return false;
            });

        partition.SendCreateCheckpointRequest("c1");
        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT(executeTransactionsEvent);

        partition.SendReadBlocksRequest(0, "c1");
        auto readBlocksResponse = partition.RecvReadBlocksResponse();

        // Check that create checkpoint transaction is not executed
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, readBlocksResponse->GetStatus());

        runtime->SendAsync(executeTransactionsEvent.release());

        partition.RecvCreateCheckpointResponse();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('1'),
            GetBlockContent(partition.ReadBlocks(0, "c1")));
    }


}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
