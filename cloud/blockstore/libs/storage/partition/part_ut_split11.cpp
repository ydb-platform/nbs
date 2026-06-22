#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"

#include "part_ut_helpers.h"

Y_UNIT_TEST_SUITE(TPartitionTestSplit11)
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

    Y_UNIT_TEST(ShouldRecoverBlocksOnReboot)
    {
        auto runtime = PrepareTestActorRuntime();

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
            0,
            stateJson["FreshBlocksFromChannel"].GetInteger());
        UNIT_ASSERT_VALUES_EQUAL(
            6,
            stateJson["FreshBlocksFromDb"].GetInteger());

        partition.RebootTablet();

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

        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(4))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(5))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(6))
        );
    }



    Y_UNIT_TEST(ShouldAutomaticallyRunCompaction)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlobCount = 0;
        ui64 compactionByReadStats = 0;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlobCount =
                            cc.CompactionByBlobCountPerRange.Value;
                        compactionByReadStats = cc.CompactionByReadStats.Value;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.WriteBlocks(i, i);
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMixedBlobsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(0, 0);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlobsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(1, compactionByBlobCount);
        UNIT_ASSERT_EQUAL(0, compactionByReadStats);
    }



    Y_UNIT_TEST(ShouldZeroBlocksWrittenToFreshChannelAfterReboot)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.ZeroBlocks(1);

        partition.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(1))
        );
    }



    Y_UNIT_TEST(ShouldRejectCheckpointRequestWhenCheckpointIsInFlight)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool interceptEvent = false;

        std::unique_ptr<IEventHandle> putEvent;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                    auto* msg = event->Get<TEvBlobStorage::TEvPut>();

                    if (msg->Id.Channel() == 0 && interceptEvent) {
                        putEvent.reset(event.Release());
                        interceptEvent = false;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        interceptEvent = true;
        partition.SendCreateCheckpointRequest("cp1", "id1");

        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT(putEvent);

        {
            partition.SendCreateCheckpointRequest("cp1", "id2");
            auto response = partition.RecvCreateCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        {
            partition.SendDeleteCheckpointRequest("cp1");
            auto response = partition.RecvDeleteCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        runtime->SendAsync(putEvent.release());

        auto response = partition.RecvCreateCheckpointResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

        partition.DeleteCheckpoint("cp1");
        partition.CreateCheckpoint("cp1", "id2");
    }



    Y_UNIT_TEST(ShouldUseMostRecentCommitIdWhenHighCheckpointIdIsNotSet)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1, 1);

        partition.CreateCheckpoint("cp2");

        partition.WriteBlocks(2, 1);

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "cp1",
                "cp2",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(384, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetMask()[0]);
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "cp1",
                "",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(384, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(6, response->Record.GetMask()[0]);
        }
    }



    Y_UNIT_TEST(ShouldSendBackpressureReportsRegularly)
    {
        const auto blockCount = 1000;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TBackpressureReport report;
        bool reportUpdated = false;

        runtime->SetObserverFunc(
            [&report, &reportUpdated] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvBackpressureReport: {
                        report = *event->Get<TEvPartition::TEvBackpressureReport>();
                        reportUpdated = true;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (int i = 0; i < 200; ++i) {
            partition.WriteBlocks(i, i);
        }

        // Manually sending TEvSendBackpressureReport here
        // In real scenarios TPartitionActor will schedule this event
        // upon executor activation
        reportUpdated = false;
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvSendBackpressureReport>()
        );

        runtime->DispatchEvents({}, TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(true, reportUpdated);
        UNIT_ASSERT_DOUBLES_EQUAL(3.5, report.FreshIndexScore, 1e-5);
    }



    Y_UNIT_TEST(ShouldReadBlocksFromBaseDisk)
    {
        TPartitionContent baseContent = {
        /*|      0      |     1     |     2 ... 5    |     6     |      7      |     8     |      9      |*/
            TBlob(1, 1) , TFresh(2) , TBlob(2, 3, 4) ,  TEmpty() , TBlob(1, 4) , TFresh(5) , TBlob(2, 6)
        };
        auto bitmap = CreateBitmap(10);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 10));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(baseContent), GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

        const auto evStats = TEvStatsService::EvVolumePartCounters;

        ui32 externalBlobReads = 0;
        ui32 externalBlobBytes = 0;
        auto obs = [&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == evStats) {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();

                    const auto& readBlobCounters =
                        msg->DiskCounters->RequestCounters.ReadBlob;
                    externalBlobReads = readBlobCounters.ExternalCount;
                    externalBlobBytes = readBlobCounters.ExternalRequestBytes;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            };

        runtime.SetObserverFunc(obs);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(evStats);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, externalBlobReads);
        UNIT_ASSERT_VALUES_EQUAL(7 * DefaultBlockSize, externalBlobBytes);
    }



    Y_UNIT_TEST(ShouldReturnErrorForUnknownIdInCompactionStatusRequest)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        partition.SendGetCompactionStatusRequest("xxx");

        auto response = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
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
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
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
                            TLogoBlobID(),  // doesn't matter
                            TLogoBlobID(),  // doesn't matter
                            0);             // doesn't matter

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
            }
        );

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



    Y_UNIT_TEST(WritingBlobsInsteadOfPatchingIfDiffIsGreaterThanThreshold)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        config.SetMaxDiffPercentageForBlobPatching(75);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        bool evPatchObserved = false;
        bool evWriteObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPatch: {
                        evPatchObserved = true;
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvWriteBlobRequest: {
                        evWriteObserved = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(0, 512 + 256 + 64);
        const auto blockRange3 = TBlockRange32::WithLength(0, 512);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 2);

        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(256))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(512 + 256 + 63))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(900))
        );

        UNIT_ASSERT(!evPatchObserved);
        UNIT_ASSERT(evWriteObserved);

        evWriteObserved = false;
        partition.WriteBlocks(blockRange3, 3);

        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(511))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(512))
        );

        UNIT_ASSERT(evPatchObserved);
        UNIT_ASSERT(evWriteObserved);
    }



    Y_UNIT_TEST(ShouldLimitUnconfirmedBlobCount)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        config.SetUnconfirmedBlobCountHardLimit(1);
        auto runtime = PrepareTestActorRuntime(config);

        runtime->SetObserverFunc([&] (auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvAddConfirmedBlobsRequest: {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(10, 1);
        {
            // can't read unconfirmed range
            partition.SendReadBlocksRequest(10);
            auto response = partition.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        }

        // but next blob should be added bypassing 'unconfirmed blobs' feature
        partition.WriteBlocks(11, 2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(11))
        );
    }



    Y_UNIT_TEST(ShouldDetectBlockCorruptionInBlobsWhenAddingUnconfirmedBlobs)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto config = DefaultConfig();
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range = TBlockRange32::WithLength(0, 1024);
        partition.WriteBlocks(range, 1);

        EnableReadBlobCorruption(*runtime);

        // direct read should fail
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            range.Size(),
            TString::TUninitialized(DefaultBlockSize));
        partition.SendReadBlocksLocalRequest(range, std::move(sglist));
        auto response = partition.RecvReadBlocksLocalResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());
    }



    Y_UNIT_TEST(CompactionShouldMoveDataFromMergedToMixedWhenDataSizeIsAboveTheTreshold)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(3_KB);
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // all data should be written to a merged channel directly because data
        // size is less than threshold
        for (int i = 0; i < 4; ++i) {
            partition.WriteBlocks(i, i);
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }

        // data should be moved to a mixed channel as a result of compaction,
        // because data is less than threshold
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        for (int i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(i),
                GetBlockContent(partition.ReadBlocks(i))
            );
        }
    }



    Y_UNIT_TEST(ShouldSendPartitionStatistics)
    {
        auto config = DefaultConfig();
        config.SetUsePullSchemeForVolumeStatistics(true);

        auto runtime = PrepareTestActorRuntime(config);

        bool partitionStatisticsSent = false;

        auto _ = runtime->AddObserver<
            TEvPartitionCommonPrivate::TEvGetPartCountersResponse>(
            [&](TEvPartitionCommonPrivate::TEvGetPartCountersResponse::TPtr& ev)
            {
                Y_UNUSED(ev);
                partitionStatisticsSent = true;
            });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.SendToPipe(
            std::make_unique<
                TEvPartitionCommonPrivate::TEvGetPartCountersRequest>());

        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        runtime->DispatchEvents();

        // Check that partition sent statistics
        UNIT_ASSERT(partitionStatisticsSent);
    }



    Y_UNIT_TEST(ShouldReadBlobInfo)
    {
        constexpr ui32 rangeSize = 1024;
        constexpr ui32 blockCount = rangeSize * 3;

        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TVector<TBlockRange32> blockRanges = {
            TBlockRange32::WithLength(0, rangeSize),
            TBlockRange32::WithLength(rangeSize / 2, rangeSize),
            TBlockRange32::WithLength(rangeSize, rangeSize),
        };

        TVector<TPartialBlobId> blobs;
        bool interceptWriteBlobRequest = true;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvWriteBlobRequest: {
                        if (interceptWriteBlobRequest) {
                            auto* msg = event->Get<TEvPartitionCommonPrivate::
                                                       TEvWriteBlobRequest>();
                            blobs.push_back(msg->BlobId);
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        char fill = '1';
        for (const auto& blockRange: blockRanges) {
            partition.WriteBlocks(blockRange, fill);
            fill++;
        }

        interceptWriteBlobRequest = false;

        TBlockMask emptyBlockMask;

        TBlockMask fullBlockMask;
        fullBlockMask.Set(0, MaxBlocksCount);

        TBlockMask halfBlockMask;
        halfBlockMask.Set(0, MaxBlocksCount / 2);

        {
            auto blobsInfo = partition.CompactionReadBlobInfo(blobs, blobs);

            UNIT_ASSERT_VALUES_EQUAL(
                blobsInfo->BlobMetasForBlobs.size(),
                3);
            for (size_t i = 0; i < blockRanges.size(); ++i) {
                const auto& blobMeta = blobsInfo->BlobMetasForBlobs[i];
                UNIT_ASSERT(blobMeta.HasMergedBlocks());
                UNIT_ASSERT_VALUES_EQUAL(
                    blockRanges[i].Start,
                    blobMeta.GetMergedBlocks().GetStart());
                UNIT_ASSERT_VALUES_EQUAL(
                    blockRanges[i].End,
                    blobMeta.GetMergedBlocks().GetEnd());
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    blobMeta.GetMergedBlocks().GetSkipped());
            }

            UNIT_ASSERT_VALUES_EQUAL(
                blobsInfo->BlockMasksForBlobs.size(),
                3);
            for (size_t i = 0; i < blockRanges.size(); ++i) {
                UNIT_ASSERT_C(
                    emptyBlockMask == blobsInfo->BlockMasksForBlobs[i],
                    TStringBuilder()
                        << BlockMaskAsString(emptyBlockMask) << " != "
                        << BlockMaskAsString(blobsInfo->BlockMasksForBlobs[i]));
            }
        }

        partition.Compaction();

        {
            auto blobsInfo = partition.CompactionReadBlobInfo(blobs, blobs);

            UNIT_ASSERT_VALUES_EQUAL(
                blobsInfo->BlockMasksForBlobs.size(),
                3);
            TVector<TBlockMask> expectedBlockMasks = {
                fullBlockMask,
                halfBlockMask,
                emptyBlockMask};
            for (size_t i = 0; i < blockRanges.size(); ++i) {
                UNIT_ASSERT_C(
                    expectedBlockMasks[i] == blobsInfo->BlockMasksForBlobs[i],
                    TStringBuilder()
                        << BlockMaskAsString(expectedBlockMasks[i]) << " != "
                        << BlockMaskAsString(blobsInfo->BlockMasksForBlobs[i]));
            }
        }

        partition.Compaction();

        {
            auto blobsInfo = partition.CompactionReadBlobInfo(blobs, blobs);

            UNIT_ASSERT_VALUES_EQUAL(blobsInfo->BlockMasksForBlobs.size(), 3);
            TVector<TBlockMask> expectedBlockMasks = {
                fullBlockMask,
                fullBlockMask,
                fullBlockMask};
            for (size_t i = 0; i < blockRanges.size(); ++i) {
                UNIT_ASSERT_C(
                    expectedBlockMasks[i] == blobsInfo->BlockMasksForBlobs[i],
                    TStringBuilder()
                        << BlockMaskAsString(expectedBlockMasks[i]) << " != "
                        << BlockMaskAsString(blobsInfo->BlockMasksForBlobs[i]));
            }
        }
    }


}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
