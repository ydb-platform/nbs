#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"

#include "part_ut_helpers.h"

Y_UNIT_TEST_SUITE(TPartitionTestSplit8)
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

    Y_UNIT_TEST(ShouldBatchIntersectingWrites)
    {
        NProto::TStorageServiceConfig config;
        config.SetWriteRequestBatchingEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(PartitionBatchWriteCollector(*runtime, 1000));

        for (ui32 i = 0; i < 10; ++i) {
            for (ui32 j = 0; j < 100; ++j) {
                partition.SendWriteBlocksRequest(
                    TBlockRange32::WithLength(i * 100, j + 1),
                    i + 1);
            }
        }

        for (ui32 i = 0; i < 1000; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT(stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(999, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(100, stats.GetFreshBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1000, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            1000,
            stats.GetUserWriteCounters().GetRequestsCount());
        const auto batchCount = stats.GetUserWriteCounters().GetBatchCount();
        UNIT_ASSERT(batchCount < 1000);
        UNIT_ASSERT(batchCount > 0);

        for (ui32 i = 0; i < 10; ++i) {
            for (ui32 j = 0; j < 100; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(
                    GetBlockContent(i + 1),
                    GetBlockContent(partition.ReadBlocks(i * 100 + j)));
            }
        }

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }



    Y_UNIT_TEST(ShouldMergeVersionsOnRead)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.Flush();

        partition.WriteBlocks(1, 11);
        partition.Flush();

        partition.WriteBlocks(2, 22);
        partition.Flush();

        partition.WriteBlocks(3, 33);
        // partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(22),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(33),
            GetBlockContent(partition.ReadBlocks(3))
        );
    }



    Y_UNIT_TEST(
        ShouldAutomaticallyRunIgnoringZeroedCompactionForSuperDirtyRanges)
    {
        DoShouldAutomaticallyRunGarbageCompactionForSuperDirtyRanges(true);
    }



    Y_UNIT_TEST(ShouldDeleteCheckpointAfterDeleteCheckpointData)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.CreateCheckpoint("checkpoint1");
        partition.DeleteCheckpointData("checkpoint1");

        auto responseDelete = partition.DeleteCheckpoint("checkpoint1");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, responseDelete->GetStatus());
    }



    Y_UNIT_TEST(ShouldSeeChangedFreshBlocksInChangedBlocksRequest)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0,1);
        partition.WriteBlocks(1,1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1,2);
        partition.WriteBlocks(2,2);
        partition.CreateCheckpoint("cp2");

        auto response = partition.GetChangedBlocks(
            TBlockRange32::WithLength(0, 1024),
            "cp1",
            "cp2",
            false);

        UNIT_ASSERT_VALUES_EQUAL(128, response->Record.GetMask().size());
        UNIT_ASSERT_VALUES_EQUAL(6, response->Record.GetMask()[0]);
    }



    Y_UNIT_TEST(ShouldRejectCompactionRequestsIfDataChannelsAreAlmostFull)
    {
        // smoke test: tests that the compaction mechanism checks partition state
        // detailed tests are located in part_state_ut.cpp
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(StorageStateChanger(
            NKikimrBlobStorage::StatusDiskSpaceLightYellowMove |
            NKikimrBlobStorage::StatusDiskSpaceYellowStop));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        {
            auto response = partition.Compaction();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetError().GetCode());
        }

        ui32 flags = NKikimrBlobStorage::StatusDiskSpaceLightYellowMove |
            NKikimrBlobStorage::StatusDiskSpaceYellowStop;

        const auto badFlags = {
            NKikimrBlobStorage::StatusDiskSpaceLightOrange,
            NKikimrBlobStorage::StatusDiskSpacePreOrange,
            NKikimrBlobStorage::StatusDiskSpaceOrange,
            NKikimrBlobStorage::StatusDiskSpaceRed,
            NKikimrBlobStorage::StatusDiskSpaceBlack
        };

        for (const auto flag: badFlags) {
            flags |= flag;

            partition.RebootTablet();

            runtime->SetObserverFunc(StorageStateChanger(flags));
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

            auto request = partition.CreateCompactionRequest();
            partition.SendToPipe(std::move(request));

            auto response = partition.RecvResponse<TEvPartitionPrivate::TEvCompactionResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_OUT_OF_SPACE, response->GetError().GetCode());
        }
    }



    Y_UNIT_TEST(ShouldCorrectlyCopyUsedBlocksCountForOverlayDisk)
    {
        ui32 blockCount =  1024 * 1024 * 1024;
        ui32 usedBlocksCount = 0;

        TPartitionContent baseContent;
        for (size_t i = 0; i < blockCount/4; i += 50) {
            baseContent.push_back(TBlob(i, 0, 49));
            usedBlocksCount += 49;
            baseContent.push_back(TEmpty());
        }

        auto partitionWithRuntime = SetupOverlayPartition(
            TestTabletId,
            TestTabletId2,
            baseContent,
            {},
            DefaultBlockSize,
            blockCount,
            DefaultConfig());

        auto& partition = *partitionWithRuntime.Partition;

        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            usedBlocksCount,
            stats.GetLogicalUsedBlocksCount()
        );
    }



    Y_UNIT_TEST(ShouldReturnErrorWhenReadingFromUnknownCheckpoint)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.SendReadBlocksRequest(TBlockRange32::MakeOneBlock(0), "unknown");

        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }



    Y_UNIT_TEST(ShouldNotReleaseTrimBarriersOnFlushError)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetMaxWriteBlobErrorsBeforeSuicide(10);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(1, 2));
        partition.WriteBlocks(TBlockRange32::WithLength(1, 2));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetFreshBlocksCount());
        }

        bool flush = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPutResult: {
                        if (flush) {
                            auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                            msg->Status = NKikimrProto::ERROR;
                            flush = false;
                        }
                        break;
                    }
                }

                return false;
            }
        );

        flush = true;
        partition.SendFlushRequest();
        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));

        partition.WriteBlocks(TBlockRange32::WithLength(1, 2));

        partition.Flush();
    }



    Y_UNIT_TEST(ShouldNotKillTabletBeforeMaxWriteBlobErrorsHappen)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetMaxWriteBlobErrorsBeforeSuicide(5);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1001), 1);

        const auto eventHandler = [&] (const TEvBlobStorage::TEvPut::TPtr& ev) {
            TLogoBlobID logoBlobId;
            auto response = std::make_unique<TEvBlobStorage::TEvPutResult>(
                NKikimrProto::ERROR,
                logoBlobId,
                0, // statusFlags
                0, // groupId
                0 // approximateFreeSpaceShare
            );

            runtime->Schedule(
                new IEventHandle(
                    ev->Sender,
                    ev->Recipient,
                    response.release(),
                    0,
                    ev->Cookie),
                TDuration());

            return true;
        };

        runtime->SetEventFilter(
            [eventHandler] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                bool handled = false;

                const auto wrapped = [&] (const auto& ev) {
                    handled = eventHandler(ev);
                };

                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvPut, wrapped);
                }
                return handled;
            }
        );

        bool suicideHappened = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvTablet::EEv::EvTabletDead: {
                        suicideHappened = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            }
        );

        for (ui32 i = 1; i < config.GetMaxWriteBlobErrorsBeforeSuicide(); i++) {
            partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1001), 1);
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
            UNIT_ASSERT(!suicideHappened);
        }

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1001), 1);
        auto response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));

        UNIT_ASSERT(suicideHappened);
    }



    Y_UNIT_TEST(ShouldWriteBlocksWhenAddingUnconfirmedBlobs)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        bool dropAddConfirmedBlobs = false;

        runtime->SetObserverFunc([&] (auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvAddConfirmedBlobsRequest: {
                    if (dropAddConfirmedBlobs) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(11, 0);
        partition.CreateCheckpoint("checkpoint");

        partition.WriteBlocks(10, 1);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetConfirmedBlobCount());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(10))
        );

        dropAddConfirmedBlobs = true;
        partition.WriteBlocks(11, 2);

        {
            // can't read unconfirmed range
            partition.SendReadBlocksRequest(11);
            auto response = partition.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        }

        {
            // can't describe unconfirmed range
            partition.SendDescribeBlocksRequest(TBlockRange32::WithLength(11, 1), "");
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        }

        {
            // can't do GetChangedBlocks on unconfirmed range
            partition.SendGetChangedBlocksRequest(
                TBlockRange32::WithLength(11, 1),
                "checkpoint",
                "",
                false);
            auto response =
                partition.RecvResponse<TEvService::TEvGetChangedBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetConfirmedBlobCount());
        }

        // but we can work with other ranges
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(10))
        );
        partition.DescribeBlocks(TBlockRange32::WithLength(10, 1), "");
        partition.GetChangedBlocks(TBlockRange32::WithLength(10, 1), "checkpoint", "", false);

        // older commits are also available even when range overlaps with
        // unconfirmed blobs
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(0),
            GetBlockContent(partition.ReadBlocks(11, "checkpoint"))
        );
        partition.DescribeBlocks(TBlockRange32::WithLength(11, 1), "checkpoint");
        partition.GetChangedBlocks(TBlockRange32::WithLength(11, 1), "", "checkpoint", false);

        dropAddConfirmedBlobs = false;
        partition.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(11))
        );

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetConfirmedBlobCount());
        }
    }



    Y_UNIT_TEST(ShouldReportLongRunningReadBlobOperationCancel)
    {
        auto sendRequest = [](TPartitionClient& partition)
        {
            partition.SendReadBlocksRequest(0);
        };
        auto receiveResponse = [](TPartitionClient& partition)
        {
            return partition.RecvReadBlocksResponse();
        };

        DoShouldReportLongRunningBlobOperations(
            sendRequest,
            receiveResponse,
            TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation::
                ReadBlob,
            true);
    }



    Y_UNIT_TEST(ShouldBatchSmallWritesToFreshChannelIfThresholdNotExceeded)
    {
        NProto::TStorageServiceConfig config;
        config.SetWriteRequestBatchingEnabled(true);
        config.SetWriteBlobThreshold(2_MB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 blockCount = 500;
        runtime->SetObserverFunc(
            PartitionBatchWriteCollector(*runtime, blockCount));

        for (ui32 i = 0; i < blockCount; ++i) {
            partition.SendWriteBlocksRequest(i, i);
        }

        for (ui32 i = 0; i < blockCount; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(blockCount, stats.GetFreshBlocksCount());

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }



    Y_UNIT_TEST(ShouldSendCorrectBarriersInfoAfterReboot)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(
            config,
            4096,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        using TBarrierKey = std::tuple<ui32, ui32, ui32>;
        using TBarrierValue = std::pair<ui32, ui32>;
        using TBarriers = std::map<TBarrierKey, TBarrierValue>;
        TBarriers barriers;

        bool rejectWrite = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvWriteBlobResponse: {
                        if (rejectWrite) {
                            auto* msg = ev->Get<
                                TEvPartitionCommonPrivate::TEvWriteBlobResponse>();
                            auto& e = const_cast<NProto::TError&>(msg->Error);
                            e.SetCode(E_REJECTED);
                        }
                        return false;
                    }
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg =
                            ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                        TBarrierKey key = std::tie(
                            msg->RecordGeneration,
                            msg->PerGenerationCounter,
                            msg->Channel);
                        TBarrierValue value = std::make_pair(
                            msg->CollectGeneration,
                            msg->CollectStep);
                        const auto it = barriers.insert(TBarriers::value_type(key, value));
                        UNIT_ASSERT(it.second || it.first->second == value);
                        return false;
                    }
                };
                return false;
            });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(11, 1), 0);
        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(10, 1), 1);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
        }

        rejectWrite = true;
        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(11, 1), 0);
        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(10, 1), 1);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
        }

        rejectWrite = false;

        partition.RebootTablet();
        partition.WaitReady();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
        }

        UNIT_ASSERT(!barriers.empty());
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


}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
