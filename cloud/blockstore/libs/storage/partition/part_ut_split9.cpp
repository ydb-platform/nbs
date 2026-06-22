#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"

#include "part_ut_helpers.h"

Y_UNIT_TEST_SUITE(TPartitionTestSplit9)
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

    Y_UNIT_TEST(ShouldRespectMaxBlobRangeSizeDuringBatching)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        const auto maxBlobRangeSize = 2048;
        config.SetMaxBlobRangeSize(maxBlobRangeSize * 4_KB);
        config.SetWriteRequestBatchingEnabled(true);
        auto runtime = PrepareTestActorRuntime(config, maxBlobRangeSize * 2);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            partition.SendWriteBlocksRequest(i % 2 ? i : maxBlobRangeSize + i, i);
        }

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == EAddBlobMode::ADD_WRITE_RESULT) {
                            const auto& mixedBlobs = msg->MixedBlobs;
                            for (const auto& blob: mixedBlobs) {
                                const auto blobRangeSize =
                                    blob.Blocks.back() - blob.Blocks.front();
                                Cdbg << blobRangeSize << Endl;
                                UNIT_ASSERT(blobRangeSize <= maxBlobRangeSize);
                            }
                        }

                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT(stats.GetMixedBlobsCount());

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            const auto block =
                partition.ReadBlocks(i % 2 ? i : maxBlobRangeSize + i);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(i), GetBlockContent(block));
        }

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }



    Y_UNIT_TEST(ShouldReplaceBlobsOnCompaction)
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
        partition.Flush();

        partition.Compaction();

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

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();

        UNIT_ASSERT(stats.GetSysReadCounters().GetExecTime() != 0);
        UNIT_ASSERT(stats.GetSysWriteCounters().GetExecTime() != 0);
        UNIT_ASSERT(stats.GetUserReadCounters().GetExecTime() != 0);
    }



    Y_UNIT_TEST(CompactionShouldTakeCareOfFreshBlocks)
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

        partition.Compaction();

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



    Y_UNIT_TEST(ShouldDeleteCheckpointAfterDeleteCheckpointDataAndReboot)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("checkpoint1");
        partition.Flush();
        partition.WriteBlocks(1, 2);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1, "checkpoint1"))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1))
        );

        partition.CreateCheckpoint("checkpoint1");
        partition.DeleteCheckpointData("checkpoint1");

        partition.SendReadBlocksRequest(1, "checkpoint1");
        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());

        partition.RebootTablet();
        partition.WaitReady();

        partition.SendReadBlocksRequest(1, "checkpoint1");
        response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());

        {
            auto responseDelete = partition.DeleteCheckpoint("checkpoint1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseDelete->GetStatus());
        }
        {
            auto responseCreate = partition.CreateCheckpoint("checkpoint1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseCreate->GetStatus());
        }
        {
            auto responseDelete = partition.DeleteCheckpoint("checkpoint1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseDelete->GetStatus());
        }
        {
            partition.CreateCheckpoint("checkpoint1", "id1");
            partition.DeleteCheckpointData("checkpoint1");
            auto responseCreate = partition.CreateCheckpoint("checkpoint1", "id1");
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, responseCreate->GetStatus());
        }
    }



    Y_UNIT_TEST(ShouldSeeChangedMergedBlocksInChangedBlocksRequest)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(TBlockRange32::WithLength(1024,1024), 2);
        partition.WriteBlocks(TBlockRange32::WithLength(2048,1024), 2);
        partition.CreateCheckpoint("cp2");

        auto response = partition.GetChangedBlocks(
            TBlockRange32::WithLength(0, 3072),
            "cp1",
            "cp2",
            false);

        const auto& mask = response->Record.GetMask();
        UNIT_ASSERT_VALUES_EQUAL(384, mask.size());
        AssertEqual(
            TVector<ui8>(256, 255),
            TVector<ui8>(mask.begin() + 128, mask.end())
        );
    }



    Y_UNIT_TEST(ShouldReassignNonwritableTabletChannelsAgainAfterErrorFromHiveProxy)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 reassignedTabletId = 0;
        TVector<ui32> channels;
        ui32 ssflags = ui32(
            NKikimrBlobStorage::StatusDiskSpaceYellowStop |
            NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvReassignTabletRequest: {
                        auto* msg = event->Get<TEvHiveProxy::TEvReassignTabletRequest>();
                        reassignedTabletId = msg->TabletId;
                        channels = msg->Channels;
                        auto response =
                            std::make_unique<TEvHiveProxy::TEvReassignTabletResponse>(
                                MakeError(E_REJECTED, "error")
                            );
                        runtime->Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            0
                        ), 0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return StorageStateChanger(ssflags)(event);
            }
        );

        // first request is successful since we don't know that our channels
        // are yellow yet
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // but upon EvPutResult we should find out that our channels are yellow
        // and should send a reassign request
        UNIT_ASSERT_VALUES_EQUAL(TestTabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

        reassignedTabletId = 0;
        channels.clear();

        for (ui32 i = 0; i < 3; ++i) {
            {
                // other requests should fail
                auto request = partition.CreateWriteBlocksRequest(
                    TBlockRange32::WithLength(0, 1024));
                partition.SendToPipe(std::move(request));

                auto response =
                    partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
                UNIT_ASSERT_VALUES_EQUAL(
                    E_BS_OUT_OF_SPACE,
                    response->GetError().GetCode()
                );
            }

            // checking that a reassign request has been sent
            UNIT_ASSERT_VALUES_EQUAL(TestTabletId, reassignedTabletId);
            UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
            UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

            reassignedTabletId = 0;
            channels.clear();
        }
    }



    Y_UNIT_TEST(ShouldCorrectlyCalculateLogicalUsedBlocksCountForOverlayDisk)
    {
        ui32 blockCount = 1024 * 128;
        TPartitionContent baseContent;
        for (size_t i = 0; i < 100; ++i) {
            baseContent.push_back(TEmpty());
        }
        for (size_t i = 100; i < 2048 + 100; ++i) {
            baseContent.push_back(TBlob(i, 1));
        }
        for (size_t i = 0; i < 100; ++i) {
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

        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 4, 2049),
            1);   // +2049
        partition.ZeroBlocks(TBlockRange32::WithLength(1024 * 5, 11));   // -11
        partition.ZeroBlocks(
            TBlockRange32::WithLength(1024 * 10, 1025));   // -0
        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 4, 11),
            1);   // +0
        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 3, 11),
            1);   // +11

        ui64 expectedUsedBlocksCount = 2048 + 1 - 11 + 11;
        ui64 expectedLogicalUsedBlocksCount = 2048 + expectedUsedBlocksCount;

        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(
            expectedUsedBlocksCount,
            stats.GetUsedBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            expectedLogicalUsedBlocksCount,
            stats.GetLogicalUsedBlocksCount()
        );

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(
            expectedUsedBlocksCount,
            stats.GetUsedBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            expectedLogicalUsedBlocksCount,
            stats.GetLogicalUsedBlocksCount()
        );

        // range [100-200) is zero on overlay disk, but non zero on base disk
        partition.ZeroBlocks(TBlockRange32::WithLength(100, 100));

        response = partition.StatPartition();
        stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(
            expectedUsedBlocksCount,
            stats.GetUsedBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            expectedLogicalUsedBlocksCount - 100,
            stats.GetLogicalUsedBlocksCount()
        );
    }



    Y_UNIT_TEST(ShouldReturnErrorIfCompactionIsAlreadyRunning)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024,1024));

        TAutoPtr<IEventHandle> addBlobs;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        if (!addBlobs) {
                            addBlobs = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        partition.SendCompactionRequest(0);

        partition.SendCompactionRequest(1024);

        {
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT(FAILED(compactResponse->GetStatus()));
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, compactResponse->GetStatus());
        }

        runtime->SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        runtime->Send(addBlobs.Release());

        {
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT(SUCCEEDED(compactResponse->GetStatus()));
        }

        {
            partition.SendCompactionRequest(0);
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, compactResponse->GetStatus());
        }
    }



    Y_UNIT_TEST(ShouldHandleFlushCorrectlyWhileBlockFromFreshChannelIsBeingDeleted)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        TAutoPtr<IEventHandle> addBlobsRequest;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddBlobsRequest) {
                    addBlobsRequest = event.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendFlushRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        UNIT_ASSERT(addBlobsRequest);
        runtime->Send(addBlobsRequest.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
    }



    Y_UNIT_TEST(ShouldProcessMultipleRangesUponCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBatchCompactionEnabled(true);
        config.SetCompactionRangeCountPerRun(3);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 = TBlockRange32::WithLength(2 * 1024 * 1024, 1024);
        const auto blockRange4 = TBlockRange32::WithLength(3 * 1024 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange1, 2);
        partition.WriteBlocks(blockRange1, 3);

        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange2, 5);
        partition.WriteBlocks(blockRange2, 6);

        partition.WriteBlocks(blockRange3, 7);
        partition.WriteBlocks(blockRange3, 8);
        partition.WriteBlocks(blockRange3, 9);

        partition.WriteBlocks(blockRange4, 10);
        partition.WriteBlocks(blockRange4, 11);

        // blockRange4 should not be compacted, other ranges - should
        partition.Compaction();
        partition.Cleanup();

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(blockRange1.Start))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(blockRange1.End))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(6),
            GetBlockContent(partition.ReadBlocks(blockRange2.Start))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(6),
            GetBlockContent(partition.ReadBlocks(blockRange2.End))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange3.Start))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange3.End))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(blockRange4.Start))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(blockRange4.End))
        );

        // checking that we now have 1 blob in each of the first 3 ranges
        // and 2 blobs in the last range
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMergedBlobsCount());
        }

        // blockRange4 and any other 2 ranges should be compacted
        partition.Compaction();
        partition.Cleanup();

        // all ranges should contain 1 blob now
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }
    }



    Y_UNIT_TEST(ShouldCompactAfterAddingConfirmedBlobs)
    {
        auto config = DefaultConfig();
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

        TAutoPtr<IEventHandle> addConfirmedBlobs;
        bool interceptAddConfirmedBlobs = true;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddConfirmedBlobsRequest: {
                        if (interceptAddConfirmedBlobs) {
                            UNIT_ASSERT(!addConfirmedBlobs);
                            addConfirmedBlobs = event.Release();
                            return true;
                        }
                        break;
                    }
                }

                return false;
            }
        );

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);
        UNIT_ASSERT(addConfirmedBlobs);

        partition.SendCompactionRequest();
        // wait for compaction to be queued
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        interceptAddConfirmedBlobs = false;
        runtime->Send(addConfirmedBlobs.Release());

        {
            auto compactResponse = partition.RecvCompactionResponse();
            // should fail on S_ALREADY and S_FALSE to ensure that compaction
            // was really taken place here
            UNIT_ASSERT_VALUES_EQUAL(S_OK, compactResponse->GetStatus());
        }
    }



    Y_UNIT_TEST(ShouldReportLongRunningWriteBlobOperationCancel)
    {
        auto sendRequest = [](TPartitionClient& partition)
        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 255));
        };
        auto receiveResponse = [](TPartitionClient& partition)
        {
            return partition.RecvWriteBlocksResponse();
        };

        DoShouldReportLongRunningBlobOperations(
            sendRequest,
            receiveResponse,
            TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation::
                WriteBlob,
            true);
    }



    Y_UNIT_TEST(
        ShouldAutomaticallyRunCompactionAndWriteBlobToMixedChannelIfBlobSizeIsBelowTheThreshold)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetHDDMaxBlobsPerRange(compactionThreshold);
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.WriteBlocks(i, i);
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(0, 0);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // data size for compaction is less than CompactionMergedBlobThresholdHDD
        // so the whole data should be moved to a mixed channel
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold * 2,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }
    }



    Y_UNIT_TEST(ShouldCleanupUnconfirmedBlobsAfterErrorOnWriteBlob)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(
            config,
            4096,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        bool shouldRejectDeleteUnconfirmedBlobsRequest = true;
        // Create event filter for WriteBlobResponse rejection
        TTestActorRuntimeBase::TEventFilter rejectionFilter =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
        {
            switch (ev->GetTypeRewrite()) {
                case TEvPartitionCommonPrivate::EvWriteBlobResponse: {
                    auto* msg =
                        ev->Get<TEvPartitionCommonPrivate::TEvWriteBlobResponse>();
                    auto& e = const_cast<NProto::TError&>(msg->Error);
                    e.SetCode(E_REJECTED);
                    return false;
                }
                case TEvPartitionPrivate::
                    EvDeleteUnconfirmedBlobsRequest: {
                    return shouldRejectDeleteUnconfirmedBlobsRequest;
                }
            };
            return false;
        };

        // Set up event order filter to ensure that
        // AddUnconfirmedBlobsResponse is processed before WriteBlobResponse
        TEventExecutionOrderFilter addWriteBlobOrderFilter(
            *runtime,
            TVector<std::pair<ui32, ui32>>{
                {TEvPartitionPrivate::EvAddUnconfirmedBlobsResponse,
                 TEvPartitionCommonPrivate::EvWriteBlobResponse}});
        runtime->SetEventFilter(addWriteBlobOrderFilter(rejectionFilter));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(10, 1), 1);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetUnconfirmedBlobCount());
        }

        // Set up event order filter to ensure that
        // WriteBlobResponse is processed before AddUnconfirmedBlobsResponse
        TEventExecutionOrderFilter writeAddBlobOrderFilter(
            *runtime,
            TVector<std::pair<ui32, ui32>>{
                {TEvPartitionCommonPrivate::EvWriteBlobResponse,
                 TEvPartitionPrivate::EvAddUnconfirmedBlobsResponse}});
        runtime->SetEventFilter(writeAddBlobOrderFilter(rejectionFilter));

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(11, 1), 1);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetUnconfirmedBlobCount());
        }

        partition.RebootTablet();
        partition.WaitReady();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
        }

        shouldRejectDeleteUnconfirmedBlobsRequest = false;

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(12, 1), 1);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
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


}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
