#include "part_ut.h"

#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/control/immediate_control_board_impl.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TStorageServiceConfig SelectiveTrimConfig()
{
    auto config = DefaultConfig();
    config.SetFreshChannelWriteRequestsEnabled(true);
    config.SetFreshDeadBlobByteCountTrimThresholdHDD(3 * DefaultBlockSize);
    config.SetFreshSelectiveTrimEnabledHDD(true);
    return config;
}

TVector<ui64> GetCommitIds(const TVector<TLogoBlobID>* blobIds)
{
    TVector<ui64> result;
    if (blobIds) {
        for (const auto& blobId: *blobIds) {
            result.push_back(MakeCommitId(blobId.Generation(), blobId.Step()));
        }
    }
    Sort(result);
    return result;
}

struct TFreshLogTrimCounters
{
    // TEvCollectGarbage requests sent to the fresh channel.
    ui32 HardCollectRequestsSent = 0;
    ui32 SoftCollectRequestsSent = 0;
    // 'H' for hard commands and 'S' for soft ones, in order.
    TString CommandKinds;
    ui32 TrimsCompleted = 0;
    ui32 TrimsFailed = 0;
    ui32 FlushesCompleted = 0;

    // The barrier and flags of the last soft command.
    ui64 SelectiveTrimToCommitId = 0;
    TVector<ui64> Keep;
    TVector<ui64> DoNotKeep;

    // Fresh blobs in the order they were added to the partition.
    TVector<ui64> FreshBlobCommitIds;
    // Fresh blobs loaded at the last start.
    TVector<ui64> LoadedCommitIds;

    void OnEvent(IEventHandle& event)
    {
        switch (event.GetTypeRewrite()) {
            case TEvBlobStorage::EvCollectGarbage: {
                constexpr ui32 FreshChannelId = 4;
                auto* msg = event.Get<TEvBlobStorage::TEvCollectGarbage>();
                if (msg->Channel != FreshChannelId) {
                    break;
                }
                if (msg->Hard) {
                    ++HardCollectRequestsSent;
                    CommandKinds += 'H';
                    break;
                }
                ++SoftCollectRequestsSent;
                CommandKinds += 'S';
                SelectiveTrimToCommitId =
                    MakeCommitId(msg->CollectGeneration, msg->CollectStep);
                Keep = GetCommitIds(msg->Keep.Get());
                DoNotKeep = GetCommitIds(msg->DoNotKeep.Get());
                break;
            }
            case TEvPartitionCommonPrivate::EvAddFreshBlocksRequest: {
                auto* msg = event.Get<
                    TEvPartitionCommonPrivate::TEvAddFreshBlocksRequest>();
                FreshBlobCommitIds.push_back(msg->CommitId);
                break;
            }
            case TEvPartitionCommonPrivate::EvLoadFreshBlobsCompleted: {
                auto* msg = event.Get<
                    TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted>();
                LoadedCommitIds.clear();
                for (const auto& blob: msg->Blobs) {
                    LoadedCommitIds.push_back(blob.CommitId);
                }
                Sort(LoadedCommitIds);
                break;
            }
            case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                auto* msg = event.Get<
                    TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted>();
                ++TrimsCompleted;
                if (FAILED(msg->GetStatus())) {
                    ++TrimsFailed;
                }
                break;
            }
            case TEvPartitionPrivate::EvFlushCompleted: {
                ++FlushesCompleted;
                break;
            }
        }
    }
};

// SetObserverFunc is deprecated. New tests use CountFreshLogTrimsFilter.
TTestActorRuntime::TEventObserver CountFreshLogTrims(
    TFreshLogTrimCounters& counters)
{
    return [&counters](TAutoPtr<IEventHandle>& event)
    {
        counters.OnEvent(*event);
        return TTestActorRuntime::DefaultObserverFunc(event);
    };
}

TTestActorRuntime::TEventFilter CountFreshLogTrimsFilter(
    TFreshLogTrimCounters& counters)
{
    return [&counters](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
    {
        counters.OnEvent(*event);
        return false;
    };
}

void WaitForTrimsCompleted(
    TTestActorRuntime& runtime,
    const TFreshLogTrimCounters& counters,
    ui32 count)
{
    TDispatchOptions options;
    options.CustomFinalCondition = [&]
    {
        return counters.TrimsCompleted >= count;
    };
    runtime.DispatchEvents(options, TDuration::Seconds(5));
    UNIT_ASSERT_GE(counters.TrimsCompleted, count);
}

// Unlike RebootTablet, does not change how scheduled events fire while the
// tablet starts.
void RestartTablet(TPartitionClient& partition)
{
    partition.KillTablet();
    partition.ReconnectPipe();
    partition.WaitReady();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionTrimFreshLogTest)
{
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

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg =
                            event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            trimSeen = true;
                        }
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                        auto* msg = event->Get<TEvPartitionCommonPrivate::
                                                   TEvTrimFreshLogCompleted>();
                        UNIT_ASSERT(SUCCEEDED(msg->GetStatus()));
                        trimCompletedSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // With a zero dead blob trim threshold, killing the oldest blob
        // doesn't trim; only a flush does.
        partition.WriteBlocks(1, 1);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(!trimSeen);

        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(true, trimSeen);
        UNIT_ASSERT_VALUES_EQUAL(true, trimCompletedSeen);
    }

    Y_UNIT_TEST(ShouldNotReloadFlushedFreshBlobsAfterRestartBeforeTrim)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetWaitForFreshWritesBeforeFlushEnabled(true);
        config.SetMaxCompactionRangesLoadingPerTx(1);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        TAutoPtr<IEventHandle> trimFreshLogRequest;
        bool interceptTrimFreshLogRequest = true;
        bool trimFreshLogCompleted = false;
        THashMap<ui32, ui32> flushCountByBlock;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                    if (msg->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                        for (const auto& blob: msg->FreshBlobs) {
                            for (const auto& block: blob.Blocks) {
                                ++flushCountByBlock[block.BlockIndex];
                            }
                        }
                    }
                }

                if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvTrimFreshLogRequest &&
                    interceptTrimFreshLogRequest)
                {
                    trimFreshLogRequest = event.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }

                trimFreshLogCompleted |=
                    event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvTrimFreshLogCompleted;

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Flush();

        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return !!trimFreshLogRequest;
        };
        runtime->DispatchEvents(options);

        UNIT_ASSERT(trimFreshLogRequest);
        UNIT_ASSERT(!trimFreshLogCompleted);
        interceptTrimFreshLogRequest = false;

        partition.RebootTablet();
        partition.WaitReady();

        auto flushResponse = partition.Flush();
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, flushResponse->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(3, flushCountByBlock.size());
        UNIT_ASSERT_VALUES_EQUAL(1, flushCountByBlock[1]);
        UNIT_ASSERT_VALUES_EQUAL(1, flushCountByBlock[2]);
        UNIT_ASSERT_VALUES_EQUAL(1, flushCountByBlock[3]);

        // wait for compaction map loading to complete
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Compaction(0);
        partition.Cleanup();

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
    }

    Y_UNIT_TEST(ShouldAutomaticallyTrimFreshBlobsFromPreviousGeneration)
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

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg =
                            event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            trimSeen = true;
                        }
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                        trimCompletedSeen = true;
                        auto* msg = event->Get<TEvPartitionCommonPrivate::
                                                   TEvTrimFreshLogCompleted>();
                        UNIT_ASSERT(SUCCEEDED(msg->GetStatus()));

                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.KillTablet();

        {
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(true, trimSeen);
            UNIT_ASSERT_VALUES_EQUAL(true, trimCompletedSeen);
        }
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

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvTrimFreshLogRequest: {
                        UNIT_ASSERT(!trim);
                        trim = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg =
                            event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            collectGen = msg->CollectGeneration;
                            collectStep = msg->CollectStep;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

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

    Y_UNIT_TEST(ShouldTrimFreshLogWithHardBarrierOnlyWhenBarrierAdvances)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui32 trimCount = 0;
        ui64 lastBarrierCommitId = 0;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage)
                {
                    auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                    if (msg->Channel == 4) {
                        UNIT_ASSERT(msg->Hard);
                        UNIT_ASSERT(!msg->Keep || msg->Keep->empty());
                        UNIT_ASSERT(!msg->DoNotKeep || msg->DoNotKeep->empty());
                        const ui64 barrierCommitId = MakeCommitId(
                            msg->CollectGeneration,
                            msg->CollectStep);
                        UNIT_ASSERT_GT(barrierCommitId, lastBarrierCommitId);
                        lastBarrierCommitId = barrierCommitId;
                        ++trimCount;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // The live blob keeps the barrier below it.
        partition.WriteBlocks(1, 1);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, partition.TrimFreshLog()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1, trimCount);

        // The barrier cannot move, so nothing is sent.
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            partition.TrimFreshLog()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1, trimCount);

        // The overwrite kills the blob, so the barrier can pass it.
        partition.WriteBlocks(1, 2);
        partition.WriteBlocks(2, 3);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, partition.TrimFreshLog()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(2, trimCount);

        partition.Flush();
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(3, trimCount);

        partition.WriteBlocks(2, 4);

        partition.RebootTablet();
        partition.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(4),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldTrimFreshLogWithHardBarrierBeforeStartupFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(1, 3);

        bool dropFlush = true;
        ui32 trimCount = 0;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvFlushRequest: {
                        if (dropFlush) {
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg =
                            event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4 && dropFlush) {
                            UNIT_ASSERT(msg->Hard);
                            // Loaded fresh blobs keep the barrier in the
                            // previous generation.
                            UNIT_ASSERT_LT(
                                msg->CollectGeneration,
                                msg->RecordGeneration);
                            ++trimCount;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // RebootTablet replaces the observer while the tablet starts.
        partition.KillTablet();
        partition.ReconnectPipe();
        partition.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, partition.TrimFreshLog()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1, trimCount);

        auto check = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(3),
                GetBlockContent(partition.ReadBlocks(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(2),
                GetBlockContent(partition.ReadBlocks(2)));
        };

        check();

        // The restart loads the fresh blobs that the hard barrier kept.
        dropFlush = false;
        partition.KillTablet();
        partition.ReconnectPipe();
        partition.WaitReady();

        check();
    }

    Y_UNIT_TEST(ShouldFlushFreshBlobsTrimmedDuringFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(1, 2);
        partition.WriteBlocks(2, 3);

        bool holdAddBlobs = true;
        TAutoPtr<IEventHandle> addBlobs;
        size_t flushedFreshBlobCount = 0;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        using TRequest =
                            TEvPartitionPrivate::TEvAddBlobsRequest;
                        const auto* msg = event->Get<TRequest>();
                        if (holdAddBlobs &&
                            msg->Mode == EAddBlobMode::ADD_FLUSH_RESULT)
                        {
                            holdAddBlobs = false;
                            addBlobs = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                    case TEvPartitionPrivate::EvFlushCompleted: {
                        using TEvent = TEvPartitionPrivate::TEvFlushCompleted;
                        flushedFreshBlobCount =
                            event->Get<TEvent>()
                                ->FlushedFreshBlobCommitIds.size();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // The flush captures all three blobs, including the dead one.
        partition.SendFlushRequest();
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!addBlobs;
            };
            runtime->DispatchEvents(options);
        }

        // The trim removes the dead blob while the flush runs.
        UNIT_ASSERT_VALUES_EQUAL(S_OK, partition.TrimFreshLog()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            partition.StatPartition()->Record.GetStats().GetFreshBlobsCount());

        runtime->Send(addBlobs.Release());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            partition.RecvFlushResponse()->GetStatus());

        const auto stats = partition.StatPartition()->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(3, flushedFreshBlobCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldTrimFreshLogWhenDeadBlobBytesReachThreshold)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshDeadBlobByteCountTrimThresholdHDD(3 * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetObserverFunc(CountFreshLogTrims(counters));

        // Each overwrite kills one blob of a block and a small header. Two
        // dead blobs stay below the threshold of three blocks.
        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(1, 2);
        partition.WriteBlocks(1, 3);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsCompleted);

        partition.WriteBlocks(1, 4);
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.FlushesCompleted);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            partition.StatPartition()->Record.GetStats().GetFreshBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(4),
            GetBlockContent(partition.ReadBlocks(1)));

        // A flush requests an ordinary trim too.
        partition.Flush();
        WaitForTrimsCompleted(*runtime, counters, 2);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(2, counters.TrimsCompleted);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            partition.StatPartition()->Record.GetStats().GetFreshBlobsCount());
    }

    Y_UNIT_TEST(ShouldTrimFreshLogWithoutFlushWhenOverwritesKillBlobs)
    {
        constexpr ui32 RegionSize = 4;
        constexpr ui32 WriteCount = 200;

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshBlobCountFlushThresholdHDD(20);
        config.SetFreshByteCountHardLimitHDD(64_KB);
        config.SetFreshDeadBlobByteCountTrimThresholdHDD(3 * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetObserverFunc(CountFreshLogTrims(counters));

        auto fill = [](ui32 i)
        {
            return static_cast<char>('a' + i % 26);
        };

        for (ui32 i = 0; i < WriteCount; ++i) {
            partition.WriteBlocks(i % RegionSize, fill(i));
        }
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));

        UNIT_ASSERT_VALUES_EQUAL(0, counters.FlushesCompleted);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsFailed);
        UNIT_ASSERT_GT(counters.TrimsCompleted, 10);
        UNIT_ASSERT_LT(
            partition.StatPartition()->Record.GetStats().GetFreshBlobsCount(),
            10);

        auto check = [&]
        {
            for (ui32 i = WriteCount - RegionSize; i < WriteCount; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    GetBlockContent(fill(i)),
                    GetBlockContent(partition.ReadBlocks(i % RegionSize)));
            }
        };

        check();

        partition.RebootTablet();
        partition.WaitReady();

        check();
    }

    Y_UNIT_TEST(ShouldTrimMultiBlockFreshBlobOnlyAfterAllBlocksAreOverwritten)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshChannelZeroRequestsEnabled(true);
        config.SetFreshDeadBlobByteCountTrimThresholdHDD(2 * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetObserverFunc(CountFreshLogTrims(counters));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 2), 1);
        partition.WriteBlocks(0, 2);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsCompleted);

        // A zero request kills it, and both its blocks reach the threshold.
        partition.ZeroBlocks(1);
        WaitForTrimsCompleted(*runtime, counters, 1);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldNotTrimFreshLogWhileLiveBlobHoldsOrdinaryTrimToCommitId)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshDeadBlobByteCountTrimThresholdHDD(3 * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetObserverFunc(CountFreshLogTrims(counters));

        // The live blob holds OrdinaryTrimToCommitId.
        partition.WriteBlocks(0, 1);

        // Dead blobs above it can't be trimmed.
        for (ui32 i = 0; i < 7; ++i) {
            partition.WriteBlocks(1, 2);
        }
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsCompleted);

        // Killing the live blob lets one trim collect every dead blob.
        partition.WriteBlocks(0, 3);
        WaitForTrimsCompleted(*runtime, counters, 1);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(1, counters.TrimsCompleted);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            partition.StatPartition()->Record.GetStats().GetFreshBlobsCount());
    }

    Y_UNIT_TEST(ShouldRetryFailedOrdinaryTrim)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetFreshDeadBlobByteCountTrimThresholdHDD(3 * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        auto countTrims = CountFreshLogTrims(counters);
        bool failTrim = true;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvBlobStorage::EvCollectGarbageResult)
                {
                    auto* msg =
                        event->Get<TEvBlobStorage::TEvCollectGarbageResult>();
                    if (msg->Channel == 4 && failTrim) {
                        failTrim = false;
                        msg->Status = NKikimrProto::ERROR;
                    }
                }
                return countTrims(event);
            });

        // The flush kills the only blob. It is smaller than the threshold, so
        // only the flush's request starts the trim and only the failure's
        // request restarts it.
        partition.WriteBlocks(1, 1);
        partition.Flush();
        WaitForTrimsCompleted(*runtime, counters, 2);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.TrimsFailed);
    }

    Y_UNIT_TEST(ShouldUseDeadBlobTrimThresholdOfPartitionMediaKind)
    {
        for (const auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_SSD,
              NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_HYBRID})
        {
            const bool isSSD = mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD;
            for (const bool ownMediaKind: {false, true}) {
                auto config = DefaultConfig();
                config.SetFreshChannelWriteRequestsEnabled(true);
                if (isSSD == ownMediaKind) {
                    config.SetFreshDeadBlobByteCountTrimThresholdSSD(
                        3 * DefaultBlockSize);
                } else {
                    config.SetFreshDeadBlobByteCountTrimThresholdHDD(
                        3 * DefaultBlockSize);
                }

                TTestPartitionInfo partitionInfo;
                partitionInfo.MediaKind = mediaKind;
                auto runtime =
                    PrepareTestActorRuntime(config, 1024, {}, partitionInfo);

                TPartitionClient partition(*runtime);
                partition.WaitReady();

                TFreshLogTrimCounters counters;
                runtime->SetObserverFunc(CountFreshLogTrims(counters));

                for (ui32 i = 0; i < 4; ++i) {
                    partition.WriteBlocks(1, 1);
                }
                runtime->DispatchEvents({}, TDuration::MilliSeconds(100));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    ownMediaKind ? 1 : 0,
                    counters.TrimsCompleted,
                    "media kind: " << static_cast<int>(mediaKind)
                                   << ", own: " << ownMediaKind);
            }
        }
    }

    Y_UNIT_TEST(ShouldTrimFreshLogSelectivelyBehindLiveBlob)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetEventFilter(CountFreshLogTrimsFilter(counters));
        const auto& commits = counters.FreshBlobCommitIds;

        // The two dead blobs below the pin do not reach the threshold, so no
        // ordinary trim is due. The three dead blobs above it start a
        // selective trim, which moves the soft barrier onto the newest dead
        // blob and keeps the pin.
        //
        // blob:     0   1   2   3   4   5   6
        // block:    1   1 100   1   1   1   1
        // state:    D   D   L   D   D   D   L
        partition.WriteBlocks(1, 'a');
        partition.WriteBlocks(1, 'b');
        partition.WriteBlocks(100, 'p');
        for (char c = 'c'; c <= 'f'; ++c) {
            partition.WriteBlocks(1, c);
        }

        WaitForTrimsCompleted(*runtime, counters, 1);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(7, commits.size());
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(commits[5], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({commits[2]}), counters.Keep);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(), counters.DoNotKeep);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.FlushesCompleted);
        // The soft barrier collected the blobs below the pin too, so they
        // leave the accounting without waiting for an ordinary trim.
        UNIT_ASSERT_VALUES_EQUAL(2, GetFreshBlobsCount(partition));

        auto check = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('p'),
                GetBlockContent(partition.ReadBlocks(100)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('f'),
                GetBlockContent(partition.ReadBlocks(1)));
        };

        check();

        // The restart loads only the kept blob and the live one.
        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[2], commits[6]}),
            counters.LoadedCommitIds);

        check();
    }

    Y_UNIT_TEST(ShouldSendSelectiveTrimOnlyToCurrentGroupOfFreshChannel)
    {
        const ui32 channelCount = DataChannelOffset + 2;
        const ui32 freshChannelNo = channelCount - 1;
        // One group for the other channels and three for the fresh channel.
        TTestEnv env(0, 1, channelCount, 4);
        auto& runtime = env.GetRuntime();

        const auto config = SelectiveTrimConfig();
        env.CreateSubDomain("nbs");
        env.CreateBlockStoreNode(
            "nbs",
            CreateTestStorageConfig(config),
            CreateTestDiagnosticsConfig());

        const auto tabletId = NKikimr::MakeTabletID(1, HiveId, 1);
        auto tabletInfo = std::make_unique<TTabletStorageInfo>();
        tabletInfo->TabletID = tabletId;
        tabletInfo->TabletType = TTabletTypes::BlockStorePartition;
        auto& channels = tabletInfo->Channels;
        channels.resize(channelCount);
        for (ui32 channel = 0; channel < channelCount; ++channel) {
            channels[channel].Channel = channel;
            channels[channel].Type = TBlobStorageGroupType(BootGroupErasure);
            channels[channel].History.resize(1);
            channels[channel].History[0].FromGeneration = 0;
            channels[channel].History[0].GroupID = env.GetGroupIds()[0];
        }

        // The fresh channel moved once before the tablet's generation, and its
        // next move is already recorded for a future generation.
        const ui32 oldGroupId = env.GetGroupIds()[1];
        const ui32 currentGroupId = env.GetGroupIds()[2];
        const ui32 futureGroupId = env.GetGroupIds()[3];
        auto& history = channels[freshChannelNo].History;
        history.resize(3);
        history[0].FromGeneration = 0;
        history[0].GroupID = oldGroupId;
        history[1].FromGeneration = 1;
        history[1].GroupID = currentGroupId;
        history[2].FromGeneration = 1000;
        history[2].GroupID = futureGroupId;

        InitTestActorRuntime(
            runtime,
            config,
            1024,
            channelCount,
            std::move(tabletInfo));

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        TVector<ui32> softCommandGroupIds;
        TVector<ui32> hardCommandGroupIds;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);
                if (event->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage)
                {
                    auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                    if (msg->Channel == freshChannelNo) {
                        auto& groupIds = msg->Hard ? hardCommandGroupIds
                                                   : softCommandGroupIds;
                        groupIds.push_back(
                            GroupIDFromBlobStorageProxyID(event->Recipient));
                    }
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        // The writes of ShouldTrimFreshLogSelectivelyBehindLiveBlob start a
        // selective trim. All its blobs are in the current generation, so its
        // soft barrier goes only to the current group.
        //
        // blob:     0   1   2   3   4   5   6
        // block:    1   1 100   1   1   1   1
        // state:    D   D   L   D   D   D   L
        partition.WriteBlocks(1, 'a');
        partition.WriteBlocks(1, 'b');
        partition.WriteBlocks(100, 'p');
        for (char c = 'c'; c <= 'f'; ++c) {
            partition.WriteBlocks(1, c);
        }

        WaitForTrimsCompleted(runtime, counters, 1);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(7, commits.size());
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui32>({currentGroupId}),
            softCommandGroupIds);
        UNIT_ASSERT_VALUES_EQUAL(commits[5], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({commits[2]}), counters.Keep);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsFailed);
        UNIT_ASSERT_VALUES_EQUAL(2, GetFreshBlobsCount(partition));

        // Three dead blobs below OrdinaryTrimToCommitId start an ordinary trim.
        // Its hard barrier still goes to every group in the history.
        //
        // blob:     2   6   7   8   9
        // block:  100   1 100   1 100
        // state:    D   D   D   L   L
        partition.WriteBlocks(100, 'P');
        partition.WriteBlocks(1, 'g');
        partition.WriteBlocks(100, 'Q');

        WaitForTrimsCompleted(runtime, counters, 2);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(10, commits.size());
        UNIT_ASSERT_VALUES_EQUAL("SHHH", counters.CommandKinds);
        TVector<ui32> allGroupIds{oldGroupId, currentGroupId, futureGroupId};
        Sort(allGroupIds);
        Sort(hardCommandGroupIds);
        UNIT_ASSERT_VALUES_EQUAL(allGroupIds, hardCommandGroupIds);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsFailed);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('Q'),
            GetBlockContent(partition.ReadBlocks(100)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('g'),
            GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldShowSelectiveTrimOnMonitoringPage)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetEventFilter(CountFreshLogTrimsFilter(counters));

        // The selective trim keeps the pin and collects the dead blobs above
        // it.
        partition.WriteBlocks(100, 'p');
        for (char c = 'a'; c <= 'd'; ++c) {
            partition.WriteBlocks(1, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 1);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);

        const TString html =
            partition.RemoteHttpInfo(BuildRemoteHttpQuery(TestTabletId, {}))
                ->Html;
        UNIT_ASSERT_C(html.Contains("Fresh Log"), html);
        // The pin and the last write are live, and the pin is kept.
        UNIT_ASSERT_C(html.Contains("Live</span> 2 blobs"), html);
        UNIT_ASSERT_C(html.Contains("1 kept since start"), html);
        UNIT_ASSERT_C(!html.Contains("not acknowledged"), html);
        UNIT_ASSERT_C(!html.Contains("failed)"), html);

        // Each kind shows the time since its own last trim. Scheduled events
        // may move the simulated clock further, so the seconds are not checked.
        runtime->AdvanceCurrentTime(TDuration::Minutes(1));
        const TString later =
            partition.RemoteHttpInfo(BuildRemoteHttpQuery(TestTabletId, {}))
                ->Html;
        UNIT_ASSERT_C(
            later.Contains("Ordinary</span></td><td>Idle</td>"),
            later);
        UNIT_ASSERT_C(
            later.Contains("Selective</span></td><td>Idle for 1m "),
            later);
        // The State table reads the same clock.
        UNIT_ASSERT_C(
            later.Contains("TrimFreshLog</td><td>Idle for 1m "),
            later);
    }

    Y_UNIT_TEST(ShouldNotFlushFreshLogPinnedByLiveBlob)
    {
        constexpr ui32 RegionSize = 4;
        constexpr ui32 WriteCount = 200;

        auto config = SelectiveTrimConfig();
        config.SetFreshBlobCountFlushThresholdHDD(20);
        config.SetFreshByteCountHardLimitHDD(64_KB);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetEventFilter(CountFreshLogTrimsFilter(counters));
        const auto& commits = counters.FreshBlobCommitIds;

        auto fill = [](ui32 i)
        {
            return static_cast<char>('a' + i % 26);
        };

        partition.WriteBlocks(100, 'p');
        for (ui32 i = 0; i < WriteCount; ++i) {
            partition.WriteBlocks(i % RegionSize, fill(i));
        }
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));

        // The pin never dies, so no dead blob is below OrdinaryTrimToCommitId,
        // and no ordinary trim starts.
        UNIT_ASSERT_VALUES_EQUAL(0, counters.FlushesCompleted);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsFailed);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.HardCollectRequestsSent);
        UNIT_ASSERT_GT(counters.SoftCollectRequestsSent, 10);
        UNIT_ASSERT_LT(GetFreshBlobsCount(partition), 20);

        auto check = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('p'),
                GetBlockContent(partition.ReadBlocks(100)));
            for (ui32 i = WriteCount - RegionSize; i < WriteCount; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    GetBlockContent(fill(i)),
                    GetBlockContent(partition.ReadBlocks(i % RegionSize)));
            }
        };

        check();

        RestartTablet(partition);
        UNIT_ASSERT(IsIn(counters.LoadedCommitIds, commits[0]));
        UNIT_ASSERT_LT(counters.LoadedCommitIds.size(), 20);

        check();
    }

    Y_UNIT_TEST(ShouldNotPassInFlightFreshWriteWithSoftBarrier)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        bool holdPut = false;
        TAutoPtr<IEventHandle> heldPut;
        ui64 heldCommitId = 0;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                if (holdPut && event->GetTypeRewrite() == TEvBlobStorage::EvPut)
                {
                    auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                    if (msg->Id.Channel() == 4) {
                        holdPut = false;
                        heldCommitId =
                            MakeCommitId(msg->Id.Generation(), msg->Id.Step());
                        heldPut = event.Release();
                        return true;
                    }
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        partition.WriteBlocks(1, 'a');

        holdPut = true;
        partition.SendWriteBlocksRequest(50, 'x');
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!heldPut;
            };
            runtime->DispatchEvents(options);
        }

        // The dead blobs above OrdinaryTrimToCommitId reach the threshold, but
        // they are above SelectiveTrimToCommitId too. The held write keeps both
        // of them below itself, so no soft barrier fits, and the partition does
        // not scan. Only the first blob is below OrdinaryTrimToCommitId, so no
        // ordinary trim is due.
        for (char c = 'b'; c <= 'e'; ++c) {
            partition.WriteBlocks(1, c);
        }
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("", counters.CommandKinds);

        runtime->Send(heldPut.Release());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            partition.RecvWriteBlocksResponse()->GetStatus());

        // Once the write is added, the next write starts a soft barrier that
        // passes it with a Keep flag. No scan ran while the write was held, so
        // none deferred this one.
        partition.WriteBlocks(1, 'f');
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(7, commits.size());
        UNIT_ASSERT_VALUES_EQUAL(heldCommitId, commits[5]);
        UNIT_ASSERT_VALUES_EQUAL(commits[4], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({heldCommitId}), counters.Keep);

        auto check = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('x'),
                GetBlockContent(partition.ReadBlocks(50)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('f'),
                GetBlockContent(partition.ReadBlocks(1)));
        };

        check();

        // The soft barrier also collected the first blob, which was below
        // OrdinaryTrimToCommitId.
        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({heldCommitId, commits[6]}),
            counters.LoadedCommitIds);

        check();
    }

    Y_UNIT_TEST(ShouldNotScanForSelectiveTrimUntilOverwrittenWriteCompletes)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        ui32 trimRequests = 0;
        bool holdCompletion = true;
        TAutoPtr<IEventHandle> heldCompletion;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvTrimFreshLogRequest: {
                        ++trimRequests;
                        break;
                    }
                    case TEvPartitionCommonPrivate::
                        EvWriteFreshBlocksCompleted: {
                        if (holdCompletion) {
                            holdCompletion = false;
                            heldCompletion = event.Release();
                            return true;
                        }
                        break;
                    }
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        // The partition adds the first write and replies to it, but its
        // completion is held. The second write overwrites its block, so
        // OrdinaryTrimToCommitId passes the first write, while
        // SelectiveTrimToCommitId stays below it. The dead blobs above
        // OrdinaryTrimToCommitId reach the threshold, but no soft barrier fits,
        // so the partition does not scan.
        //
        // blob:     0   1   2   3   4   5
        // block:    1   1   2   2   2   2
        // state:    D   L   D   D   D   L
        partition.WriteBlocks(1, 'a');
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!heldCompletion;
            };
            runtime->DispatchEvents(options);
        }
        partition.WriteBlocks(1, 'b');
        for (char c = 'c'; c <= 'f'; ++c) {
            partition.WriteBlocks(2, c);
        }
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(6, commits.size());
        UNIT_ASSERT_VALUES_EQUAL(0, trimRequests);
        UNIT_ASSERT_VALUES_EQUAL("", counters.CommandKinds);

        // No scan deferred the next one, so once the first write completes,
        // the next write starts a soft barrier on the newest dead blob.
        runtime->Send(heldCompletion.Release());
        partition.WriteBlocks(3, 'g');
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(7, commits.size());
        UNIT_ASSERT_VALUES_EQUAL(commits[4], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({commits[1]}), counters.Keep);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(), counters.DoNotKeep);
        // The soft barrier also collected the first blob, which was below
        // OrdinaryTrimToCommitId.
        UNIT_ASSERT_VALUES_EQUAL(3, GetFreshBlobsCount(partition));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('b'),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('f'),
            GetBlockContent(partition.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('g'),
            GetBlockContent(partition.ReadBlocks(3)));
    }

    Y_UNIT_TEST(ShouldNotSendSoftBarrierOnEveryDeathBehindInFlightFreshWrite)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        bool holdPut = false;
        TAutoPtr<IEventHandle> heldPut;
        bool holdCommands = false;
        TVector<TAutoPtr<IEventHandle>> heldCommands;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                if (holdPut && event->GetTypeRewrite() == TEvBlobStorage::EvPut)
                {
                    auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                    if (msg->Id.Channel() == 4) {
                        holdPut = false;
                        heldPut = event.Release();
                        return true;
                    }
                }
                if (holdCommands &&
                    event->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage)
                {
                    auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                    if (msg->Channel == 4) {
                        heldCommands.push_back(event.Release());
                        return true;
                    }
                }
                return false;
            });

        // The pin keeps OrdinaryTrimToCommitId below the blobs that die.
        partition.WriteBlocks(100, 'p');
        for (ui32 blockIndex = 10; blockIndex < 19; ++blockIndex) {
            partition.WriteBlocks(blockIndex, 'a');
        }

        holdPut = true;
        partition.SendWriteBlocksRequest(50, 'x');
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!heldPut;
            };
            runtime->DispatchEvents(options);
        }

        // These dead blobs are above SelectiveTrimToCommitId, so no trim can
        // collect them while the write is held, and they keep the selective
        // trim due.
        for (char c = 'b'; c <= 'e'; ++c) {
            partition.WriteBlocks(1, c);
        }
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("", counters.CommandKinds);

        // Every overwrite below kills a blob that a soft barrier can collect,
        // while the previous soft command is in flight. Still, a soft barrier
        // is sent only once per threshold of dead bytes.
        holdCommands = true;
        for (ui32 blockIndex = 10; blockIndex < 19; ++blockIndex) {
            auto commandsInFlight = std::exchange(heldCommands, {});
            partition.WriteBlocks(blockIndex, 'A');
            for (auto& command: commandsInFlight) {
                runtime->Send(command.Release());
            }
            runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL("SSS", counters.CommandKinds);

        holdCommands = false;
        for (auto& command: heldCommands) {
            runtime->Send(command.Release());
        }
        heldCommands.clear();

        runtime->Send(heldPut.Release());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            partition.RecvWriteBlocksResponse()->GetStatus());

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('x'),
            GetBlockContent(partition.ReadBlocks(50)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('A'),
            GetBlockContent(partition.ReadBlocks(18)));
    }

    Y_UNIT_TEST(ShouldServeSelectiveTrimRequestAfterFlush)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        bool holdAddBlobs = false;
        TAutoPtr<IEventHandle> addBlobs;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                if (holdAddBlobs &&
                    event->GetTypeRewrite() ==
                        TEvPartitionPrivate::EvAddBlobsRequest &&
                    event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>()
                            ->Mode == EAddBlobMode::ADD_FLUSH_RESULT)
                {
                    holdAddBlobs = false;
                    addBlobs = event.Release();
                    return true;
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        partition.WriteBlocks(5, 'f');

        holdAddBlobs = true;
        partition.SendFlushRequest();
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!addBlobs;
            };
            runtime->DispatchEvents(options);
        }

        // Written after the flush started, so the flush does not release it.
        partition.WriteBlocks(100, 'p');
        for (char c = 'a'; c <= 'd'; ++c) {
            partition.WriteBlocks(1, c);
        }

        // While the flush runs, nothing starts: no dead blob is below
        // OrdinaryTrimToCommitId, and the selective trim waits for the flush.
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("", counters.CommandKinds);

        // The flush requests an ordinary trim, which runs first. The dead blobs
        // above OrdinaryTrimToCommitId stay counted, so the selective trim
        // starts after it.
        runtime->Send(addBlobs.Release());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            partition.RecvFlushResponse()->GetStatus());
        WaitForTrimsCompleted(*runtime, counters, 2);
        UNIT_ASSERT_VALUES_EQUAL("HS", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(6, commits.size());
        UNIT_ASSERT_VALUES_EQUAL(commits[4], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({commits[1]}), counters.Keep);

        // A flush requests only an ordinary trim.
        partition.Flush();
        WaitForTrimsCompleted(*runtime, counters, 3);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("HSH", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(0, GetFreshBlobsCount(partition));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('p'),
            GetBlockContent(partition.ReadBlocks(100)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('d'),
            GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldReleaseKeptFreshBlobsThatDied)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetEventFilter(CountFreshLogTrimsFilter(counters));
        const auto& commits = counters.FreshBlobCommitIds;

        partition.WriteBlocks(100, 'p');
        partition.WriteBlocks(2, 'k');
        for (char c = 'a'; c <= 'd'; ++c) {
            partition.WriteBlocks(1, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(commits[4], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[0], commits[1]}),
            counters.Keep);

        // A kept blob dies above OrdinaryTrimToCommitId: the next selective
        // trim releases it.
        partition.WriteBlocks(2, 'K');
        partition.WriteBlocks(1, 'e');
        partition.WriteBlocks(1, 'f');
        WaitForTrimsCompleted(*runtime, counters, 2);
        UNIT_ASSERT_VALUES_EQUAL("SS", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(commits[7], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({commits[6]}), counters.Keep);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[1]}),
            counters.DoNotKeep);

        // The pin and the other kept blob die, and OrdinaryTrimToCommitId moves
        // past them: the hard barrier of the ordinary trim deletes them without
        // DoNotKeep flags.
        partition.WriteBlocks(100, 'P');
        partition.WriteBlocks(2, 'L');
        partition.WriteBlocks(1, 'g');
        WaitForTrimsCompleted(*runtime, counters, 3);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("SSH", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(12, commits.size());

        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[9], commits[10], commits[11]}),
            counters.LoadedCommitIds);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('P'),
            GetBlockContent(partition.ReadBlocks(100)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('L'),
            GetBlockContent(partition.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('g'),
            GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldFlushKeptFreshBlobThatDiedBeforeRestart)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        // Commit ids of the fresh blocks that flushes write, by block index.
        THashMap<ui32, TVector<ui64>> flushedCommitIds;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    auto* msg =
                        event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                    if (msg->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                        for (const auto& blob: msg->FreshBlobs) {
                            for (const auto& block: blob.Blocks) {
                                flushedCommitIds[block.BlockIndex].push_back(
                                    block.CommitId);
                            }
                        }
                    }
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        partition.WriteBlocks(100, 'p');
        partition.WriteBlocks(2, 'k');
        for (char c = 'a'; c <= 'd'; ++c) {
            partition.WriteBlocks(1, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(commits[4], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[0], commits[1]}),
            counters.Keep);

        // A kept blob dies. Too few bytes die for the next selective trim, so
        // its DoNotKeep is not sent.
        partition.WriteBlocks(2, 'K');
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.FlushesCompleted);

        auto check = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('p'),
                GetBlockContent(partition.ReadBlocks(100)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('K'),
                GetBlockContent(partition.ReadBlocks(2)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('d'),
                GetBlockContent(partition.ReadBlocks(1)));
        };

        // The restart loads the dead kept blob, but not the blobs that the soft
        // barrier collected next to it. The overwritten version starts live,
        // and the startup flush writes it with its own commit id. Reads take
        // the highest commit id, so they return the overwrite.
        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[0], commits[1], commits[5], commits[6]}),
            counters.LoadedCommitIds);

        check();

        // The startup flush moves OrdinaryTrimToCommitId into the current
        // generation, and the hard barrier of the ordinary trim deletes the
        // dead kept blob without a DoNotKeep flag.
        WaitForTrimsCompleted(*runtime, counters, 2);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("SH", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.FlushesCompleted);
        auto& flushedBlock2 = flushedCommitIds[2];
        Sort(flushedBlock2);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[1], commits[6]}),
            flushedBlock2);

        check();

        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(), counters.LoadedCommitIds);

        check();
    }

    Y_UNIT_TEST(ShouldResendKeepFlagsAfterFailedSoftCommand)
    {
        for (const bool restart: {false, true}) {
            auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

            TPartitionClient partition(*runtime);
            partition.WaitReady();

            TFreshLogTrimCounters counters;
            bool failSoftCommand = true;
            bool failNextResult = false;
            runtime->SetEventFilter(
                [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
                {
                    counters.OnEvent(*event);

                    switch (event->GetTypeRewrite()) {
                        case TEvBlobStorage::EvCollectGarbage: {
                            auto* msg =
                                event->Get<TEvBlobStorage::TEvCollectGarbage>();
                            if (msg->Channel == 4 && !msg->Hard &&
                                failSoftCommand) {
                                failSoftCommand = false;
                                failNextResult = true;
                            }
                            break;
                        }
                        case TEvBlobStorage::EvCollectGarbageResult: {
                            auto* msg = event->Get<
                                TEvBlobStorage::TEvCollectGarbageResult>();
                            // The fake BlobStorage has applied the command.
                            if (msg->Channel == 4 && failNextResult) {
                                failNextResult = false;
                                msg->Status = NKikimrProto::ERROR;
                            }
                            break;
                        }
                    }
                    return false;
                });
            const auto& commits = counters.FreshBlobCommitIds;

            partition.WriteBlocks(100, 'p');
            for (char c = 'a'; c <= 'd'; ++c) {
                partition.WriteBlocks(1, c);
            }
            WaitForTrimsCompleted(*runtime, counters, 1);
            UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.TrimsFailed);
            UNIT_ASSERT_VALUES_EQUAL(
                commits[3],
                counters.SelectiveTrimToCommitId);

            if (restart) {
                RestartTablet(partition);
                UNIT_ASSERT_VALUES_EQUAL(
                    TVector<ui64>({commits[0], commits[4]}),
                    counters.LoadedCommitIds);
            } else {
                // The dead blobs stay counted, so the trim is retried without
                // more writes. The retry waits for the backoff. Its barrier is
                // not lower, it resends Keep for the live blob, and it releases
                // the dead ones.
                WaitForTrimsCompleted(*runtime, counters, 2);
                UNIT_ASSERT_VALUES_EQUAL("SS", counters.CommandKinds);
                UNIT_ASSERT_VALUES_EQUAL(1, counters.TrimsFailed);
                UNIT_ASSERT_VALUES_EQUAL(
                    commits[3],
                    counters.SelectiveTrimToCommitId);
                UNIT_ASSERT_VALUES_EQUAL(
                    TVector<ui64>({commits[0]}),
                    counters.Keep);
                UNIT_ASSERT_VALUES_EQUAL(
                    TVector<ui64>({commits[1], commits[2], commits[3]}),
                    counters.DoNotKeep);
                UNIT_ASSERT_VALUES_EQUAL(2, GetFreshBlobsCount(partition));
            }

            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('p'),
                GetBlockContent(partition.ReadBlocks(100)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('d'),
                GetBlockContent(partition.ReadBlocks(1)));
        }
    }

    Y_UNIT_TEST(ShouldKeepMoreLiveBlobsThanOneCollectGarbageMessageHolds)
    {
        constexpr ui32 FreshChannelId = 4;
        constexpr ui32 LiveBlobCount =
            static_cast<ui32>(MaxCollectGarbageFlagsPerMessage) + 1;
        constexpr ui32 HotBlockIndex = LiveBlobCount;

        auto config = SelectiveTrimConfig();
        config.SetWriteRequestBatchingEnabled(false);
        // A flush would let an ordinary trim delete every blob with a hard
        // barrier, which needs no flags.
        config.SetFlushThresholdHDD(128_MB);
        config.SetFreshBlobCountFlushThresholdHDD(64 * 1024);
        config.SetFreshBlobByteCountFlushThresholdHDD(256_MB);

        auto runtime = PrepareTestActorRuntime(config, LiveBlobCount + 1);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        ui64 maxKeepFlagCount = 0;
        // The DS proxy sends the flags that do not fit into the message with
        // the barrier in commands without a barrier.
        ui32 flagOnlyCommandCount = 0;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);
                if (event->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage)
                {
                    auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                    if (msg->Channel == FreshChannelId && !msg->Hard) {
                        maxKeepFlagCount = Max<ui64>(
                            maxKeepFlagCount,
                            msg->Keep ? msg->Keep->size() : 0);
                        if (!msg->Collect) {
                            ++flagOnlyCommandCount;
                        }
                    }
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        // A live single-block blob per block. Writes run in parallel only to
        // make the test faster.
        constexpr ui32 InFlightWriteCount = 128;
        for (ui32 start = 0; start < LiveBlobCount; start += InFlightWriteCount)
        {
            const ui32 end = Min(start + InFlightWriteCount, LiveBlobCount);
            for (ui32 i = start; i < end; ++i) {
                partition.SendWriteBlocksRequest(i, 'a');
            }
            for (ui32 i = start; i < end; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    S_OK,
                    partition.RecvWriteBlocksResponse()->GetStatus());
            }
        }

        // Overwrites of the hot block kill three blobs. The soft barrier on
        // the newest of them needs a Keep flag for each blob written before.
        for (char c = 'b'; c <= 'e'; ++c) {
            partition.WriteBlocks(HotBlockIndex, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsFailed);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.FlushesCompleted);
        UNIT_ASSERT_VALUES_EQUAL(LiveBlobCount, maxKeepFlagCount);
        UNIT_ASSERT_GT(flagOnlyCommandCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            commits[LiveBlobCount + 2],
            counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            LiveBlobCount + 1,
            GetFreshBlobsCount(partition));

        // The kept blobs are below the soft barrier now, so the next soft
        // command needs no flags for them.
        for (char c = 'f'; c <= 'h'; ++c) {
            partition.WriteBlocks(HotBlockIndex, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 2);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.TrimsFailed);
        UNIT_ASSERT_VALUES_EQUAL(
            commits[LiveBlobCount + 5],
            counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(), counters.Keep);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(), counters.DoNotKeep);
        UNIT_ASSERT_VALUES_EQUAL(
            LiveBlobCount + 1,
            GetFreshBlobsCount(partition));

        // Only the Keep flags saved the blobs below the soft barriers.
        TVector<ui64> liveCommits(
            commits.begin(),
            commits.begin() + LiveBlobCount);
        liveCommits.push_back(commits.back());
        Sort(liveCommits);
        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(liveCommits, counters.LoadedCommitIds);

        constexpr ui32 ReadBlockCount = 1024;
        for (ui32 start = 0; start < LiveBlobCount; start += ReadBlockCount) {
            const auto range = TBlockRange32::WithLength(
                start,
                Min(ReadBlockCount, LiveBlobCount - start));
            UNIT_ASSERT_C(
                GetBlocksContent('a', range.Size()) ==
                    GetBlocksContent(partition.ReadBlocks(range)),
                "start block: " << start);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('h'),
            GetBlockContent(partition.ReadBlocks(HotBlockIndex)));
    }

    Y_UNIT_TEST(
        ShouldNotSendSoftBarrierWhileOrdinaryTrimToCommitIdIsInPreviousGeneration)
    {
        auto config = SelectiveTrimConfig();
        config.SetMaxWriteBlobErrorsBeforeSuicide(10);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        bool failFlushPut = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                if (failFlushPut &&
                    event->GetTypeRewrite() == TEvBlobStorage::EvPutResult)
                {
                    auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                    // The flush writes to the merged channel.
                    if (msg->Id.Channel() == 3) {
                        failFlushPut = false;
                        msg->Status = NKikimrProto::ERROR;
                    }
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        // The selective trim keeps blobs below its soft barrier.
        partition.WriteBlocks(100, 'p');
        partition.WriteBlocks(2, 'k');
        partition.WriteBlocks(3, 'q');
        partition.WriteBlocks(4, 'r');
        for (char c = 'a'; c <= 'd'; ++c) {
            partition.WriteBlocks(1, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(commits[6], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[0], commits[1], commits[2], commits[3]}),
            counters.Keep);

        // The startup flush fails, so the loaded fresh blobs keep
        // OrdinaryTrimToCommitId in the previous generation, and no flush runs.
        failFlushPut = true;
        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>(
                {commits[0], commits[1], commits[2], commits[3], commits[7]}),
            counters.LoadedCommitIds);
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return counters.FlushesCompleted == 1;
            };
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT(!failFlushPut);

        // Only kept blobs die, above OrdinaryTrimToCommitId. A soft barrier on
        // them would be lower than the one of the previous lifetime, so no trim
        // runs.
        partition.WriteBlocks(2, 'K');
        partition.WriteBlocks(3, 'Q');
        partition.WriteBlocks(4, 'R');
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);

        auto check = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('p'),
                GetBlockContent(partition.ReadBlocks(100)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('K'),
                GetBlockContent(partition.ReadBlocks(2)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('Q'),
                GetBlockContent(partition.ReadBlocks(3)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('R'),
                GetBlockContent(partition.ReadBlocks(4)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('d'),
                GetBlockContent(partition.ReadBlocks(1)));
        };

        check();

        // After a flush, OrdinaryTrimToCommitId is in the current generation,
        // and the hard barrier of the ordinary trim deletes every blob kept in
        // the previous lifetime.
        partition.Flush();
        WaitForTrimsCompleted(*runtime, counters, 2);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("SH", counters.CommandKinds);

        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(), counters.LoadedCommitIds);

        check();
    }

    Y_UNIT_TEST(ShouldFlushFreshBlobsCollectedBySelectiveTrimDuringFlush)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        bool holdSoftResult = false;
        TAutoPtr<IEventHandle> softResult;
        bool holdAddBlobs = false;
        TAutoPtr<IEventHandle> addBlobs;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg =
                            event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4 && !msg->Hard && !softResult) {
                            holdSoftResult = true;
                        }
                        break;
                    }
                    case TEvBlobStorage::EvCollectGarbageResult: {
                        auto* msg = event->Get<
                            TEvBlobStorage::TEvCollectGarbageResult>();
                        if (msg->Channel == 4 && holdSoftResult) {
                            holdSoftResult = false;
                            softResult = event.Release();
                            return true;
                        }
                        break;
                    }
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (holdAddBlobs &&
                            msg->Mode == EAddBlobMode::ADD_FLUSH_RESULT)
                        {
                            holdAddBlobs = false;
                            addBlobs = event.Release();
                            return true;
                        }
                        break;
                    }
                }
                return false;
            });

        partition.WriteBlocks(100, 'p');
        for (char c = 'a'; c <= 'd'; ++c) {
            partition.WriteBlocks(1, c);
        }

        // The selective trim waits for its result.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!softResult;
            };
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);

        // A flush captures the dead blobs that the selective trim collects.
        holdAddBlobs = true;
        partition.SendFlushRequest();
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!addBlobs;
            };
            runtime->DispatchEvents(options);
        }

        // The selective trim completes first and removes them.
        runtime->Send(softResult.Release());
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL(2, GetFreshBlobsCount(partition));

        // The flush finds them missing and counts nothing for them.
        runtime->Send(addBlobs.Release());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            partition.RecvFlushResponse()->GetStatus());
        WaitForTrimsCompleted(*runtime, counters, 2);
        UNIT_ASSERT_VALUES_EQUAL("SH", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(0, GetFreshBlobsCount(partition));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('p'),
            GetBlockContent(partition.ReadBlocks(100)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('d'),
            GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldServeSelectiveTrimRequestAfterEmptyFlush)
    {
        auto config = SelectiveTrimConfig();
        config.SetWaitForFreshWritesBeforeFlushEnabled(true);
        config.SetMaxWriteBlobErrorsBeforeSuicide(10);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        bool holdPut = false;
        TAutoPtr<IEventHandle> heldPut;
        bool failPut = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                        if (holdPut && msg->Id.Channel() == 4) {
                            holdPut = false;
                            heldPut = event.Release();
                            return true;
                        }
                        break;
                    }
                    case TEvBlobStorage::EvPutResult: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                        if (failPut && msg->Id.Channel() == 4) {
                            failPut = false;
                            msg->Status = NKikimrProto::ERROR;
                        }
                        break;
                    }
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        partition.WriteBlocks(1, 'a');

        holdPut = true;
        partition.SendWriteBlocksRequest(50, 'x');
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!heldPut;
            };
            runtime->DispatchEvents(options);
        }

        // The flush waits for the write in flight.
        partition.SendFlushRequest();
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // Written after the flush started. Only the first blob is below
        // OrdinaryTrimToCommitId, and the selective trim waits for the flush,
        // so nothing starts.
        partition.WriteBlocks(100, 'q');
        for (char c = 'b'; c <= 'e'; ++c) {
            partition.WriteBlocks(1, c);
        }
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("", counters.CommandKinds);

        // The write in flight fails, and newer writes overwrote every version
        // that the flush could see, so it has nothing to flush.
        failPut = true;
        runtime->Send(heldPut.Release());
        UNIT_ASSERT(FAILED(partition.RecvWriteBlocksResponse()->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            partition.RecvFlushResponse()->GetStatus());

        // The empty flush still starts the selective trim it blocked.
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(6, commits.size());
        UNIT_ASSERT_VALUES_EQUAL(commits[4], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({commits[1]}), counters.Keep);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('q'),
            GetBlockContent(partition.ReadBlocks(100)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('e'),
            GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldNotStartSelectiveTrimWhenFlushIsDue)
    {
        auto config = SelectiveTrimConfig();
        config.SetFreshBlobCountFlushThresholdHDD(6);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetEventFilter(CountFreshLogTrimsFilter(counters));

        partition.WriteBlocks(100, 'p');
        for (char c = 'a'; c <= 'd'; ++c) {
            partition.WriteBlocks(1, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.FlushesCompleted);

        // The write that makes the dead blobs above OrdinaryTrimToCommitId
        // reach the threshold also reaches the blob count flush threshold. The
        // flush moves OrdinaryTrimToCommitId past these blobs, so the selective
        // trim does not start before it.
        partition.WriteBlocks(2, 'k');
        for (char c = 'e'; c <= 'g'; ++c) {
            partition.WriteBlocks(1, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 2);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(1, counters.FlushesCompleted);
        UNIT_ASSERT_VALUES_EQUAL("SH", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(0, GetFreshBlobsCount(partition));
    }

    Y_UNIT_TEST(ShouldKeepFreshVersionNeededByCheckpointInFlight)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        bool holdPut = false;
        TAutoPtr<IEventHandle> heldPut;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                counters.OnEvent(*event);

                if (holdPut &&
                    event->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                    event->Get<TEvBlobStorage::TEvPut>()->Id.Channel() == 4)
                {
                    holdPut = false;
                    heldPut = event.Release();
                    return true;
                }
                return false;
            });
        const auto& commits = counters.FreshBlobCommitIds;

        partition.WriteBlocks(1, 'v');
        partition.WriteBlocks(2, 'a');
        partition.WriteBlocks(2, 'b');

        holdPut = true;
        partition.SendWriteBlocksRequest(50, 'x');
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !!heldPut;
            };
            runtime->DispatchEvents(options);
        }

        // The checkpoint waits for the write in flight. A write made meanwhile
        // keeps the version that the checkpoint needs.
        partition.SendCreateCheckpointRequest("c1");
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        partition.WriteBlocks(1, 'w');
        for (char c = 'z'; c >= 'x'; --c) {
            partition.WriteBlocks(3, c);
        }

        // The selective trim keeps that version, and its barrier stays below
        // the write in flight.
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(commits[1], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({commits[0]}), counters.Keep);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(), counters.DoNotKeep);

        runtime->Send(heldPut.Release());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            partition.RecvWriteBlocksResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            partition.RecvCreateCheckpointResponse()->GetStatus());

        auto check = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('v'),
                GetBlockContent(partition.ReadBlocks(1, "c1")));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('w'),
                GetBlockContent(partition.ReadBlocks(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('x'),
                GetBlockContent(partition.ReadBlocks(50)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('b'),
                GetBlockContent(partition.ReadBlocks(2, "c1")));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('x'),
                GetBlockContent(partition.ReadBlocks(3)));
        };

        check();

        RestartTablet(partition);
        UNIT_ASSERT(IsIn(counters.LoadedCommitIds, commits[0]));

        check();
    }

    Y_UNIT_TEST(ShouldUseSelectiveTrimSettingOfPartitionMediaKind)
    {
        for (const auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_SSD,
              NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_HYBRID})
        {
            const bool isSSD = mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD;
            for (const bool ownMediaKind: {false, true}) {
                auto config = DefaultConfig();
                config.SetFreshChannelWriteRequestsEnabled(true);
                config.SetFreshDeadBlobByteCountTrimThresholdSSD(
                    3 * DefaultBlockSize);
                config.SetFreshDeadBlobByteCountTrimThresholdHDD(
                    3 * DefaultBlockSize);
                if (isSSD == ownMediaKind) {
                    config.SetFreshSelectiveTrimEnabledSSD(true);
                } else {
                    config.SetFreshSelectiveTrimEnabledHDD(true);
                }

                TTestPartitionInfo partitionInfo;
                partitionInfo.MediaKind = mediaKind;
                auto runtime =
                    PrepareTestActorRuntime(config, 1024, {}, partitionInfo);

                TPartitionClient partition(*runtime);
                partition.WaitReady();

                TFreshLogTrimCounters counters;
                runtime->SetEventFilter(CountFreshLogTrimsFilter(counters));

                partition.WriteBlocks(100, 'p');
                for (char c = 'a'; c <= 'd'; ++c) {
                    partition.WriteBlocks(1, c);
                }
                // Every dead blob is above OrdinaryTrimToCommitId, so only a
                // selective trim can collect them.
                WaitForTrimsCompleted(*runtime, counters, ownMediaKind ? 1 : 0);
                runtime->DispatchEvents({}, TDuration::MilliSeconds(100));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    ownMediaKind ? "S" : "",
                    counters.CommandKinds,
                    "media kind: " << static_cast<int>(mediaKind)
                                   << ", own: " << ownMediaKind);
            }
        }
    }

    Y_UNIT_TEST(ShouldKeepFreshVersionsNeededByCheckpoints)
    {
        auto runtime = PrepareTestActorRuntime(SelectiveTrimConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetEventFilter(CountFreshLogTrimsFilter(counters));
        const auto& commits = counters.FreshBlobCommitIds;

        // The pin keeps OrdinaryTrimToCommitId below the versions that the
        // checkpoints need.
        partition.WriteBlocks(100, 'p');
        partition.WriteBlocks(1, 'a');
        partition.CreateCheckpoint("c1");
        partition.WriteBlocks(1, 'b');
        partition.CreateCheckpoint("c2");
        for (char c = 'c'; c <= 'f'; ++c) {
            partition.WriteBlocks(1, c);
        }

        // The checkpoints keep their versions live.
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(commits[5], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[0], commits[1], commits[2]}),
            counters.Keep);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('a'),
            GetBlockContent(partition.ReadBlocks(1, "c1")));

        // Without c1, the next overwrite kills its version, and the next
        // selective trim releases it.
        partition.DeleteCheckpoint("c1");
        partition.WriteBlocks(1, 'g');
        partition.WriteBlocks(1, 'h');
        WaitForTrimsCompleted(*runtime, counters, 2);
        UNIT_ASSERT_VALUES_EQUAL("SS", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(commits[7], counters.SelectiveTrimToCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(), counters.Keep);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[1]}),
            counters.DoNotKeep);

        auto check = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('b'),
                GetBlockContent(partition.ReadBlocks(1, "c2")));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('h'),
                GetBlockContent(partition.ReadBlocks(1)));
        };

        check();

        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[0], commits[2], commits[8]}),
            counters.LoadedCommitIds);

        check();
    }

    Y_UNIT_TEST(ShouldDeleteKeptFreshBlobsAfterSelectiveTrimIsDisabled)
    {
        TControlBoard controlBoard;
        auto runtime = PrepareTestActorRuntime(
            SelectiveTrimConfig(),
            1024,
            {},
            {},
            {},
            EStorageAccessMode::Default,
            {},
            &controlBoard);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TFreshLogTrimCounters counters;
        runtime->SetEventFilter(CountFreshLogTrimsFilter(counters));
        const auto& commits = counters.FreshBlobCommitIds;

        partition.WriteBlocks(100, 'p');
        partition.WriteBlocks(2, 'k');
        for (char c = 'a'; c <= 'd'; ++c) {
            partition.WriteBlocks(1, c);
        }
        WaitForTrimsCompleted(*runtime, counters, 1);
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[0], commits[1]}),
            counters.Keep);

        TAtomic previousValue = {};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_FreshSelectiveTrimEnabledHDD",
            0,
            previousValue));
        UNIT_ASSERT_VALUES_EQUAL(1, AtomicGet(previousValue));

        // Nothing new is kept or released.
        partition.WriteBlocks(2, 'K');
        partition.WriteBlocks(1, 'e');
        partition.WriteBlocks(1, 'f');
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("S", counters.CommandKinds);

        // Once OrdinaryTrimToCommitId passes them, the hard barrier of the
        // ordinary trim deletes the kept blobs.
        partition.WriteBlocks(100, 'P');
        WaitForTrimsCompleted(*runtime, counters, 2);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL("SH", counters.CommandKinds);

        RestartTablet(partition);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({commits[6], commits[7], commits[8], commits[9]}),
            counters.LoadedCommitIds);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('P'),
            GetBlockContent(partition.ReadBlocks(100)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('K'),
            GetBlockContent(partition.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('f'),
            GetBlockContent(partition.ReadBlocks(1)));
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

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
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
            });

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
            GetBlocksContent(response));

        response = partition.ReadBlocks(2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(2)),
            GetBlocksContent(response));

        response = partition.ReadBlocks(3);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(3)),
            GetBlocksContent(response));
    }

    Y_UNIT_TEST(
        ShouldNotTrimUnflushedBlocksWhileThereIsFlushedBlockWithLargerCommitId)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);

        TAutoPtr<IEventHandle> addFreshBlocks;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
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
            });

        partition.SendWriteBlocksRequest(2, 2);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(3, 3);

        partition.Flush();

        UNIT_ASSERT(addFreshBlocks);
        runtime->Send(addFreshBlocks.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.TrimFreshLog();

        partition.RebootTablet();

        auto response = partition.ReadBlocks(1);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)),
            GetBlocksContent(response));

        response = partition.ReadBlocks(2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(2)),
            GetBlocksContent(response));

        response = partition.ReadBlocks(3);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(3)),
            GetBlocksContent(response));
    }

    Y_UNIT_TEST(ShouldReleaseTrimBarrierOnBlockDeletion)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

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

        partition.Flush();
        partition.TrimFreshLog();

        partition.RebootTablet();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
        }
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
                            auto* msg =
                                event->Get<TEvBlobStorage::TEvPutResult>();
                            msg->Status = NKikimrProto::ERROR;
                            flush = false;
                        }
                        break;
                    }
                }

                return false;
            });

        flush = true;
        partition.SendFlushRequest();
        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));

        partition.WriteBlocks(TBlockRange32::WithLength(1, 2));

        partition.Flush();
    }

    Y_UNIT_TEST(ShouldRaiseCriticalEventIfTrimFreshLogTimesOut)
    {
        constexpr ui32 FreshChannelId = 4;

        auto config = DefaultConfig();
        config.SetFlushThresholdHDD(4_MB);
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetTrimFreshLogTimeout(TDuration::Seconds(1).MilliSeconds());

        auto runtime = PrepareTestActorRuntime(config);

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto trimCounter =
            counters->GetCounter("AppCriticalEvents/TrimFreshLogTimeout", true);

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

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbageResult: {
                        auto* msg = event->Get<
                            TEvBlobStorage::TEvCollectGarbageResult>();
                        if (msg->Channel == FreshChannelId) {
                            trimSeen = true;
                            msg->Status = NKikimrProto::EReplyStatus::DEADLINE;
                        }
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                        auto* msg = event->Get<TEvPartitionCommonPrivate::
                                                   TEvTrimFreshLogCompleted>();
                        UNIT_ASSERT(FAILED(msg->GetStatus()));
                        trimCompletedSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Flush();

        // wait for trimfreshlog to complete
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return trimCompletedSeen;
        };
        runtime->DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(true, trimSeen);
        UNIT_ASSERT_VALUES_UNEQUAL(0, trimCounter->Val());
    }

    Y_UNIT_TEST(ShouldLoadFreshBlobsFromLastTrimFreshLogToCommitIdInMeta)
    {
        constexpr ui32 FreshChannelId = 4;

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

        bool saveBlobs = false;
        TSet<ui64> expectedBlobs;

        auto eventFilter =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
        {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvCollectGarbage: {
                    auto* msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                    if (msg->Channel == FreshChannelId) {
                        return true;
                    }
                    break;
                }
                case TEvBlobStorage::EvRangeResult: {
                    using TEvent = TEvBlobStorage::TEvRangeResult;
                    auto* msg = ev->Get<TEvent>();
                    bool isRangeResponseFromFresh = false;
                    for (const auto& r: msg->Responses) {
                        if (r.Id.Channel() == FreshChannelId) {
                            isRangeResponseFromFresh = true;
                            auto commitId =
                                MakeCommitId(r.Id.Generation(), r.Id.Step());
                            UNIT_ASSERT_VALUES_EQUAL(
                                1,
                                expectedBlobs.count(commitId));
                        }
                    }
                    if (isRangeResponseFromFresh) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            expectedBlobs.size(),
                            msg->Responses.size());
                    }
                    break;
                }
                case TEvPartitionCommonPrivate::EvWriteBlobCompleted: {
                    using TEvent =
                        TEvPartitionCommonPrivate::TEvWriteBlobCompleted;
                    auto* msg = ev->Get<TEvent>();
                    if (msg->BlobId.Channel() == 4 && saveBlobs) {
                        expectedBlobs.emplace(msg->BlobId.CommitId());
                    }
                    break;
                }
            }
            return false;
        };

        runtime->SetEventFilter(eventFilter);

        // update TrimFreshLogToCommitId in meta to gen:step = 0:0
        partition.Flush();

        runtime->DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));

        saveBlobs = true;

        partition.WriteBlocks(4, 4);
        partition.WriteBlocks(5, 5);
        partition.WriteBlocks(6, 6);

        // update TrimFreshLogToCommitId in meta to gen:step = 2:4
        partition.Flush();

        partition.RebootTablet();
        partition.WaitReady();

        // expect to read last 3 blobs ([2:5], [2:6], [2:7])

        UNIT_ASSERT_VALUES_EQUAL(3, expectedBlobs.size());
        UNIT_ASSERT_VALUES_EQUAL(1, expectedBlobs.count(MakeCommitId(2, 5)));
        UNIT_ASSERT_VALUES_EQUAL(1, expectedBlobs.count(MakeCommitId(2, 6)));
        UNIT_ASSERT_VALUES_EQUAL(1, expectedBlobs.count(MakeCommitId(2, 7)));
    }

    Y_UNIT_TEST(ShouldReleaseTrimFreshLogBarrierInCaseOfWriteBlobError)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 trimFreshLogToCommitId = 0llu;
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            trimFreshLogToCommitId = stats.GetTrimFreshLogToCommitId();
        }

        bool shouldRejectWriteBlob = false;
        auto eventFilter =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
        {
            switch (ev->GetTypeRewrite()) {
                case TEvPartitionCommonPrivate::EvWriteBlobResponse: {
                    if (shouldRejectWriteBlob) {
                        auto* msg = ev->Get<
                            TEvPartitionCommonPrivate::TEvWriteBlobResponse>();
                        auto& e = const_cast<NProto::TError&>(msg->Error);
                        e.SetCode(E_REJECTED);
                    }
                    break;
                }
                case TEvPartitionPrivate::EvWriteBlocksCompleted: {
                    auto* msg =
                        ev->Get<TEvPartitionPrivate::TEvWriteBlocksCompleted>();
                    UNIT_ASSERT_EQUAL(
                        shouldRejectWriteBlob,
                        HasError(msg->GetError()));
                    break;
                }
            }
            return false;
        };

        runtime->SetEventFilter(eventFilter);

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                trimFreshLogToCommitId,
                stats.GetTrimFreshLogToCommitId());
        }

        partition.Flush();
        trimFreshLogToCommitId += 4llu;
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                trimFreshLogToCommitId,
                stats.GetTrimFreshLogToCommitId());
        }

        shouldRejectWriteBlob = true;

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(4, 1), 4);
        {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(5, 1), 5);
        {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        trimFreshLogToCommitId += 2llu;
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                trimFreshLogToCommitId,
                stats.GetTrimFreshLogToCommitId());
        }

        shouldRejectWriteBlob = false;

        partition.Flush();
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                trimFreshLogToCommitId,
                stats.GetTrimFreshLogToCommitId());
        }

        partition.WriteBlocks(6, 6);
        partition.Flush();
        trimFreshLogToCommitId += 2llu;
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                trimFreshLogToCommitId,
                stats.GetTrimFreshLogToCommitId());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
