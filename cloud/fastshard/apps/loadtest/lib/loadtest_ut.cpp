#include "loadtest.h"

#include "context.h"

#include <cloud/fastshard/libs/sn/client/durable.h>
#include <cloud/fastshard/libs/sn/testlib/fake_storage_node.h>
#include <cloud/fastshard/libs/sn/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>

#include <gtest/gtest.h>

#include <memory>
#include <thread>

using namespace NCloud::NFastShard;
using namespace NCloud::NFastShard::NLoadTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

////////////////////////////////////////////////////////////////////////////////

TLoadTestConfig MakeConfig()
{
    TLoadTestConfig config;
    config.DeviceUUID = "dev";
    config.Generation = 7;
    config.IoDepth = 4;
    config.Requests = 200;
    config.PageSize = 512;
    config.PageCount = 16;
    config.WritePages = 2;
    config.ReadPages = 3;
    config.ReadPercent = 50;
    config.AdvanceEvery = 32;
    return config;
}

// Wraps the storage node in a durable client, as the bootstrap does.
ILoadTestPtr CreateTest(
    const TAppContext& ctx,
    const TLoadTestConfig& config,
    IStorageNodePtr storage,
    TDuration retryTimeout = TDuration::Seconds(30))
{
    auto metrics = std::make_shared<TDurableClientMetrics>();
    auto client = CreateDurableClient(
        std::move(storage),
        {.RetryTimeout = retryTimeout},
        metrics);
    return CreateLoadTest(ctx, config, std::move(client), metrics);
}

const NCloud::NFileStore::NProto::TTestStats::TStats* FindStats(
    const NCloud::NFileStore::NProto::TTestStats& stats,
    const TString& action)
{
    for (const auto& s: stats.GetStats()) {
        if (s.GetAction() == action) {
            return &s;
        }
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFastShardLoadTest, ShouldRunMixedLoadAgainstFakeStorageNode)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    auto config = MakeConfig();
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage);

    const auto stats = test->Run();
    EXPECT_TRUE(stats.GetSuccess());

    // acquired once with the configured generation, released once
    ASSERT_EQ(1u, storage->AcquireCalls.size());
    EXPECT_EQ("dev", storage->AcquireCalls[0].GetDeviceUUIDs(0));
    EXPECT_EQ(7u, storage->AcquireCalls[0].GetGeneration());
    EXPECT_EQ(
        "fastshard-loadtest",
        storage->AcquireCalls[0].GetHeaders().GetClientId());
    ASSERT_EQ(1u, storage->ReleaseCalls.size());

    // exactly the requested number of requests, split between the actions
    const auto writes = storage->WriteCalls.size();
    const auto reads = storage->ReadCalls.size();
    EXPECT_EQ(config.Requests, writes + reads);
    EXPECT_GT(writes, 0u);
    EXPECT_GT(reads, 0u);

    const auto* writeStats = FindStats(stats, "WriteLogRecord");
    ASSERT_TRUE(writeStats);
    EXPECT_EQ(writes, writeStats->GetCount());
    EXPECT_EQ(
        writes * config.WritePages * config.PageSize,
        writeStats->GetRequestBytes());
    EXPECT_GT(writeStats->GetLatency().GetMax(), 0u);

    const auto* readStats = FindStats(stats, "ReadPages");
    ASSERT_TRUE(readStats);
    EXPECT_EQ(reads, readStats->GetCount());

    // written records form one chain starting right after the empty
    // journal, and every request stays inside the device page range
    TVector<ui64> lsns;
    for (const auto& call: storage->WriteCalls) {
        EXPECT_EQ("dev", call.GetDeviceUUID());
        EXPECT_EQ(
            call.GetLogSequenceNumber() - 1,
            call.GetPrevLogSequenceNumber());
        lsns.push_back(call.GetLogSequenceNumber());

        ASSERT_EQ(1u, call.PageGroupsSize());
        const auto& group = call.GetPageGroups(0);
        EXPECT_EQ(config.WritePages, group.ContentSize());
        EXPECT_LE(
            group.GetFirstPageNo() + config.WritePages,
            config.PageCount);
        for (const auto& page: group.GetContent()) {
            EXPECT_EQ(config.PageSize, page.size());
        }
    }
    Sort(lsns);
    for (size_t i = 0; i < lsns.size(); ++i) {
        EXPECT_EQ(i + 1, lsns[i]);
    }

    for (const auto& call: storage->ReadCalls) {
        ASSERT_EQ(1u, call.PageGroupRefsSize());
        const auto& ref = call.GetPageGroupRefs(0);
        EXPECT_EQ(config.ReadPages, ref.GetPageCount());
        EXPECT_EQ(config.PageSize, ref.GetPageSize());
        EXPECT_LE(ref.GetFirstPageNo() + config.ReadPages, config.PageCount);
    }

    // the watermark was advanced once per AdvanceEvery records
    EXPECT_EQ(
        writes / config.AdvanceEvery,
        storage->AdvanceLsnLowWatermarkCalls.size());
    for (const auto& call: storage->AdvanceLsnLowWatermarkCalls) {
        EXPECT_EQ(0u, call.GetLsnLowWatermark() % config.AdvanceEvery);
    }
}

TEST(TFastShardLoadTest, ShouldContinueChainFromJournalTail)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->ReadJournalTailResp.SetLsnLowWatermark(40);
    storage->ReadJournalTailResp.AddRecords()->SetLogSequenceNumber(41);
    storage->ReadJournalTailResp.AddRecords()->SetLogSequenceNumber(42);

    auto config = MakeConfig();
    config.ReadPercent = 0;
    config.Requests = 10;
    config.NoAcquire = true;
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage);

    EXPECT_TRUE(test->Run().GetSuccess());
    EXPECT_TRUE(storage->AcquireCalls.empty());
    EXPECT_TRUE(storage->ReleaseCalls.empty());

    TVector<ui64> lsns;
    for (const auto& call: storage->WriteCalls) {
        lsns.push_back(call.GetLogSequenceNumber());
    }
    Sort(lsns);
    ASSERT_EQ(10u, lsns.size());
    EXPECT_EQ(43u, lsns.front());
    EXPECT_EQ(52u, lsns.back());
}

TEST(TFastShardLoadTest, ShouldAssumeEmptyJournalWhenTailIsNotImplemented)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->ReadJournalTailResp.MutableError()->SetCode(
        NCloud::E_NOT_IMPLEMENTED);

    auto config = MakeConfig();
    config.ReadPercent = 0;
    config.Requests = 3;
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage);

    EXPECT_TRUE(test->Run().GetSuccess());
    TVector<ui64> lsns;
    for (const auto& call: storage->WriteCalls) {
        lsns.push_back(call.GetLogSequenceNumber());
    }
    Sort(lsns);
    ASSERT_EQ(3u, lsns.size());
    EXPECT_EQ(1u, lsns.front());
}

TEST(TFastShardLoadTest, ShouldStopAdvancingWhenNotImplemented)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->AdvanceLsnLowWatermarkResp.MutableError()->SetCode(
        NCloud::E_NOT_IMPLEMENTED);

    auto config = MakeConfig();
    config.ReadPercent = 0;
    config.Requests = 100;
    config.AdvanceEvery = 10;
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage);

    EXPECT_TRUE(test->Run().GetSuccess());
    EXPECT_EQ(100u, storage->WriteCalls.size());
    EXPECT_EQ(1u, storage->AdvanceLsnLowWatermarkCalls.size());
}

TEST(TFastShardLoadTest, ShouldReportErrors)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->WriteResp.MutableError()->SetCode(NCloud::E_ARGUMENT);

    auto config = MakeConfig();
    config.ReadPercent = 0;
    config.Requests = 5;
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage);

    const auto stats = test->Run();
    EXPECT_FALSE(stats.GetSuccess());
    const auto* writeStats = FindStats(stats, "WriteLogRecord");
    ASSERT_TRUE(writeStats);
    EXPECT_EQ(0u, writeStats->GetCount());
    EXPECT_EQ(5u, storage->WriteCalls.size());
}

TEST(TFastShardLoadTest, ShouldRetryRejectedWriteUnderSameLsn)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    for (int i = 0; i < 2; ++i) {
        storage->WriteRespQueue.emplace_back().MutableError()->SetCode(
            NCloud::E_REJECTED);
    }

    auto config = MakeConfig();
    config.ReadPercent = 0;
    config.IoDepth = 1;
    config.Requests = 5;
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage);

    const auto stats = test->Run();
    EXPECT_TRUE(stats.GetSuccess());

    const auto* writeStats = FindStats(stats, "WriteLogRecord");
    ASSERT_TRUE(writeStats);
    EXPECT_EQ(5u, writeStats->GetCount());

    // the first record was sent three times, then the chain went on
    // without a gap
    TVector<ui64> lsns;
    for (const auto& call: storage->WriteCalls) {
        lsns.push_back(call.GetLogSequenceNumber());
    }
    EXPECT_EQ((TVector<ui64>{1, 1, 1, 2, 3, 4, 5}), lsns);
}

TEST(TFastShardLoadTest, ShouldRetryReads)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->ReadRespQueue.emplace_back().MutableError()->SetCode(
        NCloud::E_TIMEOUT);

    auto config = MakeConfig();
    config.ReadPercent = 100;
    config.IoDepth = 1;
    config.Requests = 2;
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage);

    const auto stats = test->Run();
    EXPECT_TRUE(stats.GetSuccess());
    EXPECT_EQ(3u, storage->ReadCalls.size());

    const auto* readStats = FindStats(stats, "ReadPages");
    ASSERT_TRUE(readStats);
    EXPECT_EQ(2u, readStats->GetCount());
}

TEST(TFastShardLoadTest, ShouldGiveUpRetryingAfterTimeout)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->WriteResp.MutableError()->SetCode(NCloud::E_REJECTED);

    auto config = MakeConfig();
    config.ReadPercent = 0;
    config.IoDepth = 1;
    config.Requests = 2;
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage, TDuration::MilliSeconds(50));

    const auto stats = test->Run();
    EXPECT_FALSE(stats.GetSuccess());

    const auto* writeStats = FindStats(stats, "WriteLogRecord");
    ASSERT_TRUE(writeStats);
    EXPECT_EQ(0u, writeStats->GetCount());
    // both records were attempted more than once before being given up
    EXPECT_GT(storage->WriteCalls.size(), 2u);
    EXPECT_EQ(2u, storage->WriteCalls.back().GetLogSequenceNumber());
}

TEST(TFastShardLoadTest, ShouldNotRetryWhenDisabled)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->WriteResp.MutableError()->SetCode(NCloud::E_REJECTED);

    auto config = MakeConfig();
    config.ReadPercent = 0;
    config.Requests = 3;
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage, TDuration::Zero());

    EXPECT_FALSE(test->Run().GetSuccess());
    EXPECT_EQ(3u, storage->WriteCalls.size());
}

TEST(TFastShardLoadTest, ShouldStopWhileRetryingReads)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->ReadResp.MutableError()->SetCode(NCloud::E_REJECTED);

    auto config = MakeConfig();
    config.ReadPercent = 100;
    config.IoDepth = 1;
    config.Requests = 0;
    config.Duration = TDuration::Minutes(10);
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage, TDuration::Minutes(10));

    std::thread stopper(
        [&]
        {
            while (storage->ReadCalls.size() < 3) {
                Sleep(TDuration::MilliSeconds(1));
            }
            ctx.ShouldStop.store(true);
        });

    // the single read is given up at the stop instead of being retried
    // for the rest of the retry timeout, and is not counted as an error
    EXPECT_TRUE(test->Run().GetSuccess());
    stopper.join();
    EXPECT_EQ(1u, storage->ReleaseCalls.size());
}

TEST(TFastShardLoadTest, ShouldKeepRetryingWritesAfterStop)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->WriteResp.MutableError()->SetCode(NCloud::E_REJECTED);

    auto config = MakeConfig();
    config.ReadPercent = 0;
    config.IoDepth = 1;
    config.Requests = 0;
    config.Duration = TDuration::Minutes(10);
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage, TDuration::MilliSeconds(200));

    size_t callsAtStop = 0;
    std::thread stopper(
        [&]
        {
            while (storage->WriteCalls.size() < 3) {
                Sleep(TDuration::MilliSeconds(1));
            }
            callsAtStop = storage->WriteCalls.size();
            ctx.ShouldStop.store(true);
        });

    // the record is retried past the stop until the retry timeout, so a
    // record that another worker may have already written after it is
    // not skipped
    EXPECT_FALSE(test->Run().GetSuccess());
    stopper.join();
    EXPECT_GT(storage->WriteCalls.size(), callsAtStop);
    for (const auto& call: storage->WriteCalls) {
        EXPECT_EQ(1u, call.GetLogSequenceNumber());
    }
    EXPECT_EQ(1u, storage->ReleaseCalls.size());
}

TEST(TFastShardLoadTest, ShouldFailWhenAcquireFails)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->AcquireResp.MutableError()->SetCode(NCloud::E_REJECTED);

    auto config = MakeConfig();
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage, TDuration::Zero());
    EXPECT_FALSE(test->Run().GetSuccess());
    EXPECT_TRUE(storage->WriteCalls.empty());
    EXPECT_TRUE(storage->ReadCalls.empty());
    EXPECT_TRUE(storage->ReleaseCalls.empty());
}

TEST(TFastShardLoadTest, ShouldStopOnRequest)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    auto config = MakeConfig();
    config.Requests = 0;
    config.Duration = TDuration::Minutes(10);
    TAppContext ctx;
    auto test = CreateTest(ctx, config, storage);

    std::thread stopper(
        [&]
        {
            while (storage->WriteCalls.size() + storage->ReadCalls.size() < 50)
            {
                Sleep(TDuration::MilliSeconds(1));
            }
            ctx.ShouldStop.store(true);
        });

    EXPECT_TRUE(test->Run().GetSuccess());
    stopper.join();
    EXPECT_EQ(1u, storage->ReleaseCalls.size());
}

}   // namespace
