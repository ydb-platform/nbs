#include "loadtest.h"

#include <cloud/fastshard/testlib/fake_storage_node.h>
#include <cloud/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/fastshard/protos/device.pb.h>

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

TOptions MakeOptions()
{
    TOptions options;
    options.DeviceUUID = "dev";
    options.Generation = 7;
    options.IoDepth = 4;
    options.Requests = 200;
    options.PageSize = 512;
    options.PageCount = 16;
    options.WritePages = 2;
    options.ReadPages = 3;
    options.ReadPercent = 50;
    options.AdvanceEvery = 32;
    options.Validate();
    return options;
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
    auto options = MakeOptions();
    auto test = CreateLoadTest(options, storage);

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
    EXPECT_EQ(options.Requests, writes + reads);
    EXPECT_GT(writes, 0u);
    EXPECT_GT(reads, 0u);

    const auto* writeStats = FindStats(stats, "WriteLogRecord");
    ASSERT_TRUE(writeStats);
    EXPECT_EQ(writes, writeStats->GetCount());
    EXPECT_EQ(
        writes * options.WritePages * options.PageSize,
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
        EXPECT_EQ(options.WritePages, group.ContentSize());
        EXPECT_LE(
            group.GetFirstPageNo() + options.WritePages,
            options.PageCount);
        for (const auto& page: group.GetContent()) {
            EXPECT_EQ(options.PageSize, page.size());
        }
    }
    Sort(lsns);
    for (size_t i = 0; i < lsns.size(); ++i) {
        EXPECT_EQ(i + 1, lsns[i]);
    }

    for (const auto& call: storage->ReadCalls) {
        ASSERT_EQ(1u, call.PageGroupRefsSize());
        const auto& ref = call.GetPageGroupRefs(0);
        EXPECT_EQ(options.ReadPages, ref.GetPageCount());
        EXPECT_EQ(options.PageSize, ref.GetPageSize());
        EXPECT_LE(ref.GetFirstPageNo() + options.ReadPages, options.PageCount);
    }

    // the watermark was advanced once per AdvanceEvery records
    EXPECT_EQ(
        writes / options.AdvanceEvery,
        storage->AdvanceLsnLowWatermarkCalls.size());
    for (const auto& call: storage->AdvanceLsnLowWatermarkCalls) {
        EXPECT_EQ(0u, call.GetLsnLowWatermark() % options.AdvanceEvery);
    }
}

TEST(TFastShardLoadTest, ShouldContinueChainFromJournalTail)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->ReadJournalTailResp.SetLsnLowWatermark(40);
    storage->ReadJournalTailResp.AddRecords()->SetLogSequenceNumber(41);
    storage->ReadJournalTailResp.AddRecords()->SetLogSequenceNumber(42);

    auto options = MakeOptions();
    options.ReadPercent = 0;
    options.Requests = 10;
    options.NoAcquire = true;
    auto test = CreateLoadTest(options, storage);

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

    auto options = MakeOptions();
    options.ReadPercent = 0;
    options.Requests = 3;
    auto test = CreateLoadTest(options, storage);

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

    auto options = MakeOptions();
    options.ReadPercent = 0;
    options.Requests = 100;
    options.AdvanceEvery = 10;
    auto test = CreateLoadTest(options, storage);

    EXPECT_TRUE(test->Run().GetSuccess());
    EXPECT_EQ(100u, storage->WriteCalls.size());
    EXPECT_EQ(1u, storage->AdvanceLsnLowWatermarkCalls.size());
}

TEST(TFastShardLoadTest, ShouldReportErrors)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->WriteResp.MutableError()->SetCode(NCloud::E_ARGUMENT);

    auto options = MakeOptions();
    options.ReadPercent = 0;
    options.Requests = 5;
    auto test = CreateLoadTest(options, storage);

    const auto stats = test->Run();
    EXPECT_FALSE(stats.GetSuccess());
    const auto* writeStats = FindStats(stats, "WriteLogRecord");
    ASSERT_TRUE(writeStats);
    EXPECT_EQ(0u, writeStats->GetCount());
    EXPECT_EQ(5u, storage->WriteCalls.size());
}

TEST(TFastShardLoadTest, ShouldFailWhenAcquireFails)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    storage->AcquireResp.MutableError()->SetCode(NCloud::E_REJECTED);

    auto test = CreateLoadTest(MakeOptions(), storage);
    EXPECT_FALSE(test->Run().GetSuccess());
    EXPECT_TRUE(storage->WriteCalls.empty());
    EXPECT_TRUE(storage->ReadCalls.empty());
    EXPECT_TRUE(storage->ReleaseCalls.empty());
}

TEST(TFastShardLoadTest, ShouldStopOnRequest)
{
    auto storage = std::make_shared<TFakeStorageNode>();
    auto options = MakeOptions();
    options.Requests = 0;
    options.Duration = TDuration::Minutes(10);
    auto test = CreateLoadTest(options, storage);

    std::thread stopper(
        [&]
        {
            while (storage->WriteCalls.size() + storage->ReadCalls.size() < 50)
            {
                Sleep(TDuration::MilliSeconds(1));
            }
            test->Stop();
        });

    EXPECT_TRUE(test->Run().GetSuccess());
    stopper.join();
    EXPECT_EQ(1u, storage->ReleaseCalls.size());
}

}   // namespace
