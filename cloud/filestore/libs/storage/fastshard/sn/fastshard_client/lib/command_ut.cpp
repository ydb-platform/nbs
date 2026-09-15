#include "factory.h"

#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

#include <silk/fibers/event.h>

#include <gtest/gtest.h>

#include <atomic>
#include <initializer_list>
#include <memory>
#include <thread>

using namespace NCloud;
using namespace NCloud::NFileStore::NStorage::NFastShard;
using namespace NCloud::NFileStore::NStorage::NFastShard::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

////////////////////////////////////////////////////////////////////////////////
// Runs a command against a fake storage node injected in place of the TCP
// client, capturing its output.

struct TFixture
{
    std::shared_ptr<TFakeStorageNode> Storage =
        std::make_shared<TFakeStorageNode>();
    std::shared_ptr<TStringStream> Output = std::make_shared<TStringStream>();

    bool Run(
        const TString& name,
        std::initializer_list<const char*> args,
        const TString& input = {})
    {
        auto command = GetCommand(name, Storage);
        EXPECT_TRUE(command) << name;
        if (!command) {
            return false;
        }

        command->SetOutputStream(Output);
        if (input) {
            command->SetInputStream(std::make_unique<TStringStream>(input));
        }

        TVector<const char*> argv{name.c_str()};
        argv.insert(argv.end(), args.begin(), args.end());
        command->ParseOpts(static_cast<int>(argv.size()), argv.data());
        return command->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////
// Storage node whose AcquireDevices parks the calling fiber on a gate until
// the test opens it. Used to prove that Shutdown returns from a hung request.

struct TBlockingStorageNode: public TFakeStorageNode
{
    silk::FiberEvent Gate;
    std::atomic<bool> Completed{false};

    NCloud::NProto::TAcquireDevicesResponse AcquireDevices(
        NCloud::NProto::TAcquireDevicesRequest request) override
    {
        Gate.wait();
        Completed.store(true);
        return TFakeStorageNode::AcquireDevices(std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TFastShardClientTest, ShouldListEveryStorageNodeMethod)
{
    const TVector<TString> expected = {
        "acquiredevices",
        "advancelsnlowwatermark",
        "readjournaltail",
        "readpages",
        "releasedevices",
        "writelogrecord",
    };
    EXPECT_EQ(GetCommandNames(), expected);

    EXPECT_EQ(NormalizeCommand("Read-Pages"), "readpages");
    EXPECT_EQ(NormalizeCommand("read_pages"), "readpages");
    EXPECT_FALSE(GetCommand("nosuchcommand"));
}

TEST(TFastShardClientTest, ShouldAcquireDevices)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "acquiredevices",
        {"--device-uuid", "d1", "--device-uuid", "d2", "--generation", "7",
         "--client-id", "cli", "--request-timeout", "500"}));

    ASSERT_EQ(f.Storage->AcquireCalls.size(), 1u);
    const auto& req = f.Storage->AcquireCalls[0];
    ASSERT_EQ(req.DeviceUUIDsSize(), 2u);
    EXPECT_EQ(req.GetDeviceUUIDs(0), "d1");
    EXPECT_EQ(req.GetDeviceUUIDs(1), "d2");
    EXPECT_EQ(req.GetGeneration(), 7u);
    EXPECT_EQ(req.GetHeaders().GetClientId(), "cli");
    EXPECT_EQ(req.GetHeaders().GetRequestTimeout(), 500u);
    EXPECT_EQ(f.Output->Str(), "OK\n");
}

TEST(TFastShardClientTest, ShouldReleaseDevices)
{
    TFixture f;
    EXPECT_TRUE(f.Run("releasedevices", {"--device-uuid", "d1"}));

    ASSERT_EQ(f.Storage->ReleaseCalls.size(), 1u);
    ASSERT_EQ(f.Storage->ReleaseCalls[0].DeviceUUIDsSize(), 1u);
    EXPECT_EQ(f.Storage->ReleaseCalls[0].GetDeviceUUIDs(0), "d1");
    EXPECT_EQ(f.Output->Str(), "OK\n");
}

TEST(TFastShardClientTest, ShouldReadPagesAsRawBytes)
{
    TFixture f;
    auto* group = f.Storage->ReadResp.AddPageGroups();
    group->SetFirstPageNo(3);
    group->AddContent("AAAA");
    group->AddContent("BBBB");

    EXPECT_TRUE(f.Run(
        "readpages",
        {"--device-uuid", "d1", "--first-page-no", "3", "--page-count", "2",
         "--page-size", "4"}));

    ASSERT_EQ(f.Storage->ReadCalls.size(), 1u);
    const auto& req = f.Storage->ReadCalls[0];
    EXPECT_EQ(req.GetDeviceUUID(), "d1");
    ASSERT_EQ(req.PageGroupRefsSize(), 1u);
    EXPECT_EQ(req.GetPageGroupRefs(0).GetFirstPageNo(), 3u);
    EXPECT_EQ(req.GetPageGroupRefs(0).GetPageCount(), 2u);
    EXPECT_EQ(req.GetPageGroupRefs(0).GetPageSize(), 4u);
    EXPECT_EQ(f.Output->Str(), "AAAABBBB");
}

TEST(TFastShardClientTest, ShouldSplitWriteInputIntoPages)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "writelogrecord",
        {"--device-uuid", "d1", "--first-page-no", "5", "--page-size", "4",
         "--lsn", "10", "--prev-lsn", "9"},
        "abcdefgh"));

    ASSERT_EQ(f.Storage->WriteCalls.size(), 1u);
    const auto& req = f.Storage->WriteCalls[0];
    EXPECT_EQ(req.GetDeviceUUID(), "d1");
    EXPECT_EQ(req.GetLogSequenceNumber(), 10u);
    EXPECT_EQ(req.GetPrevLogSequenceNumber(), 9u);
    ASSERT_EQ(req.PageGroupsSize(), 1u);
    EXPECT_EQ(req.GetPageGroups(0).GetFirstPageNo(), 5u);
    ASSERT_EQ(req.GetPageGroups(0).ContentSize(), 2u);
    EXPECT_EQ(req.GetPageGroups(0).GetContent(0), "abcd");
    EXPECT_EQ(req.GetPageGroups(0).GetContent(1), "efgh");
    EXPECT_EQ(f.Output->Str(), "OK\n");
}

TEST(TFastShardClientTest, ShouldRejectWriteInputNotAlignedToPageSize)
{
    TFixture f;
    EXPECT_THROW(
        f.Run(
            "writelogrecord",
            {"--device-uuid", "d1", "--page-size", "4"},
            "abcde"),
        yexception);
    EXPECT_TRUE(f.Storage->WriteCalls.empty());
}

TEST(TFastShardClientTest, ShouldSummarizeJournalTail)
{
    TFixture f;
    auto& resp = f.Storage->ReadJournalTailResp;
    resp.SetLastAckedLogSequenceNumber(42);
    auto* record = resp.AddRecords();
    record->SetLogSequenceNumber(41);
    record->SetPrevLogSequenceNumber(40);
    auto* group = record->AddPageGroups();
    group->SetFirstPageNo(8);
    group->AddContent("xxxx");
    group->AddContent("yyyy");

    EXPECT_TRUE(f.Run(
        "readjournaltail",
        {"--device-uuid", "d1", "--after-lsn", "40", "--max-record-count", "3"}));

    ASSERT_EQ(f.Storage->ReadJournalTailCalls.size(), 1u);
    const auto& req = f.Storage->ReadJournalTailCalls[0];
    EXPECT_EQ(req.GetDeviceUUID(), "d1");
    EXPECT_EQ(req.GetAfterLogSequenceNumber(), 40u);
    EXPECT_EQ(req.GetMaxRecordCount(), 3u);
    EXPECT_EQ(
        f.Output->Str(),
        "LastAckedLogSequenceNumber: 42\n"
        "Records: 1\n"
        "  Lsn: 41 PrevLsn: 40 [FirstPageNo: 8 Pages: 2 Bytes: 8]\n");
}

TEST(TFastShardClientTest, ShouldAdvanceLsnLowWatermark)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "advancelsnlowwatermark",
        {"--device-uuid", "d1", "--lsn-low-watermark", "17"}));

    ASSERT_EQ(f.Storage->AdvanceLsnLowWatermarkCalls.size(), 1u);
    const auto& req = f.Storage->AdvanceLsnLowWatermarkCalls[0];
    EXPECT_EQ(req.GetDeviceUUID(), "d1");
    EXPECT_EQ(req.GetLsnLowWatermark(), 17u);
    EXPECT_EQ(f.Output->Str(), "OK\n");
}

TEST(TFastShardClientTest, ShouldFailOnErrorResponse)
{
    TFixture f;
    *f.Storage->AcquireResp.MutableError() =
        MakeError(E_ARGUMENT, "bad device");

    EXPECT_FALSE(f.Run("acquiredevices", {"--device-uuid", "d1"}));
    EXPECT_EQ(f.Storage->AcquireCalls.size(), 1u);
    EXPECT_EQ(f.Output->Str(), "");
}

TEST(TFastShardClientTest, ShouldSpeakProtoText)
{
    TFixture f;
    *f.Storage->AcquireResp.MutableError() =
        MakeError(E_REJECTED, "not now");

    // the request comes from input; the whole response goes to output
    // even when it carries an error
    EXPECT_FALSE(f.Run(
        "acquiredevices",
        {"--proto", "--client-id", "cli"},
        "DeviceUUIDs: \"d1\"\nGeneration: 3\n"));

    ASSERT_EQ(f.Storage->AcquireCalls.size(), 1u);
    const auto& req = f.Storage->AcquireCalls[0];
    ASSERT_EQ(req.DeviceUUIDsSize(), 1u);
    EXPECT_EQ(req.GetDeviceUUIDs(0), "d1");
    EXPECT_EQ(req.GetGeneration(), 3u);
    EXPECT_EQ(req.GetHeaders().GetClientId(), "cli");

    NCloud::NProto::TAcquireDevicesResponse resp;
    ParseFromTextFormat(*f.Output, resp);
    EXPECT_EQ(resp.GetError().GetCode(), E_REJECTED);
    EXPECT_EQ(resp.GetError().GetMessage(), "not now");
}

TEST(TFastShardClientTest, ShouldReturnOnShutdownWhileRequestIsInFlight)
{
    auto storage = std::make_shared<TBlockingStorageNode>();
    auto command = GetCommand("acquiredevices", storage);
    ASSERT_TRUE(command);
    command->SetOutputStream(std::make_shared<TStringStream>());

    const char* argv[] = {"acquiredevices", "--device-uuid", "d1"};
    bool result = true;
    std::thread runner([&] {
        command->ParseOpts(std::size(argv), argv);
        result = command->Run();
    });

    command->Shutdown();
    runner.join();

    EXPECT_FALSE(result);
    EXPECT_TRUE(command->IsStopped());
    EXPECT_FALSE(storage->Completed.load());

    // Let the abandoned fiber finish before the command and the storage
    // node it references go away. Completed is set inside the fake before
    // the fiber is done with the command, so wait for the fiber itself.
    storage->Gate.set();
    command->WaitForFiber();
    EXPECT_TRUE(storage->Completed.load());
}

TEST(TFastShardClientTest, ShouldGiveUpOnRequestTimeout)
{
    auto storage = std::make_shared<TBlockingStorageNode>();
    auto command = GetCommand("acquiredevices", storage);
    ASSERT_TRUE(command);
    command->SetOutputStream(std::make_shared<TStringStream>());

    const char* argv[] = {
        "acquiredevices",
        "--device-uuid",
        "d1",
        "--request-timeout",
        "100"};
    command->ParseOpts(std::size(argv), argv);

    const auto started = TInstant::Now();
    EXPECT_FALSE(command->Run());
    EXPECT_TRUE(command->IsStopped());
    EXPECT_FALSE(storage->Completed.load());
    // 100ms timeout + 1s margin, but well before anything unbounded
    EXPECT_LT(TInstant::Now() - started, TDuration::Seconds(10));

    storage->Gate.set();
    command->WaitForFiber();
    EXPECT_TRUE(storage->Completed.load());
}

TEST(TFastShardClientTest, ShouldRequireDeviceUuidOutsideProtoMode)
{
    TFixture f;
    EXPECT_THROW(
        f.Run("readpages", {"--page-count", "1"}),
        NLastGetopt::TUsageException);
    EXPECT_TRUE(f.Storage->ReadCalls.empty());
}

}   // namespace
