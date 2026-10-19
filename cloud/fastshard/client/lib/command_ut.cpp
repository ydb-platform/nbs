#include "factory.h"

#include <cloud/fastshard/protos/device.pb.h>
#include <cloud/fastshard/testlib/fake_storage_node.h>
#include <cloud/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>

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
#include <utility>

using namespace NCloud;
using namespace NCloud::NFastShard;
using namespace NCloud::NFastShard::NClient;

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
        "formatdevice",
        "readjournaltail",
        "readpages",
        "releasedevices",
        "writelogrecord",
    };
    EXPECT_EQ(expected, GetCommandNames());

    EXPECT_EQ("readpages", NormalizeCommand("Read-Pages"));
    EXPECT_EQ("readpages", NormalizeCommand("read_pages"));
    EXPECT_FALSE(GetCommand("nosuchcommand"));
}

TEST(TFastShardClientTest, ShouldAcquireDevices)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "acquiredevices",
        {"--device-uuid", "d1", "--device-uuid", "d2", "--generation", "7",
         "--access-mode", "ro", "--fastshard-id", "fs", "--client-id", "cli",
         "--request-timeout", "500"}));

    ASSERT_EQ(1u, f.Storage->AcquireCalls.size());
    const auto& req = f.Storage->AcquireCalls[0];
    ASSERT_EQ(2u, req.DeviceUUIDsSize());
    EXPECT_EQ("d1", req.GetDeviceUUIDs(0));
    EXPECT_EQ("d2", req.GetDeviceUUIDs(1));
    EXPECT_EQ(7u, req.GetGeneration());
    EXPECT_EQ(NCloud::NProto::ACCESS_READ_ONLY, req.GetAccessMode());
    EXPECT_EQ("fs", req.GetFastshardId());
    EXPECT_EQ("cli", req.GetHeaders().GetClientId());
    EXPECT_EQ(500u, req.GetHeaders().GetRequestTimeout());
    EXPECT_EQ("OK\n", f.Output->Str());
}

TEST(TFastShardClientTest, ShouldAcquireDevicesForWritingByDefault)
{
    TFixture f;
    EXPECT_TRUE(f.Run("acquiredevices", {"--device-uuid", "d1"}));

    ASSERT_EQ(1u, f.Storage->AcquireCalls.size());
    const auto& req = f.Storage->AcquireCalls[0];
    EXPECT_EQ(NCloud::NProto::ACCESS_READ_WRITE, req.GetAccessMode());
    EXPECT_EQ(0u, req.GetGeneration());
}

TEST(TFastShardClientTest, ShouldRejectUnknownAccessMode)
{
    TFixture f;
    EXPECT_THROW(
        f.Run("acquiredevices", {"--device-uuid", "d1", "--access-mode", "x"}),
        NLastGetopt::TUsageException);
    EXPECT_TRUE(f.Storage->AcquireCalls.empty());
}

TEST(TFastShardClientTest, ShouldReleaseDevices)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "releasedevices",
        {"--device-uuid", "d1", "--generation", "7", "--fastshard-id", "fs"}));

    ASSERT_EQ(1u, f.Storage->ReleaseCalls.size());
    EXPECT_EQ(7u, f.Storage->ReleaseCalls[0].GetGeneration());
    EXPECT_EQ("fs", f.Storage->ReleaseCalls[0].GetFastshardId());
    ASSERT_EQ(1u, f.Storage->ReleaseCalls[0].DeviceUUIDsSize());
    EXPECT_EQ("d1", f.Storage->ReleaseCalls[0].GetDeviceUUIDs(0));
    EXPECT_EQ("OK\n", f.Output->Str());
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

    ASSERT_EQ(1u, f.Storage->ReadCalls.size());
    const auto& req = f.Storage->ReadCalls[0];
    EXPECT_EQ("d1", req.GetDeviceUUID());
    ASSERT_EQ(1u, req.PageGroupRefsSize());
    EXPECT_EQ(3u, req.GetPageGroupRefs(0).GetFirstPageNo());
    EXPECT_EQ(2u, req.GetPageGroupRefs(0).GetPageCount());
    EXPECT_EQ(4u, req.GetPageGroupRefs(0).GetPageSize());
    EXPECT_EQ("AAAABBBB", f.Output->Str());
}

TEST(TFastShardClientTest, ShouldSplitWriteInputIntoPages)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "writelogrecord",
        {"--device-uuid", "d1", "--first-page-no", "5", "--page-size", "4",
         "--lsn", "10", "--prev-lsn", "9"},
        "abcdefgh"));

    ASSERT_EQ(1u, f.Storage->WriteCalls.size());
    const auto& req = f.Storage->WriteCalls[0];
    EXPECT_EQ("d1", req.GetDeviceUUID());
    EXPECT_EQ(10u, req.GetLogSequenceNumber());
    EXPECT_EQ(9u, req.GetPrevLogSequenceNumber());
    ASSERT_EQ(1u, req.PageGroupsSize());
    EXPECT_EQ(5u, req.GetPageGroups(0).GetFirstPageNo());
    ASSERT_EQ(2u, req.GetPageGroups(0).ContentSize());
    EXPECT_EQ("abcd", req.GetPageGroups(0).GetContent(0));
    EXPECT_EQ("efgh", req.GetPageGroups(0).GetContent(1));
    EXPECT_EQ("OK\n", f.Output->Str());
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
    resp.SetLsnLowWatermark(42);
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

    ASSERT_EQ(1u, f.Storage->ReadJournalTailCalls.size());
    const auto& req = f.Storage->ReadJournalTailCalls[0];
    EXPECT_EQ("d1", req.GetDeviceUUID());
    EXPECT_EQ(40u, req.GetAfterLogSequenceNumber());
    EXPECT_EQ(3u, req.GetMaxRecordCount());
    EXPECT_EQ(
        "LsnLowWatermark: 42\n"
        "Records: 1\n"
        "  Lsn: 41 PrevLsn: 40 [FirstPageNo: 8 Pages: 2 Bytes: 8]\n",
        f.Output->Str());
}

TEST(TFastShardClientTest, ShouldAdvanceLsnLowWatermark)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "advancelsnlowwatermark",
        {"--device-uuid", "d1", "--lsn-low-watermark", "17"}));

    ASSERT_EQ(1u, f.Storage->AdvanceLsnLowWatermarkCalls.size());
    const auto& req = f.Storage->AdvanceLsnLowWatermarkCalls[0];
    EXPECT_EQ("d1", req.GetDeviceUUID());
    EXPECT_EQ(17u, req.GetLsnLowWatermark());
    EXPECT_EQ("OK\n", f.Output->Str());
}

TEST(TFastShardClientTest, ShouldFormatDevice)
{
    TFixture f;
    EXPECT_TRUE(f.Run("formatdevice", {"--device-uuid", "d1"}));

    ASSERT_EQ(1u, f.Storage->FormatCalls.size());
    EXPECT_EQ("d1", f.Storage->FormatCalls[0].GetDeviceUUID());
    EXPECT_FALSE(f.Storage->FormatCalls[0].GetWholeDevice());
    EXPECT_EQ("OK\n", f.Output->Str());
}

TEST(TFastShardClientTest, ShouldFormatWholeDevice)
{
    TFixture f;
    EXPECT_TRUE(
        f.Run("formatdevice", {"--device-uuid", "d1", "--whole-device"}));

    ASSERT_EQ(1u, f.Storage->FormatCalls.size());
    EXPECT_EQ("d1", f.Storage->FormatCalls[0].GetDeviceUUID());
    EXPECT_TRUE(f.Storage->FormatCalls[0].GetWholeDevice());
    EXPECT_EQ("OK\n", f.Output->Str());
}

TEST(TFastShardClientTest, ShouldAcquireAndReleaseDeviceAroundRequest)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "formatdevice",
        {"--acquire", "--acquire-generation", "7", "--acquire-fastshard-id",
         "fs", "--device-uuid", "d1", "--client-id", "cli"}));

    ASSERT_EQ(1u, f.Storage->AcquireCalls.size());
    const auto& acquire = f.Storage->AcquireCalls[0];
    ASSERT_EQ(1u, acquire.DeviceUUIDsSize());
    EXPECT_EQ("d1", acquire.GetDeviceUUIDs(0));
    EXPECT_EQ("cli", acquire.GetHeaders().GetClientId());
    EXPECT_EQ(7u, acquire.GetGeneration());
    EXPECT_EQ(NCloud::NProto::ACCESS_READ_WRITE, acquire.GetAccessMode());
    EXPECT_EQ("fs", acquire.GetFastshardId());

    ASSERT_EQ(1u, f.Storage->FormatCalls.size());

    ASSERT_EQ(1u, f.Storage->ReleaseCalls.size());
    const auto& release = f.Storage->ReleaseCalls[0];
    ASSERT_EQ(1u, release.DeviceUUIDsSize());
    EXPECT_EQ("d1", release.GetDeviceUUIDs(0));
    EXPECT_EQ("cli", release.GetHeaders().GetClientId());
    EXPECT_EQ(7u, release.GetGeneration());
    EXPECT_EQ("fs", release.GetFastshardId());

    EXPECT_EQ("OK\n", f.Output->Str());
}

TEST(TFastShardClientTest, ShouldAcquireWithAccessModeOfCommand)
{
    struct TCase
    {
        const char* Name;
        std::initializer_list<const char*> Args;
        TString Input;
        NCloud::NProto::EAccessMode AccessMode;
    };

    const TCase cases[] = {
        {"readpages",
         {"--acquire", "--device-uuid", "d1", "--page-count", "1"},
         {},
         NCloud::NProto::ACCESS_READ_ONLY},
        {"readjournaltail",
         {"--acquire", "--device-uuid", "d1"},
         {},
         NCloud::NProto::ACCESS_READ_ONLY},
        {"formatdevice",
         {"--acquire", "--device-uuid", "d1"},
         {},
         NCloud::NProto::ACCESS_READ_WRITE},
        {"writelogrecord",
         {"--acquire", "--device-uuid", "d1", "--page-size", "4"},
         "abcd",
         NCloud::NProto::ACCESS_READ_WRITE},
        {"advancelsnlowwatermark",
         {"--acquire", "--device-uuid", "d1", "--lsn-low-watermark", "1"},
         {},
         NCloud::NProto::ACCESS_READ_WRITE},
    };

    for (const auto& c: cases) {
        TFixture f;
        EXPECT_TRUE(f.Run(c.Name, c.Args, c.Input)) << c.Name;
        ASSERT_EQ(1u, f.Storage->AcquireCalls.size()) << c.Name;
        EXPECT_EQ(c.AccessMode, f.Storage->AcquireCalls[0].GetAccessMode())
            << c.Name;
        EXPECT_EQ(1u, f.Storage->ReleaseCalls.size()) << c.Name;
    }
}

TEST(TFastShardClientTest, ShouldRejectAcquireOptionsWithoutAcquire)
{
    for (auto [arg, value]:
         {std::pair{"--acquire-generation", "7"},
          std::pair{"--acquire-fastshard-id", "fs"}})
    {
        TFixture f;
        EXPECT_THROW(
            f.Run("formatdevice", {arg, value, "--device-uuid", "d1"}),
            NLastGetopt::TUsageException)
            << arg;
        EXPECT_TRUE(f.Storage->AcquireCalls.empty()) << arg;
        EXPECT_TRUE(f.Storage->FormatCalls.empty()) << arg;
    }
}

TEST(TFastShardClientTest, ShouldTakeAcquiredDeviceFromProtoRequest)
{
    TFixture f;
    EXPECT_TRUE(f.Run(
        "readjournaltail",
        {"--acquire", "--proto"},
        "DeviceUUID: \"d1\"\n"));

    ASSERT_EQ(1u, f.Storage->AcquireCalls.size());
    EXPECT_EQ("d1", f.Storage->AcquireCalls[0].GetDeviceUUIDs(0));
    EXPECT_EQ(1u, f.Storage->ReadJournalTailCalls.size());
    ASSERT_EQ(1u, f.Storage->ReleaseCalls.size());
    EXPECT_EQ("d1", f.Storage->ReleaseCalls[0].GetDeviceUUIDs(0));
}

TEST(TFastShardClientTest, ShouldNotSendRequestIfAcquireFails)
{
    TFixture f;
    *f.Storage->AcquireResp.MutableError() =
        MakeError(E_REJECTED, "busy");

    EXPECT_FALSE(f.Run("formatdevice", {"--acquire", "--device-uuid", "d1"}));
    EXPECT_EQ(1u, f.Storage->AcquireCalls.size());
    EXPECT_TRUE(f.Storage->FormatCalls.empty());
    EXPECT_TRUE(f.Storage->ReleaseCalls.empty());
    EXPECT_EQ("", f.Output->Str());
}

TEST(TFastShardClientTest, ShouldReleaseDeviceIfRequestFails)
{
    TFixture f;
    *f.Storage->FormatResp.MutableError() = MakeError(E_IO, "io");

    EXPECT_FALSE(f.Run("formatdevice", {"--acquire", "--device-uuid", "d1"}));
    EXPECT_EQ(1u, f.Storage->FormatCalls.size());
    EXPECT_EQ(1u, f.Storage->ReleaseCalls.size());
}

TEST(TFastShardClientTest, ShouldFailIfReleaseFails)
{
    TFixture f;
    *f.Storage->ReleaseResp.MutableError() = MakeError(E_REJECTED, "nope");

    EXPECT_FALSE(f.Run("formatdevice", {"--acquire", "--device-uuid", "d1"}));
    EXPECT_EQ(1u, f.Storage->FormatCalls.size());
    EXPECT_EQ(1u, f.Storage->ReleaseCalls.size());
    EXPECT_EQ("OK\n", f.Output->Str());
}

TEST(TFastShardClientTest, ShouldNotOfferAcquireForAcquireAndRelease)
{
    for (const char* name: {"acquiredevices", "releasedevices"}) {
        TFixture f;
        EXPECT_THROW(
            f.Run(name, {"--acquire", "--device-uuid", "d1"}),
            NLastGetopt::TUsageException)
            << name;
    }
}

TEST(TFastShardClientTest, ShouldAcceptLogLevels)
{
    for (const char* arg:
         {"--verbose", "--verbose=error", "--verbose=warn", "--verbose=info",
          "--verbose=debug", "--verbose=trace"})
    {
        TFixture f;
        EXPECT_TRUE(f.Run("formatdevice", {arg, "--device-uuid", "d1"})) << arg;
        EXPECT_EQ(1u, f.Storage->FormatCalls.size()) << arg;
    }
}

TEST(TFastShardClientTest, ShouldRejectUnknownLogLevel)
{
    TFixture f;
    EXPECT_THROW(
        f.Run("formatdevice", {"--verbose=loud", "--device-uuid", "d1"}),
        NLastGetopt::TUsageException);
}

TEST(TFastShardClientTest, ShouldFailOnErrorResponse)
{
    TFixture f;
    *f.Storage->AcquireResp.MutableError() =
        MakeError(E_ARGUMENT, "bad device");

    EXPECT_FALSE(f.Run("acquiredevices", {"--device-uuid", "d1"}));
    EXPECT_EQ(1u, f.Storage->AcquireCalls.size());
    EXPECT_EQ("", f.Output->Str());
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

    ASSERT_EQ(1u, f.Storage->AcquireCalls.size());
    const auto& req = f.Storage->AcquireCalls[0];
    ASSERT_EQ(1u, req.DeviceUUIDsSize());
    EXPECT_EQ("d1", req.GetDeviceUUIDs(0));
    EXPECT_EQ(3u, req.GetGeneration());
    EXPECT_EQ("cli", req.GetHeaders().GetClientId());

    NCloud::NProto::TAcquireDevicesResponse resp;
    ParseFromTextFormat(*f.Output, resp);
    EXPECT_EQ(E_REJECTED, resp.GetError().GetCode());
    EXPECT_EQ("not now", resp.GetError().GetMessage());
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
