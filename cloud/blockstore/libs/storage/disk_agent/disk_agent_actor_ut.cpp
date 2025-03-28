#include "disk_agent_actor.h"

#include "disk_agent.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/storage/disk_agent/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <contrib/ydb/library/actors/core/mon.h>

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>

#include <util/folder/tempdir.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NServer;
using namespace NThreading;

using namespace NDiskAgentTest;

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TDuration WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

NLWTrace::TQuery QueryFromString(const TString& text)
{
    TStringInput in(text);

    NLWTrace::TQuery query;
    ParseFromTextFormat(in, query);
    return query;
}

TFsPath TryGetRamDrivePath()
{
    auto p = GetRamDrivePath();
    return !p
        ? GetSystemTempDir()
        : p;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestNvmeManager
    : NNvme::INvmeManager
{
    THashMap<TString, TString> PathToSerial;

    explicit TTestNvmeManager(
            const TVector<std::pair<TString, TString>>& pathToSerial)
        : PathToSerial{pathToSerial.cbegin(), pathToSerial.cend()}
    {
    }

    TFuture<NProto::TError> Format(
        const TString& path,
        NNvme::nvme_secure_erase_setting ses) override
    {
        Y_UNUSED(path);
        Y_UNUSED(ses);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) override
    {
        Y_UNUSED(path);
        Y_UNUSED(offsetBytes);
        Y_UNUSED(sizeBytes);

        return MakeFuture<NProto::TError>();
    }

    TResultOrError<TString> GetSerialNumber(const TString& path) override
    {
        const auto filename = TFsPath{path}.Basename();
        auto it = PathToSerial.find(filename);
        if (it == PathToSerial.end()) {
            return MakeError(MAKE_SYSTEM_ERROR(42), filename);
        }

        return it->second;
    }

    TResultOrError<bool> IsSsd(const TString& path) override
    {
        Y_UNUSED(path);

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct THttpGetRequest: NMonitoring::IHttpRequest
{
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;

    const char* GetURI() const override
    {
        return "";
    }

    const char* GetPath() const override
    {
        return "";
    }

    const TCgiParameters& GetParams() const override
    {
        return CgiParameters;
    }

    const TCgiParameters& GetPostParams() const override
    {
        return CgiParameters;
    }

    TStringBuf GetPostContent() const override
    {
        return {};
    }

    HTTP_METHOD GetMethod() const override
    {
        return HTTP_METHOD_GET;
    }

    const THttpHeaders& GetHeaders() const override
    {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;
    const TString CachedSessionsPath =
        TempDir.Path() / "nbs-disk-agent-sessions.txt";
    const TString CachedConfigPath = TempDir.Path() / "nbs-disk-agent.txt";
    const TDuration ReleaseInactiveSessionsTimeout = 10s;

    std::optional<TTestBasicRuntime> Runtime;
    NMonitoring::TDynamicCountersPtr Counters =
        MakeIntrusive<NMonitoring::TDynamicCounters>();

    TVector<TFsPath> Files;

    const ui64 DefaultFileSize = DefaultDeviceBlockSize * DefaultBlocksCount;

    void PrepareFile(const TString& name, size_t size)
    {
        TFile fileData(TempDir.Path() / name, EOpenModeFlag::CreateNew);
        fileData.Resize(size);

        Files.push_back(fileData.GetName());
    }

    auto CreateDiskAgentConfig()
    {
        auto config = DiskAgentConfig();

        auto& discovery = *config.MutableStorageDiscoveryConfig();
        auto& path = *discovery.AddPathConfigs();
        path.SetPathRegExp(TempDir.Path() / "NVMENBS([0-9]{2})");
        auto& pool = *path.AddPoolConfigs();
        pool.SetMaxSize(DefaultFileSize);

        config.SetCachedSessionsPath(CachedSessionsPath);
        config.SetCachedConfigPath(CachedConfigPath);
        config.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        config.SetAcquireRequired(true);
        config.SetReleaseInactiveSessionsTimeout(
            ReleaseInactiveSessionsTimeout.MilliSeconds());
        config.SetDisableBrokenDevices(true);

        return config;
    }

    NProto::TError Write(
        TDiskAgentClient& diskAgent,
        const TString& clientId,
        const TString& deviceId)
    {
        auto request = std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetDeviceUUID(deviceId);
        request->Record.SetStartIndex(1);
        request->Record.SetBlockSize(DefaultBlockSize);

        auto sgList = ResizeIOVector(*request->Record.MutableBlocks(), 10, 4_KB);

        for (auto& buffer: sgList) {
            memset(const_cast<char*>(buffer.Data()), 'Y', buffer.Size());
        }

        diskAgent.SendRequest(std::move(request));
        Runtime->DispatchEvents({});
        auto response = diskAgent.RecvWriteDeviceBlocksResponse();

        return response->Record.GetError();
    }

    NProto::TError Read(
        TDiskAgentClient& diskAgent,
        const TString& clientId,
        const TString& deviceId)
    {
        const auto response = ReadDeviceBlocks(
            *Runtime, diskAgent, deviceId, 1, 10, clientId);

        return response->Record.GetError();
    }

    auto LoadSessionCache()
    {
        NProto::TDiskAgentDeviceSessionCache cache;
        ParseProtoTextFromFileRobust(CachedSessionsPath, cache);

        TVector<NProto::TDiskAgentDeviceSession> sessions(
            std::make_move_iterator(cache.MutableSessions()->begin()),
            std::make_move_iterator(cache.MutableSessions()->end())
        );

        SortBy(sessions, [] (auto& session) {
            return session.GetClientId();
        });

        for (auto& session: sessions) {
            Sort(*session.MutableDeviceIds());
        }

        return sessions;
    }

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Runtime.emplace();

        InitCriticalEventsCounter(Counters);

        PrepareFile("NVMENBS01", DefaultFileSize);
        PrepareFile("NVMENBS02", DefaultFileSize);
        PrepareFile("NVMENBS03", DefaultFileSize);
        PrepareFile("NVMENBS04", DefaultFileSize);
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        for (const auto& path: Files) {
            path.DeleteIfExists();
        }
    }
};

struct TCopyRangeFixture: public NUnitTest::TBaseFixture
{
    using TEvDirectCopyBlocksRequest = TEvDiskAgent::TEvDirectCopyBlocksRequest;
    using TEvDirectCopyBlocksResponse =
        TEvDiskAgent::TEvDirectCopyBlocksResponse;

    const ui32 BlockSize = DefaultBlockSize;
    const ui32 BlockCount = 5;
    const ui64 SourceStartIndex = 5;
    const ui64 TargetStartIndex = 7;

    const TString ClientId = "client-id";
    const TString SourceClientId = "client-reader";
    const TString TargetClientId = "client-writer";

    std::optional<TTestBasicRuntime> Runtime;
    std::optional<TTestEnv> Environment;
    std::optional<TDiskAgentClient> DiskAgent1;
    std::optional<TDiskAgentClient> DiskAgent2;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Runtime.emplace(2);
        Environment.emplace(TTestEnvBuilder(*Runtime)
                        .With(DiskAgentConfig1())
                        .WithSecondAgent(DiskAgentConfig2())
                        .Build());

        DiskAgent1.emplace(*Runtime, 0);
        DiskAgent2.emplace(*Runtime, 1);

        DiskAgent1->WaitReady();
        DiskAgent2->WaitReady();

        DiskAgent1->AcquireDevices(
            TVector<TString>({"DA1-1"}),
            ClientId,
            NProto::VOLUME_ACCESS_READ_WRITE);
        DiskAgent1->AcquireDevices(
            TVector<TString>({"DA1-1"}),
            SourceClientId,
            NProto::VOLUME_ACCESS_READ_ONLY);
        DiskAgent1->AcquireDevices(
            TVector<TString>({"DA1-2"}),
            TargetClientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        DiskAgent2->AcquireDevices(
            TVector<TString>({"DA2-1", "DA2-2"}),
            TargetClientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        PrepareContent();
    }

    static NProto::TDiskAgentConfig DiskAgentConfig1()
    {
        auto config = DiskAgentConfig({
            "DA1-1",
            "DA1-2",
        });
        config.SetOffloadAllIORequestsParsingEnabled(false);
        config.SetIOParserActorCount(0);
        config.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);

        auto* throttling = config.MutableThrottlingConfig();
        throttling->SetDirectCopyBandwidthFraction(0.5);
        throttling->SetDefaultNetworkMbitThroughput(800);

        for (const auto& memDevice: config.GetMemoryDevices()) {
            *config.AddMemoryDevices() = PrepareMemoryDevice(
                memDevice.GetDeviceId(),
                DefaultBlockSize,
                100 * DefaultBlockSize);
        }

        return config;
    }

    static NProto::TDiskAgentConfig DiskAgentConfig2()
    {
        auto config = DiskAgentConfig({
            "DA2-1",
            "DA2-2",
        });
        config.SetOffloadAllIORequestsParsingEnabled(false);
        config.SetIOParserActorCount(0);
        config.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);

        for (const auto& memDevice: config.GetMemoryDevices()) {
            *config.AddMemoryDevices() = PrepareMemoryDevice(
                memDevice.GetDeviceId(),
                DefaultBlockSize,
                100 * DefaultBlockSize);
        }

        return config;
    }

    void WriteBlocks(
        TDiskAgentClient& diskAgent,
        const TString& clientId,
        const TString& deviceId,
        TBlockRange64 range,
        char pattern) const
    {
        TVector<TString> blocks;
        auto sglist =
            ResizeBlocks(blocks, range.Size(), TString(BlockSize, pattern));
        diskAgent.WriteDeviceBlocks(deviceId, range.Start, sglist, clientId);
    }

    TString ReadBlock(
        TDiskAgentClient& diskAgent,
        const TString& deviceId,
        ui64 startIndex) const
    {
        auto request = std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(TString(BackgroundOpsClientId));
        request->Record.SetDeviceUUID(deviceId);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlocksCount(1);

        diskAgent.SendRequest(std::move(request));
        const auto response = diskAgent.RecvReadDeviceBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

        return response->Record.GetBlocks().GetBuffers(0);
    }

    TString ReadBlocks(
        TDiskAgentClient& diskAgent,
        const TString& deviceId,
        ui64 blockCount = 12) const
    {
        TString data;
        for (size_t i = 0; i < blockCount; ++i) {
            auto buffer = ReadBlock(diskAgent, deviceId, i);
            char c = buffer ? buffer[0] : 0;
            data.push_back(c == 0 ? '.' : c);
        }
        return data;
    }

    void PrepareContent()
    {
        // DiskAgent 1
        //   DA1-1: .....XY..... (. - mean \0)
        //   DA1-2: ffffffffffff
        // DiskAgent 2
        //   DA2-1: ............
        //   DA2-2: ffffffffffff


        // Prepare content on source device DA1-1
        WriteBlocks(
            *DiskAgent1,
            ClientId,
            "DA1-1",
            TBlockRange64::MakeOneBlock(SourceStartIndex),
            'X');
        WriteBlocks(
            *DiskAgent1,
            ClientId,
            "DA1-1",
            TBlockRange64::MakeOneBlock(SourceStartIndex + 1),
            'Y');

        // Prepare content on target devices
        WriteBlocks(
            *DiskAgent1,
            TargetClientId,
            "DA1-2",
            TBlockRange64::WithLength(0, 12),
            'f');
        WriteBlocks(
            *DiskAgent2,
            TargetClientId,
            "DA2-2",
            TBlockRange64::WithLength(0, 12),
            'f');

        // Check content on DiskAgent 1
        UNIT_ASSERT_VALUES_EQUAL(
            ".....XY.....",
            ReadBlocks(*DiskAgent1, "DA1-1"));
        UNIT_ASSERT_VALUES_EQUAL(
            "ffffffffffff",
            ReadBlocks(*DiskAgent2, "DA2-2"));

        // Check content on DiskAgent 2
        UNIT_ASSERT_VALUES_EQUAL(
            "............",
            ReadBlocks(*DiskAgent2, "DA2-1"));
        UNIT_ASSERT_VALUES_EQUAL(
            "ffffffffffff",
            ReadBlocks(*DiskAgent2, "DA2-2"));
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskAgentTest)
{
    Y_UNIT_TEST(ShouldRegisterDevicesOnStartup)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig()
                | WithBackend(NProto::DISK_AGENT_BACKEND_SPDK)
                | WithMemoryDevices({
                    MemoryDevice("MemoryDevice1") | WithPool("memory"),
                    MemoryDevice("MemoryDevice2")
                })
                | WithFileDevices({
                    FileDevice("FileDevice3"),
                    FileDevice("FileDevice4") | WithPool("local-ssd")
                })
                | WithNVMeDevices({
                    NVMeDevice("nvme1", {"NVMeDevice5"}) | WithPool("nvme"),
                    NVMeDevice("nvme2", {"NVMeDevice6"})
                }))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const auto& devices = env.DiskRegistryState->Devices;
        UNIT_ASSERT_VALUES_EQUAL(6, devices.size());

        // common properties
        for (auto& [uuid, device]: devices) {
            UNIT_ASSERT_VALUES_EQUAL(uuid, device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(uuid, device.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("the-rack", device.GetRack());
        }

        {
            auto& device = devices.at("MemoryDevice1");
            UNIT_ASSERT_VALUES_EQUAL("", device.GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("memory", device.GetPoolName());
            UNIT_ASSERT_VALUES_EQUAL(DefaultDeviceBlockSize, device.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlocksCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL("agent:", device.GetBaseName());
        }

        {
            auto& device = devices.at("MemoryDevice2");
            UNIT_ASSERT_VALUES_EQUAL("", device.GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("", device.GetPoolName());
            UNIT_ASSERT_VALUES_EQUAL(DefaultDeviceBlockSize, device.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlocksCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL("agent:", device.GetBaseName());
        }

        {
            auto& device = devices.at("FileDevice3");
            UNIT_ASSERT_VALUES_EQUAL("", device.GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("", device.GetPoolName());
            UNIT_ASSERT_VALUES_EQUAL(DefaultDeviceBlockSize, device.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultStubBlocksCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL("agent:", device.GetBaseName());
        }

        {
            auto& device = devices.at("FileDevice4");
            UNIT_ASSERT_VALUES_EQUAL("", device.GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("local-ssd", device.GetPoolName());
            UNIT_ASSERT_VALUES_EQUAL(DefaultDeviceBlockSize, device.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultStubBlocksCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL("agent:", device.GetBaseName());
        }

        {
            auto& device = devices.at("NVMeDevice5");
            UNIT_ASSERT_VALUES_EQUAL("", device.GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("nvme", device.GetPoolName());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, device.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultStubBlocksCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL("agent:nvme1:", device.GetBaseName());
        }

        {
            auto& device = devices.at("NVMeDevice6");
            UNIT_ASSERT_VALUES_EQUAL("", device.GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("", device.GetPoolName());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, device.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultStubBlocksCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL("agent:nvme2:", device.GetBaseName());
        }
    }

    Y_UNIT_TEST(ShouldAcquireAndReleaseDevices)
    {
        TTestBasicRuntime runtime;

        const TVector<TString> uuids {
            "MemoryDevice1",
            "MemoryDevice2",
            "MemoryDevice3"
        };

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig(uuids))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        diskAgent.AcquireDevices(
            uuids,
            "client-1",
            NProto::VOLUME_ACCESS_READ_WRITE
        );
        diskAgent.ReleaseDevices(uuids, "client-1");
    }

    Y_UNIT_TEST(ShouldAcquireWithRateLimits)
    {
        TTestBasicRuntime runtime;

        const TVector<TString> uuids {
            "MemoryDevice1",
            "MemoryDevice2",
            "MemoryDevice3"
        };

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig(uuids))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        NSpdk::TDeviceRateLimits limits {
            .IopsLimit = 1000
        };

        diskAgent.AcquireDevices(
            uuids,
            "client-1",
            NProto::VOLUME_ACCESS_READ_WRITE,
            0,  // mountSeqNumber
            "", // diskId
            0,  // volumeGeneration
            limits);
        diskAgent.ReleaseDevices(uuids, "client-1");
    }

    Y_UNIT_TEST(ShouldAcquireDevicesOnlyOnce)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({"MemoryDevice1"}))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(env.DiskRegistryState->Devices.contains("MemoryDevice1"));

        const TVector<TString> uuids{
            "MemoryDevice1"
        };

        {
            auto response = diskAgent.AcquireDevices(
                uuids,
                "client-id",
                NProto::VOLUME_ACCESS_READ_WRITE
            );
            UNIT_ASSERT(!HasError(response->Record));
        }

        {
            diskAgent.SendAcquireDevicesRequest(
                uuids,
                "client-id2",
                NProto::VOLUME_ACCESS_READ_WRITE
            );
            auto response = diskAgent.RecvAcquireDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_INVALID_SESSION);
            UNIT_ASSERT(response->GetErrorReason().Contains("already acquired"));
        }

        // reacquire with same client id is ok
        {
            auto response = diskAgent.AcquireDevices(
                uuids,
                "client-id",
                NProto::VOLUME_ACCESS_READ_WRITE
            );
            UNIT_ASSERT(!HasError(response->Record));
        }
    }

    Y_UNIT_TEST(ShouldAcquireDevicesWithMountSeqNumber)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({"MemoryDevice1"}))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1"
        };

        {
            auto response = diskAgent.AcquireDevices(
                uuids,
                "client-id",
                NProto::VOLUME_ACCESS_READ_WRITE
            );
            UNIT_ASSERT(!HasError(response->Record));
        }

        {
            diskAgent.SendAcquireDevicesRequest(
                uuids,
                "client-id2",
                NProto::VOLUME_ACCESS_READ_WRITE,
                1
            );
            auto response = diskAgent.RecvAcquireDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            diskAgent.SendAcquireDevicesRequest(
                uuids,
                "client-id3",
                NProto::VOLUME_ACCESS_READ_WRITE,
                2
            );
            auto response = diskAgent.RecvAcquireDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            diskAgent.SendAcquireDevicesRequest(
                uuids,
                "client-id4",
                NProto::VOLUME_ACCESS_READ_WRITE,
                2
            );
            auto response = diskAgent.RecvAcquireDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldAcquireDevicesOnlyOnceInGroup)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({"MemoryDevice1", "MemoryDevice2"}))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(env.DiskRegistryState->Devices.size(), 2);
        UNIT_ASSERT(env.DiskRegistryState->Devices.contains("MemoryDevice1"));
        UNIT_ASSERT(env.DiskRegistryState->Devices.contains("MemoryDevice2"));

        {
            const TVector<TString> uuids{
                "MemoryDevice1"
            };

            auto response = diskAgent.AcquireDevices(
                uuids,
                "client-id",
                NProto::VOLUME_ACCESS_READ_WRITE
            );
            UNIT_ASSERT(!HasError(response->Record));
        }

        {
            const TVector<TString> uuids{
                "MemoryDevice1",
                "MemoryDevice2"
            };

            diskAgent.SendAcquireDevicesRequest(
                uuids,
                "client-id2",
                NProto::VOLUME_ACCESS_READ_WRITE
            );
            auto response = diskAgent.RecvAcquireDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_INVALID_SESSION);
            UNIT_ASSERT(response->GetErrorReason().Contains("already acquired"));
            UNIT_ASSERT(response->GetErrorReason().Contains(
                "MemoryDevice1"
            ));
        }

        // reacquire with same client id is ok
        {
            const TVector<TString> uuids{
                "MemoryDevice1"
            };

            auto response = diskAgent.AcquireDevices(
                uuids,
                "client-id",
                NProto::VOLUME_ACCESS_READ_WRITE
            );
            UNIT_ASSERT(!HasError(response->Record));
        }
    }

    Y_UNIT_TEST(ShouldRejectAcquireNonexistentDevice)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({"MemoryDevice1"}))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1",
            "nonexistent uuid"
        };

        diskAgent.SendAcquireDevicesRequest(
            uuids,
            "client-id",
            NProto::VOLUME_ACCESS_READ_WRITE
        );
        auto response = diskAgent.RecvAcquireDevicesResponse();
        UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_NOT_FOUND);
        UNIT_ASSERT(response->GetErrorReason().Contains(uuids.back()));
    }

    Y_UNIT_TEST(ShouldRejectAcquireAndReleaseWhenPartiallySuspended)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({
                "MemoryDevice1",
                "MemoryDevice2",
                "MemoryDevice3",
            }))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1",
            "MemoryDevice2"
        };

        const TString writerClientId1 = "writer-1";
        const TString writerClientId2 = "writer-2";

        diskAgent.AcquireDevices(
            uuids,
            writerClientId1,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        constexpr auto BlocksCount = 10;
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            BlocksCount,
            TString(DefaultBlockSize, 'X'));

        const auto response = WriteDeviceBlocks(
            runtime,
            diskAgent,
            uuids[0],
            0,
            sglist,
            writerClientId1);
        UNIT_ASSERT(!HasError(response->GetError()));

        constexpr auto CancelSuspensionDelay = TDuration::Minutes(1);
        diskAgent.PartiallySuspendAgent(CancelSuspensionDelay);

        // Acquire of an already existing session works fine.
        diskAgent.AcquireDevices(
            uuids,
            writerClientId1,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        // Can't acquire a new session when an agent is partially suspended.
        diskAgent.SendAcquireDevicesRequest(
            TVector<TString>{"MemoryDevice3"},
            writerClientId2,
            NProto::VOLUME_ACCESS_READ_WRITE);
        {
            auto response = diskAgent.RecvAcquireDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_REJECTED);
            UNIT_ASSERT(response->GetErrorReason().Contains(
                "Disk agent is partially suspended"));
        }

        // Can't re-acquire existing session with a new one.
        diskAgent.SendAcquireDevicesRequest(
            uuids,
            writerClientId2,
            NProto::VOLUME_ACCESS_READ_WRITE);
        {
            auto response = diskAgent.RecvAcquireDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_REJECTED);
            UNIT_ASSERT(response->GetErrorReason().Contains(
                "Disk agent is partially suspended"));
        }

        // Can't release a session when an agent is partially suspended.
        diskAgent.SendReleaseDevicesRequest(
            uuids,
            writerClientId1);
        {
            auto response = diskAgent.RecvReleaseDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_REJECTED);
            UNIT_ASSERT(response->GetErrorReason().Contains(
                "Disk agent is partially suspended"));
        }

        runtime.AdvanceCurrentTime(CancelSuspensionDelay / 2);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // Still can't re-acquire existing session with a new one.
        diskAgent.SendAcquireDevicesRequest(
            uuids,
            writerClientId2,
            NProto::VOLUME_ACCESS_READ_WRITE);
        {
            auto response = diskAgent.RecvAcquireDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_REJECTED);
            UNIT_ASSERT(response->GetErrorReason().Contains(
                "Disk agent is partially suspended"));
        }

        runtime.AdvanceCurrentTime(CancelSuspensionDelay);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // After a delay disk agent returns to normal work.
        diskAgent.AcquireDevices(
            TVector<TString>{"MemoryDevice3"},
            writerClientId2,
            NProto::VOLUME_ACCESS_READ_WRITE
        );
        diskAgent.AcquireDevices(
            uuids,
            writerClientId1,
            NProto::VOLUME_ACCESS_READ_WRITE
        );
    }

    Y_UNIT_TEST(ShouldPerformIo)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({
                "MemoryDevice1",
                "MemoryDevice2",
                "MemoryDevice3",
            }))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1",
            "MemoryDevice2"
        };

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        const auto blocksCount = 10;

        {
            const auto response = ReadDeviceBlocks(
                runtime, diskAgent, uuids[0], 0, blocksCount, clientId);

            const auto& record = response->Record;

            UNIT_ASSERT(record.HasBlocks());

            const auto& iov = record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(iov.BuffersSize(), blocksCount);
            for (auto& buffer : iov.GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), DefaultBlockSize);
            }
        }

        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                blocksCount,
                TString(DefaultBlockSize, 'X'));

            WriteDeviceBlocks(runtime, diskAgent, uuids[0], 0, sglist, clientId);
            ZeroDeviceBlocks(runtime, diskAgent, uuids[0], 0, 10, clientId);
        }

        {
            auto request = std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(uuids[0]);
            request->Record.SetStartIndex(0);
            request->Record.SetBlockSize(DefaultBlockSize);

            auto sgList = ResizeIOVector(*request->Record.MutableBlocks(), 1, 1_MB);

            for (auto& buffer: sgList) {
                memset(const_cast<char*>(buffer.Data()), 'Y', buffer.Size());
            }

            diskAgent.SendRequest(std::move(request));
            runtime.DispatchEvents(NActors::TDispatchOptions());
            auto response = diskAgent.RecvWriteDeviceBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), S_OK);
        }
    }

    Y_UNIT_TEST(ShouldDenyOverlappedRequests)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
                       .With(DiskAgentConfig({
                           "MemoryDevice1",
                           "MemoryDevice2",
                           "MemoryDevice3",
                       }))
                       .With([] {
                           NProto::TStorageServiceConfig config;
                           config.SetRejectLateRequestsAtDiskAgentEnabled(true);
                           return config;
                       }())
                       .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{"MemoryDevice1", "MemoryDevice2"};

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        auto writeRequest = [&](ui64 volumeRequestId,
                                ui64 blockStart,
                                ui32 blockCount,
                                EWellKnownResultCodes expected) {
            auto request =
                std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(uuids[0]);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetVolumeRequestId(volumeRequestId);

            auto sgList =
                ResizeIOVector(*request->Record.MutableBlocks(), 1, blockCount * DefaultBlockSize);

            for (auto& buffer : sgList) {
                memset(const_cast<char*>(buffer.Data()), 'Y', buffer.Size());
            }

            diskAgent.SendRequest(std::move(request));
            runtime.DispatchEvents(NActors::TDispatchOptions());
            auto response = diskAgent.RecvWriteDeviceBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(expected, response->GetStatus());
        };

        auto zeroRequest = [&](ui64 volumeRequestId,
                               ui64 blockStart,
                               ui32 blockCount,
                               EWellKnownResultCodes expected) {
            auto request =
                std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(uuids[0]);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetBlocksCount(blockCount);
            request->Record.SetVolumeRequestId(volumeRequestId);

            diskAgent.SendRequest(std::move(request));
            runtime.DispatchEvents(NActors::TDispatchOptions());
            auto response = diskAgent.RecvZeroDeviceBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(expected, response->GetStatus());
        };
        {
            auto requestId = TCompositeId::FromGeneration(2);
            writeRequest(requestId.GetValue(), 1024, 10, S_OK);

            // Same requestId rejected.
            writeRequest(requestId.GetValue(), 1024, 10, E_REJECTED);

            // Next requestId accepted.
            writeRequest(requestId.Advance(), 1024, 10, S_OK);

            // Fill zeros block started at 64k
            zeroRequest(requestId.Advance(), 2048, 1024, S_OK);
        }

        {
            // requestId from past (previous generation) with full overlap should return S_ALREADY.
            auto requestId = TCompositeId::FromGeneration(1);
            writeRequest(requestId.Advance(), 1024, 10, S_ALREADY);
            writeRequest(requestId.Advance(), 2048, 10, S_ALREADY);

            zeroRequest(requestId.Advance(), 1024, 8, S_ALREADY);
            zeroRequest(requestId.Advance(), 2048, 8, S_ALREADY);

            // partial overlapped request should return E_REJECTED.
            writeRequest(requestId.Advance(), 1000, 30, E_REJECTED);
            zeroRequest(requestId.Advance(), 2000, 50, E_REJECTED);
        }
    }

    Y_UNIT_TEST(ShouldDelayOverlappedRequests)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
                       .With(DiskAgentConfig({
                           "MemoryDevice1",
                           "MemoryDevice2",
                           "MemoryDevice3",
                       }))
                       .With([] {
                           NProto::TStorageServiceConfig config;
                           config.SetRejectLateRequestsAtDiskAgentEnabled(true);
                           return config;
                       }())
                       .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{"MemoryDevice1", "MemoryDevice2"};

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        // Steal TEvWriteOrZeroCompleted message.
        // We will return it later to start the execution of the next overlapped
        // request.
        std::vector<std::unique_ptr<IEventHandle>> stolenWriteCompletedRequests;
        auto stealFirstDeviceRequest = [&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() ==
                TEvDiskAgentPrivate::EvWriteOrZeroCompleted)
            {
                stolenWriteCompletedRequests.push_back(
                    std::unique_ptr<IEventHandle>{event.Release()});
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        auto oldObserverFunc = runtime.SetObserverFunc(stealFirstDeviceRequest);

        auto sendWriteRequest = [&](ui64 volumeRequestId,
                                    ui64 blockStart,
                                    ui32 blockCount) {
            auto request =
                std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(uuids[0]);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetVolumeRequestId(volumeRequestId);

            auto sgList = ResizeIOVector(
                *request->Record.MutableBlocks(),
                1,
                blockCount * DefaultBlockSize);

            for (auto& buffer : sgList) {
                memset(const_cast<char*>(buffer.Data()), 'Y', buffer.Size());
            }

            diskAgent.SendRequest(std::move(request));
        };

        auto sendZeroRequest = [&](ui64 volumeRequestId,
                                   ui64 blockStart,
                                   ui32 blockCount) {
            auto request =
                std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(uuids[0]);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetBlocksCount(blockCount);
            request->Record.SetVolumeRequestId(volumeRequestId);

            diskAgent.SendRequest(std::move(request));
        };

        // Send write and zero requests. Their messages TWriteOrZeroCompleted will be stolen.
        sendWriteRequest(100, 1024, 10);
        sendZeroRequest(101, 2048, 10);

        runtime.DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(!stolenWriteCompletedRequests.empty());
        // N.B. We are delaying the internal TEvWriteOrZeroCompleted request.
        // The response to the client was not delayed, so here we receive write and zero responses.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            diskAgent.RecvZeroDeviceBlocksResponse()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            diskAgent.RecvWriteDeviceBlocksResponse()->GetStatus());

        // Turning off the theft of TWriteOrZeroCompleted messages.
        runtime.SetObserverFunc(oldObserverFunc);

        {
            // Send new write request (from past) with range NOT overlapped with
            // delayed request. New request is executed immediately.
            sendWriteRequest(90, 3072, 10);
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                diskAgent.RecvWriteDeviceBlocksResponse()->GetStatus());
        }
        {
            // Send new zero request (from past) with range NOT overlapped with
            // delayed request. New request is executed immediately.
            sendZeroRequest(91, 4096, 10);
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                diskAgent.RecvZeroDeviceBlocksResponse()->GetStatus());
        }
        {
            // Send new write request (from past) with range overlapped with
            // delayed request. New request will be delayed untill overlapped
            // request completed. Request will be completed with E_ALREADY as it
            // fits into the already completed request.
            sendWriteRequest(98, 1024, 5);
            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<
                TEvDiskAgent::TEvWriteDeviceBlocksResponse>(
                handle,
                TDuration::Seconds(1));
            UNIT_ASSERT_EQUAL(nullptr, handle);
        }
        {
            // Send new zero request (from past) with range partial overlapped
            // with delayed request. New request will be delayed untill
            // overlapped request completed and comleted with E_REJECTED since
            // it partially intersects with the already completed request.
            sendZeroRequest(99, 2048, 15);
            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<
                TEvDiskAgent::TEvZeroDeviceBlocksResponse>(
                handle,
                TDuration::Seconds(1));
            UNIT_ASSERT_EQUAL(nullptr, handle);
        }
        {
            // Send new write request (from future) with range overlapped with
            // delayed request. New request is executed immediately.
            sendWriteRequest(110, 1024, 5);
            auto response =
                diskAgent.RecvWriteDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
        {
            // Send new zero request (from future) with range overlapped with
            // delayed request. New request is executed immediately.
            sendWriteRequest(111, 2048, 3);
            auto response =
                diskAgent.RecvWriteDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // Return all stolen request - write/zero request will be completed
        // after this
        for (auto& request: stolenWriteCompletedRequests) {
            runtime.Send(request.release());
        }

        {
            // Get reponse for delayed write request
            auto response =
                diskAgent.RecvWriteDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, response->GetStatus());
        }
        {
            // Get reponse for delayed zero request
            auto response =
                diskAgent.RecvZeroDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldRejectSecureEraseWhenInflightIosPresent)
    {
        TTestBasicRuntime runtime;

        const TVector<TString> uuids = {"MemoryDevice1"};

        auto env = TTestEnvBuilder(runtime)
                       .With(DiskAgentConfig(uuids))
                       .With([] {
                           NProto::TStorageServiceConfig config;
                           config.SetRejectLateRequestsAtDiskAgentEnabled(true);
                           return config;
                       }())
                       .Build();

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        // Steal TEvWriteOrZeroCompleted message.
        std::vector<std::unique_ptr<IEventHandle>> stolenWriteCompletedRequests;
        auto stealFirstDeviceRequest = [&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() ==
                TEvDiskAgentPrivate::EvWriteOrZeroCompleted)
            {
                stolenWriteCompletedRequests.push_back(
                    std::unique_ptr<IEventHandle>{event.Release()});
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        auto oldObserverFunc = runtime.SetObserverFunc(stealFirstDeviceRequest);

        auto sendWriteRequest = [&](ui64 volumeRequestId,
                                    ui64 blockStart,
                                    ui32 blockCount) {
            auto request =
                std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(uuids[0]);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetVolumeRequestId(volumeRequestId);

            auto sgList = ResizeIOVector(
                *request->Record.MutableBlocks(),
                1,
                blockCount * DefaultBlockSize);

            for (auto& buffer : sgList) {
                memset(const_cast<char*>(buffer.Data()), 'Y', buffer.Size());
            }

            diskAgent.SendRequest(std::move(request));
        };

        // Send write request. It's message TWriteOrZeroCompleted will be stolen.
        sendWriteRequest(100, 1024, 10);
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(!stolenWriteCompletedRequests.empty());

        // N.B. We are delaying the internal TEvWriteOrZeroCompleted request.
        // The response to the client was not delayed, so here we receive write and response.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            diskAgent.RecvWriteDeviceBlocksResponse()->GetStatus());

        // secure erase will be rejected since we have inflight write
        diskAgent.SendSecureEraseDeviceRequest(uuids[0]);
        auto response = diskAgent.RecvSecureEraseDeviceResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response->Record.GetError().GetCode());

        auto counter = counters->GetCounter(
            "AppCriticalEvents/DiskAgentSecureEraseDuringIo",
            true);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 1);
    }

    Y_UNIT_TEST(ShouldRejectMultideviceOverlappedRequests)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
                       .With(DiskAgentConfig({
                           "MemoryDevice1",
                           "MemoryDevice2",
                           "MemoryDevice3",
                       }))
                       .With([] {
                           NProto::TStorageServiceConfig config;
                           config.SetRejectLateRequestsAtDiskAgentEnabled(true);
                           return config;
                       }())
                       .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{"MemoryDevice1", "MemoryDevice2"};

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        // Steal TEvWriteOrZeroCompleted request.
        // We will return it later to start the execution of the next overlapped
        // request.
        std::vector<std::unique_ptr<IEventHandle>> stolenWriteCompletedRequests;
        auto stealFirstDeviceRequest = [&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() ==
                TEvDiskAgentPrivate::EvWriteOrZeroCompleted)
            {
                stolenWriteCompletedRequests.push_back(
                    std::unique_ptr<IEventHandle>{event.Release()});
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        auto oldObserverFunc = runtime.SetObserverFunc(stealFirstDeviceRequest);

        auto sendZeroRequest = [&](ui64 volumeRequestId,
                                   ui64 blockStart,
                                   ui32 blockCount) {
            auto request =
                std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(uuids[0]);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetBlocksCount(blockCount);
            request->Record.SetVolumeRequestId(volumeRequestId);
            request->Record.SetMultideviceRequest(
                true); // This will lead to E_REJECT

            diskAgent.SendRequest(std::move(request));
        };

        // Send zero request. The TWriteOrZeroCompleted message from this request will
        // be stolen.
        sendZeroRequest(100, 2048, 16);
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(!stolenWriteCompletedRequests.empty());
        // N.B. We are delaying the internal TEvWriteOrZeroCompleted request.
        // The response to the client was not delayed, so here we receive a
        // ZeroDeviceBlocksResponse.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            diskAgent.RecvZeroDeviceBlocksResponse()->GetStatus());

        // Turning off the theft of responses.
        runtime.SetObserverFunc(oldObserverFunc);

        {
            // Send new zero request (from past) with range coverred by delayed
            // request. This request will be delayed untill overlapped request
            // completed.
            sendZeroRequest(98, 2048, 8);
            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<
                TEvDiskAgent::TEvWriteDeviceBlocksResponse>(
                handle,
                TDuration::Seconds(1));
            UNIT_ASSERT_EQUAL(nullptr, handle);
        }

        // Return all stolen request - write/zero request will be completed
        // after this
        for (auto& request: stolenWriteCompletedRequests) {
            runtime.Send(request.release());
        }
        {
            // Get reponse for delayed zero request. It will rejected
            auto response =
                diskAgent.RecvZeroDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldNotMessDifferentDevicesRequests)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
                       .With(DiskAgentConfig({
                           "MemoryDevice1",
                           "MemoryDevice2",
                       }))
                       .With([] {
                           NProto::TStorageServiceConfig config;
                           config.SetRejectLateRequestsAtDiskAgentEnabled(true);
                           return config;
                       }())
                       .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{"MemoryDevice1", "MemoryDevice2"};

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        // Steal TEvWriteOrZeroCompleted request.
        // We will return it later. This should not delay the request of another device.
        std::vector<std::unique_ptr<IEventHandle>> stolenWriteCompletedRequests;
        auto stealFirstDeviceRequest = [&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() ==
                TEvDiskAgentPrivate::EvWriteOrZeroCompleted)
            {
                stolenWriteCompletedRequests.push_back(
                    std::unique_ptr<IEventHandle>{event.Release()});
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        auto oldObserverFunc = runtime.SetObserverFunc(stealFirstDeviceRequest);

        auto sendZeroRequest = [&](ui64 volumeRequestId,
                                   ui64 blockStart,
                                   ui32 blockCount,
                                   const TString& deviceUUID) {
            auto request =
                std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(deviceUUID);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetBlocksCount(blockCount);
            request->Record.SetVolumeRequestId(volumeRequestId);

            diskAgent.SendRequest(std::move(request));
        };

        // Send zero request. The TWriteOrZeroCompleted message from this request will
        // be stolen.
        sendZeroRequest(100, 2048, 16, uuids[0]);
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(!stolenWriteCompletedRequests.empty());
        // N.B. We are delaying the internal TEvWriteOrZeroCompleted request.
        // The response to the client was not delayed, so here we receive a
        // ZeroDeviceBlocksResponse.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            diskAgent.RecvZeroDeviceBlocksResponse()->GetStatus());

        // Turning off the theft of responses.
        runtime.SetObserverFunc(oldObserverFunc);

        {
            // Send new zero request (from past) with range coverred by delayed
            // request. This request will not be delayed as it relates to another device.
            sendZeroRequest(98, 2048, 8, uuids[1]);
            auto response =
                diskAgent.RecvZeroDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
    }

    void DoShouldHandleSameRequestId(bool monitoringMode)
    {
        TTestBasicRuntime runtime;
        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);

        auto env = TTestEnvBuilder(runtime)
                       .With(DiskAgentConfig({
                           "MemoryDevice1",
                           "MemoryDevice2",
                       }))
                       .With(
                           [monitoringMode]()
                           {
                               NProto::TStorageServiceConfig config;
                               config.SetRejectLateRequestsAtDiskAgentEnabled(
                                   !monitoringMode);
                               return config;
                           }())
                       .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{"MemoryDevice1", "MemoryDevice2"};

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        auto sendZeroRequest = [&](ui64 volumeRequestId,
                                   ui64 blockStart,
                                   ui32 blockCount,
                                   const TString& deviceUUID)
        {
            auto request =
                std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(deviceUUID);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetBlocksCount(blockCount);
            request->Record.SetVolumeRequestId(volumeRequestId);

            diskAgent.SendRequest(std::move(request));
        };

        {
            sendZeroRequest(100, 2048, 16, uuids[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                diskAgent.RecvZeroDeviceBlocksResponse()->GetStatus());
        }
        {
            // Send first request once again with same requestId.
            sendZeroRequest(100, 2048, 8, uuids[0]);
            auto response =
                diskAgent.RecvZeroDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(
                monitoringMode ? S_OK : E_REJECTED,
                response->GetStatus());
        }
        {
            // Send request to another device.
            sendZeroRequest(100, 2048, 8, uuids[1]);
            auto response =
                diskAgent.RecvZeroDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }


        auto unexpectedIdentifierRepetition = counters->GetCounter(
            "AppCriticalEvents/UnexpectedIdentifierRepetition",
            true);
        UNIT_ASSERT_VALUES_EQUAL(
            monitoringMode ? 2 : 1,
            unexpectedIdentifierRepetition->Val());
    }

    Y_UNIT_TEST(ShouldHandleSameRequestIdMonitoringOn)
    {
        DoShouldHandleSameRequestId(true);
    }

    Y_UNIT_TEST(ShouldHandleSameRequestIdMonitoringOff)
    {
        DoShouldHandleSameRequestId(false);
    }

    Y_UNIT_TEST(ShouldRespectDeviceErasure)
    {
        TTestBasicRuntime runtime;
        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);

        auto env = TTestEnvBuilder(runtime)
                       .With(DiskAgentConfig({
                           "MemoryDevice1",
                           "MemoryDevice2",
                       }))
                       .With(
                           []()
                           {
                               NProto::TStorageServiceConfig config;
                               config.SetRejectLateRequestsAtDiskAgentEnabled(true);
                               return config;
                           }())
                       .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{"MemoryDevice1", "MemoryDevice2"};

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        auto sendZeroRequest = [&](ui64 volumeRequestId,
                                   ui64 blockStart,
                                   ui32 blockCount,
                                   const TString& deviceUUID)
        {
            auto request =
                std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDeviceUUID(deviceUUID);
            request->Record.SetStartIndex(blockStart);
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetBlocksCount(blockCount);
            request->Record.SetVolumeRequestId(volumeRequestId);

            diskAgent.SendRequest(std::move(request));
        };

        {
            // Execute first request with id = 100.
            sendZeroRequest(100, 2048, 16, uuids[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                diskAgent.RecvZeroDeviceBlocksResponse()->GetStatus());
        }

        {
            // Send request with id=99. This request should be rejected.
            sendZeroRequest(99, 2048, 32, uuids[0]);
            auto response =
                diskAgent.RecvZeroDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        // Secure erase device.
        diskAgent.ReleaseDevices(uuids, clientId);
        diskAgent.SecureEraseDevice(uuids[0]);
        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        {
            // Send request with id=99 again. This one should be executed
            // successfully.
            sendZeroRequest(99, 2048, 32, uuids[0]);
            auto response =
                diskAgent.RecvZeroDeviceBlocksResponse(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldSupportReadOnlyClients)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({
                "MemoryDevice1",
                "MemoryDevice2",
                "MemoryDevice3",
            }))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1",
            "MemoryDevice2"
        };

        const TString writerClientId = "writer-1";
        const TString readerClientId1 = "reader-1";
        const TString readerClientId2 = "reader-2";

        diskAgent.AcquireDevices(
            uuids,
            writerClientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        diskAgent.AcquireDevices(
            uuids,
            readerClientId1,
            NProto::VOLUME_ACCESS_READ_ONLY
        );

        diskAgent.AcquireDevices(
            uuids,
            readerClientId2,
            NProto::VOLUME_ACCESS_READ_ONLY
        );

        const auto blocksCount = 10;

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, 'X'));

        WriteDeviceBlocks(runtime, diskAgent, uuids[0], 0, sglist, writerClientId);
        // diskAgent.ZeroDeviceBlocks(uuids[0], 0, blocksCount / 2, writerClientId);

        for (auto sid: {readerClientId1, readerClientId2}) {
            diskAgent.SendWriteDeviceBlocksRequest(
                uuids[0],
                0,
                sglist,
                sid
            );

            auto response = diskAgent.RecvWriteDeviceBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_INVALID_SESSION);
        }

        for (auto sid: {readerClientId1, readerClientId2}) {
            diskAgent.SendZeroDeviceBlocksRequest(
                uuids[0],
                0,
                blocksCount / 2,
                sid
            );

            auto response = diskAgent.RecvZeroDeviceBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_INVALID_SESSION);
        }

        for (auto sid: {writerClientId, readerClientId1, readerClientId2}) {
            const auto response = ReadDeviceBlocks(
                runtime,
                diskAgent,
                uuids[0],
                0,
                blocksCount,
                sid);

            const auto& record = response->Record;

            UNIT_ASSERT(record.HasBlocks());

            const auto& iov = record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(iov.BuffersSize(), blocksCount);

            for (auto& buffer : iov.GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), DefaultBlockSize);
            }

            /*
            ui32 i = 0;

            while (i < blocksCount / 2) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(10, 0),
                    iov.GetBuffers(i).substr(0, 10)
                );

                ++i;
            }

            while (i < blocksCount) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(10, 'X'),
                    iov.GetBuffers(i).substr(0, 10)
                );

                ++i;
            }
            */
        }
    }

    Y_UNIT_TEST(ShouldPerformIoUnsafe)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig(
                { "MemoryDevice1", "MemoryDevice2", "MemoryDevice3" },
                false))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1",
            "MemoryDevice2"
        };

        const auto blocksCount = 10;

        {
            const auto response = ReadDeviceBlocks(
                runtime, diskAgent, uuids[0], 0, blocksCount, "");

            const auto& record = response->Record;

            UNIT_ASSERT(record.HasBlocks());

            const auto& iov = record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(iov.BuffersSize(), blocksCount);
            for (auto& buffer : iov.GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), DefaultBlockSize);
            }
        }

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, 'X'));

        WriteDeviceBlocks(runtime, diskAgent, uuids[0], 0, sglist, "");
        ZeroDeviceBlocks(runtime, diskAgent, uuids[0], 0, 10, "");
    }

    Y_UNIT_TEST(ShouldSecureEraseDevice)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({
                "MemoryDevice1",
                "MemoryDevice2",
                "MemoryDevice3",
            }))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        diskAgent.SecureEraseDevice("MemoryDevice1");
    }

    Y_UNIT_TEST(ShouldSecureEraseMultipleDevices)
    {
        TTestBasicRuntime runtime;

        auto config = DiskAgentConfig({
            "MemoryDevice1",
            "MemoryDevice2",
            "MemoryDevice3",
        });

        config.SetMaxParallelSecureErasesAllowed(2);

        auto env = TTestEnvBuilder(runtime).With(std::move(config)).Build();

        std::unique_ptr<IEventHandle>
            completeEvent;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvDiskAgentPrivate::EvSecureEraseCompleted && !completeEvent) {
                    completeEvent.reset(event.Release());
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        diskAgent.SendSecureEraseDeviceRequest("MemoryDevice1");

        runtime.DispatchEvents({}, 100ms);
        UNIT_ASSERT(completeEvent);

        diskAgent.SecureEraseDevice("MemoryDevice2");

        runtime.Send(completeEvent.release());
        auto response = diskAgent.RecvSecureEraseDeviceResponse();
        UNIT_ASSERT(!HasError(response->Record));
    }

    Y_UNIT_TEST(ShouldNotProcessUnknownDevices)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
                       .With(DiskAgentConfig({
                           "MemoryDevice1",
                           "MemoryDevice2",
                           "MemoryDevice3",
                       }))
                       .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        diskAgent.SendSecureEraseDeviceRequest("UnknownDevice");
        auto resp = diskAgent.RecvSecureEraseDeviceResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, resp->GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldUpdateStats)
    {
        auto const workingDir = TryGetRamDrivePath();

        TTestBasicRuntime runtime;

        const TString name1 = workingDir / "error";
        const TString name2 = "name2";

        auto env = TTestEnvBuilder(runtime)
            .With([&] {
                auto agentConfig = CreateDefaultAgentConfig();
                agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
                agentConfig.SetAcquireRequired(true);
                agentConfig.SetEnabled(true);
                // healthcheck generates extra requests whose presence in stats
                // makes expected values in this test harder to calculate
                agentConfig.SetDeviceHealthCheckDisabled(true);

                *agentConfig.AddFileDevices() = PrepareFileDevice(
                    workingDir / "error",
                    "dev1");

                auto* device = agentConfig.MutableMemoryDevices()->Add();
                device->SetName(name2);
                device->SetBlocksCount(1024);
                device->SetBlockSize(DefaultBlockSize);
                device->SetDeviceId("dev2");

                return agentConfig;
            }())
            .Build();

        UNIT_ASSERT(env.DiskRegistryState->Stats.empty());

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        diskAgent.AcquireDevices(
            TVector<TString> {"dev1", "dev2"},
            "client-1",
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        auto waitForStats = [&] (auto cb) {
            runtime.AdvanceCurrentTime(TDuration::Seconds(15));

            TDispatchOptions options;
            options.FinalEvents = {
                TDispatchOptions::TFinalEventCondition(
                    TEvDiskRegistry::EvUpdateAgentStatsRequest)
            };

            runtime.DispatchEvents(options);

            const auto& stats = env.DiskRegistryState->Stats;
            UNIT_ASSERT_EQUAL(1, stats.size());

            auto agentStats = stats.begin()->second;
            SortBy(*agentStats.MutableDeviceStats(), [] (auto& x) {
                return x.GetDeviceUUID();
            });

            cb(agentStats);
        };

        auto read = [&] (const auto& uuid) {
            return ReadDeviceBlocks(
                runtime,
                diskAgent,
                uuid,
                0,
                1,
                "client-1")->GetStatus();
        };

        waitForStats([&] (auto agentStats) {
            UNIT_ASSERT_EQUAL(2, agentStats.DeviceStatsSize());

            auto& dev0 = agentStats.GetDeviceStats(0);
            UNIT_ASSERT_VALUES_EQUAL("dev1", dev0.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(name1, dev0.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(0, dev0.GetErrors());
            UNIT_ASSERT_VALUES_EQUAL(0, dev0.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(0, dev0.GetBytesRead());

            auto& dev1 = agentStats.GetDeviceStats(1);
            UNIT_ASSERT_VALUES_EQUAL("dev2", dev1.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(name2, dev1.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(0, dev1.GetErrors());
            UNIT_ASSERT_VALUES_EQUAL(0, dev1.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(0, dev1.GetBytesRead());
        });

        UNIT_ASSERT_VALUES_EQUAL(E_IO, read("dev1"));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, read("dev1"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, read("dev2"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, read("dev2"));

        waitForStats([&] (auto agentStats) {
            UNIT_ASSERT_EQUAL(2, agentStats.DeviceStatsSize());

            auto& dev0 = agentStats.GetDeviceStats(0);
            UNIT_ASSERT_VALUES_EQUAL("dev1", dev0.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(name1, dev0.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(2, dev0.GetErrors());
            UNIT_ASSERT_VALUES_EQUAL(2, dev0.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(0, dev0.GetBytesRead());

            auto& dev1 = agentStats.GetDeviceStats(1);
            UNIT_ASSERT_VALUES_EQUAL("dev2", dev1.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(name2, dev1.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(0, dev1.GetErrors());
            UNIT_ASSERT_VALUES_EQUAL(2, dev1.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(2*DefaultBlockSize, dev1.GetBytesRead());
        });

        UNIT_ASSERT_EQUAL(E_IO, read("dev1"));
        UNIT_ASSERT_EQUAL(S_OK, read("dev2"));
        UNIT_ASSERT_EQUAL(S_OK, read("dev2"));
        UNIT_ASSERT_EQUAL(S_OK, read("dev2"));

        waitForStats([&] (auto agentStats) {
            UNIT_ASSERT_EQUAL(2, agentStats.DeviceStatsSize());

            auto& dev0 = agentStats.GetDeviceStats(0);
            UNIT_ASSERT_VALUES_EQUAL("dev1", dev0.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(name1, dev0.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, dev0.GetErrors());
            UNIT_ASSERT_VALUES_EQUAL(1, dev0.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(0, dev0.GetBytesRead());

            auto& dev1 = agentStats.GetDeviceStats(1);
            UNIT_ASSERT_VALUES_EQUAL("dev2", dev1.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(name2, dev1.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(0, dev1.GetErrors());
            UNIT_ASSERT_VALUES_EQUAL(3, dev1.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(3*DefaultBlockSize, dev1.GetBytesRead());
        });
    }

    Y_UNIT_TEST(ShouldCollectStats)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({
                "MemoryDevice1",
                "MemoryDevice2",
                "MemoryDevice3",
            }))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        auto response = diskAgent.CollectStats();
        auto& stats = response->Stats;

        UNIT_ASSERT_VALUES_EQUAL(0, response->Stats.GetInitErrorsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.DeviceStatsSize());

        SortBy(*stats.MutableDeviceStats(), [] (auto& x) {
            return x.GetDeviceUUID();
        });

        UNIT_ASSERT_VALUES_EQUAL(
            "MemoryDevice1",
            stats.GetDeviceStats(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "MemoryDevice2",
            stats.GetDeviceStats(1).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "MemoryDevice3",
            stats.GetDeviceStats(2).GetDeviceUUID());
    }

    Y_UNIT_TEST(ShouldPreserveCookie)
    {
        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({
                "MemoryDevice1",
                "MemoryDevice2",
                "MemoryDevice3",
            }))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids { "MemoryDevice1" };

        diskAgent.AcquireDevices(
            uuids,
            "client-id",
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        {
            const ui32 blocksCount = 10;

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                blocksCount,
                TString(DefaultBlockSize, 'X'));

            auto request = diskAgent.CreateWriteDeviceBlocksRequest(
                uuids[0], 0, sglist, "client-id");

            const ui64 cookie = 42;

            diskAgent.SendRequest(std::move(request), cookie);

            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvDiskAgent::TEvWriteDeviceBlocksResponse>(
                handle, WaitTimeout);

            UNIT_ASSERT(handle);
            UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
        }

        {
            auto request = diskAgent.CreateSecureEraseDeviceRequest(
                "MemoryDevice2");

            const ui64 cookie = 42;

            diskAgent.SendRequest(std::move(request), cookie);

            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvDiskAgent::TEvSecureEraseDeviceResponse>(
                handle, WaitTimeout);

            UNIT_ASSERT(handle);
            UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
        }

        {
            auto request = diskAgent.CreateAcquireDevicesRequest(
                {"MemoryDevice2"},
                "client-id",
                NProto::VOLUME_ACCESS_READ_WRITE
            );

            const ui64 cookie = 42;

            diskAgent.SendRequest(std::move(request), cookie);

            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvDiskAgent::TEvAcquireDevicesResponse>(
                handle, WaitTimeout);

            UNIT_ASSERT(handle);
            UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
        }
    }

    Y_UNIT_TEST(ShouldPerformIoWithoutSpdk)
    {
        auto agentConfig = CreateDefaultAgentConfig();
        agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        agentConfig.SetAcquireRequired(true);
        agentConfig.SetEnabled(true);

        auto const workingDir = TryGetRamDrivePath();

        *agentConfig.AddFileDevices() = PrepareFileDevice(
            workingDir / "test",
            "FileDevice-1");

        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TVector<TString> uuids { "FileDevice-1" };

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        {
            auto response = diskAgent.CollectStats();

            UNIT_ASSERT_VALUES_EQUAL(0, response->Stats.GetInitErrorsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Stats.GetDeviceStats().size());

            const auto& stats = response->Stats.GetDeviceStats(0);

            UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", stats.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetBytesRead());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetBytesWritten());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetNumWriteOps());
        }

        const size_t startIndex = 512;
        const size_t blocksCount = 2;

        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                blocksCount,
                TString(DefaultBlockSize, 'A'));

            diskAgent.SendWriteDeviceBlocksRequest(
                uuids[0],
                startIndex,
                sglist,
                clientId);

            runtime.DispatchEvents(TDispatchOptions());

            auto response = diskAgent.RecvWriteDeviceBlocksResponse();

            UNIT_ASSERT(!HasError(response->Record));
        }

        {
            auto response = diskAgent.CollectStats();

            UNIT_ASSERT_VALUES_EQUAL(0, response->Stats.GetInitErrorsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Stats.GetDeviceStats().size());

            const auto& stats = response->Stats.GetDeviceStats(0);

            UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", stats.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetBytesRead());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize * blocksCount,
                stats.GetBytesWritten());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetNumWriteOps());
        }

        {
            const auto response = ReadDeviceBlocks(
                runtime,
                diskAgent,
                uuids[0],
                startIndex,
                blocksCount,
                clientId);

            const auto& record = response->Record;

            UNIT_ASSERT(!HasError(record));

            auto sglist = ConvertToSgList(record.GetBlocks(), DefaultBlockSize);

            UNIT_ASSERT_VALUES_EQUAL(sglist.size(), blocksCount);

            for (const auto& buffer: sglist) {
                const char* ptr = buffer.Data();
                for (size_t i = 0; i < buffer.Size(); ++i) {
                    UNIT_ASSERT(ptr[i] == 'A');
                }
            }
        }

        {
            auto response = diskAgent.CollectStats();

            UNIT_ASSERT_VALUES_EQUAL(0, response->Stats.GetInitErrorsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Stats.GetDeviceStats().size());

            const auto& stats = response->Stats.GetDeviceStats(0);

            UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", stats.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize * blocksCount,
                stats.GetBytesRead());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize * blocksCount,
                stats.GetBytesWritten());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetNumWriteOps());
        }

        ZeroDeviceBlocks(
            runtime,
            diskAgent,
            uuids[0],
            startIndex,
            blocksCount,
            clientId);

        {
            const auto response = ReadDeviceBlocks(
                runtime, diskAgent, uuids[0], startIndex, blocksCount, clientId);

            const auto& record = response->Record;

            UNIT_ASSERT(!HasError(record));

            auto sglist = ConvertToSgList(record.GetBlocks(), DefaultBlockSize);

            UNIT_ASSERT_VALUES_EQUAL(sglist.size(), blocksCount);

            for (const auto& buffer: sglist) {
                const char* ptr = buffer.Data();
                for (size_t i = 0; i < buffer.Size(); ++i) {
                    UNIT_ASSERT(ptr[i] == 0);
                }
            }
        }

        {
            auto response = diskAgent.CollectStats();

            UNIT_ASSERT_VALUES_EQUAL(0, response->Stats.GetInitErrorsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Stats.GetDeviceStats().size());

            const auto& stats = response->Stats.GetDeviceStats(0);

            UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", stats.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                2 * DefaultBlockSize * blocksCount,
                stats.GetBytesRead());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize * blocksCount,
                stats.GetBytesWritten());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetNumWriteOps());
        }
    }

    Y_UNIT_TEST(ShouldSupportOffsetAndFileSizeInFileDevices)
    {
        auto agentConfig = CreateDefaultAgentConfig();
        agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        agentConfig.SetAcquireRequired(true);
        agentConfig.SetEnabled(true);

        const auto workingDir = TryGetRamDrivePath();
        const auto filePath = workingDir / "test";

        {
            TFile fileData(filePath, EOpenModeFlag::CreateAlways);
            fileData.Resize(16_MB);
        }

        auto prepareFileDevice = [&] (const TString& deviceName) {
            NProto::TFileDeviceArgs device;
            device.SetPath(filePath);
            device.SetBlockSize(DefaultDeviceBlockSize);
            device.SetDeviceId(deviceName);
            return device;
        };

        {
            auto* d = agentConfig.AddFileDevices();
            *d = prepareFileDevice("FileDevice-1");

            d->SetOffset(1_MB);
            d->SetFileSize(4_MB);

            d = agentConfig.AddFileDevices();
            *d = prepareFileDevice("FileDevice-2");

            d->SetOffset(5_MB);
            d->SetFileSize(4_MB);
        }

        TTestBasicRuntime runtime;

        NProto::TAgentConfig registeredAgent;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvRegisterAgentRequest: {
                        auto& msg = *event->Get<TEvDiskRegistry::TEvRegisterAgentRequest>();

                        registeredAgent = msg.Record.GetAgentConfig();
                    }
                    break;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(2, registeredAgent.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            1_MB,
            registeredAgent.GetDevices(0).GetPhysicalOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            5_MB,
            registeredAgent.GetDevices(1).GetPhysicalOffset());

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TVector<TString> uuids { "FileDevice-1", "FileDevice-2" };

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        auto writeBlocks = [&] (
            char c,
            ui64 startIndex,
            ui32 blockCount,
            ui32 devIdx)
        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                blockCount,
                TString(DefaultBlockSize, c));

            diskAgent.SendWriteDeviceBlocksRequest(
                uuids[devIdx],
                startIndex,
                sglist,
                clientId);

            runtime.DispatchEvents(TDispatchOptions());

            auto response = diskAgent.RecvWriteDeviceBlocksResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());
        };

        auto readBlocks = [&] (
            char c,
            ui64 startIndex,
            ui32 blockCount,
            ui32 devIdx,
            ui32 line)
        {
            const auto response = ReadDeviceBlocks(
                runtime,
                diskAgent,
                uuids[devIdx],
                startIndex,
                blockCount,
                clientId);

            const auto& record = response->Record;

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                record.GetError().GetCode(),
                record.GetError().GetMessage());

            auto sglist = ConvertToSgList(record.GetBlocks(), DefaultBlockSize);

            UNIT_ASSERT_VALUES_EQUAL(sglist.size(), blockCount);

            for (const auto& buffer: sglist) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    TString(DefaultBlockSize, c),
                    TString(buffer.AsStringBuf()),
                    line);
            }
        };

        auto zeroBlocks = [&] (ui64 startIndex, ui32 blockCount, ui32 devIdx)
        {
            ZeroDeviceBlocks(
                runtime,
                diskAgent,
                uuids[devIdx],
                startIndex,
                blockCount,
                clientId);
        };

        writeBlocks('A', 512, 2, 0);
        readBlocks('A', 512, 2, 0, __LINE__);
        readBlocks(0, 512, 2, 1, __LINE__);
        zeroBlocks(512, 2, 0);
        readBlocks(0, 512, 2, 0, __LINE__);

        {
            auto response = diskAgent.CollectStats();

            UNIT_ASSERT_VALUES_EQUAL(0, response->Stats.GetInitErrorsCount());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Stats.GetDeviceStats().size());

            const auto& stats = response->Stats.GetDeviceStats();

            UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", stats[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                4 * DefaultBlockSize,
                stats[0].GetBytesRead());
            UNIT_ASSERT_VALUES_EQUAL(2, stats[0].GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(
                2 * DefaultBlockSize,
                stats[0].GetBytesWritten());
            UNIT_ASSERT_VALUES_EQUAL(1, stats[0].GetNumWriteOps());
            UNIT_ASSERT_VALUES_EQUAL("FileDevice-2", stats[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                2 * DefaultBlockSize,
                stats[1].GetBytesRead());
            UNIT_ASSERT_VALUES_EQUAL(1, stats[1].GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(0, stats[1].GetBytesWritten());
            UNIT_ASSERT_VALUES_EQUAL(0, stats[1].GetNumWriteOps());
        }

        writeBlocks('A', 512, 2, 1);
        readBlocks('A', 512, 2, 1, __LINE__);
        readBlocks(0, 512, 2, 0, __LINE__);
        zeroBlocks(512, 2, 1);
        readBlocks(0, 512, 2, 1, __LINE__);

        {
            auto response = diskAgent.CollectStats();

            UNIT_ASSERT_VALUES_EQUAL(0, response->Stats.GetInitErrorsCount());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Stats.GetDeviceStats().size());

            const auto& stats = response->Stats.GetDeviceStats();

            UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", stats[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                6 * DefaultBlockSize,
                stats[0].GetBytesRead());
            UNIT_ASSERT_VALUES_EQUAL(3, stats[0].GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(
                2 * DefaultBlockSize,
                stats[0].GetBytesWritten());
            UNIT_ASSERT_VALUES_EQUAL(1, stats[0].GetNumWriteOps());
            UNIT_ASSERT_VALUES_EQUAL("FileDevice-2", stats[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                6 * DefaultBlockSize,
                stats[1].GetBytesRead());
            UNIT_ASSERT_VALUES_EQUAL(3, stats[1].GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(
                2 * DefaultBlockSize,
                stats[1].GetBytesWritten());
            UNIT_ASSERT_VALUES_EQUAL(1, stats[1].GetNumWriteOps());
        }

        writeBlocks('B', 512, 2, 0);
        writeBlocks('C', 512, 2, 1);

        TFile f(filePath, EOpenModeFlag::RdOnly);
        TString buffer(2 * DefaultBlockSize, 0);
        size_t bytes = f.Pread(
            buffer.begin(),
            2 * DefaultBlockSize,
            1_MB + 512 * DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(2 * DefaultBlockSize, bytes);
        UNIT_ASSERT_VALUES_EQUAL(TString(2 * DefaultBlockSize, 'B'), buffer);
        bytes = f.Pread(
            buffer.begin(),
            2 * DefaultBlockSize,
            5_MB + 512 * DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(2 * DefaultBlockSize, bytes);
        UNIT_ASSERT_VALUES_EQUAL(TString(2 * DefaultBlockSize, 'C'), buffer);

        diskAgent.ReleaseDevices(uuids, clientId);

        diskAgent.SecureEraseDevice("FileDevice-1");
        bytes = f.Pread(
            buffer.begin(),
            2 * DefaultBlockSize,
            1_MB + 512 * DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(2 * DefaultBlockSize, bytes);
        UNIT_ASSERT_VALUES_EQUAL(TString(2 * DefaultBlockSize, 0), buffer);
        bytes = f.Pread(
            buffer.begin(),
            2 * DefaultBlockSize,
            5_MB + 512 * DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(2 * DefaultBlockSize, bytes);
        UNIT_ASSERT_VALUES_EQUAL(TString(2 * DefaultBlockSize, 'C'), buffer);

        diskAgent.SecureEraseDevice("FileDevice-2");
        bytes = f.Pread(
            buffer.begin(),
            2 * DefaultBlockSize,
            5_MB + 512 * DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(2 * DefaultBlockSize, bytes);
        UNIT_ASSERT_VALUES_EQUAL(TString(2 * DefaultBlockSize, 0), buffer);
    }

    Y_UNIT_TEST(ShouldHandleDeviceInitError)
    {
        auto agentConfig = CreateDefaultAgentConfig();
        agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        agentConfig.SetAgentId("agent-id");
        agentConfig.SetEnabled(true);

        auto const workingDir = TryGetRamDrivePath();

        *agentConfig.AddFileDevices() = PrepareFileDevice(
            workingDir / "test-1",
            "FileDevice-1");

        {
            NProto::TFileDeviceArgs& device = *agentConfig.AddFileDevices();

            device.SetPath(workingDir / "not-exists");
            device.SetBlockSize(DefaultDeviceBlockSize);
            device.SetDeviceId("FileDevice-2");
        }

        *agentConfig.AddFileDevices() = PrepareFileDevice(
            workingDir / "broken",
            "FileDevice-3");

        *agentConfig.AddFileDevices() = PrepareFileDevice(
            workingDir / "test-4",
            "FileDevice-4");

        TTestBasicRuntime runtime;

        NProto::TAgentConfig registeredAgent;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvRegisterAgentRequest: {
                        auto& msg = *event->Get<TEvDiskRegistry::TEvRegisterAgentRequest>();

                        registeredAgent = msg.Record.GetAgentConfig();
                    }
                    break;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL("agent-id", registeredAgent.GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(4, registeredAgent.DevicesSize());

        auto& devices = *registeredAgent.MutableDevices();
        SortBy(devices.begin(), devices.end(), [] (const auto& device) {
            return device.GetDeviceUUID();
        });

        auto expectedBlocksCount =
            DefaultBlocksCount * DefaultBlockSize / DefaultDeviceBlockSize;

        UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("the-rack", devices[0].GetRack());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedBlocksCount,
            devices[0].GetBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::DEVICE_STATE_ONLINE),
            static_cast<int>(devices[0].GetState())
        );

        UNIT_ASSERT_VALUES_EQUAL("FileDevice-2", devices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("the-rack", devices[1].GetRack());
        UNIT_ASSERT_VALUES_EQUAL(1, devices[1].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::DEVICE_STATE_ERROR),
            static_cast<int>(devices[1].GetState())
        );

        UNIT_ASSERT_VALUES_EQUAL("FileDevice-3", devices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("the-rack", devices[2].GetRack());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedBlocksCount,
            devices[2].GetBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::DEVICE_STATE_ERROR),
            static_cast<int>(devices[2].GetState())
        );

        UNIT_ASSERT_VALUES_EQUAL("FileDevice-4", devices[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("the-rack", devices[3].GetRack());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedBlocksCount,
            devices[3].GetBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::DEVICE_STATE_ONLINE),
            static_cast<int>(devices[3].GetState())
        );

        auto response = diskAgent.CollectStats();
        const auto& stats = response->Stats;
        UNIT_ASSERT_VALUES_EQUAL(2, stats.GetInitErrorsCount());
    }

    Y_UNIT_TEST(ShouldHandleSpdkInitError)
    {
        TVector<NProto::TDeviceConfig> devices;

        auto agentConfig = CreateDefaultAgentConfig();
        agentConfig.SetEnabled(true);
        agentConfig.SetAgentId("agent");
        agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_SPDK);

        for (size_t i = 0; i < 10; i++) {
            auto& device = devices.emplace_back();
            device.SetDeviceName(Sprintf("%s%zu", (i & 1 ? "broken" : "file"), i));
            device.SetDeviceUUID(CreateGuidAsString());

            auto* config = agentConfig.MutableFileDevices()->Add();
            config->SetPath(device.GetDeviceName());
            config->SetDeviceId(device.GetDeviceUUID());
            config->SetBlockSize(4096);
        }

        auto nvmeConfig = agentConfig.MutableNvmeDevices()->Add();
        nvmeConfig->SetBaseName("broken");

        for (size_t i = 0; i < 10; i++) {
            auto& device = devices.emplace_back();
            device.SetDeviceName(Sprintf("agent:broken:n%zu", i));
            device.SetDeviceUUID(CreateGuidAsString());

            *nvmeConfig->MutableDeviceIds()->Add() = device.GetDeviceUUID();
        }

        TTestBasicRuntime runtime;
        TTestEnv env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .With(std::make_shared<TTestSpdkEnv>(devices))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(devices.size(), env.DiskRegistryState->Devices.size());

        for (const auto& expected: devices) {
            const auto& deviceName = expected.GetDeviceName();
            const auto& device = env.DiskRegistryState->Devices.at(deviceName);

            UNIT_ASSERT_VALUES_EQUAL(device.GetDeviceUUID(), expected.GetDeviceUUID());

            if (deviceName.Contains("broken")) {
                UNIT_ASSERT_EQUAL(device.GetState(), NProto::DEVICE_STATE_ERROR);
            } else {
                UNIT_ASSERT_EQUAL(device.GetState(), NProto::DEVICE_STATE_ONLINE);
            }
        }
    }

    Y_UNIT_TEST(ShouldSecureEraseAioDevice)
    {
        auto agentConfig = CreateDefaultAgentConfig();
        agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        agentConfig.SetDeviceEraseMethod(NProto::DEVICE_ERASE_METHOD_CRYPTO_ERASE);
        agentConfig.SetAgentId("agent-id");
        agentConfig.SetEnabled(true);

        auto const workingDir = TryGetRamDrivePath();

        *agentConfig.AddFileDevices() = PrepareFileDevice(
            workingDir / "test-1",
            "FileDevice-1");

        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();
        diskAgent.SecureEraseDevice("FileDevice-1");
    }

    Y_UNIT_TEST(ShouldZeroFillDevice)
    {
        auto agentConfig = CreateDefaultAgentConfig();
        agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        agentConfig.SetDeviceEraseMethod(NProto::DEVICE_ERASE_METHOD_ZERO_FILL);
        agentConfig.SetAgentId("agent-id");
        agentConfig.SetEnabled(true);
        agentConfig.SetAcquireRequired(true);

        auto const workingDir = TryGetRamDrivePath();

        *agentConfig.AddFileDevices() = PrepareFileDevice(
            workingDir / "test-1",
            "FileDevice-1");

        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        const TString clientId = "client-1";
        const TVector<TString> uuids { "FileDevice-1" };

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        const ui64 startIndex = 10;
        const ui32 blockCount = 16_KB / DefaultBlockSize;
        const TString content(DefaultBlockSize, 'A');

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blockCount,
            content);

        WriteDeviceBlocks(
            runtime, diskAgent, uuids[0], startIndex, sglist, clientId);

        {
            auto response = ReadDeviceBlocks(
                runtime,
                diskAgent,
                uuids[0],
                startIndex,
                blockCount,
                clientId);

            auto data = ConvertToSgList(
                response->Record.GetBlocks(),
                DefaultBlockSize);

            UNIT_ASSERT_VALUES_EQUAL(blockCount, data.size());
            for (TBlockDataRef dr: data) {
                UNIT_ASSERT_STRINGS_EQUAL(content, dr.AsStringBuf());
            }
        }

        diskAgent.ReleaseDevices(uuids, clientId);

        diskAgent.SecureEraseDevice("FileDevice-1");

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        {
            auto response = ReadDeviceBlocks(
                runtime,
                diskAgent,
                uuids[0],
                startIndex,
                blockCount,
                clientId);

            auto data = ConvertToSgList(
                response->Record.GetBlocks(),
                DefaultBlockSize);

            UNIT_ASSERT_VALUES_EQUAL(blockCount, data.size());

            for (TBlockDataRef dr: data) {
                for (char c: dr.AsStringBuf()) {
                    UNIT_ASSERT_VALUES_EQUAL(0, c);
                }
            }
        }

        diskAgent.ReleaseDevices(uuids, clientId);
    }

    Y_UNIT_TEST(ShouldInjectExceptions)
    {
        NLWTrace::TManager traceManager(
            *Singleton<NLWTrace::TProbeRegistry>(),
            true);  // allowDestructiveActions

        traceManager.RegisterCustomAction(
            "ServiceErrorAction", &CreateServiceErrorActionExecutor);

        // read errors for MemoryDevice1 & zero errors for MemoryDevice2
        traceManager.New("env", QueryFromString(R"(
            Blocks {
                ProbeDesc {
                    Name: "FaultInjection"
                    Provider: "BLOCKSTORE_DISK_AGENT_PROVIDER"
                }
                Action {
                    StatementAction {
                        Type: ST_INC
                        Argument { Variable: "counter1" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_MOD
                        Argument { Variable: "counter1" }
                        Argument { Variable: "counter1" }
                        Argument { Value: "100" }
                    }
                }
                Predicate {
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "name" }
                        Argument { Value: "ReadDeviceBlocks" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "deviceId" }
                        Argument { Value: "MemoryDevice1" }
                    }
                }
            }
            Blocks {
                ProbeDesc {
                    Name: "FaultInjection"
                    Provider: "BLOCKSTORE_DISK_AGENT_PROVIDER"
                }
                Action {
                    CustomAction {
                        Name: "ServiceErrorAction"
                        Opts: "E_IO"
                        Opts: "Io Error"
                    }
                }
                Predicate {
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "counter1" }
                        Argument { Value: "99" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "name" }
                        Argument { Value: "ReadDeviceBlocks" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "deviceId" }
                        Argument { Value: "MemoryDevice1" }
                    }
                }
            }

            Blocks {
                ProbeDesc {
                    Name: "FaultInjection"
                    Provider: "BLOCKSTORE_DISK_AGENT_PROVIDER"
                }
                Action {
                    StatementAction {
                        Type: ST_INC
                        Argument { Variable: "counter2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_MOD
                        Argument { Variable: "counter2" }
                        Argument { Variable: "counter2" }
                        Argument { Value: "100" }
                    }
                }
                Predicate {
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "name" }
                        Argument { Value: "ZeroDeviceBlocks" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "deviceId" }
                        Argument { Value: "MemoryDevice2" }
                    }
                }
            }
            Blocks {
                ProbeDesc {
                    Name: "FaultInjection"
                    Provider: "BLOCKSTORE_DISK_AGENT_PROVIDER"
                }
                Action {
                    CustomAction {
                        Name: "ServiceErrorAction"
                        Opts: "E_IO"
                        Opts: "Io Error"
                    }
                }
                Predicate {
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "counter2" }
                        Argument { Value: "99" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "name" }
                        Argument { Value: "ZeroDeviceBlocks" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "deviceId" }
                        Argument { Value: "MemoryDevice2" }
                    }
                }
            }
        )"));

        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({
                "MemoryDevice1",
                "MemoryDevice2",
                "MemoryDevice3",
            }))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1",
            "MemoryDevice2"
        };

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        auto read = [&] (int index) {
            const auto response = ReadDeviceBlocks(
                runtime, diskAgent, uuids[index], 0, 1, clientId);

            return response->GetStatus() == E_IO;
        };

        TString data(DefaultBlockSize, 'X');
        TSgList sglist = {{ data.data(), data.size() }};

        auto write = [&] (int index) {
            diskAgent.SendWriteDeviceBlocksRequest(uuids[index], 0, sglist, clientId);
            runtime.DispatchEvents(TDispatchOptions());
            auto response = diskAgent.RecvWriteDeviceBlocksResponse();

            return response->GetStatus() == E_IO;
        };

        auto zero = [&] (int index) {
            diskAgent.SendZeroDeviceBlocksRequest(uuids[index], 0, 1, clientId);
            runtime.DispatchEvents(TDispatchOptions());
            const auto response = diskAgent.RecvZeroDeviceBlocksResponse();

            return response->GetStatus() == E_IO;
        };

        {
            int errors = 0;

            for (int i = 0; i != 1000; ++i) {
                errors += read(0);
            }

            UNIT_ASSERT_VALUES_EQUAL(1000 / 100, errors);
        }

        for (int i = 0; i != 2000; ++i) {
            UNIT_ASSERT(!read(1));

            UNIT_ASSERT(!write(0));
            UNIT_ASSERT(!write(1));

            UNIT_ASSERT(!zero(0));
        }

        {
            int errors = 0;

            for (int i = 0; i != 1000; ++i) {
                errors += zero(1);
            }

            UNIT_ASSERT_VALUES_EQUAL(1000 / 100, errors);
        }
    }

    Y_UNIT_TEST(ShouldRegisterAfterDisconnect)
    {
        int registrationCount = 0;

        TTestBasicRuntime runtime;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvRegisterAgentRequest:
                        ++registrationCount;
                    break;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({"MemoryDevice1"}))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, registrationCount);

        diskAgent.SendRequest(
            std::make_unique<TEvDiskRegistryProxy::TEvConnectionLost>());

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(2, registrationCount);
    }

    Y_UNIT_TEST(ShouldRegisterAfterDRProxyIsReady)
    {
        int subscribed = 0;
        int connectionEstablished = 0;
        TTestBasicRuntime runtime;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistryProxy::EvSubscribeRequest: {
                        auto response = std::make_unique<
                            TEvDiskRegistryProxy::TEvSubscribeResponse>(
                            /*connected=*/false);
                        auto event = std::make_unique<IEventHandle>(
                            MakeDiskAgentServiceId(runtime.GetNodeId(0)),
                            MakeDiskRegistryProxyServiceId(),
                            response.release());
                        runtime.SendAsync(event.release());
                        subscribed++;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    case TEvDiskRegistryProxy::EvConnectionEstablished: {
                        UNIT_ASSERT_VALUES_EQUAL(1, subscribed);
                        connectionEstablished++;
                        break;
                    }
                    case TEvDiskAgentPrivate::EvRegisterAgentRequest: {
                        // Assert that attempt to register will be sent after
                        // DRProxy is ready.
                        UNIT_ASSERT_VALUES_EQUAL(1, connectionEstablished);
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({"MemoryDevice1"}))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        diskAgent.SendRequest(
            std::make_unique<TEvDiskRegistryProxy::TEvConnectionEstablished>());
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ShouldNotRegisterWhenNoDevicesDiscovered)
    {
        int registrationCount = 0;

        TTestBasicRuntime runtime;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvRegisterAgentRequest:
                        ++registrationCount;
                    break;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig({}))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(0, registrationCount);

    }

    Y_UNIT_TEST(ShouldSecureEraseDeviceWithExpiredClient)
    {
        TTestBasicRuntime runtime;

        const TVector<TString> uuids {
            "MemoryDevice1",
            "MemoryDevice2",
            "MemoryDevice3"
        };

        auto env = TTestEnvBuilder(runtime)
            .With(DiskAgentConfig(uuids))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        diskAgent.AcquireDevices(
            uuids,
            "client-1",
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        auto secureErase = [&] (auto expectedErrorCode) {
            for (const auto& uuid: uuids) {
                diskAgent.SendSecureEraseDeviceRequest(uuid);

                auto response = diskAgent.RecvSecureEraseDeviceResponse();
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedErrorCode,
                    response->Record.GetError().GetCode()
                );
            }
        };

        secureErase(E_INVALID_STATE);

        runtime.AdvanceCurrentTime(TDuration::Seconds(10));

        secureErase(S_OK);
    }

    Y_UNIT_TEST(ShouldRegisterDevices)
    {
        TVector<NProto::TDeviceConfig> devices;
        devices.reserve(15);
        for (size_t i = 0; i != 15; ++i) {
            auto& device = devices.emplace_back();

            device.SetDeviceName(Sprintf("/dev/disk/by-partlabel/NVMENBS%02lu", i + 1));
            device.SetBlockSize(4096);
            device.SetDeviceUUID(CreateGuidAsString());
            if (i != 11) {
                device.SetBlocksCount(24151552);
            } else {
                device.SetBlocksCount(24169728);
            }
        }

        TTestBasicRuntime runtime;

        TTestEnv env = TTestEnvBuilder(runtime)
            .With([&] {
                auto agentConfig = CreateDefaultAgentConfig();
                agentConfig.SetEnabled(true);
                for (const auto& device: devices) {
                    auto* config = agentConfig.MutableFileDevices()->Add();
                    config->SetPath(device.GetDeviceName());
                    config->SetBlockSize(device.GetBlockSize());
                    config->SetDeviceId(device.GetDeviceUUID());
                }

                return agentConfig;
            }())
            .With(std::make_shared<TTestSpdkEnv>(devices))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(
            devices.size(),
            env.DiskRegistryState->Devices.size()
        );

        for (const auto& expected: devices) {
            const auto& device = env.DiskRegistryState->Devices.at(
                expected.GetDeviceName()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                expected.GetDeviceUUID(),
                device.GetDeviceUUID()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                expected.GetDeviceName(),
                device.GetDeviceName()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                expected.GetBlockSize(),
                device.GetBlockSize()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                expected.GetBlocksCount(),
                device.GetBlocksCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldLimitSecureEraseRequests)
    {
        TTestBasicRuntime runtime;

        auto config = DiskAgentConfig({"foo", "bar"});
        config.SetDeviceEraseMethod(NProto::DEVICE_ERASE_METHOD_USER_DATA_ERASE);

        TVector<NProto::TDeviceConfig> devices;

        for (auto& md: config.GetMemoryDevices()) {
            auto& device = devices.emplace_back();
            device.SetDeviceName(md.GetName());
            device.SetDeviceUUID(md.GetDeviceId());
            device.SetBlocksCount(md.GetBlocksCount());
            device.SetBlockSize(md.GetBlockSize());
        }

        auto spdk = std::make_shared<TTestSpdkEnv>(devices);

        auto env = TTestEnvBuilder(runtime)
            .With(config)
            .With(spdk)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        auto foo = NewPromise<NProto::TError>();
        auto bar = NewPromise<NProto::TError>();

        spdk->SecureEraseResult = foo;

        for (int i = 0; i != 100; ++i) {
            diskAgent.SendSecureEraseDeviceRequest("foo");
        }

        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, AtomicGet(spdk->SecureEraseCount));

        for (int i = 0; i != 100; ++i) {
            diskAgent.SendSecureEraseDeviceRequest("bar");
        }

        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, AtomicGet(spdk->SecureEraseCount));

        spdk->SecureEraseResult = bar;
        foo.SetValue(NProto::TError());

        for (int i = 0; i != 100; ++i) {
            auto response = diskAgent.RecvSecureEraseDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->Record.GetError().GetCode()
            );
        }

        bar.SetValue(NProto::TError());

        for (int i = 0; i != 100; ++i) {
            auto response = diskAgent.RecvSecureEraseDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->Record.GetError().GetCode()
            );
        }

        UNIT_ASSERT_VALUES_EQUAL(2, AtomicGet(spdk->SecureEraseCount));
    }

    Y_UNIT_TEST(ShouldRejectSecureEraseRequestsOnPoisonPill)
    {
        TTestBasicRuntime runtime;

        NProto::TDeviceConfig device;

        device.SetDeviceName("uuid");
        device.SetDeviceUUID("uuid");
        device.SetBlocksCount(1024);
        device.SetBlockSize(DefaultBlockSize);

        auto spdk = std::make_shared<TTestSpdkEnv>(TVector{device});

        auto env = TTestEnvBuilder(runtime)
            .With([]{
                auto config = DiskAgentConfig({"uuid"});
                config.SetDeviceEraseMethod(NProto::DEVICE_ERASE_METHOD_USER_DATA_ERASE);
                return config;
            }())
            .With(spdk)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        spdk->SecureEraseResult = NewPromise<NProto::TError>();

        for (int i = 0; i != 100; ++i) {
            diskAgent.SendSecureEraseDeviceRequest("uuid");
        }

        diskAgent.SendRequest(std::make_unique<TEvents::TEvPoisonPill>());

        for (int i = 0; i != 100; ++i) {
            auto response = diskAgent.RecvSecureEraseDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                response->Record.GetError().GetCode()
            );
        }

        UNIT_ASSERT_VALUES_EQUAL(1, AtomicGet(spdk->SecureEraseCount));
    }

    Y_UNIT_TEST(ShouldRejectIORequestsDuringSecureErase)
    {
        TTestBasicRuntime runtime;

        NProto::TDeviceConfig device;

        device.SetDeviceName("uuid");
        device.SetDeviceUUID("uuid");
        device.SetBlocksCount(1024);
        device.SetBlockSize(DefaultBlockSize);

        auto spdk = std::make_shared<TTestSpdkEnv>(TVector{device});

        auto env = TTestEnvBuilder(runtime)
            .With([]{
                auto config = DiskAgentConfig({"uuid"}, false);
                config.SetDeviceEraseMethod(NProto::DEVICE_ERASE_METHOD_USER_DATA_ERASE);
                return config;
            }())
            .With(spdk)
            .Build();

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        spdk->SecureEraseResult = NewPromise<NProto::TError>();

        diskAgent.SendSecureEraseDeviceRequest("uuid");

        auto io = [&] (auto expectedErrorCode) {
            UNIT_ASSERT_VALUES_EQUAL(
                expectedErrorCode,
                ReadDeviceBlocks(runtime, diskAgent, "uuid", 0, 1, "")
                    ->Record.GetError().GetCode());

            UNIT_ASSERT_VALUES_EQUAL(
                expectedErrorCode,
                ZeroDeviceBlocks(runtime, diskAgent, "uuid", 0, 1, "")
                    ->Record.GetError().GetCode());

            {
                TVector<TString> blocks;
                auto sglist = ResizeBlocks(
                    blocks,
                    1,
                    TString(DefaultBlockSize, 'X'));

                UNIT_ASSERT_VALUES_EQUAL(
                    expectedErrorCode,
                    WriteDeviceBlocks(runtime, diskAgent, "uuid", 0, sglist, "")
                        ->Record.GetError().GetCode());
            }
        };

        io(E_REJECTED);

        auto counter = counters->GetCounter(
            "AppCriticalEvents/DiskAgentIoDuringSecureErase",
            true);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 3);

        // Read during secure erase from HealthCheck client doesn't generate
        // critical event
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            ReadDeviceBlocks(
                runtime,
                diskAgent,
                "uuid",
                0,
                1,
                TString(CheckHealthClientId))
                    ->Record.GetError()
                    .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 3);

        // Read during secure erase from BackgroundOps client doesn't generate
        // critical event
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            ReadDeviceBlocks(
                runtime,
                diskAgent,
                "uuid",
                0,
                1,
                TString(BackgroundOpsClientId))
                    ->Record.GetError()
                    .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 3);

        // Write zero during secure erase from HealthCheck client generates
        // critical event
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            ZeroDeviceBlocks(
                runtime,
                diskAgent,
                "uuid",
                0,
                1,
                TString(CheckHealthClientId))
                    ->Record.GetError()
                    .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 4);

        // Write zero during secure erase from BackgroundOps client generates
        // critical event
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            ZeroDeviceBlocks(
                runtime,
                diskAgent,
                "uuid",
                0,
                1,
                TString(BackgroundOpsClientId))
                    ->Record.GetError()
                    .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 5);

        spdk->SecureEraseResult.SetValue(NProto::TError());
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        io(S_OK);

        {
            auto response = diskAgent.RecvSecureEraseDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->Record.GetError().GetCode()
            );
        }

        UNIT_ASSERT_VALUES_EQUAL(1, AtomicGet(spdk->SecureEraseCount));
    }

    Y_UNIT_TEST(ShouldChecksumDeviceBlocks)
    {
        TTestBasicRuntime runtime;

        const auto blockSize = DefaultBlockSize;
        const auto blocksCount = 10;

        auto env = TTestEnvBuilder(runtime)
            .With([&] {
                auto config = CreateDefaultAgentConfig();
                config.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
                config.SetAcquireRequired(true);
                config.SetEnabled(true);

                *config.AddMemoryDevices() = PrepareMemoryDevice(
                    "MemoryDevice1",
                    blockSize,
                    blocksCount * blockSize);

                return config;
            }())
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1",
        };

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        TBlockChecksum checksum;

        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                blocksCount,
                TString(blockSize, 'X'));

            for (const auto& block : blocks) {
                checksum.Extend(block.data(), block.size());
            }

            WriteDeviceBlocks(runtime, diskAgent, uuids[0], 0, sglist, clientId);
        }

        {
            const auto response = ChecksumDeviceBlocks(
                runtime, diskAgent, uuids[0], 0, blocksCount, clientId);

            const auto& record = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(checksum.GetValue(), record.GetChecksum());
        }
    }

    Y_UNIT_TEST(ShouldDisableAndEnableAgentDevice)
    {
        TTestBasicRuntime runtime;

        const auto blockSize = DefaultBlockSize;
        const auto blocksCount = 10;

        auto env = TTestEnvBuilder(runtime)
            .With([&] {
                auto config = CreateDefaultAgentConfig();
                config.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
                config.SetAcquireRequired(true);
                config.SetEnabled(true);

                *config.AddMemoryDevices() = PrepareMemoryDevice(
                    "MemoryDevice1",
                    blockSize,
                    blocksCount * blockSize);
                *config.AddMemoryDevices() = PrepareMemoryDevice(
                    "MemoryDevice2",
                    blockSize,
                    blocksCount * blockSize);
                *config.AddMemoryDevices() = PrepareMemoryDevice(
                    "MemoryDevice3",
                    blockSize,
                    blocksCount * blockSize);

                return config;
            }())
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TVector<TString> uuids{
            "MemoryDevice1",
            "MemoryDevice2",
            "MemoryDevice3",
        };

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        // Disable device 0 and 1
        diskAgent.DisableConcreteAgent(TVector<TString>{uuids[0], uuids[1]});

        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                blocksCount,
                TString(blockSize, 'X'));

            TAutoPtr<NActors::IEventHandle> handle;

            // Device 0 should genereate IO error.
            diskAgent.SendWriteDeviceBlocksRequest(uuids[0], 0, sglist, clientId);
            auto response = diskAgent.RecvWriteDeviceBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_IO,
                response->Record.GetError().GetCode());

            // Device 1 should genereate IO error.
            diskAgent.SendWriteDeviceBlocksRequest(uuids[1], 0, sglist, clientId);
            response = diskAgent.RecvWriteDeviceBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_IO,
                response->Record.GetError().GetCode());

            // Device 2 has not been disabled and should be able to handle requests
            response =
                diskAgent.WriteDeviceBlocks(uuids[2], 0, sglist, clientId);
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->Record.GetError().GetCode());
        }

        {
            auto response = diskAgent.CollectStats();
            auto& stats = response->Stats;

            UNIT_ASSERT_VALUES_EQUAL(0, response->Stats.GetInitErrorsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.DeviceStatsSize());

            SortBy(*stats.MutableDeviceStats(), [] (auto& x) {
                return x.GetDeviceUUID();
            });

            // Here we get two erros for disabled devices.
            // First for DisableConcreteAgent, second for failed request.
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetDeviceStats(0).GetErrors());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetDeviceStats(1).GetErrors());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(2).GetErrors());
        }

        // Enable device 1 back
        diskAgent.EnableAgentDevice(uuids[1]);

        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                blocksCount,
                TString(blockSize, 'X'));

            TAutoPtr<NActors::IEventHandle> handle;

            // Device 0 should genereate IO error.
            diskAgent.SendWriteDeviceBlocksRequest(uuids[0], 0, sglist, clientId);
            auto response = diskAgent.RecvWriteDeviceBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_IO,
                response->Record.GetError().GetCode());

            // Device 1 has been enabled and should be able to handle requests
            response =
                diskAgent.WriteDeviceBlocks(uuids[1], 0, sglist, clientId);
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->Record.GetError().GetCode());

            // Device 2 has not been disabled and should be able to handle requests
            response =
                diskAgent.WriteDeviceBlocks(uuids[2], 0, sglist, clientId);
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->Record.GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldReceiveIOErrorFromBrokenDevice)
    {
        auto agentConfig = CreateDefaultAgentConfig();
        agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        agentConfig.SetAgentId("agent-id");
        agentConfig.SetEnabled(true);

        const TVector<TString> uuids {"FileDevice-1"};

        *agentConfig.AddFileDevices() = PrepareFileDevice("broken", uuids[0]);

        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const TString clientId = "client-1";

        diskAgent.AcquireDevices(
            uuids,
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE
        );

        const auto response = ReadDeviceBlocks(
            runtime, diskAgent, uuids[0], 0, 1024, clientId);

        const auto& error = response->GetError();

        UNIT_ASSERT_VALUES_EQUAL_C(E_IO, error.GetCode(), error.GetMessage());
    }

    auto MakeReadResponse(EWellKnownResultCodes code)
    {
        NProto::TReadBlocksLocalResponse response;
        *response.MutableError() = MakeError(code, "");
        return response;
    };

    Y_UNIT_TEST(ShouldCheckDevicesHealthInBackground)
    {
        using ::testing::Return;
        using ::testing::_;

        struct TMockStorage: public IStorage
        {
            MOCK_METHOD(
                NThreading::TFuture<NProto::TZeroBlocksResponse>,
                ZeroBlocks,
                (
                    TCallContextPtr,
                    std::shared_ptr<NProto::TZeroBlocksRequest>),
                (override));
            MOCK_METHOD(
                NThreading::TFuture<NProto::TReadBlocksLocalResponse>,
                ReadBlocksLocal,
                (
                    TCallContextPtr,
                    std::shared_ptr<NProto::TReadBlocksLocalRequest>),
                (override));
            MOCK_METHOD(
                NThreading::TFuture<NProto::TWriteBlocksLocalResponse>,
                WriteBlocksLocal,
                (
                    TCallContextPtr,
                    std::shared_ptr<NProto::TWriteBlocksLocalRequest>),
                (override));
            MOCK_METHOD(
                NThreading::TFuture<NProto::TError>,
                EraseDevice,
                (NProto::EDeviceEraseMethod),
                (override));
            MOCK_METHOD(TStorageBuffer, AllocateBuffer, (size_t), (override));
            MOCK_METHOD(void, ReportIOError, (), (override));
        };

        struct TMockStorageProvider: public IStorageProvider
        {
            IStoragePtr Storage;

            TMockStorageProvider()
                : Storage(std::make_shared<TMockStorage>())
            {
                ON_CALL((*GetStorage()), ReadBlocksLocal(_, _))
                    .WillByDefault(Return(MakeFuture(MakeReadResponse(S_OK))));
            }

            NThreading::TFuture<IStoragePtr> CreateStorage(
                const NProto::TVolume&,
                const TString&,
                NProto::EVolumeAccessMode) override
            {
                return MakeFuture(Storage);
            }

            TMockStorage* GetStorage()
            {
                return static_cast<TMockStorage*>(Storage.get());
            }
        };

        const auto workingDir = TryGetRamDrivePath();
        TTestBasicRuntime runtime;

        auto mockSp = new TMockStorageProvider();
        std::shared_ptr<IStorageProvider> sp(mockSp);

        const auto agentConfig = [&] {
            auto agentConfig = CreateDefaultAgentConfig();
            agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
            agentConfig.SetAcquireRequired(true);
            agentConfig.SetEnabled(true);
            *agentConfig.AddFileDevices() =
                PrepareFileDevice(workingDir / "dev1", "dev1");
            *agentConfig.AddFileDevices() =
                PrepareFileDevice(workingDir / "dev2", "dev2");
            return agentConfig;
        } ();

        auto env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .With(sp)
            .Build();

        size_t healthCheckCount = 0;
        NProto::TAgentStats agentStats;

        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskAgentPrivate::EvCollectStatsResponse: {
                        auto& msg = *event->Get<
                            TEvDiskAgentPrivate::TEvCollectStatsResponse>();

                        agentStats = msg.Stats;
                        break;
                    }
                    case TEvDiskAgent::EvReadDeviceBlocksRequest: {
                        auto& msg = *event->Get<
                            TEvDiskAgent::TEvReadDeviceBlocksRequest>();

                        healthCheckCount +=
                            // ignore events from the IO request parser actor
                            event->GetRecipientRewrite() == event->Recipient &&
                            msg.Record.GetHeaders().GetClientId() ==
                                CheckHealthClientId;

                        break;
                    }
                }

                return false;
            });

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(0, healthCheckCount);

        runtime.AdvanceCurrentTime(15s);
        runtime.DispatchEvents(
            {.FinalEvents = {
                 {TEvDiskAgent::EvReadDeviceBlocksResponse,
                  static_cast<ui32>(agentConfig.FileDevicesSize())}}});

        UNIT_ASSERT_VALUES_EQUAL(
            agentConfig.FileDevicesSize(),
            healthCheckCount);

        runtime.AdvanceCurrentTime(15s);
        runtime.DispatchEvents(
            {.FinalEvents = {
                 {TEvDiskAgent::EvReadDeviceBlocksResponse,
                  static_cast<ui32>(agentConfig.FileDevicesSize())}}});

        UNIT_ASSERT_VALUES_EQUAL(
            2 * agentConfig.FileDevicesSize(),
            healthCheckCount);

        runtime.AdvanceCurrentTime(15s);
        runtime.DispatchEvents(
            {.FinalEvents = {{TEvDiskRegistry::EvUpdateAgentStatsResponse}}});

        UNIT_ASSERT_VALUES_EQUAL(2, agentStats.DeviceStatsSize());
        {
            auto stats = agentStats;
            SortBy(*stats.MutableDeviceStats(), [] (auto& x) {
                return x.GetDeviceUUID();
            });

            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetInitErrorsCount());

            const auto& dev1 = stats.GetDeviceStats(0);
            UNIT_ASSERT_VALUES_EQUAL("dev1", dev1.GetDeviceUUID());
            UNIT_ASSERT_LE(2, dev1.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(0, dev1.GetErrors());

            const auto& dev2 = stats.GetDeviceStats(1);
            UNIT_ASSERT_VALUES_EQUAL("dev2", dev2.GetDeviceUUID());
            UNIT_ASSERT_LE(2, dev2.GetNumReadOps());
            UNIT_ASSERT_VALUES_EQUAL(0, dev2.GetErrors());
        }

        ON_CALL((*mockSp->GetStorage()), ReadBlocksLocal(_, _))
            .WillByDefault(Return(MakeFuture(MakeReadResponse(E_IO))));

        runtime.AdvanceCurrentTime(15s);
        runtime.DispatchEvents(
            {.FinalEvents = {
                 {TEvDiskAgent::EvReadDeviceBlocksResponse,
                  static_cast<ui32>(agentConfig.FileDevicesSize())}}});

        UNIT_ASSERT_LE(3 * agentConfig.FileDevicesSize(), healthCheckCount);

        runtime.AdvanceCurrentTime(15s);
        runtime.DispatchEvents(
            {.FinalEvents = {{TEvDiskRegistry::EvUpdateAgentStatsResponse}}});

        {
            auto stats = agentStats;
            SortBy(*stats.MutableDeviceStats(), [] (auto& x) {
                return x.GetDeviceUUID();
            });

            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetInitErrorsCount());

            const auto& dev1 = stats.GetDeviceStats(0);
            UNIT_ASSERT_VALUES_EQUAL("dev1", dev1.GetDeviceUUID());
            UNIT_ASSERT_LE(3, dev1.GetNumReadOps());
            UNIT_ASSERT_LE(1, dev1.GetErrors());

            const auto& dev2 = stats.GetDeviceStats(1);
            UNIT_ASSERT_VALUES_EQUAL("dev2", dev2.GetDeviceUUID());
            UNIT_ASSERT_LE(3, dev2.GetNumReadOps());
            UNIT_ASSERT_LE(1, dev2.GetErrors());
        }
    }

    Y_UNIT_TEST(ShouldCheckHealthOnlyForEnabledDevices)
    {
        TTestBasicRuntime runtime;
        THashMap<TString, size_t> healthCheckCount;

        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskAgent::EvReadDeviceBlocksRequest: {
                        auto& msg = *event->Get<
                            TEvDiskAgent::TEvReadDeviceBlocksRequest>();

                        if (event->GetRecipientRewrite() == event->Recipient &&
                            msg.Record.GetHeaders().GetClientId() ==
                                CheckHealthClientId)
                        {
                            healthCheckCount[msg.Record.GetDeviceUUID()] += 1;
                        }

                        break;
                    }
                }

                return false;
            });

        const auto workingDir = TryGetRamDrivePath();
        const auto agentConfig = [&] {
            auto agentConfig = CreateDefaultAgentConfig();
            agentConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
            agentConfig.SetAcquireRequired(true);
            agentConfig.SetEnabled(true);
            agentConfig.SetDisableBrokenDevices(true);

            *agentConfig.AddFileDevices() =
                PrepareFileDevice(workingDir / "dev1", "dev1");
            *agentConfig.AddFileDevices() =
                PrepareFileDevice(workingDir / "dev2", "dev2");
            *agentConfig.AddFileDevices() =
                PrepareFileDevice(workingDir / "dev3", "dev3");
            *agentConfig.AddFileDevices() =
                PrepareFileDevice(workingDir / "dev4", "dev4");

            return agentConfig;
        } ();

        auto diskregistryState = MakeIntrusive<TDiskRegistryState>();
        diskregistryState->DisabledDevices = {"dev2", "dev4"};

        auto env = TTestEnvBuilder(runtime)
            .With(agentConfig)
            .With(diskregistryState)
            .Build();

        runtime.SetLogPriority(
            TBlockStoreComponents::DISK_AGENT,
            NLog::PRI_DEBUG);
        runtime.SetLogPriority(
            TBlockStoreComponents::DISK_AGENT_WORKER,
            NLog::PRI_DEBUG);

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(0, healthCheckCount.size());

        runtime.AdvanceCurrentTime(15s);
        runtime.DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvReadDeviceBlocksResponse, 2}}});

        UNIT_ASSERT_VALUES_EQUAL(2, healthCheckCount.size());
        UNIT_ASSERT_VALUES_EQUAL(1, healthCheckCount["dev1"]);
        UNIT_ASSERT_VALUES_EQUAL(1, healthCheckCount["dev3"]);

        runtime.AdvanceCurrentTime(15s);
        runtime.DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvReadDeviceBlocksResponse, 2}}});

        UNIT_ASSERT_VALUES_EQUAL(2, healthCheckCount.size());
        UNIT_ASSERT_VALUES_EQUAL(2, healthCheckCount["dev1"]);
        UNIT_ASSERT_VALUES_EQUAL(2, healthCheckCount["dev3"]);

        // Update the disable device list
        diskregistryState->DisabledDevices = {"dev2"};

        diskAgent.SendRequest(
            std::make_unique<TEvDiskRegistryProxy::TEvConnectionLost>());

        runtime.AdvanceCurrentTime(15s);
        runtime.DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvReadDeviceBlocksResponse, 3}}});

        UNIT_ASSERT_VALUES_EQUAL(3, healthCheckCount.size());
        UNIT_ASSERT_VALUES_EQUAL(3, healthCheckCount["dev1"]);
        UNIT_ASSERT_VALUES_EQUAL(3, healthCheckCount["dev3"]);
        UNIT_ASSERT_VALUES_EQUAL(1, healthCheckCount["dev4"]);
    }

    Y_UNIT_TEST(ShouldFindDevices)
    {
        TTestBasicRuntime runtime;

        TTempDir tempDir;
        const TFsPath rootDir = tempDir.Path() / "dev/disk/by-partlabel";

        rootDir.MkDirs();

        auto prepareFile = [&] (TFsPath name, auto size) {
            TFile {rootDir / name, EOpenModeFlag::CreateAlways}
            .Resize(size);
        };

        // local
        prepareFile("NVMECOMPUTE01", 1024_KB);
        prepareFile("NVMECOMPUTE02", 1024_KB);
        prepareFile("NVMECOMPUTE03", 2000_KB);
        prepareFile("NVMECOMPUTE04", 2000_KB);
        prepareFile("NVMECOMPUTE05", 1025_KB);

        // default
        prepareFile("NVMENBS01", 1024_KB);
        prepareFile("NVMENBS02", 1024_KB);
        prepareFile("NVMENBS03", 2000_KB);
        prepareFile("NVMENBS04", 2000_KB);
        prepareFile("NVMENBS05", 1025_KB);
        prepareFile("NVMENBS06", 10000_KB);
        prepareFile("NVMENBS07", 10000_KB);

         // rot
        prepareFile("ROTNBS01", 2024_KB);
        prepareFile("ROTNBS02", 2024_KB);
        prepareFile("ROTNBS03", 2024_KB);
        prepareFile("ROTNBS04", 2024_KB);
        prepareFile("ROTNBS05", 2025_KB);

        auto config = DiskAgentConfig();
        auto& discovery = *config.MutableStorageDiscoveryConfig();

        {
            auto& path = *discovery.AddPathConfigs();
            path.SetPathRegExp(rootDir / "NVMECOMPUTE([0-9]{2})");

            auto& local = *path.AddPoolConfigs();
            local.SetPoolName("local");
            local.SetHashSuffix("-local");
            local.SetMinSize(1024_KB);
            local.SetMaxSize(2000_KB);
        }

        {
            auto& path = *discovery.AddPathConfigs();
            path.SetPathRegExp(rootDir / "NVMENBS([0-9]{2})");

            auto& def = *path.AddPoolConfigs();
            def.SetMinSize(1024_KB);
            def.SetMaxSize(2000_KB);

            auto& defLarge = *path.AddPoolConfigs();
            defLarge.SetMinSize(9000_KB);
            defLarge.SetMaxSize(15000_KB);
        }

        {
            auto& path = *discovery.AddPathConfigs();
            path.SetPathRegExp(rootDir / "ROTNBS([0-9]{2})");

            auto& local = *path.AddPoolConfigs();
            local.SetPoolName("rot");
            local.SetHashSuffix("-rot");
            local.SetMinSize(2024_KB);
            local.SetMaxSize(2200_KB);
        }

        auto env = TTestEnvBuilder(runtime)
            .With(config
                | WithBackend(NProto::DISK_AGENT_BACKEND_AIO))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        TVector<NProto::TDeviceConfig> devices;
        for (auto& [_, config]: env.DiskRegistryState->Devices) {
            devices.push_back(config);
        }
        SortBy(devices, [] (const auto& d) {
            return std::tie(d.GetPoolName(), d.GetDeviceName());
        });

        UNIT_ASSERT_VALUES_EQUAL(17, devices.size());

        for (auto& d: devices) {
            UNIT_ASSERT_VALUES_UNEQUAL("", d.GetDeviceUUID());
        }

        for (int i = 1; i <= 7; ++i) {
            const auto& d = devices[i - 1];
            const auto path = rootDir / (TStringBuilder() << "NVMENBS0" << i);
            UNIT_ASSERT_VALUES_EQUAL(path, d.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("", d.GetPoolName());
        }

        for (int i = 1; i <= 5; ++i) {
            const auto& d = devices[7 + i - 1];
            const auto path = rootDir / (TStringBuilder() << "NVMECOMPUTE0" << i);
            UNIT_ASSERT_VALUES_EQUAL(path, d.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("local", d.GetPoolName());
        }

        for (int i = 1; i <= 5; ++i) {
            const auto& d = devices[12 + i - 1];
            const auto path = rootDir / (TStringBuilder() << "ROTNBS0" << i);
            UNIT_ASSERT_VALUES_EQUAL(path, d.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("rot", d.GetPoolName());
        }
    }

    Y_UNIT_TEST(ShouldDetectConfigMismatch)
    {
        TTestBasicRuntime runtime;

        TTempDir tempDir;
        const TFsPath rootDir = tempDir.Path() / "dev/disk/by-partlabel";

        rootDir.MkDirs();

        auto prepareFile = [&] (TFsPath name, auto size) {
            TFile {rootDir / name, EOpenModeFlag::CreateAlways}
            .Resize(size);
        };

        prepareFile("NVMECOMPUTE01", 1024_KB);    // local
        prepareFile("NVMECOMPUTE02", 1000_KB);

        auto config = DiskAgentConfig();
        auto& discovery = *config.MutableStorageDiscoveryConfig();

        {
            auto& path = *discovery.AddPathConfigs();
            path.SetPathRegExp(rootDir / "NVMECOMPUTE([0-9]{2})");

            auto& local = *path.AddPoolConfigs();
            local.SetPoolName("local");
            local.SetHashSuffix("-local");
            local.SetMinSize(1024_KB);
            local.SetMaxSize(2000_KB);
        }

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);

        auto mismatch = counters->GetCounter(
            "AppCriticalEvents/DiskAgentConfigMismatch",
            true);

        UNIT_ASSERT_VALUES_EQUAL(0, mismatch->Val());

        auto env = TTestEnvBuilder(runtime)
            .With(config
                | WithBackend(NProto::DISK_AGENT_BACKEND_AIO))
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(0, env.DiskRegistryState->Devices.size());
        UNIT_ASSERT_VALUES_EQUAL(1, mismatch->Val());
    }

    Y_UNIT_TEST_F(ShouldCacheSessions, TFixture)
    {
        auto cacheRestoreError = Counters->GetCounter(
            "AppCriticalEvents/DiskAgentSessionCacheRestoreError",
            true);

        UNIT_ASSERT_EQUAL(0, *cacheRestoreError);

        NProto::TAgentConfig agentConfig;

        Runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistry::EvRegisterAgentRequest)
                {
                    auto& msg =
                        *event->Get<TEvDiskRegistry::TEvRegisterAgentRequest>();

                    agentConfig = msg.Record.GetAgentConfig();
                }

                return false;
            });

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        Runtime->AdvanceCurrentTime(1h);

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_EQUAL(0, *cacheRestoreError);

        UNIT_ASSERT_VALUES_EQUAL(4, agentConfig.DevicesSize());

        TVector<TString> devices;
        for (const auto& d: agentConfig.GetDevices()) {
            devices.push_back(d.GetDeviceUUID());
        }
        Sort(devices);

        // acquire a bunch of devices

        const auto acquireTs = Runtime->GetCurrentTime();

        diskAgent.AcquireDevices(
            TVector{devices[0], devices[1]},
            "writer-1",
            NProto::VOLUME_ACCESS_READ_WRITE,
            42,   // MountSeqNumber
            "vol0",
            1000);   // VolumeGeneration

        diskAgent.AcquireDevices(
            TVector{devices[0], devices[1]},
            "reader-1",
            NProto::VOLUME_ACCESS_READ_ONLY,
            -1,   // MountSeqNumber
            "vol0",
            1001);   // VolumeGeneration

        diskAgent.AcquireDevices(
            TVector{devices[2], devices[3]},
            "reader-2",
            NProto::VOLUME_ACCESS_READ_ONLY,
            -1,   // MountSeqNumber
            "vol1",
            2000);   // VolumeGeneration

        // validate the cache file

        {
            auto sessions = LoadSessionCache();

            UNIT_ASSERT_VALUES_EQUAL(3, sessions.size());

            {
                auto& session = sessions[0];

                UNIT_ASSERT_VALUES_EQUAL("reader-1", session.GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(2, session.DeviceIdsSize());
                ASSERT_VECTORS_EQUAL(
                    TVector({devices[0], devices[1]}),
                    session.GetDeviceIds());

                // MountSeqNumber is not applicable to read sessions
                UNIT_ASSERT_VALUES_EQUAL(0, session.GetMountSeqNumber());
                UNIT_ASSERT_VALUES_EQUAL("vol0", session.GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(1001, session.GetVolumeGeneration());
                UNIT_ASSERT_VALUES_EQUAL(
                    acquireTs.MicroSeconds(),
                    session.GetLastActivityTs());
                UNIT_ASSERT(session.GetReadOnly());
            }

            {
                auto& session = sessions[1];

                UNIT_ASSERT_VALUES_EQUAL("reader-2", session.GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(2, session.DeviceIdsSize());
                ASSERT_VECTORS_EQUAL(
                    TVector({devices[2], devices[3]}),
                    session.GetDeviceIds());

                // MountSeqNumber is not applicable to read sessions
                UNIT_ASSERT_VALUES_EQUAL(0, session.GetMountSeqNumber());
                UNIT_ASSERT_VALUES_EQUAL("vol1", session.GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(2000, session.GetVolumeGeneration());
                UNIT_ASSERT_VALUES_EQUAL(
                    acquireTs.MicroSeconds(),
                    session.GetLastActivityTs());
                UNIT_ASSERT(session.GetReadOnly());
            }

            {
                auto& session = sessions[2];

                UNIT_ASSERT_VALUES_EQUAL("writer-1", session.GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(2, session.DeviceIdsSize());
                ASSERT_VECTORS_EQUAL(
                    TVector({devices[0], devices[1]}),
                    session.GetDeviceIds());

                UNIT_ASSERT_VALUES_EQUAL(42, session.GetMountSeqNumber());
                UNIT_ASSERT_VALUES_EQUAL("vol0", session.GetDiskId());

                // VolumeGeneration was updated by reader-1
                UNIT_ASSERT_VALUES_EQUAL(1001, session.GetVolumeGeneration());
                UNIT_ASSERT_VALUES_EQUAL(
                    acquireTs.MicroSeconds(),
                    session.GetLastActivityTs());
                UNIT_ASSERT(!session.GetReadOnly());
            }
        }

        Runtime->AdvanceCurrentTime(5s);

        diskAgent.ReleaseDevices(
            TVector{devices[0], devices[1]},
            "writer-1",
            "vol0",
            1001);

        diskAgent.ReleaseDevices(
            TVector{devices[2], devices[3]},
            "reader-2",
            "vol1",
            2000);

        // check the session cache again
        {
            auto sessions = LoadSessionCache();

            // now we have only one session
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            auto& session = sessions[0];

            UNIT_ASSERT_VALUES_EQUAL("reader-1", session.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(2, session.DeviceIdsSize());
            ASSERT_VECTORS_EQUAL(
                TVector({devices[0], devices[1]}),
                session.GetDeviceIds());

            // MountSeqNumber is not applicable to read sessions
            UNIT_ASSERT_VALUES_EQUAL(0, session.GetMountSeqNumber());
            UNIT_ASSERT_VALUES_EQUAL("vol0", session.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1001, session.GetVolumeGeneration());
            UNIT_ASSERT_VALUES_EQUAL(
                acquireTs.MicroSeconds(),
                session.GetLastActivityTs());
            UNIT_ASSERT(session.GetReadOnly());
        }
    }

    Y_UNIT_TEST_F(ShouldRestoreCacheSessions, TFixture)
    {
        const TString devices[] {
            "5ea2fcdce0a180a63db2b5f6a5b34221",
            "657dabaf3d224c9177b00c437716dfb1",
            "79955ae90189fe8a89ab832a8b0cb57d",
            "e85cd1d217c3239507fc0cd180a075fd"
        };

        const auto initialTs = Now();

        {
            NProto::TDiskAgentDeviceSessionCache cache;
            auto& writeSession = *cache.AddSessions();
            writeSession.SetClientId("writer-1");
            *writeSession.MutableDeviceIds()->Add() = devices[0];
            *writeSession.MutableDeviceIds()->Add() = devices[1];
            writeSession.SetReadOnly(false);
            writeSession.SetMountSeqNumber(42);
            writeSession.SetDiskId("vol0");
            writeSession.SetVolumeGeneration(1000);
            writeSession.SetLastActivityTs(initialTs.MicroSeconds());

            auto& reader1 = *cache.AddSessions();
            reader1.SetClientId("reader-1");
            *reader1.MutableDeviceIds()->Add() = devices[0];
            *reader1.MutableDeviceIds()->Add() = devices[1];
            reader1.SetReadOnly(true);
            reader1.SetDiskId("vol0");
            reader1.SetVolumeGeneration(1000);
            reader1.SetLastActivityTs(initialTs.MicroSeconds());

            auto& reader2 = *cache.AddSessions();
            reader2.SetClientId("reader-2");
            *reader2.MutableDeviceIds()->Add() = devices[2];
            *reader2.MutableDeviceIds()->Add() = devices[3];
            reader2.SetReadOnly(true);
            reader2.SetDiskId("vol1");
            reader2.SetVolumeGeneration(2000);
            reader2.SetLastActivityTs(initialTs.MicroSeconds());

            SerializeToTextFormat(cache, CachedSessionsPath);
        }

        auto cacheRestoreError = Counters->GetCounter(
            "AppCriticalEvents/DiskAgentSessionCacheRestoreError",
            true);

        UNIT_ASSERT_EQUAL(0, *cacheRestoreError);

        auto env = TTestEnvBuilder(*Runtime)
            .With(CreateDiskAgentConfig())
            .Build();

        Runtime->UpdateCurrentTime(initialTs + 3s);

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_EQUAL(0, *cacheRestoreError);

        // should reject a request with a wrong client id
        {
            auto error = Write(diskAgent, "unknown", devices[1]);
            UNIT_ASSERT_EQUAL_C(E_BS_INVALID_SESSION, error.GetCode(), error);
        }

        // should reject a request with a wrong client id
        {
            auto error = Write(diskAgent, "reader-2", devices[0]);
            UNIT_ASSERT_EQUAL_C(E_BS_INVALID_SESSION, error.GetCode(), error);
        }

        {
            auto error = Write(diskAgent, "reader-2", devices[1]);
            UNIT_ASSERT_EQUAL_C(E_BS_INVALID_SESSION, error.GetCode(), error);
        }

        // should reject a write request for the read only session
        {
            auto error = Write(diskAgent, "reader-1", devices[1]);
            UNIT_ASSERT_EQUAL_C(E_BS_INVALID_SESSION, error.GetCode(), error);
        }

        // should be ok

        {
            auto error = Write(diskAgent, "writer-1", devices[0]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = Write(diskAgent, "writer-1", devices[1]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = Read(diskAgent, "reader-1", devices[0]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = Read(diskAgent, "reader-1", devices[1]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = Read(diskAgent, "reader-2", devices[2]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = Read(diskAgent, "reader-2", devices[3]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        // make all sessions stale
        Runtime->AdvanceCurrentTime(ReleaseInactiveSessionsTimeout);

        const auto acquireTs = Runtime->GetCurrentTime();

        diskAgent.AcquireDevices(
            TVector{devices[0], devices[1]},
            "writer-1",
            NProto::VOLUME_ACCESS_READ_WRITE,
            42,   // MountSeqNumber
            "vol0",
            1000);   // VolumeGeneration

        {
            auto sessions = LoadSessionCache();

            // now we have only one session
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            auto& session = sessions[0];

            UNIT_ASSERT_VALUES_EQUAL("writer-1", session.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(2, session.DeviceIdsSize());
            ASSERT_VECTORS_EQUAL(
                TVector({devices[0], devices[1]}),
                session.GetDeviceIds());

            UNIT_ASSERT_VALUES_EQUAL(42, session.GetMountSeqNumber());
            UNIT_ASSERT_VALUES_EQUAL("vol0", session.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1000, session.GetVolumeGeneration());
            UNIT_ASSERT_VALUES_EQUAL(
                acquireTs.MicroSeconds(),
                session.GetLastActivityTs());
        }
    }

    Y_UNIT_TEST_F(ShouldHandleBrokenCacheSessions, TFixture)
    {
        {
            TFile file {CachedSessionsPath, EOpenModeFlag::CreateAlways};
            TFileOutput(file).Write("{ broken protobuf#");
        }

        auto cacheRestoreError = Counters->GetCounter(
            "AppCriticalEvents/DiskAgentSessionCacheRestoreError",
            true);

        UNIT_ASSERT_EQUAL(0, *cacheRestoreError);

        auto env = TTestEnvBuilder(*Runtime)
            .With(CreateDiskAgentConfig())
            .Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_EQUAL(1, *cacheRestoreError);
    }

    Y_UNIT_TEST_F(ShouldGetCacheSessionsPathFromStorageConfig, TFixture)
    {
        {
            TFile file {CachedSessionsPath, EOpenModeFlag::CreateAlways};
            TFileOutput(file).Write("{ broken protobuf#");
        }

        auto cacheRestoreError = Counters->GetCounter(
            "AppCriticalEvents/DiskAgentSessionCacheRestoreError",
            true);

        UNIT_ASSERT_EQUAL(0, *cacheRestoreError);

        auto diskAgentConfig = CreateDiskAgentConfig();
        auto storageConfig = NProto::TStorageServiceConfig();
        storageConfig.SetCachedDiskAgentSessionsPath(
            diskAgentConfig.GetCachedSessionsPath());
        diskAgentConfig.ClearCachedSessionsPath();

        auto env = TTestEnvBuilder(*Runtime)
            .With(diskAgentConfig)
            .With(storageConfig)
            .Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_EQUAL(1, *cacheRestoreError);
    }

    Y_UNIT_TEST_F(ShouldNotGetCacheSessionsPathFromStorageConfig, TFixture)
    {
        const TString diskAgentCachedSessionsPath = CachedSessionsPath;
        const TString storageCachedSessionsPath =
            TempDir.Path() / "must-not-use-sessions.txt";

        {
            TFile file {storageCachedSessionsPath, EOpenModeFlag::CreateAlways};
            TFileOutput(file).Write("{ broken protobuf#");
        }

        auto cacheRestoreError = Counters->GetCounter(
            "AppCriticalEvents/DiskAgentSessionCacheRestoreError",
            true);

        UNIT_ASSERT_EQUAL(0, *cacheRestoreError);

        auto storageConfig = NProto::TStorageServiceConfig();
        storageConfig.SetCachedDiskAgentSessionsPath(storageCachedSessionsPath);
        auto env = TTestEnvBuilder(*Runtime)
            .With(CreateDiskAgentConfig())
            .With(storageConfig)
            .Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        UNIT_ASSERT_EQUAL(0, *cacheRestoreError);
    }

    Y_UNIT_TEST_F(ShouldSwitchToIdleModeWithoutDevices, TFixture)
    {
        auto config = CreateDiskAgentConfig();

        config.MutableStorageDiscoveryConfig()
            ->MutablePathConfigs(0)
            ->SetPathRegExp(TempDir.Path() / "non-existent([0-9]{2})");

        auto env = TTestEnvBuilder(*Runtime).With(std::move(config)).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        THttpGetRequest httpRequest;
        NMonitoring::TMonService2HttpRequest monService2HttpRequest{
            nullptr,
            &httpRequest,
            nullptr,
            nullptr,
            "",
            nullptr};

        diskAgent.SendRequest(
            std::make_unique<NMon::TEvHttpInfo>(monService2HttpRequest));

        auto response = diskAgent.RecvResponse<NMon::TEvHttpInfoRes>();
        UNIT_ASSERT_STRING_CONTAINS(response->Answer, "Unregistered (Idle)");
    }

    Y_UNIT_TEST(ShouldOffloadProtobufParsing)
    {
        const TString deviceId = "MemoryDevice1";
        const TString sessionId = "client-1";

        const auto config = [&] {
            auto config = DiskAgentConfig({deviceId});
            config.SetIOParserActorCount(4);
            config.SetOffloadAllIORequestsParsingEnabled(true);
            config.SetIOParserActorAllocateStorageEnabled(true);

            return config;
        }();

        TTestBasicRuntime runtime;

        auto env = TTestEnvBuilder(runtime)
            .With(config)
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.SendWaitReadyRequest();
        const TActorId diskAgentActorId = [&] {
            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvDiskAgent::TEvWaitReadyResponse>(
                handle,
                3s);

            UNIT_ASSERT(handle);

            return handle->Sender;
        }();

        runtime.DispatchEvents({}, 1s);

        diskAgent.AcquireDevices(
            TVector {deviceId},
            sessionId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        ui32 reads = 0;
        ui32 writes = 0;
        ui32 zeroes = 0;

        ui32 parsedReads = 0;
        ui32 parsedWrites = 0;
        ui32 parsedZeroes = 0;

        THashSet<TActorId> actors;

        runtime.SetEventFilter([&](auto&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDiskAgentPrivate::EvParsedReadDeviceBlocksRequest:
                    UNIT_ASSERT_EQUAL(env.DiskAgentActorId, ev->Recipient);
                    UNIT_ASSERT_EQUAL(
                        diskAgentActorId,
                        ev->GetRecipientRewrite());
                    ++parsedReads;
                    break;
                case TEvDiskAgentPrivate::EvParsedWriteDeviceBlocksRequest:
                    UNIT_ASSERT_EQUAL(env.DiskAgentActorId, ev->Recipient);
                    UNIT_ASSERT_EQUAL(
                        diskAgentActorId,
                        ev->GetRecipientRewrite());
                    ++parsedWrites;
                    break;
                case TEvDiskAgentPrivate::EvParsedZeroDeviceBlocksRequest:
                    UNIT_ASSERT_EQUAL(env.DiskAgentActorId, ev->Recipient);
                    UNIT_ASSERT_EQUAL(
                        diskAgentActorId,
                        ev->GetRecipientRewrite());
                    ++parsedZeroes;
                    break;
                case TEvDiskAgent::EvReadDeviceBlocksRequest:
                    UNIT_ASSERT_EQUAL(env.DiskAgentActorId, ev->Recipient);
                    ++reads;
                    actors.insert(ev->GetRecipientRewrite());
                    break;
                case TEvDiskAgent::EvWriteDeviceBlocksRequest:
                    UNIT_ASSERT_EQUAL(env.DiskAgentActorId, ev->Recipient);
                    ++writes;
                    actors.insert(ev->GetRecipientRewrite());
                    break;
                case TEvDiskAgent::EvZeroDeviceBlocksRequest:
                    UNIT_ASSERT_EQUAL(env.DiskAgentActorId, ev->Recipient);
                    ++zeroes;
                    actors.insert(ev->GetRecipientRewrite());
                    break;
            }
            return false;
        });

        UNIT_ASSERT_VALUES_EQUAL(0, reads);
        UNIT_ASSERT_VALUES_EQUAL(0, writes);
        UNIT_ASSERT_VALUES_EQUAL(0, zeroes);
        UNIT_ASSERT_VALUES_EQUAL(0, parsedReads);
        UNIT_ASSERT_VALUES_EQUAL(0, parsedWrites);
        UNIT_ASSERT_VALUES_EQUAL(0, parsedZeroes);

        ReadDeviceBlocks(runtime, diskAgent, deviceId, 0, 1, sessionId);
        UNIT_ASSERT_VALUES_EQUAL(2, reads);
        UNIT_ASSERT_VALUES_EQUAL(1, parsedReads);
        UNIT_ASSERT_VALUES_EQUAL(2, actors.size());

        ReadDeviceBlocks(runtime, diskAgent, deviceId, 0, 1, sessionId);
        UNIT_ASSERT_VALUES_EQUAL(4, reads);
        UNIT_ASSERT_VALUES_EQUAL(2, parsedReads);
        UNIT_ASSERT_VALUES_EQUAL(3, actors.size());

        {
            TVector<TString> blocks;
            auto sglist =
                ResizeBlocks(blocks, 1, TString(DefaultBlockSize, 'X'));

            WriteDeviceBlocks(
                runtime,
                diskAgent,
                deviceId,
                0,
                sglist,
                sessionId);
            UNIT_ASSERT_VALUES_EQUAL(2, writes);
            UNIT_ASSERT_VALUES_EQUAL(1, parsedWrites);
            UNIT_ASSERT_VALUES_EQUAL(4, actors.size());
        }

        ZeroDeviceBlocks(runtime, diskAgent, deviceId, 0, 1, sessionId);
        UNIT_ASSERT_VALUES_EQUAL(2, zeroes);
        UNIT_ASSERT_VALUES_EQUAL(1, parsedZeroes);
        UNIT_ASSERT_VALUES_EQUAL(1, parsedZeroes);
        UNIT_ASSERT_VALUES_EQUAL(5, actors.size());
    }

    Y_UNIT_TEST_F(ShouldDisableDevicesAfterRegistration, TFixture)
    {
        const TString uuids[] {
            "79955ae90189fe8a89ab832a8b0cb57d", // NVMENBS01
            "657dabaf3d224c9177b00c437716dfb1", // NVMENBS02
            "e85cd1d217c3239507fc0cd180a075fd", // NVMENBS03
            "5ea2fcdce0a180a63db2b5f6a5b34221"  // NVMENBS04
        };

        TVector<std::pair<TString, TString>> pathToSerial{
            {"NVMENBS01", "W"},
            {"NVMENBS02", "X"},
            {"NVMENBS03", "Y"},
            {"NVMENBS04", "Z"},
        };

        // build the config cache
        {
            NProto::TDiskAgentConfig config;

            size_t i = 0;
            for (const auto& [filename, sn]: pathToSerial) {
                auto& file = *config.AddFileDevices();
                file.SetPath(TempDir.Path() / filename);
                file.SetSerialNumber(sn);
                file.SetDeviceId(uuids[i]);
                file.SetBlockSize(4_KB);

                ++i;
            }

            auto error = SaveDiskAgentConfig(CachedConfigPath, config);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        // change serial numbers of NVMENBS02 & NVMENBS04
        pathToSerial[1].second = "new-X";   // NVMENBS02
        pathToSerial[3].second = "new-Z";   // NVMENBS04

        auto diskregistryState = MakeIntrusive<TDiskRegistryState>();

        // Disable only NVMENBS02
        diskregistryState->DisabledDevices = {uuids[1]};

        // Postpone the registration in DR
        auto oldEventFilterFn = Runtime->SetEventFilter(
            [](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistry::EvRegisterAgentRequest)
                {
                    auto response = std::make_unique<
                        TEvDiskRegistry::TEvRegisterAgentResponse>(
                        MakeError(E_REJECTED));

                    runtime.Send(new NActors::IEventHandle(
                        event->Sender,
                        event->GetRecipientRewrite(),
                        response.release()));

                    return true;
                }

                return false;
            });

        auto env = TTestEnvBuilder(*Runtime)
            .With(CreateDiskAgentConfig())
            .With(std::make_shared<TTestNvmeManager>(pathToSerial))
            .With(diskregistryState)
            .Build();

        Runtime->UpdateCurrentTime(Now());

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        diskAgent.AcquireDevices(
            TVector{
                uuids[0],   // NVMENBS01
                uuids[1],   // NVMENBS02
                uuids[3],   // NVMENBS04
            },
            "reader-1",
            NProto::VOLUME_ACCESS_READ_ONLY,
            -1,   // MountSeqNumber
            "vol0",
            1000);   // VolumeGeneration

        // NVMENBS01 is OK
        {
            auto error = Read(diskAgent, "reader-1", uuids[0]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        // Before the registration in DR NVMENBS02 & NVMENBS04 should be
        // suspended
        {
            auto error = Read(diskAgent, "reader-1", uuids[1]);
            UNIT_ASSERT_EQUAL_C(E_REJECTED, error.GetCode(), error);
            UNIT_ASSERT_C(
                error.GetMessage().Contains("Device disabled"),
                error);
        }

        {
            auto error = Read(diskAgent, "reader-1", uuids[3]);
            UNIT_ASSERT_EQUAL_C(E_REJECTED, error.GetCode(), error);
            UNIT_ASSERT_C(
                error.GetMessage().Contains("Device disabled"),
                error);
        }

        // check the config cache
        {
            auto [config, error] = LoadDiskAgentConfig(CachedConfigPath);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(4, config.FileDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(2, config.DevicesWithSuspendedIOSize());

            Sort(*config.MutableDevicesWithSuspendedIO());

            UNIT_ASSERT_VALUES_EQUAL(
                uuids[3],   // NVMENBS04
                config.GetDevicesWithSuspendedIO(0));

            UNIT_ASSERT_VALUES_EQUAL(
                uuids[1],   // NVMENBS02
                config.GetDevicesWithSuspendedIO(1));
        }

        Runtime->SetEventFilter(oldEventFilterFn);
        Runtime->AdvanceCurrentTime(5s);
        Runtime->DispatchEvents(TDispatchOptions{
            .FinalEvents = {TDispatchOptions::TFinalEventCondition(
                TEvDiskRegistry::EvRegisterAgentResponse)}});

        // NVMENBS01 is OK
        {
            auto error = Read(diskAgent, "reader-1", uuids[0]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        // After the registration in DR NVMENBS02 should be disabled
        {
            auto error = Read(diskAgent, "reader-1", uuids[1]);
            UNIT_ASSERT_EQUAL_C(E_IO, error.GetCode(), error);
            UNIT_ASSERT_C(
                error.GetMessage().Contains("Device disabled"),
                error);
        }

        // NVMENBS04 should be OK
        {
            auto error = Read(diskAgent, "reader-1", uuids[3]);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
        }

        // check the config cache
        {
            auto [config, error] = LoadDiskAgentConfig(CachedConfigPath);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(4, config.FileDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, config.DevicesWithSuspendedIOSize());
            UNIT_ASSERT_VALUES_EQUAL(
                uuids[1],   // NVMENBS02
                config.GetDevicesWithSuspendedIO(0));
        }
    }

    Y_UNIT_TEST_F(
        ShouldIgnorePossibleButUnknownDevicesUUIDsDuringAcquire,
        TFixture)
    {
        const TString uuids[]{
            "79955ae90189fe8a89ab832a8b0cb57d",   // NVMENBS01
            "657dabaf3d224c9177b00c437716dfb1",   // NVMENBS02
            "e85cd1d217c3239507fc0cd180a075fd",   // NVMENBS03
            "5ea2fcdce0a180a63db2b5f6a5b34221"    // unknown NVMENBS04
        };

        TTempDir tempDir;
        const TFsPath rootDir = tempDir.Path() / "dev/disk/by-partlabel";

        rootDir.MkDirs();

        auto prepareFile = [&] (TFsPath name, auto size) {
            TFile {rootDir / name, EOpenModeFlag::CreateAlways}
            .Resize(size);
        };

        // default
        prepareFile("NVMENBS01", 1024_KB);
        prepareFile("NVMENBS02", 1024_KB);
        prepareFile("NVMENBS03", 2000_KB);


        auto config = DiskAgentConfig();
        auto& discovery = *config.MutableStorageDiscoveryConfig();

        {
            auto& path = *discovery.AddPathConfigs();
            path.SetPathRegExp(rootDir / "NVMENBS([0-3]{2})");

            auto& def = *path.AddPoolConfigs();
            def.SetMinSize(1024_KB);
            def.SetMaxSize(2000_KB);

            auto& defLarge = *path.AddPoolConfigs();
            defLarge.SetMinSize(9000_KB);
            defLarge.SetMaxSize(15000_KB);
        }

        auto env =
            TTestEnvBuilder(*Runtime)
                .With(config | WithBackend(NProto::DISK_AGENT_BACKEND_AIO))
                .Build();

        Runtime->UpdateCurrentTime(Now());

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        diskAgent.AcquireDevices(
            TVector{
                uuids[0],   // NVMENBS01
                uuids[1],   // NVMENBS02
                uuids[3],   // unknown NVMENBS04
            },
            "reader-1",
            NProto::VOLUME_ACCESS_READ_ONLY,
            -1,   // MountSeqNumber
            "vol0",
            1000);   // VolumeGeneration

        diskAgent.SendAcquireDevicesRequest(
            TVector{
                TString("not possible uuid"),
            },
            "reader-1",
            NProto::VOLUME_ACCESS_READ_ONLY,
            -1,   // MountSeqNumber
            "vol0",
            1000);
        auto resp = diskAgent.RecvAcquireDevicesResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, resp->GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldPerformDirectCopyToRemoteDiskAgent, TCopyRangeFixture)
    {
        // Setup message filter for checking reads and writes.
        ui32 readRequestCount = 0;
        ui32 writeRequestCount = 0;
        auto checkRequests = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvReadDeviceBlocksRequest)
            {
                ++readRequestCount;
                auto* msg =
                    event->Get<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
                UNIT_ASSERT_VALUES_EQUAL(
                    SourceClientId,
                    msg->Record.GetHeaders().GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(
                    true,
                    msg->Record.GetHeaders().GetIsBackgroundRequest());
                UNIT_ASSERT_VALUES_EQUAL(BlockSize, msg->Record.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    BlockCount,
                    msg->Record.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL("DA1-1", msg->Record.GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    SourceStartIndex,
                    msg->Record.GetStartIndex());
            }
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                ++writeRequestCount;
                auto* msg =
                    event->Get<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
                UNIT_ASSERT_VALUES_EQUAL(
                    TargetClientId,
                    msg->Record.GetHeaders().GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(
                    true,
                    msg->Record.GetHeaders().GetIsBackgroundRequest());
                UNIT_ASSERT_VALUES_EQUAL(BlockSize, msg->Record.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    BlockCount,
                    msg->Record.GetBlocks().BuffersSize());
                UNIT_ASSERT_VALUES_EQUAL("DA2-2", msg->Record.GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    TargetStartIndex,
                    msg->Record.GetStartIndex());
            }
            return false;
        };
        auto oldFilter = Runtime->SetEventFilter(checkRequests);

        // We send a request to the first disk agent, it must do the reading
        // itself, and write blocks to the second disk agent.
        //   DA1-1: .....XY.....
        //               |||||
        //                \\\\\
        //                 vvvvv
        //   DA2-2: fffffffXY...
        auto request =
            std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(SourceClientId);
        request->Record.MutableHeaders()->SetIsBackgroundRequest(true);

        request->Record.SetSourceDeviceUUID("DA1-1");
        request->Record.SetSourceStartIndex(SourceStartIndex);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlockCount(BlockCount);

        request->Record.SetTargetNodeId(Runtime->GetNodeId(1));
        request->Record.SetTargetClientId(TargetClientId);
        request->Record.SetTargetDeviceUUID("DA2-2");
        request->Record.SetTargetStartIndex(TargetStartIndex);

        DiskAgent1->SendRequest(std::move(request));
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        auto response =
            DiskAgent1
                ->RecvResponse<TEvDiskAgent::TEvDirectCopyBlocksResponse>();
        UNIT_ASSERT(!HasError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, readRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, writeRequestCount);

        Runtime->SetEventFilter(oldFilter);

        // Check that content on source device is not changed
        UNIT_ASSERT_VALUES_EQUAL(
            ".....XY.....",
            ReadBlocks(*DiskAgent1, "DA1-1"));

        // Check that the content on the target device meets the expectations
        UNIT_ASSERT_VALUES_EQUAL(
            "fffffffXY...",
            ReadBlocks(*DiskAgent2, "DA2-2"));
    }

    Y_UNIT_TEST_F(ShouldPerformDirectCopyOnSameDiskAgent, TCopyRangeFixture)
    {
        // Setup message filter for checking reads and writes.
        ui32 readRequestCount = 0;
        ui32 writeRequestCount = 0;
        auto checkRequests = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvReadDeviceBlocksRequest)
            {
                ++readRequestCount;
                auto* msg =
                    event->Get<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
                UNIT_ASSERT_VALUES_EQUAL(
                    SourceClientId,
                    msg->Record.GetHeaders().GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(
                    false,
                    msg->Record.GetHeaders().GetIsBackgroundRequest());
                UNIT_ASSERT_VALUES_EQUAL(BlockSize, msg->Record.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    BlockCount,
                    msg->Record.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL("DA1-1", msg->Record.GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    SourceStartIndex,
                    msg->Record.GetStartIndex());
            }
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                ++writeRequestCount;
                auto* msg =
                    event->Get<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
                UNIT_ASSERT_VALUES_EQUAL(
                    TargetClientId,
                    msg->Record.GetHeaders().GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(
                    false,
                    msg->Record.GetHeaders().GetIsBackgroundRequest());
                UNIT_ASSERT_VALUES_EQUAL(BlockSize, msg->Record.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    BlockCount,
                    msg->Record.GetBlocks().BuffersSize());
                UNIT_ASSERT_VALUES_EQUAL("DA1-2", msg->Record.GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    TargetStartIndex,
                    msg->Record.GetStartIndex());
            }
            return false;
        };
        auto oldFilter = Runtime->SetEventFilter(checkRequests);

        // We send a request to the first disk agent, it must do the reading
        // from device DA1-1, and write blocks to device DA1-2.
        //   DA1-1: .....XY.....
        //               |||||
        //                \\\\\
        //                 vvvvv
        //   DA1-2: fffffffXY...
        auto request =
            std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(SourceClientId);
        request->Record.MutableHeaders()->SetIsBackgroundRequest(false);

        request->Record.SetSourceDeviceUUID("DA1-1");
        request->Record.SetSourceStartIndex(SourceStartIndex);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlockCount(BlockCount);

        request->Record.SetTargetNodeId(Runtime->GetNodeId(0));
        request->Record.SetTargetClientId(TargetClientId);
        request->Record.SetTargetDeviceUUID("DA1-2");
        request->Record.SetTargetStartIndex(TargetStartIndex);

        DiskAgent1->SendRequest(std::move(request));
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        auto response =
            DiskAgent1
                ->RecvResponse<TEvDiskAgent::TEvDirectCopyBlocksResponse>();
        UNIT_ASSERT(!HasError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, readRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, writeRequestCount);

        Runtime->SetEventFilter(oldFilter);

        // Check that content on source device is not changed
        UNIT_ASSERT_VALUES_EQUAL(
            ".....XY.....",
            ReadBlocks(*DiskAgent1, "DA1-1"));

        // Check that the content on the target device meets the expectations
        UNIT_ASSERT_VALUES_EQUAL(
            "fffffffXY...",
            ReadBlocks(*DiskAgent1, "DA1-2"));
    }

    Y_UNIT_TEST_F(ShouldPerformDirectCopyAsZeroRequest, TCopyRangeFixture)
    {
        // Setup message filter for checking writes and zeroes.
        ui32 zeroRequestCount = 0;
        ui32 writeRequestCount = 0;
        auto checkRequests = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);

            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvZeroDeviceBlocksRequest)
            {
                ++zeroRequestCount;
                auto* msg =
                    event->Get<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
                UNIT_ASSERT_VALUES_EQUAL(
                    TargetClientId,
                    msg->Record.GetHeaders().GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(
                    true,
                    msg->Record.GetHeaders().GetIsBackgroundRequest());
                UNIT_ASSERT_VALUES_EQUAL(BlockSize, msg->Record.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(2, msg->Record.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL("DA2-2", msg->Record.GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    TargetStartIndex + 2,
                    msg->Record.GetStartIndex());
            }

            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                ++writeRequestCount;
                auto* msg =
                    event->Get<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
                UNIT_ASSERT_VALUES_EQUAL(
                    TargetClientId,
                    msg->Record.GetHeaders().GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(
                    true,
                    msg->Record.GetHeaders().GetIsBackgroundRequest());
                UNIT_ASSERT_VALUES_EQUAL(BlockSize, msg->Record.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    2,
                    msg->Record.GetBlocks().BuffersSize());
                UNIT_ASSERT_VALUES_EQUAL("DA2-2", msg->Record.GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    TargetStartIndex - 1,
                    msg->Record.GetStartIndex());
            }
            return false;
        };
        auto oldFilter = Runtime->SetEventFilter(checkRequests);

        {
            // Source data contains only zeroes
            //   DA1-1: .....XY.....
            //                 ||
            //                  \\
            //                   vv
            //   DA1-2: fffffffff..f

            auto request = std::make_unique<TEvDirectCopyBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(SourceClientId);
            request->Record.MutableHeaders()->SetIsBackgroundRequest(true);

            request->Record.SetSourceDeviceUUID("DA1-1");
            request->Record.SetSourceStartIndex(SourceStartIndex + 2);
            request->Record.SetBlockSize(BlockSize);
            request->Record.SetBlockCount(2);

            request->Record.SetTargetNodeId(Runtime->GetNodeId(1));
            request->Record.SetTargetClientId(TargetClientId);
            request->Record.SetTargetDeviceUUID("DA2-2");
            request->Record.SetTargetStartIndex(TargetStartIndex + 2);

            DiskAgent1->SendRequest(std::move(request));
            Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
            auto response =
                DiskAgent1->RecvResponse<TEvDirectCopyBlocksResponse>();
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(true, response->Record.GetAllZeroes());

            UNIT_ASSERT_VALUES_EQUAL(1, zeroRequestCount);
            UNIT_ASSERT_VALUES_EQUAL(0, writeRequestCount);
            zeroRequestCount = 0;

            UNIT_ASSERT_VALUES_EQUAL(
                "fffffffff..f",
                ReadBlocks(*DiskAgent2, "DA2-2"));
        }
        {
            // Second request contains zeroes and data
            //   DA1-1: .....XY.....
            //              ||
            //               \\
            //                vv
            //   DA2-2: ffffff.Xf..f
            auto request = std::make_unique<TEvDirectCopyBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(SourceClientId);
            request->Record.MutableHeaders()->SetIsBackgroundRequest(true);

            request->Record.SetSourceDeviceUUID("DA1-1");
            request->Record.SetSourceStartIndex(SourceStartIndex - 1);
            request->Record.SetBlockSize(BlockSize);
            request->Record.SetBlockCount(2);

            request->Record.SetTargetNodeId(Runtime->GetNodeId(1));
            request->Record.SetTargetClientId(TargetClientId);
            request->Record.SetTargetDeviceUUID("DA2-2");
            request->Record.SetTargetStartIndex(TargetStartIndex - 1);

            DiskAgent1->SendRequest(std::move(request));
            Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
            auto response =
                DiskAgent1->RecvResponse<TEvDirectCopyBlocksResponse>();
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(false, response->Record.GetAllZeroes());
            UNIT_ASSERT_VALUES_EQUAL(0, zeroRequestCount);
            UNIT_ASSERT_VALUES_EQUAL(1, writeRequestCount);

            UNIT_ASSERT_VALUES_EQUAL(
                "ffffff.Xf..f",
                ReadBlocks(*DiskAgent2, "DA2-2"));
        }
    }

    Y_UNIT_TEST_F(ShouldHandleUndeliveryForDirectCopy, TCopyRangeFixture)
    {
        // Setup message filter for intercepting write.
        auto undeliverWrite = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                auto sendTo = event->Sender;
                auto extractedEvent =
                    event->Release<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
                runtime.Send(
                    new IEventHandle(
                        sendTo,
                        sendTo,
                        extractedEvent.Release(),
                        0,
                        event->Cookie,
                        nullptr),
                    0);

                return true;
            }
            return false;
        };

        Runtime->SetEventFilter(undeliverWrite);

        // We send a request to the first disk agent, it must do the reading
        // from device DA1-1, and write blocks to device DA2-2 on second disk
        // agent.
        auto request =
            std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(SourceClientId);
        request->Record.MutableHeaders()->SetIsBackgroundRequest(false);

        request->Record.SetSourceDeviceUUID("DA1-1");
        request->Record.SetSourceStartIndex(SourceStartIndex);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlockCount(BlockCount);

        request->Record.SetTargetNodeId(Runtime->GetNodeId(1));
        request->Record.SetTargetClientId(TargetClientId);
        request->Record.SetTargetDeviceUUID("DA2-2");
        request->Record.SetTargetStartIndex(TargetStartIndex);

        DiskAgent1->SendRequest(std::move(request));
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        auto response =
            DiskAgent1
                ->RecvResponse<TEvDiskAgent::TEvDirectCopyBlocksResponse>();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldHandleReadingErrorForDirectCopy, TCopyRangeFixture)
    {
        // Setup message filter for intercepting read.
        auto interceptRead = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvReadDeviceBlocksRequest)
            {
                auto response =
                    std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksResponse>(
                        MakeError(E_IO, "io error"));

                runtime.Send(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,   // flags
                        event->Cookie),
                    0);
                return true;
            }
            return false;
        };

        Runtime->SetEventFilter(interceptRead);

        auto request =
            std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(SourceClientId);
        request->Record.MutableHeaders()->SetIsBackgroundRequest(false);

        request->Record.SetSourceDeviceUUID("DA1-1");
        request->Record.SetSourceStartIndex(SourceStartIndex);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlockCount(BlockCount);

        request->Record.SetTargetNodeId(Runtime->GetNodeId(1));
        request->Record.SetTargetClientId(TargetClientId);
        request->Record.SetTargetDeviceUUID("DA2-2");
        request->Record.SetTargetStartIndex(TargetStartIndex);

        DiskAgent1->SendRequest(std::move(request));
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        auto response =
            DiskAgent1
                ->RecvResponse<TEvDiskAgent::TEvDirectCopyBlocksResponse>();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldHandleWriteErrorForDirectCopy, TCopyRangeFixture)
    {
        // Setup message filter for intercepting write.
        auto interceptWrite = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                auto response =
                    std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksResponse>(
                        MakeError(E_IO, "io error"));

                runtime.Send(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,   // flags
                        event->Cookie),
                    0);
                return true;
            }
            return false;
        };

        Runtime->SetEventFilter(interceptWrite);

        auto request =
            std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(SourceClientId);
        request->Record.MutableHeaders()->SetIsBackgroundRequest(false);

        request->Record.SetSourceDeviceUUID("DA1-1");
        request->Record.SetSourceStartIndex(SourceStartIndex);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlockCount(BlockCount);

        request->Record.SetTargetNodeId(Runtime->GetNodeId(1));
        request->Record.SetTargetClientId(TargetClientId);
        request->Record.SetTargetDeviceUUID("DA2-2");
        request->Record.SetTargetStartIndex(TargetStartIndex);

        DiskAgent1->SendRequest(std::move(request));
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        auto response =
            DiskAgent1
                ->RecvResponse<TEvDiskAgent::TEvDirectCopyBlocksResponse>();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldPerformDirectCopyAndCalcTime, TCopyRangeFixture)
    {
        // Setup message filter for checking reads and writes.
        auto checkRequests = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvReadDeviceBlocksRequest)
            {
                Runtime->AdvanceCurrentTime(5s);
            }
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                Runtime->AdvanceCurrentTime(10s);
            }
            return false;
        };
        auto oldFilter = Runtime->SetEventFilter(checkRequests);

        auto request =
            std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(SourceClientId);
        request->Record.MutableHeaders()->SetIsBackgroundRequest(false);

        request->Record.SetSourceDeviceUUID("DA1-1");
        request->Record.SetSourceStartIndex(SourceStartIndex);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlockCount(BlockCount);

        request->Record.SetTargetNodeId(Runtime->GetNodeId(0));
        request->Record.SetTargetClientId(TargetClientId);
        request->Record.SetTargetDeviceUUID("DA1-2");
        request->Record.SetTargetStartIndex(TargetStartIndex);

        DiskAgent1->SendRequest(std::move(request));
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        auto response =
            DiskAgent1
                ->RecvResponse<TEvDiskAgent::TEvDirectCopyBlocksResponse>();
        UNIT_ASSERT(!HasError(response->GetError()));
        UNIT_ASSERT_DOUBLES_EQUAL(
            5000.0,
            TDuration::MicroSeconds(response->Record.GetReadDuration())
                .MilliSeconds(),
            10);
        UNIT_ASSERT_DOUBLES_EQUAL(
            10000.0,
            TDuration::MicroSeconds(response->Record.GetWriteDuration())
                .MilliSeconds(),
            10);
    }

    Y_UNIT_TEST_F(ShouldPerformDirectCopyAndCalcBandwidth, TCopyRangeFixture)
    {
        {   // The first disk agent has network bandwidth config. It should
            // recommend the bandwidth.
            auto request =
                std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(SourceClientId);

            request->Record.SetSourceDeviceUUID("DA1-1");
            request->Record.SetSourceStartIndex(SourceStartIndex);
            request->Record.SetBlockSize(BlockSize);
            request->Record.SetBlockCount(BlockCount);

            request->Record.SetTargetNodeId(Runtime->GetNodeId(1));
            request->Record.SetTargetClientId(TargetClientId);
            request->Record.SetTargetDeviceUUID("DA2-1");
            request->Record.SetTargetStartIndex(TargetStartIndex);

            DiskAgent1->SendRequest(std::move(request));
            auto response =
                DiskAgent1
                    ->RecvResponse<TEvDiskAgent::TEvDirectCopyBlocksResponse>();
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                50_MB,
                response->Record.GetRecommendedBandwidth());
        }
        {   // The second disk agent has no network bandwidth configuration. It
            // shouldn't recommend the bandwidth.
            auto request =
                std::make_unique<TEvDiskAgent::TEvDirectCopyBlocksRequest>();
            request->Record.MutableHeaders()->SetClientId(TargetClientId);

            request->Record.SetSourceDeviceUUID("DA2-2");
            request->Record.SetSourceStartIndex(SourceStartIndex);
            request->Record.SetBlockSize(BlockSize);
            request->Record.SetBlockCount(BlockCount);

            request->Record.SetTargetNodeId(Runtime->GetNodeId(0));
            request->Record.SetTargetClientId(ClientId);
            request->Record.SetTargetDeviceUUID("DA1-1");
            request->Record.SetTargetStartIndex(TargetStartIndex);

            DiskAgent2->SendRequest(std::move(request));
            auto response =
                DiskAgent2
                    ->RecvResponse<TEvDiskAgent::TEvDirectCopyBlocksResponse>();
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetRecommendedBandwidth());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
