#include "disk_agent.h"

#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/disk_agent/testlib/test_env.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <filesystem>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NServer;
using namespace NThreading;

using namespace NDiskAgentTest;

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;
    const TFsPath DevPath = TempDir.Path() / "dev";
    const TFsPath PartLabelsPath = TempDir.Path() / "by-partlabel";

    const TString CachedSessionsPath =
        TempDir.Path() / "nbs-disk-agent-sessions.txt";
    const TString CachedConfigPath = TempDir.Path() / "nbs-disk-agent.txt";
    const TDuration ReleaseInactiveSessionsTimeout = 10s;

    std::optional<TTestBasicRuntime> Runtime;
    NMonitoring::TDynamicCountersPtr Counters =
        MakeIntrusive<NMonitoring::TDynamicCounters>();

    TVector<TFsPath> Devices;
    TVector<TFsPath> PartLabels;
    const TVector<TString> IDs = {
        "79955ae90189fe8a89ab832a8b0cb57d",   // NVMENBS01
        "657dabaf3d224c9177b00c437716dfb1",   // NVMENBS02
        "e85cd1d217c3239507fc0cd180a075fd",   // NVMENBS03
        "5ea2fcdce0a180a63db2b5f6a5b34221"    // NVMENBS04
    };

    const ui64 DefaultFileSize = DefaultDeviceBlockSize * DefaultBlocksCount;

    void PrepareFile(const TString& label, const TString& dev, size_t size)
    {
        TFile file(DevPath / dev, EOpenModeFlag::CreateNew);
        file.Resize(size);

        Devices.push_back(file.GetName());
        PartLabels.push_back(PartLabelsPath / label);

        const bool ok = NFs::SymLink(Devices.back(), PartLabels.back());
        UNIT_ASSERT(ok);
    }

    auto CreateDiskAgentConfig()
    {
        auto config = DiskAgentConfig();

        auto& discovery = *config.MutableStorageDiscoveryConfig();
        auto& path = *discovery.AddPathConfigs();
        path.SetPathRegExp(PartLabelsPath / "NVMENBS([0-9]{2})");
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
        auto request =
            std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetDeviceUUID(deviceId);
        request->Record.SetStartIndex(1);
        request->Record.SetBlockSize(DefaultBlockSize);

        auto sgList =
            ResizeIOVector(*request->Record.MutableBlocks(), 10, 4_KB);

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
        const auto response =
            ReadDeviceBlocks(*Runtime, diskAgent, deviceId, 1, 10, clientId);

        return response->Record.GetError();
    }

    auto LoadSessionCache()
    {
        NProto::TDiskAgentDeviceSessionCache cache;
        ParseProtoTextFromFileRobust(CachedSessionsPath, cache);

        TVector<NProto::TDiskAgentDeviceSession> sessions(
            std::make_move_iterator(cache.MutableSessions()->begin()),
            std::make_move_iterator(cache.MutableSessions()->end()));

        SortBy(sessions, [](auto& session) { return session.GetClientId(); });

        for (auto& session: sessions) {
            Sort(*session.MutableDeviceIds());
        }

        return sessions;
    }

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Runtime.emplace();

        InitCriticalEventsCounter(Counters);

        PartLabelsPath.MkDirs();
        DevPath.MkDirs();

        PrepareFile("NVMENBS01", "nvme0n1p1", DefaultFileSize);
        PrepareFile("NVMENBS02", "nvme1n1p1", DefaultFileSize);
        PrepareFile("NVMENBS03", "nvme2n1p1", DefaultFileSize);
        PrepareFile("NVMENBS04", "nvme3n1p1", DefaultFileSize);
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        for (const auto& path: PartLabels) {
            path.DeleteIfExists();
        }

        for (const auto& path: Devices) {
            path.DeleteIfExists();
        }
    }

public:
    void ShouldIgnoreRemovedDeviceImpl(const TString& labelToRemove)
    {
        // build the config cache
        {
            NProto::TDiskAgentConfig config;

            for (ui32 i = 0; i != PartLabels.size(); ++i) {
                auto& device = *config.AddFileDevices();
                device.SetPath(PartLabels[i]);
                device.SetDeviceId(IDs[i]);
                device.SetBlockSize(4_KB);
            }

            auto error = SaveDiskAgentConfig(CachedConfigPath, config);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        NFs::Remove(labelToRemove);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);

        auto mismatch = counters->GetCounter(
            "DiskAgentCriticalEvents/DiskAgentConfigMismatch",
            true);

        UNIT_ASSERT_VALUES_EQUAL(0, mismatch->Val());

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        // ignore the absence of NVMENBS0X
        UNIT_ASSERT_VALUES_EQUAL(0, mismatch->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            PartLabels.size() - 1,
            env.DiskRegistryState->Devices.size());

        // check the config cache
        {
            TVector<TString> expected(PartLabels.begin(), PartLabels.end());
            Erase(expected, labelToRemove);
            Sort(expected);

            auto [config, error] = LoadDiskAgentConfig(CachedConfigPath);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), config.FileDevicesSize());
            TVector<TString> paths;
            for (const auto& device: config.GetFileDevices()) {
                paths.push_back(device.GetPath());
            }
            Sort(paths);

            for (ui32 i = 0; i != expected.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(expected[i], paths[i]);
            }
        }
    }
};

TVector<ui64> FindProcessesWithOpenFile(const TString& targetPath)
{
    TVector<ui64> result;

    namespace NFs = std::filesystem;

    for (const auto& pid: NFs::directory_iterator{"/proc"}) {
        try {
            for (const auto& fd: NFs::directory_iterator{pid.path() / "fd"}) {
                if (!fd.is_symlink()) {
                    continue;
                }

                if (targetPath == NFs::read_symlink(fd).string()) {
                    result.emplace_back(
                        FromString<i64>(pid.path().filename().string()));
                    break;
                }
            }
        } catch (...) {
            continue;
        }
    }

    SortUnique(result);

    return result;
}

class TDiskAgentAttachDetachModel
{
private:
    ui64 DiskRegistryGeneration = 0;
    ui64 RequestNumber = 0;
    THashMap<TString, bool> PathAttached;

public:
    explicit TDiskAgentAttachDetachModel(const TVector<TString>& paths)
    {
        for (const auto& path: paths) {
            PathAttached[path] = true;
        }
    }

    bool AttachDetachPath(
        ui64 diskRegistryGeneration,
        ui64 requestNumber,
        const TVector<TString>& paths,
        bool attach)
    {
        if (!CheckGenerationAndUpdateItIfNeeded(
                diskRegistryGeneration,
                requestNumber))
        {
            return false;
        }

        for (const auto& path: paths) {
            PathAttached[path] = attach;
        }

        return true;
    }

    bool CheckGenerationAndUpdateItIfNeeded(
        ui64 diskRegistryGeneration,
        ui64 requestNumber)
    {
        if (diskRegistryGeneration < DiskRegistryGeneration) {
            return false;
        }

        if (diskRegistryGeneration > DiskRegistryGeneration) {
            RequestNumber = 0;
        }

        DiskRegistryGeneration = diskRegistryGeneration;

        if (requestNumber < RequestNumber) {
            return false;
        }

        RequestNumber = requestNumber;

        return true;
    }

    [[nodiscard]] bool IsPathAttached(const TString& path) const
    {
        return PathAttached.at(path);
    }
};

class TAttachDetachRequestsGenerator
{
private:
    static constexpr ui64 GenerationSpread = 100;
    static constexpr ui64 MoveGenerationWindowPerRun = 10;

    ui64 DevicesCount;
    TVector<TString> Paths;

    ui64 MinRequestNumber = 1;
    TVector<bool> GeneratedShouldAttachPath;

    ui64 MinDiskRegistryGeneration = 1;
    ui32 GeneratedDiskRegistryGeneration = 0;

public:
    explicit TAttachDetachRequestsGenerator(const TVector<TString>& paths)
        : DevicesCount(paths.size())
        , Paths(std::move(paths))
    {}

    ui64 GenerateDiskRegistryGeneration()
    {
        MinDiskRegistryGeneration += MoveGenerationWindowPerRun;
        GeneratedDiskRegistryGeneration =
            RandomNumber<ui32>(GenerationSpread) + MinDiskRegistryGeneration;
        MinRequestNumber = 1;

        return GeneratedDiskRegistryGeneration;
    }

    struct TRequest
    {
        ui64 RequestNumber;
        TVector<TString> Paths;
    };

    TVector<TRequest> GetAttachDetachRequests(bool attach)
    {
        GenerateShouldAttachPath();
        auto pathIdxs = GetPathIdxsToAttachDetach(attach);
        TVector<TRequest> requests;

        while (pathIdxs.size() > 0) {
            TRequest request;
            request.RequestNumber = GenerateRequestNumber();

            const ui64 devicesInRequest =
                Max(RandomNumber<ui64>(pathIdxs.size() + 1), ui64{1});

            for (ui64 i = 0; i < devicesInRequest; ++i) {
                auto pathIdx = pathIdxs.back();

                request.Paths.emplace_back(Paths[pathIdx]);

                pathIdxs.pop_back();
            }

            requests.push_back(request);
        }

        return requests;
    }

private:
    ui64 GenerateRequestNumber()
    {
        MinRequestNumber += MoveGenerationWindowPerRun;
        return RandomNumber<ui64>(GenerationSpread) + MinRequestNumber;
    }

    void GenerateShouldAttachPath()
    {
        GeneratedShouldAttachPath.clear();
        for (ui64 i = 0; i < DevicesCount; ++i) {
            GeneratedShouldAttachPath.push_back(RandomNumber<ui64>(2) != 0);
        }
    }

    TVector<ui64> GetPathIdxsToAttachDetach(bool attach)
    {
        TVector<ui64> result;
        for (ui64 i = 0; i < DevicesCount; ++i) {
            if (GeneratedShouldAttachPath[i] == attach) {
                result.push_back(i);
            }
        }
        return result;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskAgentStressTest)
{
    Y_UNIT_TEST_F(AttachDetachPathStressTest, TFixture)
    {
        auto storageConfig = NProto::TStorageServiceConfig();
        storageConfig.SetAttachDetachPathsEnabled(true);

        auto env = TTestEnvBuilder(*Runtime)
                       .With(CreateDiskAgentConfig())
                       .With(storageConfig)
                       .Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TVector<TString> paths;
        for (const auto& path: PartLabels) {
            paths.emplace_back(path.GetPath());
        }

        TAttachDetachRequestsGenerator requestsGenerator(paths);
        TDiskAgentAttachDetachModel diskAgentModel(paths);

        static constexpr ui64 EventsCount = 100;
        static constexpr ui64 RegenerateDiskRegistryGenerationInterval = 10;

        ui64 diskRegistryGeneration = 0;

        for (ui64 eventIdx = 0; eventIdx < EventsCount; ++eventIdx) {
            if (eventIdx % RegenerateDiskRegistryGenerationInterval == 0) {
                diskRegistryGeneration =
                    requestsGenerator.GenerateDiskRegistryGeneration();
            }

            for (bool attach: {true, false}) {
                auto requests =
                    requestsGenerator.GetAttachDetachRequests(attach);

                for (auto& request: requests) {
                    NProto::TError error;

                    if (attach) {
                        diskAgent.SendAttachPathsRequest(
                            request.Paths,
                            diskRegistryGeneration,
                            request.RequestNumber);
                        error = diskAgent.RecvAttachPathsResponse()->GetError();
                    } else {
                        diskAgent.SendDetachPathsRequest(
                            request.Paths,
                            diskRegistryGeneration,
                            request.RequestNumber);
                        error = diskAgent.RecvDetachPathsResponse()->GetError();
                    }

                    bool shouldBeSuccessful = diskAgentModel.AttachDetachPath(
                        diskRegistryGeneration,
                        request.RequestNumber,
                        request.Paths,
                        attach);

                    UNIT_ASSERT_VALUES_EQUAL(
                        shouldBeSuccessful,
                        !HasError(error));
                }
            }

            for (size_t i = 0; i < Devices.size(); ++i) {
                auto procesesWithOpenFileExpected =
                    diskAgentModel.IsPathAttached(PartLabels[i]) ? 1 : 0;
                UNIT_ASSERT_VALUES_EQUAL(
                    procesesWithOpenFileExpected,
                    FindProcessesWithOpenFile(Devices[i]).size());
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
