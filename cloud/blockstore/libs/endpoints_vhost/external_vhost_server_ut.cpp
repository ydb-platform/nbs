#include "external_vhost_server.h"

#include <cloud/blockstore/libs/client/session_test.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/vector.h>

#include <variant>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStartEndpoint
{
    NProto::TStartEndpointRequest Request;
    NProto::TVolume Volume;
};

struct TAlterEndpoint
{
    NProto::TStartEndpointRequest Request;
    NProto::TVolume Volume;
};

struct TStopEndpoint
{
    TString SocketPath;
};

struct TRefreshEndpoint
{
    TString SocketPath;
    NProto::TVolume Volume;
};

struct TCreateExternalEndpoint
{
    TString ClientId;
    TString DiskId;
    TVector<TString> CmdArgs;
    TVector<TString> Cgroups;
};

struct TPrepareStartExternalEndpoint
{};

struct TStartExternalEndpoint
{};

struct TStopExternalEndpoint
{};

using TEntry = std::variant<
    TStartEndpoint,
    TAlterEndpoint,
    TStopEndpoint,
    TRefreshEndpoint,
    TCreateExternalEndpoint,
    TPrepareStartExternalEndpoint,
    TStartExternalEndpoint,
    TStopExternalEndpoint>;

using THistory = TVector<TEntry>;

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpointListener
    : public IEndpointListener
{
    THistory& History;

    explicit TTestEndpointListener(THistory& history)
        : History {history}
    {}

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(session);

        History.push_back(TStartEndpoint {request, volume});

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(session);

        History.push_back(TAlterEndpoint {request, volume});

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(
        const TString& socketPath) override
    {
        History.push_back(TStopEndpoint {socketPath});

        return MakeFuture<NProto::TError>();
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        History.push_back(TRefreshEndpoint {socketPath, volume});

        return {};
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture<NProto::TError>();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestExternalEndpoint
    : public IExternalEndpoint
{
    THistory& History;

    explicit TTestExternalEndpoint(THistory& history)
        : History {history}
    {}

    void PrepareToStart() override
    {
        History.push_back(TPrepareStartExternalEndpoint{});
    }

    void Start() override
    {
        History.push_back(TStartExternalEndpoint{});
    }

    TFuture<NProto::TError> Stop() override
    {
        History.push_back(TStopExternalEndpoint{});

        return MakeFuture<NProto::TError>();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    const ILoggingServicePtr Logging = CreateLoggingService("console");
    const IServerStatsPtr ServerStats = CreateServerStatsStub();
    const TExecutorPtr Executor = TExecutor::Create("TestService");
    const TString LocalAgentId = "localhost";
    const TString SocketPath = "/tmp/socket.vhost";
    const NClient::ISessionPtr Session = std::make_shared<NClient::TTestSession>();
    const NProto::TVolume Volume = [] {
        NProto::TVolume volume;

        volume.SetDiskId("vol0");
        volume.SetBlocksCount(10'000);
        volume.SetBlockSize(512_B);
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);

        {
            auto* device = volume.AddDevices();
            device->SetDeviceName("/dev/disk/by-path/pci-0000:00:16.0-sas-phy2-lun-0");
            device->SetDeviceUUID("uuid1");
            device->SetAgentId("localhost");
            device->SetBlockCount(4'000);
            device->SetPhysicalOffset(32'000);
        }

        {
            auto* device = volume.AddDevices();
            device->SetDeviceName("/dev/disk/by-path/pci-0000:00:16.0-sas-phy2-lun-0");
            device->SetDeviceUUID("uuid2");
            device->SetAgentId("localhost");
            device->SetBlockCount(6'000);
            device->SetPhysicalOffset(0);
        }

        return volume;
    } ();

    const NProto::TVolume RemoteVolume = [&] {
        NProto::TVolume volume = Volume;

        volume.SetDiskId("vol1");

        auto* device = volume.AddDevices();
        device->SetDeviceName("/dev/disk/by-partlable/NVMENBS02");
        device->SetDeviceUUID("uuid3");
        device->SetAgentId("remote");

        return volume;
    } ();

    const NProto::TVolume FastPathVolume = [&] {
        NProto::TVolume volume;

        volume.SetDiskId("vol0");
        volume.SetBlocksCount(10'000);
        volume.SetBlockSize(4_KB);
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        volume.SetIsFastPathEnabled(true);

        {
            auto* device = volume.AddDevices();
            device->SetDeviceName("/dev/disk/by-path/pci-0000:00:16.0-sas-phy2-lun-0");
            device->SetDeviceUUID("uuid1");
            device->SetAgentId("host1");
            device->SetBlockCount(4'000);
            device->SetPhysicalOffset(32'000);
            auto *rdma = device->MutableRdmaEndpoint();
            rdma->SetHost("host1");
            rdma->SetPort(1111);
        }

        {
            auto* device = volume.AddDevices();
            device->SetDeviceName("/dev/disk/by-path/pci-0000:00:16.0-sas-phy2-lun-0");
            device->SetDeviceUUID("uuid2");
            device->SetAgentId("host2");
            device->SetBlockCount(6'000);
            device->SetPhysicalOffset(0);
            auto *rdma = device->MutableRdmaEndpoint();
            rdma->SetHost("host2");
            rdma->SetPort(2222);
        }

        return volume;
    } ();

    THistory History;

    IEndpointListenerPtr Listener =
        CreateEndpointListener(false);   // no rdma aligned data

public:
    IEndpointListenerPtr CreateEndpointListener(bool isAlignedDataEnabled)
    {
        return CreateExternalVhostEndpointListener(
            {.Logging = Logging,
             .ServerStats = ServerStats,
             .Executor = Executor,
             .LocalAgentId = LocalAgentId,
             .SocketAccessMode = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR,
             .VhostServerTimeoutAfterParentExit = TDuration::Seconds(30),
             .IsAlignedDataEnabled = isAlignedDataEnabled,
             .LocalSSDBlockSizeLimitEnabled = true,
             .FallbackListener = CreateFallbackListener(),
             .Factory = CreateExternalEndpointFactory()});
    }

    NProto::TStartEndpointRequest CreateDefaultStartEndpointRequest()
    {
        NProto::TStartEndpointRequest request;
        request.SetUnixSocketPath(SocketPath);
        request.SetDiskId(Volume.GetDiskId());
        request.SetInstanceId("vm");
        request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
        request.SetVolumeMountMode(NProto::VOLUME_MOUNT_LOCAL);
        request.SetIpcType(NProto::IPC_VHOST);
        request.SetMountSeqNumber(0);
        request.SetClientId("client");
        request.SetVhostQueuesCount(2);
        request.SetDeviceName("local0");
        request.AddClientCGroups("cg-1");
        request.AddClientCGroups("cg-2");

        return request;
    }

private:
    IEndpointListenerPtr CreateFallbackListener()
    {
        return std::make_shared<TTestEndpointListener>(History);
    }

    TExternalEndpointFactory CreateExternalEndpointFactory()
    {
        return [this] (auto ... args) {
            History.push_back(TCreateExternalEndpoint {args...});

            return std::make_shared<TTestExternalEndpoint>(History);
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

bool Equal(const NProto::TDevice& lhs, const NProto::TDevice& rhs)
{
    return lhs.GetBlockCount() == rhs.GetBlockCount()
        && lhs.GetBaseName() == rhs.GetBaseName()
        && lhs.GetAgentId() == rhs.GetAgentId()
        && lhs.GetDeviceUUID() == rhs.GetDeviceUUID()
        && lhs.GetDeviceName() == rhs.GetDeviceName();
}

template <typename T>
bool Equal(
    const google::protobuf::RepeatedPtrField<T>& lhs,
    const google::protobuf::RepeatedPtrField<T>& rhs)
{
    return std::equal(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [] (const auto& x, const auto& y) {
            return Equal(x, y);
        });
}

bool Equal(
    const NProto::TStartEndpointRequest& lhs,
    const NProto::TStartEndpointRequest& rhs)
{
    return lhs.GetUnixSocketPath() == rhs.GetUnixSocketPath()
        && lhs.GetDiskId() == rhs.GetDiskId()
        && lhs.GetInstanceId() == rhs.GetInstanceId()
        && lhs.GetVolumeAccessMode() == rhs.GetVolumeAccessMode()
        && lhs.GetVolumeMountMode() == rhs.GetVolumeMountMode()
        && lhs.GetIpcType() == rhs.GetIpcType()
        && lhs.GetMountSeqNumber() == rhs.GetMountSeqNumber()
        && lhs.GetClientId() == rhs.GetClientId()
        && lhs.GetVhostQueuesCount() == rhs.GetVhostQueuesCount()
        && lhs.GetDeviceName() == rhs.GetDeviceName()
        && std::equal(
            lhs.GetClientCGroups().begin(),
            lhs.GetClientCGroups().end(),
            rhs.GetClientCGroups().begin(),
            rhs.GetClientCGroups().end());
}

bool Equal(const NProto::TVolume& lhs, const NProto::TVolume& rhs)
{
    return lhs.GetDiskId() == rhs.GetDiskId()
        && lhs.GetBlocksCount() == rhs.GetBlocksCount()
        && lhs.GetBlockSize() == rhs.GetBlockSize()
        && lhs.GetStorageMediaKind() == rhs.GetStorageMediaKind()
        && Equal(lhs.GetDevices(), rhs.GetDevices());
}

TString GetArg(const TVector<TString>& args, TStringBuf name)
{
    auto it = Find(args, name);
    if (it == args.end()) {
        return {};
    }
    return *std::next(it);
}

bool HasArg(const TVector<TString>& args, TStringBuf name)
{
    return Find(args, name) != args.end();
}

TVector<TString> GetArgN(const TVector<TString>& args, TStringBuf name)
{
    TVector<TString> values;
    for (size_t i = 0; i != args.size(); ++i) {
        if (args[i] == name && i + 1 != args.size()) {
            values.push_back(args[i + 1]);
        }
    }

    return values;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TExternalEndpointTest)
{
    Y_UNIT_TEST_F(ShouldStartAioExternalEndpoint, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, History.size());

        {
            auto request = CreateDefaultStartEndpointRequest();

            auto error = Listener->StartEndpoint(request, Volume, Session)
                .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(3, History.size());

            auto* create = std::get_if<TCreateExternalEndpoint>(&History[0]);
            UNIT_ASSERT_C(create, "actual entry: " << History[0].index());

            UNIT_ASSERT_VALUES_EQUAL(Volume.GetDiskId(), create->DiskId);

            /*
                --serial local0                     2
                --disk-id vol0                      2
                --block-size 512                    2
                --socket-path /tmp/socket.vhost     2
                --socket-access-mode ...            2
                -q 2                                2
                --device ...                        2
                --device ...                        2
                --read-only                         1
                --wait-after-parent-exit ...        2
                                                   19
            */

            UNIT_ASSERT_VALUES_EQUAL_C(
                19,
                create->CmdArgs.size(),
                JoinStrings(create->CmdArgs, " "));
            UNIT_ASSERT_VALUES_EQUAL("local0", GetArg(create->CmdArgs, "--serial"));
            UNIT_ASSERT_VALUES_EQUAL(
                "vol0",
                GetArg(create->CmdArgs, "--disk-id"));
            UNIT_ASSERT_VALUES_EQUAL(
                "512",
                GetArg(create->CmdArgs, "--block-size"));
            UNIT_ASSERT_VALUES_EQUAL(
                "30",
                GetArg(create->CmdArgs, "--wait-after-parent-exit"));

            UNIT_ASSERT_VALUES_EQUAL(
                "/tmp/socket.vhost",
                GetArg(create->CmdArgs, "--socket-path"));

            UNIT_ASSERT_VALUES_EQUAL("2", GetArg(create->CmdArgs, "-q"));
            UNIT_ASSERT(FindPtr(create->CmdArgs, "--read-only"));

            auto devices = GetArgN(create->CmdArgs, "--device");

            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());

            UNIT_ASSERT_VALUES_EQUAL(TStringBuilder()
                    << "/dev/disk/by-path/pci-0000:00:16.0-sas-phy2-lun-0:"
                    << (4'000 * 512_B)
                    << ":32000",
                devices[0]);

            UNIT_ASSERT_VALUES_EQUAL(TStringBuilder()
                    << "/dev/disk/by-path/pci-0000:00:16.0-sas-phy2-lun-0:"
                    << (6'000 * 512_B)
                    << ":0",
                devices[1]);

            UNIT_ASSERT_VALUES_EQUAL(request.GetClientId(), create->ClientId);
            UNIT_ASSERT_VALUES_EQUAL(2, create->Cgroups.size());
            UNIT_ASSERT_VALUES_EQUAL("cg-1", create->Cgroups[0]);
            UNIT_ASSERT_VALUES_EQUAL("cg-2", create->Cgroups[1]);

            auto* prepareStart =
                std::get_if<TPrepareStartExternalEndpoint>(&History[1]);
            UNIT_ASSERT_C(prepareStart, "actual entry: " << History[1].index());

            auto* start = std::get_if<TStartExternalEndpoint>(&History[2]);
            UNIT_ASSERT_C(start, "actual entry: " << History[2].index());

            History.clear();
        }

        {
            auto error = Listener->StopEndpoint(SocketPath).GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(1, History.size());
            auto* stop = std::get_if<TStopExternalEndpoint>(&History[0]);
            UNIT_ASSERT_C(stop, "actual entry: " << History[0].index());
        }
    }

    Y_UNIT_TEST_F(ShouldStartEncryptedAioExternalEndpointWithPath, TFixture)
    {
        auto request = CreateDefaultStartEndpointRequest();
        auto* encryption = request.MutableEncryptionSpec();
        encryption->SetMode(NProto::EEncryptionMode::ENCRYPTION_AES_XTS);
        encryption->MutableKeyPath()->SetFilePath("/tmp/secret.key");

        auto error =
            Listener->StartEndpoint(request, Volume, Session).GetValueSync();
        UNIT_ASSERT_C(!HasError(error), error);

        auto* create = std::get_if<TCreateExternalEndpoint>(&History[0]);

        /*
            --serial local0                     2
            --disk-id vol0                      2
            --block-size 512                    2
            --socket-path /tmp/socket.vhost     2
            --socket-access-mode ...            2
            -q 2                                2
            --device ...                        2
            --device ...                        2
            --read-only                         1
            --wait-after-parent-exit ...        2
            --wait-after-parent-exit ...        2
            --encryption-mode ...               2
            --encryption-key-path ...           2
                                               23
        */

        UNIT_ASSERT_VALUES_EQUAL_C(
            23,
            create->CmdArgs.size(),
            JoinStrings(create->CmdArgs, " "));

        UNIT_ASSERT_VALUES_EQUAL(
            "aes-xts",
            GetArg(create->CmdArgs, "--encryption-mode"));
        UNIT_ASSERT_VALUES_EQUAL(
            "/tmp/secret.key",
            GetArg(create->CmdArgs, "--encryption-key-path"));
    }

    Y_UNIT_TEST_F(ShouldStartEncryptedAioExternalEndpointWithKeyring, TFixture)
    {
        auto request = CreateDefaultStartEndpointRequest();
        auto* encryption = request.MutableEncryptionSpec();
        encryption->SetMode(NProto::EEncryptionMode::ENCRYPTION_AES_XTS);
        encryption->MutableKeyPath()->SetKeyringId(100);

        auto error =
            Listener->StartEndpoint(request, Volume, Session).GetValueSync();
        UNIT_ASSERT_C(!HasError(error), error);

        auto* create = std::get_if<TCreateExternalEndpoint>(&History[0]);

        /*
            --serial local0                     2
            --disk-id vol0                      2
            --block-size 512                    2
            --socket-path /tmp/socket.vhost     2
            --socket-access-mode ...            2
            -q 2                                2
            --device ...                        2
            --device ...                        2
            --read-only                         1
            --wait-after-parent-exit ...        2
            --wait-after-parent-exit ...        2
            --encryption-mode ...               2
            --encryption-keyring-id ...         2
                                               23
        */

        UNIT_ASSERT_VALUES_EQUAL_C(
            23,
            create->CmdArgs.size(),
            JoinStrings(create->CmdArgs, " "));

        UNIT_ASSERT_VALUES_EQUAL(
            "aes-xts",
            GetArg(create->CmdArgs, "--encryption-mode"));
        UNIT_ASSERT_VALUES_EQUAL(
            "100",
            GetArg(create->CmdArgs, "--encryption-keyring-id"));
    }

    Y_UNIT_TEST_F(ShouldStartRdmaExternalEndpoint, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, History.size());

        {
            auto request = CreateDefaultStartEndpointRequest();

            auto error = Listener->StartEndpoint(request, FastPathVolume, Session)
                .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(3, History.size());

            auto* create = std::get_if<TCreateExternalEndpoint>(&History[0]);
            UNIT_ASSERT_C(create, "actual entry: " << History[0].index());

            UNIT_ASSERT_VALUES_EQUAL(Volume.GetDiskId(), create->DiskId);

            /*
                --serial local0                     2
                --block-size 512                    2
                --socket-path /tmp/socket.vhost     2
                --socket-access-mode ...            2
                -q 2                                2
                --client-id ...                     2
                --disk-id ...                       2
                --device-backend ...                2
                --block-size ...                    2
                --device ...                        2
                --device ...                        2
                --read-only                         1
                --wait-after-parent-exit ...        2
                                                   23
            */

            UNIT_ASSERT_VALUES_EQUAL_C(
                23,
                create->CmdArgs.size(),
                JoinStrings(create->CmdArgs, " "));
            UNIT_ASSERT_VALUES_EQUAL("local0", GetArg(create->CmdArgs, "--serial"));

            UNIT_ASSERT_VALUES_EQUAL(
                "/tmp/socket.vhost",
                GetArg(create->CmdArgs, "--socket-path"));

            UNIT_ASSERT_VALUES_EQUAL("2", GetArg(create->CmdArgs, "-q"));

            UNIT_ASSERT_VALUES_EQUAL("client", GetArg(create->CmdArgs, "--client-id"));

            UNIT_ASSERT_VALUES_EQUAL("vol0", GetArg(create->CmdArgs, "--disk-id"));
            UNIT_ASSERT_VALUES_EQUAL(
                "30",
                GetArg(create->CmdArgs, "--wait-after-parent-exit"));

            UNIT_ASSERT_VALUES_EQUAL("rdma", GetArg(create->CmdArgs, "--device-backend"));

            UNIT_ASSERT_VALUES_EQUAL("4096", GetArg(create->CmdArgs, "--block-size"));

            UNIT_ASSERT(FindPtr(create->CmdArgs, "--read-only"));

            auto devices = GetArgN(create->CmdArgs, "--device");

            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());

            UNIT_ASSERT_VALUES_EQUAL(TStringBuilder()
                    << "rdma://host1:1111/uuid1:"
                    << (4'000 * 4_KB)
                    << ":0",
                devices[0]);

            UNIT_ASSERT_VALUES_EQUAL(TStringBuilder()
                    << "rdma://host2:2222/uuid2:"
                    << (6'000 * 4_KB)
                    << ":0",
                devices[1]);

            UNIT_ASSERT_VALUES_EQUAL(request.GetClientId(), create->ClientId);
            UNIT_ASSERT_VALUES_EQUAL(2, create->Cgroups.size());
            UNIT_ASSERT_VALUES_EQUAL("cg-1", create->Cgroups[0]);
            UNIT_ASSERT_VALUES_EQUAL("cg-2", create->Cgroups[1]);

            auto* prepareStart =
                std::get_if<TPrepareStartExternalEndpoint>(&History[1]);
            UNIT_ASSERT_C(prepareStart, "actual entry: " << History[1].index());

            auto* start = std::get_if<TStartExternalEndpoint>(&History[2]);
            UNIT_ASSERT_C(start, "actual entry: " << History[2].index());

            History.clear();
        }

        {
            auto error = Listener->StopEndpoint(SocketPath).GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(1, History.size());
            auto* stop = std::get_if<TStopExternalEndpoint>(&History[0]);
            UNIT_ASSERT_C(stop, "actual entry: " << History[0].index());

            History.clear();
        }

        {
            auto alignedDataListener = CreateEndpointListener(true);

            auto request = CreateDefaultStartEndpointRequest();

            auto error = alignedDataListener
                             ->StartEndpoint(request, FastPathVolume, Session)
                             .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(3, History.size());

            auto* create = std::get_if<TCreateExternalEndpoint>(&History[0]);
            UNIT_ASSERT_C(create, "actual entry: " << History[0].index());

            UNIT_ASSERT_C(
                HasArg(create->CmdArgs, "--rdma-aligned-data"),
                "missing --rdma-aligned-data arg");

            History.clear();
        }
    }

    Y_UNIT_TEST_F(ShouldFallbackOnRemoteVolume, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, History.size());

        {
            auto request = CreateDefaultStartEndpointRequest();
            request.SetDiskId(RemoteVolume.GetDiskId());

            auto error = Listener->StartEndpoint(request, RemoteVolume, Session)
                .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(1, History.size());

            auto* start = std::get_if<TStartEndpoint>(&History[0]);
            UNIT_ASSERT_C(start, "actual entry: " << History[0].index());
            UNIT_ASSERT_C(Equal(request, start->Request), start->Request);
            UNIT_ASSERT_C(Equal(RemoteVolume, start->Volume), start->Volume);

            History.clear();
        }

        {
            auto error = Listener->StopEndpoint(SocketPath).GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(1, History.size());
            auto* stop = std::get_if<TStopEndpoint>(&History[0]);
            UNIT_ASSERT_C(stop, "actual entry: " << History[0].index());
        }
    }

    Y_UNIT_TEST_F(ShouldAlterEndpoint, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, History.size());

        {
            auto request = CreateDefaultStartEndpointRequest();
            request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
            request.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);

            auto error = Listener->StartEndpoint(request, Volume, Session)
                .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(1, History.size());
            auto* start = std::get_if<TStartEndpoint>(&History[0]);
            UNIT_ASSERT_C(start, "actual entry: " << History[0].index());

            History.clear();
        }

        {
            auto request = CreateDefaultStartEndpointRequest();
            request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_WRITE);
            request.SetVolumeMountMode(NProto::VOLUME_MOUNT_LOCAL);

            auto error = Listener->AlterEndpoint(request, Volume, Session)
                .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(4, History.size());

            auto* stop = std::get_if<TStopEndpoint>(&History[0]);
            UNIT_ASSERT_C(stop, "actual entry: " << History[0].index());

            auto* create = std::get_if<TCreateExternalEndpoint>(&History[1]);
            UNIT_ASSERT_C(create, "actual entry: " << History[1].index());

            auto* prepareStart =
                std::get_if<TPrepareStartExternalEndpoint>(&History[2]);
            UNIT_ASSERT_C(prepareStart, "actual entry: " << History[2].index());

            auto* start = std::get_if<TStartExternalEndpoint>(&History[3]);
            UNIT_ASSERT_C(start, "actual entry: " << History[3].index());

            History.clear();
        }

        {
            auto request = CreateDefaultStartEndpointRequest();
            request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
            request.SetVolumeMountMode(NProto::VOLUME_MOUNT_LOCAL);

            auto error = Listener->AlterEndpoint(request, Volume, Session)
                .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(4, History.size());

            auto* stop = std::get_if<TStopExternalEndpoint>(&History[0]);
            UNIT_ASSERT_C(stop, "actual entry: " << History[0].index());

            auto* create = std::get_if<TCreateExternalEndpoint>(&History[1]);
            UNIT_ASSERT_C(create, "actual entry: " << History[1].index());

            auto* prepareStart =
                std::get_if<TPrepareStartExternalEndpoint>(&History[2]);
            UNIT_ASSERT_C(prepareStart, "actual entry: " << History[2].index());

            auto* start = std::get_if<TStartExternalEndpoint>(&History[3]);
            UNIT_ASSERT_C(start, "actual entry: " << History[3].index());

            History.clear();
        }

        {
            auto request = CreateDefaultStartEndpointRequest();
            request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_WRITE);
            request.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);

            auto error = Listener->AlterEndpoint(request, Volume, Session)
                .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(2, History.size());

            auto* stop = std::get_if<TStopExternalEndpoint>(&History[0]);
            UNIT_ASSERT_C(stop, "actual entry: " << History[0].index());

            auto* create = std::get_if<TStartEndpoint>(&History[1]);
            UNIT_ASSERT_C(create, "actual entry: " << History[1].index());

            History.clear();
        }

        {
            auto request = CreateDefaultStartEndpointRequest();
            request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
            request.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);

            auto error = Listener->AlterEndpoint(request, Volume, Session)
                .GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(1, History.size());

            auto* alter = std::get_if<TAlterEndpoint>(&History[0]);
            UNIT_ASSERT_C(alter, "actual entry: " << History[0].index());

            History.clear();
        }

        {
            auto error = Listener->StopEndpoint(SocketPath).GetValueSync();
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(1, History.size());
            auto* stop = std::get_if<TStopEndpoint>(&History[0]);
            UNIT_ASSERT_C(stop, "actual entry: " << History[0].index());
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
