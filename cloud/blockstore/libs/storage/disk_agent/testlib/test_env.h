#pragma once

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/fault_injection.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/service_local/storage_local.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>
#include <cloud/blockstore/libs/spdk/iface/env_test.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/testlib/common_properties.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <contrib/ydb/core/tablet_flat/tablet_flat_executed.h>
#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NDiskAgentTest {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t DefaultBlocksCount = 1024;
constexpr size_t DefaultDeviceBlockSize = 512;
constexpr size_t DefaultBlockSize = 4_KB;
constexpr size_t DefaultStubBlocksCount = 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TSgList ConvertToSgList(const NProto::TIOVector& iov, ui32 blockSize);

////////////////////////////////////////////////////////////////////////////////

struct TDiskRegistryState
    : public TAtomicRefCount<TDiskRegistryState>
{
    using TPtr = TIntrusivePtr<TDiskRegistryState>;

    THashMap<TString, NProto::TDeviceConfig> Devices;
    THashMap<ui32, NProto::TAgentStats> Stats;

    TVector<TString> DisabledDevices;
};

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryMock final
    : public NActors::TActor<TDiskRegistryMock>
{
private:
    TDiskRegistryState::TPtr State;

public:
    explicit TDiskRegistryMock(TDiskRegistryState::TPtr state);

private:
    STFUNC(StateWork);

    void HandleRegisterAgent(
        const TEvDiskRegistry::TEvRegisterAgentRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateAgentStats(
        const TEvDiskRegistry::TEvUpdateAgentStatsRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSubscribe(
        const TEvDiskRegistryProxy::TEvSubscribeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentClient
{
private:
    NActors::TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    NActors::TActorId Sender;

public:
    TDiskAgentClient(NActors::TTestActorRuntime& runtime, ui32 nodeIdx = 0)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , Sender(runtime.AllocateEdgeActor(nodeIdx))
    {}

    NActors::TActorId DiskAgentActorId() const
    {
        return MakeDiskAgentServiceId(Runtime.GetNodeId(NodeIdx));
    }

    [[nodiscard]] ui32 GetNodeId() const
    {
        return Runtime.GetNodeId(NodeIdx);
    }

    template <typename TRequest>
    void SendRequest(std::unique_ptr<TRequest> request, ui64 cookie = 0)
    {
        auto* ev = new NActors::IEventHandle(
            DiskAgentActorId(),
            Sender,
            request.release(),
            0,   // flags
            cookie,
            nullptr);   // forwardOnNondelivery

        Runtime.Send(ev, NodeIdx);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse(TDuration simTimeout = TDuration::Seconds(5))
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, simTimeout);

        UNIT_ASSERT(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    auto CreateWaitReadyRequest()
    {
        return std::make_unique<TEvDiskAgent::TEvWaitReadyRequest>();
    }

    auto CreateAcquireDevicesRequest(
        const TVector<TString>& ids,
        const TString& clientId,
        const NProto::EVolumeAccessMode accessMode,
        const ui64 mountSeqNumber = 0,
        const TString& diskId = "",
        const ui32 volumeGeneration = 0,
        const NSpdk::TDeviceRateLimits& limits = {})
    {
        auto request = std::make_unique<TEvDiskAgent::TEvAcquireDevicesRequest>();

        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetAccessMode(accessMode);
        request->Record.SetMountSeqNumber(mountSeqNumber);
        request->Record.SetDiskId(diskId);
        request->Record.SetVolumeGeneration(volumeGeneration);

        for (const auto& id : ids) {
            *request->Record.AddDeviceUUIDs() = id;
        }

        if (limits.IopsLimit ||
            limits.BandwidthLimit ||
            limits.ReadBandwidthLimit ||
            limits.WriteBandwidthLimit)
        {
            auto& rateLimits = *request->Record.MutableRateLimits();

            rateLimits.SetIopsLimit(limits.IopsLimit);
            rateLimits.SetBandwidthLimit(limits.BandwidthLimit);
            rateLimits.SetReadBandwidthLimit(limits.ReadBandwidthLimit);
            rateLimits.SetWriteBandwidthLimit(limits.WriteBandwidthLimit);
        }

        return request;
    }

    auto CreateReleaseDevicesRequest(
        const TVector<TString>& ids,
        const TString& clientId,
        const TString& diskId = "",
        const ui32 volumeGeneration = 0)
    {
        auto request = std::make_unique<TEvDiskAgent::TEvReleaseDevicesRequest>();

        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetDiskId(diskId);
        request->Record.SetVolumeGeneration(volumeGeneration);

        for (const auto& id : ids) {
            *request->Record.AddDeviceUUIDs() = id;
        }

        return request;
    }

    auto CreateDisableConcreteAgentRequest(const TVector<TString>& deviceIds)
    {
        auto request =
            std::make_unique<TEvDiskAgent::TEvDisableConcreteAgentRequest>();

        for (const auto& deviceId: deviceIds) {
            *request->Record.AddDeviceUUIDs() = deviceId;
        }

        return request;
    }

    auto CreateEnableAgentDeviceRequest(const TString deviceId)
    {
        auto request =
            std::make_unique<TEvDiskAgent::TEvEnableAgentDeviceRequest>();

        request->Record.SetDeviceUUID(deviceId);
        return request;
    }

    auto CreateReadDeviceBlocksRequest(
        const TString& uuid,
        ui64 startIndex,
        ui32 blocksCount,
        const TString& clientId)
    {
        auto request = std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetDeviceUUID(uuid);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlockSize(DefaultBlockSize);
        request->Record.SetBlocksCount(blocksCount);

        return request;
    }

    auto CreateZeroDeviceBlocksRequest(
        const TString& uuid,
        ui64 startIndex,
        ui32 blocksCount,
        const TString& clientId)
    {
        auto request = std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetDeviceUUID(uuid);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlockSize(DefaultBlockSize);
        request->Record.SetBlocksCount(blocksCount);

        return request;
    }

    auto CreateWriteDeviceBlocksRequest(
        const TString& uuid,
        ui64 startIndex,
        const TSgList& data,
        const TString& clientId)
    {
        auto request = std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetDeviceUUID(uuid);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlockSize(DefaultBlockSize);

        auto dst = ResizeIOVector(
            *request->Record.MutableBlocks(),
            data.size(),
            DefaultBlockSize);

        SgListCopy(data, dst);

        return request;
    }

    auto CreateSecureEraseDeviceRequest(TString uuid)
    {
        auto request = std::make_unique<TEvDiskAgent::TEvSecureEraseDeviceRequest>();

        request->Record.SetDeviceUUID(std::move(uuid));

        return request;
    }

    auto CreateChecksumDeviceBlocksRequest(
        const TString& uuid,
        ui64 startIndex,
        ui32 blocksCount,
        const TString& clientId)
    {
        auto request = std::make_unique<TEvDiskAgent::TEvChecksumDeviceBlocksRequest>();
        request->Record.MutableHeaders()->SetClientId(clientId);
        request->Record.SetDeviceUUID(uuid);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlockSize(DefaultBlockSize);
        request->Record.SetBlocksCount(blocksCount);

        return request;
    }

    auto CreateCollectStatsRequest()
    {
        return std::make_unique<TEvDiskAgentPrivate::TEvCollectStatsRequest>();
    }

    auto CreatePartiallySuspendAgentRequest(TDuration cancelSuspensionDelay)
    {
        auto request =
            std::make_unique<TEvDiskAgent::TEvPartiallySuspendAgentRequest>();
        request->Record.SetCancelSuspensionDelay(
            cancelSuspensionDelay.MilliSeconds());
        return request;
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(std::move(request));                                       \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response(             \
        TDuration simTimeout = TDuration::Seconds(5))                          \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>(simTimeout);              \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(std::move(request));                                       \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_DISK_AGENT_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvDiskAgent)
    BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvDiskAgentPrivate)

#undef BLOCKSTORE_DECLARE_METHOD
};

template <typename ... T>
auto WriteDeviceBlocks(
    NActors::TTestActorRuntime& runtime,
    TDiskAgentClient& diskAgent,
    T && ... args)
{
    diskAgent.SendWriteDeviceBlocksRequest(std::forward<T>(args)...);
    runtime.DispatchEvents(NActors::TDispatchOptions());
    return diskAgent.RecvWriteDeviceBlocksResponse();
}

template <typename ... T>
auto ReadDeviceBlocks(
    NActors::TTestActorRuntime& runtime,
    TDiskAgentClient& diskAgent,
    T && ... args)
{
    diskAgent.SendReadDeviceBlocksRequest(std::forward<T>(args)...);
    runtime.DispatchEvents(NActors::TDispatchOptions());
    return diskAgent.RecvReadDeviceBlocksResponse();
}

template <typename ... T>
auto ZeroDeviceBlocks(
    NActors::TTestActorRuntime& runtime,
    TDiskAgentClient& diskAgent,
    T && ... args)
{
    diskAgent.SendZeroDeviceBlocksRequest(std::forward<T>(args)...);
    runtime.DispatchEvents(NActors::TDispatchOptions());
    return diskAgent.RecvZeroDeviceBlocksResponse();
}

template <typename ... T>
auto ChecksumDeviceBlocks(
    NActors::TTestActorRuntime& runtime,
    TDiskAgentClient& diskAgent,
    T && ... args)
{
    diskAgent.SendChecksumDeviceBlocksRequest(std::forward<T>(args)...);
    runtime.DispatchEvents(NActors::TDispatchOptions());
    return diskAgent.RecvChecksumDeviceBlocksResponse();
}

////////////////////////////////////////////////////////////////////////////////

struct TTestSpdkTarget final
    : NSpdk::TTestSpdkTarget
{
    TAtomic& EraseCount;
    NThreading::TPromise<NProto::TError>& EraseResult;

    explicit TTestSpdkTarget(
        TAtomic& eraseCount,
        NThreading::TPromise<NProto::TError>& eraseResult);

    NSpdk::ISpdkDevicePtr GetDevice(const TString& name) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSpdkEnv final
    : NSpdk::TTestSpdkEnv
{
    THashMap<TString, NProto::TDeviceConfig> Devices;
    TAtomic SecureEraseCount = 0;
    NThreading::TPromise<NProto::TError> SecureEraseResult;

    explicit TTestSpdkEnv(const TVector<NProto::TDeviceConfig>& devices);

    NThreading::TFuture<TString> RegisterFileDevice(
        const TString& name,
        const TString& path,
        ui32 blockSize) override;

    NThreading::TFuture<TString> RegisterMemoryDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) override;

    NThreading::TFuture<TVector<TString>> RegisterNVMeDevices(
        const TString& baseName,
        const TString& transportId) override;

    NThreading::TFuture<void> UnregisterDevice(const TString& deviceName) override;

    NThreading::TFuture<void> EnableHistogram(const TString& deviceName, bool enable) override;

    NThreading::TFuture<void> StartListen(const TString& transportId) override;

    NThreading::TFuture<NSpdk::TDeviceStats> QueryDeviceStats(const TString& name) override;

    NThreading::TFuture<NSpdk::ISpdkTargetPtr> CreateNVMeTarget(
        const TString& nqn,
        const TVector<TString>& devices,
        const TVector<TString>& transportIds) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    NActors::TTestActorRuntime& Runtime;
    TDiskRegistryState::TPtr DiskRegistryState;
    IFileIOServicePtr FileIOService;
    NNvme::INvmeManagerPtr NvmeManager;
    NActors::TActorId DiskAgentActorId;

    ~TTestEnv();
};

struct TTestEnvBuilder
{
    NActors::TTestActorRuntime& Runtime;

    NProto::TDiskAgentConfig AgentConfigProto;
    TVector<NProto::TDiskAgentConfig> AdditionalAgentConfigsProto;
    IStorageProviderPtr StorageProvider;
    IFileIOServicePtr FileIOService;
    NNvme::INvmeManagerPtr NvmeManager;
    NSpdk::ISpdkEnvPtr Spdk;
    NProto::TStorageServiceConfig StorageServiceConfig;
    TDiskRegistryState::TPtr DiskRegistryState;

    explicit TTestEnvBuilder(NActors::TTestActorRuntime& runtime);

    TTestEnvBuilder& With(NSpdk::ISpdkEnvPtr spdk);
    TTestEnvBuilder& With(IStorageProviderPtr sp);
    TTestEnvBuilder& With(IFileIOServicePtr fileIO);
    TTestEnvBuilder& With(NNvme::INvmeManagerPtr nvmeManager);
    TTestEnvBuilder& With(NProto::TDiskAgentConfig config);
    TTestEnvBuilder& WithAgents(TVector<NProto::TDiskAgentConfig> configs);
    TTestEnvBuilder& With(NProto::TStorageServiceConfig storageServiceConfig);
    TTestEnvBuilder& With(TDiskRegistryState::TPtr diskRegistryState);

    TTestEnv Build();
};

////////////////////////////////////////////////////////////////////////////////

NProto::TDiskAgentConfig DiskAgentConfig();

NProto::TDiskAgentConfig DiskAgentConfig(
    TVector<TString> devices,
    bool acquireRequired = true);

NProto::TFileDeviceArgs PrepareFileDevice(
    const TString& filePath,
    const TString& deviceName,
    ui32 deviceBlockSize = DefaultDeviceBlockSize,
    ui64 byteCount = DefaultBlockSize * DefaultBlocksCount);

NProto::TMemoryDeviceArgs PrepareMemoryDevice(
    const TString& deviceName,
    ui32 deviceBlockSize = DefaultDeviceBlockSize,
    ui64 byteCount = DefaultBlockSize * DefaultBlocksCount);

IStorageProviderPtr CreateTestStorageProvider(
    IFileIOServicePtr fileIO,
    NNvme::INvmeManagerPtr nvmeManager);

////////////////////////////////////////////////////////////////////////////////

inline auto WithAcquireRequired()
{
    return TPipeableProperty{[] (auto& config) {
        config.SetAcquireRequired(true);
    }};
}

inline auto WithBackend(NProto::EDiskAgentBackendType backend)
{
    return TPipeableProperty{[=] (NProto::TDiskAgentConfig& config) {
        config.SetBackend(backend);
    }};
}

inline auto WithMemoryDevices(TVector<NProto::TMemoryDeviceArgs> args)
{
    return TPipeableProperty{[=] (auto& config) mutable {
        auto& devices = *config.MutableMemoryDevices();
        devices.Assign(
            std::make_move_iterator(args.begin()),
            std::make_move_iterator(args.end())
        );
    }};
}

inline auto WithFileDevices(TVector<NProto::TFileDeviceArgs> args)
{
    return TPipeableProperty{[=] (auto& config) mutable {
        auto& devices = *config.MutableFileDevices();
        devices.Assign(
            std::make_move_iterator(args.begin()),
            std::make_move_iterator(args.end())
        );
    }};
}

inline auto WithNVMeDevices(TVector<NProto::TNVMeDeviceArgs> args)
{
    return TPipeableProperty{[=] (auto& config) mutable {
        auto& devices = *config.MutableNvmeDevices();
        devices.Assign(
            std::make_move_iterator(args.begin()),
            std::make_move_iterator(args.end())
        );
    }};
}

////////////////////////////////////////////////////////////////////////////////

NProto::TMemoryDeviceArgs MemoryDevice(TString uuid, TString name = {});

NProto::TFileDeviceArgs FileDevice(TString uuid, TString path = {});

NProto::TNVMeDeviceArgs NVMeDevice(
    TString baseName,
    TVector<TString> uuids,
    TString transportId = {});

NProto::TDiskAgentConfig CreateDefaultAgentConfig();

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgentTest
