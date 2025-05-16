#include "test_env.h"

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>
#include <cloud/blockstore/libs/nvme/nvme_stub.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/core/mind/bscontroller/bsc.h>

namespace NCloud::NBlockStore::NStorage::NDiskAgentTest {

using namespace NActors;

using namespace NKikimr;

using namespace NThreading;

using namespace NNvme;

////////////////////////////////////////////////////////////////////////////////

TSgList ConvertToSgList(const NProto::TIOVector& iov, ui32 blockSize)
{
    TSgList sglist(Reserve(iov.BuffersSize()));

    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer) {
            UNIT_ASSERT(buffer.size() == blockSize);
            sglist.emplace_back(buffer.data(), buffer.size());
        } else {
            sglist.emplace_back(TBlockDataRef::CreateZeroBlock(blockSize));
        }
    }

    return sglist;
}

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryMock::TDiskRegistryMock(TDiskRegistryState::TPtr state)
    : TActor(&TThis::StateWork)
    , State(std::move(state))
{}

STFUNC(TDiskRegistryMock::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvDiskRegistry::TEvRegisterAgentRequest, HandleRegisterAgent);
        HFunc(TEvDiskRegistry::TEvUpdateAgentStatsRequest, HandleUpdateAgentStats);
        HFunc(TEvDiskRegistryProxy::TEvSubscribeRequest, HandleSubscribe);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY,
                __PRETTY_FUNCTION__);
    }
}

void TDiskRegistryMock::HandleRegisterAgent(
    const TEvDiskRegistry::TEvRegisterAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& agentConfig = ev->Get()->Record.GetAgentConfig();

    for (const auto& device: agentConfig.GetDevices()) {
        State->Devices[device.GetDeviceName()].CopyFrom(device);
    }

    auto response =
        std::make_unique<TEvDiskRegistry::TEvRegisterAgentResponse>();

    response->Record.MutableDevicesToDisableIO()->Assign(
        State->DisabledDevices.cbegin(),
        State->DisabledDevices.cend());

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TDiskRegistryMock::HandleUpdateAgentStats(
    const TEvDiskRegistry::TEvUpdateAgentStatsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& stats = ev->Get()->Record.GetAgentStats();

    State->Stats[stats.GetNodeId()] = stats;

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvDiskRegistry::TEvUpdateAgentStatsResponse>());
}

void TDiskRegistryMock::HandleSubscribe(
    const TEvDiskRegistryProxy::TEvSubscribeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvDiskRegistryProxy::TEvSubscribeResponse>(
            true)); // connected
}

////////////////////////////////////////////////////////////////////////////////

TTestSpdkEnv::TTestSpdkEnv(const TVector<NProto::TDeviceConfig>& devices)
{
    for (const auto& device: devices) {
        Devices[device.GetDeviceName()] = device;
    }
}

TFuture<TString> TTestSpdkEnv::RegisterFileDevice(
    const TString& name,
    const TString& path,
    ui32 blockSize)
{
    Y_UNUSED(blockSize);

    if (name.Contains("broken")) {
        return MakeErrorFuture<TString>(
            std::make_exception_ptr(TServiceError(E_FAIL)));
    }

    return MakeFuture(path);
}

TFuture<TVector<TString>> TTestSpdkEnv::RegisterNVMeDevices(
    const TString& baseName,
    const TString& transportId)
{
    Y_UNUSED(transportId);

    if (baseName.Contains("broken")) {
        return MakeErrorFuture<TVector<TString>>(
            std::make_exception_ptr(TServiceError(E_FAIL)));
    }

    TVector<TString> devices;
    for (const auto& [name, _]: Devices) {
        if (name.Contains("nvme")) {
            devices.emplace_back(name);
        }
    }

    return MakeFuture(devices);
}

TFuture<TString> TTestSpdkEnv::RegisterMemoryDevice(
    const TString& name,
    ui64 blocksCount,
    ui32 blockSize)
{
    Y_UNUSED(blocksCount);
    Y_UNUSED(blockSize);

    return MakeFuture(name);
}

TFuture<void> TTestSpdkEnv::UnregisterDevice(const TString& name)
{
    Y_UNUSED(name);

    return MakeFuture();
}

TFuture<void> TTestSpdkEnv::EnableHistogram(const TString& deviceName, bool enable)
{
    Y_UNUSED(deviceName);
    Y_UNUSED(enable);

    return MakeFuture();
}

TFuture<void> TTestSpdkEnv::StartListen(const TString& transportId)
{
    Y_UNUSED(transportId);

    return MakeFuture();
}

TFuture<NSpdk::TDeviceStats> TTestSpdkEnv::QueryDeviceStats(const TString& name)
{
    auto& device = Devices.at(name);

    return MakeFuture(NSpdk::TDeviceStats {
        .DeviceUUID = {},
        .BlockSize = device.GetBlockSize(),
        .BlocksCount = device.GetBlocksCount()
    });
}

TFuture<NSpdk::ISpdkTargetPtr> TTestSpdkEnv::CreateNVMeTarget(
    const TString& nqn,
    const TVector<TString>& devices,
    const TVector<TString>& transportIds)
{
    Y_UNUSED(nqn);
    Y_UNUSED(devices);
    Y_UNUSED(transportIds);

    return MakeFuture<NSpdk::ISpdkTargetPtr>(
        std::make_shared<TTestSpdkTarget>(
            SecureEraseCount,
            SecureEraseResult));
}

////////////////////////////////////////////////////////////////////////////////

TTestSpdkTarget::TTestSpdkTarget(
        TAtomic& eraseCount,
        NThreading::TPromise<NProto::TError>& eraseResult)
    : EraseCount(eraseCount)
    , EraseResult(eraseResult)
{}

NSpdk::ISpdkDevicePtr TTestSpdkTarget::GetDevice(const TString& name)
{
    Y_UNUSED(name);

    auto device = std::make_shared<NSpdk::TTestSpdkDevice>();

    device->EraseHandler = [&] (auto method) {
        Y_UNUSED(method);

        AtomicIncrement(EraseCount);
        return EraseResult.GetFuture();
    };

    auto handler = [&] (auto... args) {
        Y_UNUSED(args...);

        return MakeFuture(NProto::TError());
    };

    device->WriteZeroesHandler = handler;
    device->ReadSgListHandler = handler;
    device->WriteSgListHandler = handler;

    return device;
}

////////////////////////////////////////////////////////////////////////////////

TTestEnv::~TTestEnv()
{
    if (FileIOService) {
        FileIOService->Stop();
    }
}

////////////////////////////////////////////////////////////////////////////////

TTestEnvBuilder::TTestEnvBuilder(TTestActorRuntime& runtime)
        : Runtime(runtime)
{}

TTestEnvBuilder& TTestEnvBuilder::With(NSpdk::ISpdkEnvPtr spdk)
{
    Spdk = spdk;
    return *this;
}

TTestEnvBuilder& TTestEnvBuilder::With(IStorageProviderPtr sp)
{
    StorageProvider = sp;
    return *this;
}

TTestEnvBuilder& TTestEnvBuilder::With(IFileIOServicePtr fileIO)
{
    FileIOService = fileIO;
    return *this;
}

TTestEnvBuilder& TTestEnvBuilder::With(NProto::TDiskAgentConfig config)
{
    AgentConfigProto = config;
    return *this;
}

TTestEnvBuilder& TTestEnvBuilder::WithAgents(TVector<NProto::TDiskAgentConfig> configs)
{
    AdditionalAgentConfigsProto = std::move(configs);
    return *this;
}

TTestEnvBuilder& TTestEnvBuilder::With(INvmeManagerPtr nvmeManager)
{
    NvmeManager = nvmeManager;
    return *this;
}

TTestEnvBuilder& TTestEnvBuilder::With(NProto::TStorageServiceConfig storageServiceConfig)
{
    StorageServiceConfig = std::move(storageServiceConfig);
    return *this;
}

TTestEnvBuilder& TTestEnvBuilder::With(TDiskRegistryState::TPtr diskRegistryState)
{
    DiskRegistryState = std::move(diskRegistryState);
    return *this;
}

TTestEnv TTestEnvBuilder::Build()
{
    Runtime.AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    // for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
    //    Runtime.SetLogPriority(i, NLog::PRI_DEBUG);
    // }
    // Runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);

    Runtime.SetLogPriority(TBlockStoreComponents::DISK_AGENT, NLog::PRI_INFO);
    Runtime.SetLogPriority(TBlockStoreComponents::DISK_AGENT_WORKER, NLog::PRI_INFO);

    Runtime.SetRegistrationObserverFunc(
        [] (auto& runtime, const auto& parentId, const auto& actorId)
    {
        Y_UNUSED(parentId);
        runtime.EnableScheduleForActor(actorId);
    });

    if (!DiskRegistryState) {
        DiskRegistryState = MakeIntrusive<TDiskRegistryState>();
    }

    Runtime.AddLocalService(
        MakeDiskRegistryProxyServiceId(),
        TActorSetupCmd(
            new TDiskRegistryMock(DiskRegistryState),
            TMailboxType::Simple,
            0));

    auto allocator = CreateCachingAllocator(
        TDefaultAllocator::Instance(), 0, 0, 0);
    auto config = std::make_shared<TStorageConfig>(
        std::move(StorageServiceConfig),
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig())
    );
    auto agentConfig = std::make_shared<TDiskAgentConfig>(
        std::move(AgentConfigProto),
        "the-rack",
        0);

    if (!Spdk && agentConfig->GetBackend() == NProto::DISK_AGENT_BACKEND_SPDK) {
        Spdk = NSpdk::CreateEnvStub();
    }

    if (!Spdk) {
        if (!FileIOService) {
            FileIOService = CreateAIOService();
            FileIOService->Start();
        }

        if (!NvmeManager) {
            NvmeManager = CreateNvmeManagerStub();
        }

        if (!StorageProvider) {
            StorageProvider = CreateTestStorageProvider(FileIOService, NvmeManager);
        }
    } else {
        Spdk->Start();
    }

    auto diskAgent = CreateDiskAgent(
        config,
        agentConfig,
        nullptr,    // rdmaConfig
        Spdk,
        allocator,
        StorageProvider,
        CreateProfileLogStub(),
        CreateBlockDigestGeneratorStub(),
        CreateLoggingService("console"),
        nullptr,    // rdmaServer
        NvmeManager);

    const ui32 firstDiskAgentNodeIndex = 0;

    Runtime.AddLocalService(
        MakeDiskAgentServiceId(Runtime.GetNodeId(firstDiskAgentNodeIndex)),
        TActorSetupCmd(diskAgent.release(), TMailboxType::Simple, 0),
        firstDiskAgentNodeIndex);

    ui32 additionalDiskAgentNodeIndex = 1;
    for (auto& additionalAgent : AdditionalAgentConfigsProto) {
        auto diskAgent = CreateDiskAgent(
            config,
            std::make_shared<TDiskAgentConfig>(
                std::move(additionalAgent),
                "the-rack",
                0),
            nullptr,   // rdmaConfig
            Spdk,
            allocator,
            StorageProvider,
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            CreateLoggingService("console"),
            nullptr,   // rdmaServer
            NvmeManager);

        Runtime.AddLocalService(
            MakeDiskAgentServiceId(Runtime.GetNodeId(additionalDiskAgentNodeIndex)),
            TActorSetupCmd(diskAgent.release(), TMailboxType::Simple, 0),
            additionalDiskAgentNodeIndex);
        ++additionalDiskAgentNodeIndex;
    }

    SetupTabletServices(Runtime);

    Runtime.EnableScheduleForActor(NKikimr::CreateTestBootstrapper(
        Runtime,
        NKikimr::CreateTestTabletInfo(
            NKikimr::MakeBSControllerID(0),
            NKikimr::TTabletTypes::BSController
        ),
        &NKikimr::CreateFlatBsController,
        0));

    return TTestEnv{
        .Runtime = Runtime,
        .DiskRegistryState = std::move(DiskRegistryState),
        .FileIOService = FileIOService,
        .NvmeManager = NvmeManager,
        .DiskAgentActorId =
            MakeDiskAgentServiceId(Runtime.GetNodeId(firstDiskAgentNodeIndex))
    };
}

////////////////////////////////////////////////////////////////////////////////

NProto::TDiskAgentConfig DiskAgentConfig(
    TVector<TString> devices,
    bool acquireRequired)
{
    auto agentConfig = CreateDefaultAgentConfig();
    agentConfig.SetEnabled(true);
    agentConfig.SetAcquireRequired(acquireRequired);

    for (const auto& name: devices) {
        auto* device = agentConfig.MutableMemoryDevices()->Add();
        device->SetName(name);
        device->SetBlocksCount(DefaultBlocksCount);
        device->SetBlockSize(DefaultBlockSize);
        device->SetDeviceId(name);
    }

    return agentConfig;
}

NProto::TDiskAgentConfig DiskAgentConfig()
{
    auto config = CreateDefaultAgentConfig();

    config.SetEnabled(true);
    config.SetAgentId("agent");

    return config;
}

NProto::TMemoryDeviceArgs MemoryDevice(TString uuid, TString name)
{
    NProto::TMemoryDeviceArgs args;
    args.SetName(std::move(name));
    args.SetDeviceId(std::move(uuid));
    args.SetBlockSize(DefaultDeviceBlockSize);
    args.SetBlocksCount(DefaultBlocksCount);

    return args;
}

NProto::TFileDeviceArgs FileDevice(TString uuid, TString path)
{
    NProto::TFileDeviceArgs args;
    args.SetPath(std::move(path));
    args.SetDeviceId(std::move(uuid));
    args.SetBlockSize(DefaultDeviceBlockSize);

    return args;
}

NProto::TNVMeDeviceArgs NVMeDevice(
    TString baseName,
    TVector<TString> uuids,
    TString transportId)
{
    NProto::TNVMeDeviceArgs args;

    for (auto&& uuid: uuids) {
        *args.MutableDeviceIds()->Add() = std::move(uuid);
    }

    args.SetBaseName(std::move(baseName));
    args.SetTransportId(std::move(transportId));

    return args;
}

NProto::TFileDeviceArgs PrepareFileDevice(
    const TString& filePath,
    const TString& deviceName,
    ui32 deviceBlockSize,
    ui64 byteCount)
{
    UNIT_ASSERT(byteCount % deviceBlockSize == 0);

    TFile fileData(filePath, EOpenModeFlag::CreateAlways);
    fileData.Resize(byteCount);

    NProto::TFileDeviceArgs device;
    device.SetPath(filePath);
    device.SetBlockSize(deviceBlockSize);
    device.SetDeviceId(deviceName);
    return device;
}

NProto::TMemoryDeviceArgs PrepareMemoryDevice(
    const TString& deviceName,
    ui32 deviceBlockSize,
    ui64 byteCount)
{
    UNIT_ASSERT(byteCount % deviceBlockSize == 0);

    NProto::TMemoryDeviceArgs device;
    device.SetName(deviceName);
    device.SetBlocksCount(byteCount / deviceBlockSize);
    device.SetBlockSize(deviceBlockSize);
    device.SetDeviceId(deviceName);
    return device;
}

////////////////////////////////////////////////////////////////////////////////

struct TErrorStorage final
    : public IStorage
{
    template <typename T>
    TFuture<T> MakeResponse()
    {
        T response;
        auto& error = *response.MutableError();
        error.SetCode(E_IO);

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeResponse<NProto::TZeroBlocksResponse>();
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeResponse<NProto::TReadBlocksLocalResponse>();
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeResponse<NProto::TWriteBlocksLocalResponse>();
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return MakeFuture(NProto::TError());
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TTestStorage final
    : public IStorage
{
    IStoragePtr Impl;

    explicit TTestStorage(IStoragePtr impl)
        : Impl(impl)
    {
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Impl->ZeroBlocks(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Impl->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return Impl->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        switch (method) {
        case NProto::DEVICE_ERASE_METHOD_ZERO_FILL:
            return Impl->EraseDevice(method);

        default:
            return MakeFuture(NProto::TError());
        }
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Impl->AllocateBuffer(bytesCount);
    }

    void ReportIOError() override
    {
        Impl->ReportIOError();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestStorageProvider final
    : public IStorageProvider
{
    IStorageProviderPtr Impl;

    explicit TTestStorageProvider(IStorageProviderPtr impl)
        : Impl(std::move(impl))
    {}

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        if (volume.GetDiskId().Contains("broken")) {
            return MakeErrorFuture<IStoragePtr>(
                std::make_exception_ptr(TServiceError(E_FAIL)));
        }

        if (volume.GetDiskId().Contains("error")) {
            return MakeFuture<IStoragePtr>(std::make_shared<TErrorStorage>());
        }

        auto result = NewPromise<IStoragePtr>();
        Impl->CreateStorage(volume, clientId, accessMode).Apply(
            [&] (auto& future)
            {
                result.SetValue(std::make_shared<TTestStorage>(future.GetValue()));
            });
        return result.GetFuture();
    }
};

IStorageProviderPtr CreateTestStorageProvider(
    IFileIOServicePtr fileIO,
    INvmeManagerPtr nvmeManager)
{
    return std::make_shared<TTestStorageProvider>(
        NServer::CreateAioStorageProvider(
            NServer::CreateSingleFileIOServiceProvider(std::move(fileIO)),
            std::move(nvmeManager),
            false,  // directIO
            NServer::EAioSubmitQueueOpt::DontUse
        ));
}

NProto::TDiskAgentConfig CreateDefaultAgentConfig()
{
    NProto::TDiskAgentConfig config;

    config.SetIOParserActorCount(4);
    config.SetOffloadAllIORequestsParsingEnabled(true);
    config.SetIOParserActorAllocateStorageEnabled(true);

    return config;
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgentTest
