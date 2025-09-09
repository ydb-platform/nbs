#include "disk_agent_state.h"

#include "rdma_target.h"
#include "spdk_initializer.h"
#include "storage_initializer.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/rdma/iface/config.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/spdk/iface/target.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_common/monitoring_utils.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/string/join.h>
#include <util/system/fs.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

using TInitializeResult = TDiskAgentState::TInitializeResult;

namespace {

////////////////////////////////////////////////////////////////////////////////

void ToDeviceStats(
    const TString& uuid,
    const TString& name,
    const TStorageIoStats& ioStats,
    NProto::TDeviceStats& stats)
{
    stats.SetDeviceUUID(uuid);
    stats.SetDeviceName(name);
    stats.SetBytesRead(AtomicGet(ioStats.BytesRead));
    stats.SetNumReadOps(AtomicGet(ioStats.NumReadOps));
    stats.SetBytesWritten(AtomicGet(ioStats.BytesWritten));
    stats.SetNumWriteOps(AtomicGet(ioStats.NumWriteOps));
    stats.SetBytesZeroed(AtomicGet(ioStats.BytesZeroed));
    stats.SetNumZeroOps(AtomicGet(ioStats.NumZeroOps));
    stats.SetNumEraseOps(AtomicGet(ioStats.NumEraseOps));

    stats.SetErrors(AtomicGet(ioStats.Errors));

    auto& buckets = *stats.MutableHistogramBuckets();
    buckets.Reserve(TStorageIoStats::BUCKETS_COUNT);
    for (int i = 0; i != TStorageIoStats::BUCKETS_COUNT; ++i) {
        if (auto value = AtomicGet(ioStats.Buckets[i])) {
            auto& bucket = *buckets.Add();
            bucket.SetValue(TStorageIoStats::Limits[i].MicroSeconds());
            bucket.SetCount(value);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TCollectStatsContext
{
    TPromise<NProto::TAgentStats> Promise = NewPromise<NProto::TAgentStats>();

    TVector<TString> Ids;
    TVector<NSpdk::TDeviceIoStats> Stats;
    TVector<TVector<TBucketInfo>> Buckets;

    TAtomic RemainingRequests;

    const ui32 InitErrorsCount;

    TCollectStatsContext(
            size_t deviceCount,
            ui32 initErrorsCount)
        : Ids(deviceCount)
        , Stats(deviceCount)
        , Buckets(deviceCount)
        , RemainingRequests(deviceCount * 2) // stats & buckets per device
        , InitErrorsCount(initErrorsCount)
    {}

    void OnBuckets(size_t index, TVector<TBucketInfo> buckets)
    {
        Buckets[index] = std::move(buckets);

        if (AtomicDecrement(RemainingRequests) == 0) {
            OnFinish();
        }
    }

    void OnStats(size_t index, NSpdk::TDeviceIoStats stats)
    {
        Stats[index] = stats;

        if (AtomicDecrement(RemainingRequests) == 0) {
            OnFinish();
        }
    }

    void OnError(size_t index)
    {
        Ids[index].clear();

        if (AtomicDecrement(RemainingRequests) == 0) {
            OnFinish();
        }
    }

    void OnFinish()
    {
        NProto::TAgentStats agentStats;
        agentStats.SetInitErrorsCount(InitErrorsCount);

        auto& deviceStats = *agentStats.MutableDeviceStats();
        deviceStats.Reserve(Ids.size());

        for (size_t i = 0; i != Ids.size(); ++i) {
            if (Ids[i].empty()) {
                continue;
            }

            auto& stats = *deviceStats.Add();

            stats.SetDeviceUUID(Ids[i]);

            stats.SetBytesRead(Stats[i].BytesRead);
            stats.SetNumReadOps(Stats[i].NumReadOps);
            stats.SetBytesWritten(Stats[i].BytesWritten);
            stats.SetNumWriteOps(Stats[i].NumWriteOps);

            auto& buckets = *stats.MutableHistogramBuckets();
            buckets.Reserve(Buckets[i].size());
            for (auto [value, count]: Buckets[i]) {
                auto& bucket = *buckets.Add();
                bucket.SetValue(value);
                bucket.SetCount(count);
            }
        }

        Promise.SetValue(std::move(agentStats));
    }
};

////////////////////////////////////////////////////////////////////////////////

TVector<IProfileLog::TBlockInfo> ComputeDigest(
    const IBlockDigestGenerator& generator,
    const NProto::TWriteBlocksRequest& req,
    ui32 blockSize,
    TStringBuf buffer)
{
    const ui64 bytesCount = CalculateBytesCount(req, blockSize);
    if (bytesCount % blockSize != 0) {
        Y_DEBUG_ABORT_UNLESS(false);
        return {};
    }

    const bool shouldCalcDigests = generator.ShouldProcess(
        req.GetStartIndex(),
        bytesCount / blockSize,
        blockSize);

    if (!shouldCalcDigests) {
        return {};
    }

    TSgList sglist;
    if (buffer) {
        sglist = {{buffer.data(), buffer.size()}};
    } else {
        sglist = GetSgList(req);
    }

    auto sglistOrError = SgListNormalize(std::move(sglist), blockSize);
    if (HasError(sglistOrError)) {
        Y_DEBUG_ABORT_UNLESS(false);
        return {};
    }

    sglist = sglistOrError.ExtractResult();

    TVector<IProfileLog::TBlockInfo> blockInfos;
    blockInfos.reserve(sglist.size());

    ui64 blockIndex = req.GetStartIndex();
    for (const auto& ref: sglist) {
        const auto digest = generator.ComputeDigest(blockIndex, ref);

        if (digest.Defined()) {
            blockInfos.push_back({blockIndex, *digest});
        }

        ++blockIndex;
    }

    return blockInfos;
}

TVector<IProfileLog::TBlockInfo> ComputeDigest(
    const IBlockDigestGenerator& generator,
    const NProto::TZeroBlocksRequest& req,
    ui32 blockSize)
{
    const bool shouldCalcDigests = generator.ShouldProcess(
        req.GetStartIndex(),
        req.GetBlocksCount(),
        blockSize);

    if (!shouldCalcDigests) {
        return {};
    }

    TVector<IProfileLog::TBlockInfo> blockInfos;
    for (ui32 i = 0; i < req.GetBlocksCount(); ++i) {
        const auto blockIndex = req.GetStartIndex() + i;
        const auto digest = generator.ComputeDigest(
            blockIndex,
            TBlockDataRef::CreateZeroBlock(blockSize)
        );

        if (digest.Defined()) {
            blockInfos.push_back({blockIndex, *digest});
        }
    }

    return blockInfos;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDiskAgentState::TDiskAgentState(
        TStorageConfigPtr storageConfig,
        TDiskAgentConfigPtr agentConfig,
        NSpdk::ISpdkEnvPtr spdk,
        ICachingAllocatorPtr allocator,
        IStorageProviderPtr storageProvider,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ILoggingServicePtr logging,
        NRdma::IServerPtr rdmaServer,
        NNvme::INvmeManagerPtr nvmeManager,
        TRdmaTargetConfigPtr rdmaTargetConfig,
        TOldRequestCounters oldRequestCounters,
        IMultiAgentWriteHandlerPtr multiAgentWriteHandler)
    : StorageConfig(std::move(storageConfig))
    , AgentConfig(std::move(agentConfig))
    , Spdk(std::move(spdk))
    , Allocator(std::move(allocator))
    , StorageProvider(std::move(storageProvider))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , MultiAgentWriteHandler(std::move(multiAgentWriteHandler))
    , Logging(std::move(logging))
    , Log(Logging->CreateLog("BLOCKSTORE_DISK_AGENT"))
    , RdmaServer(std::move(rdmaServer))
    , NvmeManager(std::move(nvmeManager))
    , RdmaTargetConfig(std::move(rdmaTargetConfig))
    , OldRequestCounters(std::move(oldRequestCounters))
{
}

const TDiskAgentState::TDeviceState& TDiskAgentState::GetDeviceState(
    const TString& uuid,
    const TString& clientId,
    const NProto::EVolumeAccessMode accessMode) const
{
    return AgentConfig->GetAcquireRequired()
        ? GetDeviceStateImpl(uuid, clientId, accessMode)
        : GetDeviceStateImpl(uuid);
}

const TDiskAgentState::TDeviceState& TDiskAgentState::GetDeviceStateImpl(
    const TString& uuid,
    const TString& clientId,
    const NProto::EVolumeAccessMode accessMode) const
{
    auto error = DeviceClient->AccessDevice(uuid, clientId, accessMode);

    if (HasError(error)) {
        ythrow TServiceError(error.GetCode()) << error.GetMessage();
    }

    auto d = Devices.FindPtr(uuid);
    STORAGE_VERIFY(d, TWellKnownEntityTypes::DEVICE, uuid);

    return *d;
}

const TDiskAgentState::TDeviceState& TDiskAgentState::GetDeviceStateImpl(
    const TString& uuid) const
{
    auto it = Devices.find(uuid);
    if (it == Devices.cend()) {
        ythrow TServiceError(E_NOT_FOUND)
            << "Device " << uuid.Quote() << " not found";
    }

    return it->second;
}

const TString& TDiskAgentState::GetDeviceName(const TString& uuid) const
{
    auto it = Devices.find(uuid);
    if (it == Devices.cend()) {
        ythrow TServiceError(E_NOT_FOUND)
            << "Device " << uuid.Quote() << " not found";
    }

    return it->second.Config.GetDeviceName();
}

const NProto::TDeviceConfig* TDiskAgentState::FindDeviceConfig(
    const TString& uuid) const
{
    const auto* device = Devices.FindPtr(uuid);
    if (!device) {
        return nullptr;
    }

    return &device->Config;
}

TVector<NProto::TDeviceConfig> TDiskAgentState::GetDevices() const
{
    TVector<NProto::TDeviceConfig> devices;
    devices.reserve(Devices.size());

    for (const auto& kv: Devices) {
        devices.push_back(kv.second.Config);
    }

    return devices;
}

TVector<NProto::TDeviceConfig> TDiskAgentState::GetEnabledDevices() const
{
    TVector<NProto::TDeviceConfig> devices;
    devices.reserve(Devices.size());

    for (const auto& [uuid, state]: Devices) {
        if (DeviceClient->IsDeviceEnabled(uuid)) {
            devices.push_back(state.Config);
        }
    }

    return devices;
}

TVector<TString> TDiskAgentState::GetDeviceIds() const
{
    TVector<TString> uuids;
    uuids.reserve(Devices.size());

    for (const auto& [_, d]: Devices) {
        uuids.push_back(d.Config.GetDeviceUUID());
    }

    return uuids;
}

ui32 TDiskAgentState::GetDevicesCount() const
{
    return Devices.size();
}

TFuture<TInitializeResult> TDiskAgentState::InitSpdkStorage()
{
    return InitializeSpdk(AgentConfig, Spdk, Allocator)
        .Apply([this] (auto future) {
            TInitializeSpdkResult r = future.ExtractValue();

            InitErrorsCount = r.Errors.size();

            SpdkTarget = std::move(r.SpdkTarget);

            TDuration ioTimeout;
            if (!AgentConfig->GetDeviceIOTimeoutsDisabled()) {
                ioTimeout = AgentConfig->GetDeviceIOTimeout();
            }

            for (size_t i = 0; i != r.Configs.size(); ++i) {
                const auto& config = r.Configs[i];

                TDeviceState device {
                    .Config = config,
                    .StorageAdapter = std::make_shared<TStorageAdapter>(
                        std::move(r.Devices[i]),
                        config.GetBlockSize(),
                        false,  // normalize
                        ioTimeout,
                        AgentConfig->GetShutdownTimeout())
                };

                Devices.emplace(
                    config.GetDeviceUUID(),
                    std::move(device));
            }

            return TInitializeResult {
                .Configs = std::move(r.Configs),
                .Errors = std::move(r.Errors)
            };
        });
}

TFuture<TInitializeResult> TDiskAgentState::InitAioStorage()
{
    return InitializeStorage(
            Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
            StorageConfig,
            AgentConfig,
            StorageProvider,
            NvmeManager)
        .Apply([=, this] (auto future) {
            TInitializeStorageResult r = future.ExtractValue();

            InitErrorsCount = r.Errors.size();

            Y_ABORT_UNLESS(r.Configs.size() == r.Devices.size()
                  && r.Configs.size() == r.Stats.size());

            TDuration ioTimeout;
            if (!AgentConfig->GetDeviceIOTimeoutsDisabled()) {
                ioTimeout = AgentConfig->GetDeviceIOTimeout();
            }

            for (size_t i = 0; i != r.Configs.size(); ++i) {
                const auto& config = r.Configs[i];

                TDeviceState device {
                    .Config = config,
                    .StorageAdapter = std::make_shared<TStorageAdapter>(
                        std::move(r.Devices[i]),
                        config.GetBlockSize(),
                        false,  // normalize
                        ioTimeout,
                        AgentConfig->GetShutdownTimeout()),
                    .Stats = std::move(r.Stats[i])
                };

                Devices.emplace(config.GetDeviceUUID(), std::move(device));
            }

            return TInitializeResult{
                .Configs = std::move(r.Configs),
                .Errors = std::move(r.Errors),
                .ConfigMismatchErrors = std::move(r.ConfigMismatchErrors),
                .DevicesWithSuspendedIO = std::move(r.DevicesWithSuspendedIO),
                .LostDevicesIds = std::move(r.LostDevicesIds),
                .Guard = std::move(r.Guard)};
        });
}

void TDiskAgentState::InitRdmaTarget()
{
    if (RdmaServer && RdmaTargetConfig) {
        THashMap<TString, TStorageAdapterPtr> devices;

        for (auto& [uuid, state]: Devices) {
            auto* endpoint = state.Config.MutableRdmaEndpoint();
            endpoint->SetHost(RdmaTargetConfig->Host);
            endpoint->SetPort(RdmaTargetConfig->Port);
            devices.emplace(uuid, state.StorageAdapter);
        }

        RdmaTarget = CreateRdmaTarget(
            RdmaTargetConfig,
            OldRequestCounters,
            Logging,
            RdmaServer,
            DeviceClient,
            MultiAgentWriteHandler,
            std::move(devices));

        RdmaTarget->Start();
    }
}

TFuture<TInitializeResult> TDiskAgentState::Initialize()
{
    auto future = Spdk ? InitSpdkStorage() : InitAioStorage();

    return future.Apply(
        [this](TFuture<TInitializeResult> future) mutable
        {
            TInitializeResult r = future.ExtractValue();

            TVector<TString> uuids(Reserve(Devices.size()));
            for (const auto& x: Devices) {
                uuids.push_back(x.first);
            }

            DeviceClient = std::make_shared<TDeviceClient>(
                AgentConfig->GetReleaseInactiveSessionsTimeout(),
                std::move(uuids),
                Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
                AgentConfig->GetKickOutOldClientsEnabled());

            InitRdmaTarget();

            if (AgentConfig->GetDisableBrokenDevices()) {
                for (const auto& uuid: r.DevicesWithSuspendedIO) {
                    SuspendDevice(uuid);
                }
            }

            RestoreSessions(
                *DeviceClient,
                THashSet<TString>{
                    r.LostDevicesIds.begin(),
                    r.LostDevicesIds.end()});

            return r;
        });
}

TFuture<NProto::TAgentStats> TDiskAgentState::CollectStats()
{
    if (!Spdk) {
        NProto::TAgentStats stats;
        stats.SetInitErrorsCount(InitErrorsCount);

        auto& ds = *stats.MutableDeviceStats();
        ds.Reserve(Devices.size());

        for (const auto& [id, device]: Devices) {
            ToDeviceStats(
                id,
                device.Config.GetDeviceName(),
                *device.Stats,
                *ds.Add());
        }

        return MakeFuture(std::move(stats));
    }

    auto context = std::make_shared<TCollectStatsContext>(
        Devices.size(),
        InitErrorsCount);

    size_t i = 0;
    for (const auto& [id, device]: Devices) {
        const size_t index = i++;

        context->Ids[index] = device.Config.GetDeviceUUID();

        const auto& deviceName = device.Config.GetDeviceName();

        Spdk->GetDeviceIoStats(deviceName).Subscribe([=] (const auto& future) {
            try {
                context->OnStats(index, future.GetValue());
            } catch (...) {
                context->OnError(index);
            }
        });

        Spdk->GetHistogramBuckets(deviceName).Subscribe([=] (auto future) {
            try {
                context->OnBuckets(index, future.ExtractValue());
            } catch (...) {
                context->OnError(index);
            }
        });
    }

    return context->Promise;
}

void TDiskAgentState::CheckIfDeviceIsDisabled(
    const TString& uuid,
    const TString& clientId)
{
    auto ec = DeviceClient->GetDeviceIOErrorCode(uuid);
    if (!ec) {
        return;
    }

    if (GetErrorKind(MakeError(*ec)) != EErrorKind::ErrorRetriable) {
        STORAGE_ERROR(
            "[" << uuid << "/" << clientId
                << "] Device disabled. Drop request.");

        ReportDisabledDeviceError(uuid);
    } else {
        STORAGE_TRACE(
            "[" << uuid << "/" << clientId
                << "] Device suspended. Reject request.");
    }

    ythrow TServiceError(*ec) << "Device disabled";
}

TFuture<NProto::TReadDeviceBlocksResponse> TDiskAgentState::Read(
    TInstant now,
    NProto::TReadDeviceBlocksRequest request)
{
    CheckIfDeviceIsDisabled(
        request.GetDeviceUUID(),
        request.GetHeaders().GetClientId());

    const auto& device = GetDeviceState(
        request.GetDeviceUUID(),
        request.GetHeaders().GetClientId(),
        NProto::VOLUME_ACCESS_READ_ONLY);

    auto readRequest = std::make_shared<NProto::TReadBlocksRequest>();
    readRequest->MutableHeaders()->CopyFrom(request.GetHeaders());
    readRequest->SetStartIndex(request.GetStartIndex());
    readRequest->SetBlocksCount(request.GetBlocksCount());

    auto result = device.StorageAdapter->ReadBlocks(
        now,
        MakeIntrusive<TCallContext>(),
        std::move(readRequest),
        request.GetBlockSize(),
        {} // no data buffer
    );

    return result.Apply(
        [] (auto future) {
            auto data = future.ExtractValue();
            NProto::TReadDeviceBlocksResponse response;

            *response.MutableError() = data.GetError();
            response.MutableBlocks()->Swap(data.MutableBlocks());

            return response;
        });
}

void TDiskAgentState::WriteProfileLog(
    TInstant now,
    const TString& uuid,
    const NProto::TWriteBlocksRequest& req,
    ui32 blockSize,
    TStringBuf buffer)
{
    auto blockInfos =
        ComputeDigest(*BlockDigestGenerator, req, blockSize, buffer);
    if (!blockInfos) {
        return;
    }

    ProfileLog->Write({
        .DiskId = uuid,
        .Ts = now,
        .Request = IProfileLog::TSysReadWriteRequestBlockInfos{
            .RequestType = ESysRequestType::WriteDeviceBlocks,
            .BlockInfos = std::move(blockInfos),
            .CommitId = 0,
        },
    });
}

void TDiskAgentState::WriteProfileLog(
    TInstant now,
    const TString& uuid,
    const NProto::TZeroBlocksRequest& req,
    ui32 blockSize)
{
    auto blockInfos = ComputeDigest(*BlockDigestGenerator, req, blockSize);
    if (!blockInfos) {
        return;
    }

    ProfileLog->Write({
        .DiskId = uuid,
        .Ts = now,
        .Request = IProfileLog::TSysReadWriteRequestBlockInfos{
            .RequestType = ESysRequestType::ZeroDeviceBlocks,
            .BlockInfos = std::move(blockInfos),
            .CommitId = 0,
        },
    });
}

TFuture<NProto::TWriteDeviceBlocksResponse> TDiskAgentState::Write(
    TInstant now,
    NProto::TWriteDeviceBlocksRequest request)
{
    auto writeRequest = std::make_shared<NProto::TWriteBlocksRequest>();
    writeRequest->MutableHeaders()->CopyFrom(request.GetHeaders());
    writeRequest->SetStartIndex(request.GetStartIndex());
    writeRequest->MutableBlocks()->Swap(request.MutableBlocks());

    return WriteBlocks(
        now,
        request.GetDeviceUUID(),
        std::move(writeRequest),
        request.GetBlockSize(),
        {}  // buffer
    );
}

TFuture<NProto::TWriteDeviceBlocksResponse> TDiskAgentState::WriteBlocks(
    TInstant now,
    const TString& deviceUUID,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    ui32 blockSize,
    TStringBuf buffer)
{
    CheckIfDeviceIsDisabled(
        deviceUUID,
        request->GetHeaders().GetClientId());

    const auto& device = GetDeviceState(
        deviceUUID,
        request->GetHeaders().GetClientId(),
        NProto::VOLUME_ACCESS_READ_WRITE);

    WriteProfileLog(
        now,
        deviceUUID,
        *request,
        blockSize,
        buffer);

    auto result = device.StorageAdapter->WriteBlocks(
        now,
        MakeIntrusive<TCallContext>(),
        std::move(request),
        blockSize,
        buffer);

    return result.Apply(
        [] (const auto& future) {
            NProto::TWriteDeviceBlocksResponse response;

            *response.MutableError() = future.GetValue().GetError();

            return response;
        });
}

TFuture<NProto::TZeroDeviceBlocksResponse> TDiskAgentState::WriteZeroes(
    TInstant now,
    NProto::TZeroDeviceBlocksRequest request)
{
    CheckIfDeviceIsDisabled(
        request.GetDeviceUUID(),
        request.GetHeaders().GetClientId());

    const auto& device = GetDeviceState(
        request.GetDeviceUUID(),
        request.GetHeaders().GetClientId(),
        NProto::VOLUME_ACCESS_READ_WRITE);

    auto zeroRequest = std::make_shared<NProto::TZeroBlocksRequest>();
    zeroRequest->SetStartIndex(request.GetStartIndex());
    zeroRequest->SetBlocksCount(request.GetBlocksCount());

    WriteProfileLog(
        now,
        request.GetDeviceUUID(),
        *zeroRequest,
        request.GetBlockSize());

    auto result = device.StorageAdapter->ZeroBlocks(
        now,
        MakeIntrusive<TCallContext>(),
        std::move(zeroRequest),
        request.GetBlockSize());

    return result.Apply(
        [] (const auto& future) {
            NProto::TZeroDeviceBlocksResponse response;

            *response.MutableError() = future.GetValue().GetError();

            return response;
        });
}

TFuture<NProto::TError> TDiskAgentState::SecureErase(
    const TString& uuid,
    TInstant now)
{
    const auto& device = GetDeviceStateImpl(uuid);
    const auto& sessionInfo = DeviceClient->GetWriterSession(uuid);
    if (sessionInfo.Id
            && sessionInfo.LastActivityTs
                + AgentConfig->GetReleaseInactiveSessionsTimeout() > now)
    {
        ReportAcquiredDiskEraseAttempt(
            {{"device", uuid}, {"client", sessionInfo.Id}});

        ythrow TServiceError(E_INVALID_STATE)
            << "Device " << uuid.Quote()
            << " already acquired by client " << sessionInfo.Id;
    }

    if (AgentConfig->GetDeviceEraseMethod() == NProto::DEVICE_ERASE_METHOD_NONE) {
        return MakeFuture(NProto::TError());
    }

    auto onDeviceSecureEraseFinish =
        [weakRdmaTarget = std::weak_ptr<IRdmaTarget>(RdmaTarget),
         uuid](const TFuture<NProto::TError>& future)
    {
        // The device has been secure erased and now a new client can use it.
        if (auto rdmaTarget = weakRdmaTarget.lock()) {
            rdmaTarget->DeviceSecureEraseFinish(uuid, future.GetValue());
        }
    };

    if (RdmaTarget) {
        auto error = RdmaTarget->DeviceSecureEraseStart(uuid);
        if (HasError(error)) {
            return MakeFuture(std::move(error));
        }
    }

    return device.StorageAdapter
        ->EraseDevice(AgentConfig->GetDeviceEraseMethod())
        .Subscribe(std::move(onDeviceSecureEraseFinish));
}

TFuture<NProto::TChecksumDeviceBlocksResponse> TDiskAgentState::Checksum(
    TInstant now,
    NProto::TChecksumDeviceBlocksRequest request)
{
    Y_UNUSED(now);

    const auto& device = GetDeviceState(
        request.GetDeviceUUID(),
        request.GetHeaders().GetClientId(),
        NProto::VOLUME_ACCESS_READ_ONLY);

    // TODO: Pass checksum request down to the device to avoid data copying
    auto readRequest = std::make_shared<NProto::TReadBlocksRequest>();
    readRequest->MutableHeaders()->CopyFrom(request.GetHeaders());
    // Reset the optimization flag just in case.
    readRequest->MutableHeaders()->SetOptimizeNetworkTransfer(
        NProto::EOptimizeNetworkTransfer::NO_OPTIMIZATION);

    readRequest->SetStartIndex(request.GetStartIndex());
    readRequest->SetBlocksCount(request.GetBlocksCount());

    auto result = device.StorageAdapter->ReadBlocks(
        now,
        MakeIntrusive<TCallContext>(),
        std::move(readRequest),
        request.GetBlockSize(),
        {} // no data buffer
    );

    return result.Apply(
        [] (auto future) {
            auto data = future.ExtractValue();
            NProto::TChecksumDeviceBlocksResponse response;

            if (HasError(data.GetError())) {
                *response.MutableError() = data.GetError();
            } else {
                TBlockChecksum checksum;
                for (const auto& buffer : data.GetBlocks().GetBuffers()) {
                    checksum.Extend(buffer.data(), buffer.size());
                }
                response.SetChecksum(checksum.GetValue());
            }

            return response;
        });
}

void TDiskAgentState::CheckIOTimeouts(TInstant now)
{
    for (auto& x: Devices) {
        x.second.StorageAdapter->CheckIOTimeouts(now);
    }
}

TDeviceClient::TSessionInfo TDiskAgentState::GetWriterSession(
    const TString& uuid) const
{
    return DeviceClient->GetWriterSession(uuid);
}

TVector<TDeviceClient::TSessionInfo> TDiskAgentState::GetReaderSessions(
    const TString& uuid) const
{
    return DeviceClient->GetReaderSessions(uuid);
}

bool TDiskAgentState::AcquireDevices(
    const TVector<TString>& uuids,
    const TString& clientId,
    TInstant now,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    const TString& diskId,
    ui32 volumeGeneration)
{
    if (PartiallySuspended) {
        EnsureAccessToDevices(uuids, clientId, accessMode);
    }

    auto [updated, error] = DeviceClient->AcquireDevices(
        uuids,
        clientId,
        now,
        accessMode,
        mountSeqNumber,
        diskId,
        volumeGeneration);

    Y_DEBUG_ABORT_UNLESS(
        !PartiallySuspended || !updated,
        "We shouldn't update the sessions config if disk agent is in partially "
        "suspended state.");

    CheckError(error);

    return updated;
}

void TDiskAgentState::ReleaseDevices(
    const TVector<TString>& uuids,
    const TString& clientId,
    const TString& diskId,
    ui32 volumeGeneration)
{
    if (PartiallySuspended) {
        ythrow TServiceError(E_REJECTED)
            << "Disk agent is partially suspended. Can't "
               "release any sessions at this state.";
    }

    CheckError(DeviceClient->ReleaseDevices(
        uuids,
        clientId,
        diskId,
        volumeGeneration));
}

void TDiskAgentState::DisableDevice(const TString& uuid)
{
    DeviceClient->DisableDevice(uuid);
}

void TDiskAgentState::SuspendDevice(const TString& uuid)
{
    STORAGE_INFO("Suspend device " << uuid);
    DeviceClient->SuspendDevice(uuid);
}

void TDiskAgentState::EnableDevice(const TString& uuid)
{
    DeviceClient->EnableDevice(uuid);
}

bool TDiskAgentState::IsDeviceDisabled(const TString& uuid) const
{
    return DeviceClient->IsDeviceDisabled(uuid);
}

bool TDiskAgentState::IsDeviceSuspended(const TString& uuid) const
{
    return DeviceClient->IsDeviceSuspended(uuid);
}

void TDiskAgentState::ReportDisabledDeviceError(const TString& uuid)
{
    if (auto* d = Devices.FindPtr(uuid)) {
        d->Stats->OnError();
    }
}

void TDiskAgentState::StopTarget()
{
    if (SpdkTarget) {
        SpdkTarget->Stop();
    }

    if (RdmaTarget) {
        RdmaTarget->Stop();
    }
}

void TDiskAgentState::SetPartiallySuspended(bool partiallySuspended)
{
    PartiallySuspended = partiallySuspended;
}

bool TDiskAgentState::GetPartiallySuspended() const
{
    return PartiallySuspended;
}

void TDiskAgentState::RestoreSessions(
    TDeviceClient& client,
    const THashSet<TString>& lostDevicesIds) const
{
    const TString storagePath = StorageConfig->GetCachedDiskAgentSessionsPath();
    const TString agentPath = AgentConfig->GetCachedSessionsPath();
    const TString& path = agentPath.empty() ? storagePath : agentPath;

    if (path.empty()) {
        STORAGE_INFO("Session cache is not configured.");
        return;
    }

    if (!NFs::Exists(path)) {
        STORAGE_INFO("Session cache is empty.");
        return;
    }

    try {
        NProto::TDiskAgentDeviceSessionCache proto;

        ParseProtoTextFromFileRobust(path, proto);

        auto& sessions = *proto.MutableSessions();

        STORAGE_INFO("Found " << sessions.size()
            << " sessions in the session cache: " << JoinSeq(" ", sessions));

        int errors = 0;

        for (auto& session: sessions) {
            TVector<TString> uuids(
                std::make_move_iterator(session.MutableDeviceIds()->begin()),
                std::make_move_iterator(session.MutableDeviceIds()->end()));

            EraseIf(uuids, [&] (const auto& uuid) {
                return lostDevicesIds.contains(uuid);
            });

            const auto [_, error] = client.AcquireDevices(
                uuids,
                session.GetClientId(),
                TInstant::MicroSeconds(session.GetLastActivityTs()),
                session.GetReadOnly() ? NProto::VOLUME_ACCESS_READ_ONLY
                                      : NProto::VOLUME_ACCESS_READ_WRITE,
                session.GetMountSeqNumber(),
                session.GetDiskId(),
                session.GetVolumeGeneration());

            if (HasError(error)) {
                ++errors;

                STORAGE_ERROR("Can't restore session "
                    << session.GetClientId().Quote() << " from the cache: "
                    << FormatError(error));

                client.ReleaseDevices(
                    uuids,
                    session.GetClientId(),
                    session.GetDiskId(),
                    session.GetVolumeGeneration());
            }
        }

        if (errors) {
            ReportDiskAgentSessionCacheRestoreError(
                "some sessions have not recovered");
        }
    } catch (...) {
        STORAGE_ERROR("Can't restore sessions from the cache: "
            << CurrentExceptionMessage());
        ReportDiskAgentSessionCacheRestoreError();
    }
}

void TDiskAgentState::EnsureAccessToDevices(
    const TVector<TString>& uuids,
    const TString& clientId,
    NProto::EVolumeAccessMode accessMode) const
{
    for (const TString& uuid: uuids) {
        auto error = DeviceClient->AccessDevice(uuid, clientId, accessMode);
        if (HasError(error)) {
            ythrow TServiceError(E_REJECTED)
                << "Disk agent is partially suspended. "
                   "Can't acquire previously not acquired "
                   "devices. Access returned an error: "
                << FormatError(error);
        }
    }
}

TVector<NProto::TDiskAgentDeviceSession> TDiskAgentState::GetSessions() const
{
    return DeviceClient->GetSessions();
}

}   // namespace NCloud::NBlockStore::NStorage
