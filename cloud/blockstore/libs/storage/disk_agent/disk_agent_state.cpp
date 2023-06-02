#include "disk_agent_state.h"

#include "rdma_target.h"
#include "spdk_initializer.h"
#include "storage_initializer.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/sglist.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/spdk/iface/target.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_common/monitoring_utils.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

using TInitializeResult = TDiskAgentState::TInitializeResult;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxRequestSize = 128*1024*1024;  // TODO

////////////////////////////////////////////////////////////////////////////////

void ToDeviceStats(
    const TString& uuid,
    const TStorageIoStats& ioStats,
    NProto::TDeviceStats& stats)
{
    stats.SetDeviceUUID(uuid);
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
    ui32 blockSize)
{
    auto bytesCount = CalculateBytesCount(req, blockSize);
    if (bytesCount % blockSize != 0) {
        Y_VERIFY_DEBUG(false);
        return {};
    }

    const bool shouldCalcDigests = generator.ShouldProcess(
        req.GetStartIndex(),
        bytesCount / blockSize,
        blockSize);

    if (!shouldCalcDigests) {
        return {};
    }

    auto sglistOrError = SgListNormalize(GetSgList(req), blockSize);
    if (HasError(sglistOrError)) {
        Y_VERIFY_DEBUG(false);
        return {};
    }
    auto sglist = sglistOrError.ExtractResult();

    TVector<IProfileLog::TBlockInfo> blockInfos;

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
        TDiskAgentConfigPtr agentConfig,
        NSpdk::ISpdkEnvPtr spdk,
        ICachingAllocatorPtr allocator,
        IStorageProviderPtr storageProvider,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ILoggingServicePtr logging,
        NRdma::IServerPtr rdmaServer,
        NNvme::INvmeManagerPtr nvmeManager)
    : AgentConfig(std::move(agentConfig))
    , Spdk(std::move(spdk))
    , Allocator(std::move(allocator))
    , StorageProvider(std::move(storageProvider))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , Logging(std::move(logging))
    , RdmaServer(std::move(rdmaServer))
    , NvmeManager(std::move(nvmeManager))
{
}

const TDiskAgentState::TDeviceState& TDiskAgentState::GetDeviceState(
    const TString& uuid,
    const TString& sessionId,
    const NProto::EVolumeAccessMode accessMode) const
{
    return AgentConfig->GetAcquireRequired()
        ? GetDeviceStateImpl(uuid, sessionId, accessMode)
        : GetDeviceStateImpl(uuid);
}

const TDiskAgentState::TDeviceState& TDiskAgentState::GetDeviceStateImpl(
    const TString& uuid,
    const TString& sessionId,
    const NProto::EVolumeAccessMode accessMode) const
{
    auto error = DeviceClient->AccessDevice(uuid, sessionId, accessMode);

    if (HasError(error)) {
        ythrow TServiceError(error);
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

TString TDiskAgentState::GetDeviceName(const TString& uuid) const
{
    auto it = Devices.find(uuid);
    if (it == Devices.cend()) {
        ythrow TServiceError(E_NOT_FOUND)
            << "Device " << uuid.Quote() << " not found";
    }

    return it->second.Config.GetDeviceName();
}

TVector<NProto::TDeviceConfig> TDiskAgentState::GetDevices() const
{
    TVector<NProto::TDeviceConfig> devices;
    devices.reserve(Devices.size());

    for (auto& kv : Devices) {
        devices.push_back(kv.second.Config);
    }

    return devices;
}

TFuture<TInitializeResult> TDiskAgentState::InitSpdkStorage()
{
    return InitializeSpdk(AgentConfig, Spdk, Allocator)
        .Apply([=] (auto future) {
            TInitializeSpdkResult r = future.ExtractValue();

            InitErrorsCount = r.Errors.size();

            SpdkTarget = std::move(r.SpdkTarget);

            for (size_t i = 0; i != r.Configs.size(); ++i) {
                const auto& config = r.Configs[i];

                TDeviceState device {
                    .Config = config,
                    .StorageAdapter = std::make_shared<TStorageAdapter>(
                        std::move(r.Devices[i]),
                        config.GetBlockSize(),
                        false,  // normalize
                        MaxRequestSize)
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
    return InitializeStorage(AgentConfig, StorageProvider, NvmeManager)
        .Apply([=] (auto future) {
            TInitializeStorageResult r = future.ExtractValue();

            InitErrorsCount = r.Errors.size();

            Y_VERIFY(r.Configs.size() == r.Devices.size()
                  && r.Configs.size() == r.Stats.size());

            for (size_t i = 0; i != r.Configs.size(); ++i) {
                const auto& config = r.Configs[i];

                TDeviceState device {
                    .Config = config,
                    .StorageAdapter = std::make_shared<TStorageAdapter>(
                        std::move(r.Devices[i]),
                        config.GetBlockSize(),
                        false,  // normalize
                        MaxRequestSize),
                    .Stats = std::move(r.Stats[i])
                };

                Devices.emplace(
                    config.GetDeviceUUID(),
                    std::move(device));
            }

            return TInitializeResult {
                .Configs = std::move(r.Configs),
                .Errors = std::move(r.Errors),
                .Guard = std::move(r.Guard)
            };
        });
}

void TDiskAgentState::InitRdmaTarget()
{
    if (RdmaServer) {
        THashMap<TString, TStorageAdapterPtr> devices;

        auto endpoint = AgentConfig->GetRdmaTarget().GetEndpoint();

        for (auto& [uuid, state]: Devices) {
            state.Config.MutableRdmaEndpoint()->CopyFrom(endpoint);
            devices.emplace(uuid, state.StorageAdapter);
        }

        RdmaTarget = CreateRdmaTarget(
            endpoint,
            Logging,
            RdmaServer,
            DeviceClient,
            std::move(devices));

        RdmaTarget->Start();
    }
}

TFuture<TInitializeResult> TDiskAgentState::Initialize()
{
    auto future = Spdk ? InitSpdkStorage() : InitAioStorage();

    return future.Subscribe([=] (auto) {
        TVector<TString> uuids(Reserve(Devices.size()));
        for (const auto& x: Devices) {
            uuids.push_back(x.first);
        }

        DeviceClient = std::make_shared<TDeviceClient>(
            AgentConfig->GetReleaseInactiveSessionsTimeout(),
            std::move(uuids));

        InitRdmaTarget();
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
            ToDeviceStats(id, *device.Stats, *ds.Add());
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

TFuture<NProto::TReadDeviceBlocksResponse> TDiskAgentState::Read(
    TInstant now,
    NProto::TReadDeviceBlocksRequest request)
{
    Y_UNUSED(now);

    const auto& device = GetDeviceState(
        request.GetDeviceUUID(),
        request.GetSessionId(),
        NProto::VOLUME_ACCESS_READ_ONLY);

    auto readRequest = std::make_shared<NProto::TReadBlocksRequest>();
    readRequest->MutableHeaders()->CopyFrom(request.GetHeaders());
    readRequest->SetStartIndex(request.GetStartIndex());
    readRequest->SetBlocksCount(request.GetBlocksCount());

    auto result = device.StorageAdapter->ReadBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(readRequest),
        request.GetBlockSize());

    return result.Apply(
        [=] (auto future) {
            auto data = future.ExtractValue();
            NProto::TReadDeviceBlocksResponse response;

            *response.MutableError() = data.GetError();
            response.MutableBlocks()->Swap(data.MutableBlocks());

            return response;
        });
}

template <typename T>
void TDiskAgentState::WriteProfileLog(
    TInstant now,
    const TString& uuid,
    const T& req,
    ui32 blockSize,
    ESysRequestType requestType)
{
    auto blockInfos = ComputeDigest(*BlockDigestGenerator, req, blockSize);

    if (blockInfos) {
        ProfileLog->Write({
            .DiskId = uuid,
            .Ts = now,
            .Request = IProfileLog::TSysReadWriteRequestBlockInfos{
                .RequestType = requestType,
                .BlockInfos = std::move(blockInfos),
                .CommitId = 0,
            },
        });
    }
}

TFuture<NProto::TWriteDeviceBlocksResponse> TDiskAgentState::Write(
    TInstant now,
    NProto::TWriteDeviceBlocksRequest request)
{
    const auto& device = GetDeviceState(
        request.GetDeviceUUID(),
        request.GetSessionId(),
        NProto::VOLUME_ACCESS_READ_WRITE);

    auto writeRequest = std::make_shared<NProto::TWriteBlocksRequest>();
    writeRequest->MutableHeaders()->CopyFrom(request.GetHeaders());
    writeRequest->SetStartIndex(request.GetStartIndex());
    writeRequest->MutableBlocks()->Swap(request.MutableBlocks());

    WriteProfileLog(
        now,
        request.GetDeviceUUID(),
        *writeRequest,
        request.GetBlockSize(),
        ESysRequestType::WriteDeviceBlocks
    );

    auto result = device.StorageAdapter->WriteBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(writeRequest),
        request.GetBlockSize());

    return result.Apply(
        [=] (const auto& future) {
            NProto::TWriteDeviceBlocksResponse response;

            *response.MutableError() = future.GetValue().GetError();

            return response;
        });
}

TFuture<NProto::TZeroDeviceBlocksResponse> TDiskAgentState::WriteZeroes(
    TInstant now,
    NProto::TZeroDeviceBlocksRequest request)
{
    const auto& device = GetDeviceState(
        request.GetDeviceUUID(),
        request.GetSessionId(),
        NProto::VOLUME_ACCESS_READ_WRITE);

    auto zeroRequest = std::make_shared<NProto::TZeroBlocksRequest>();
    zeroRequest->SetStartIndex(request.GetStartIndex());
    zeroRequest->SetBlocksCount(request.GetBlocksCount());

    WriteProfileLog(
        now,
        request.GetDeviceUUID(),
        *zeroRequest,
        request.GetBlockSize(),
        ESysRequestType::ZeroDeviceBlocks
    );

    auto result = device.StorageAdapter->ZeroBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(zeroRequest),
        request.GetBlockSize());

    return result.Apply(
        [=] (const auto& future) {
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
        ReportAcquiredDiskEraseAttempt();

        ythrow TServiceError(E_INVALID_STATE)
            << "Device " << uuid.Quote()
            << " already acquired by session " << sessionInfo.Id;
    }

    if (AgentConfig->GetDeviceEraseMethod() == NProto::DEVICE_ERASE_METHOD_NONE) {
        return MakeFuture(NProto::TError());
    }

    return device.StorageAdapter->EraseDevice(
        AgentConfig->GetDeviceEraseMethod());
}

TFuture<NProto::TChecksumDeviceBlocksResponse> TDiskAgentState::Checksum(
    TInstant now,
    NProto::TChecksumDeviceBlocksRequest request)
{
    Y_UNUSED(now);

    const auto& device = GetDeviceState(
        request.GetDeviceUUID(),
        request.GetSessionId(),
        NProto::VOLUME_ACCESS_READ_ONLY);

    // TODO: Pass checksum request down to the device to avoid data copying
    auto readRequest = std::make_shared<NProto::TReadBlocksRequest>();
    readRequest->MutableHeaders()->CopyFrom(request.GetHeaders());
    readRequest->SetStartIndex(request.GetStartIndex());
    readRequest->SetBlocksCount(request.GetBlocksCount());

    auto result = device.StorageAdapter->ReadBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(readRequest),
        request.GetBlockSize());

    return result.Apply(
        [=] (auto future) {
            auto data = future.ExtractValue();
            NProto::TChecksumDeviceBlocksResponse response;

            if (HasError(data.GetError())) {
                *response.MutableError() = data.GetError();
            } else {
                TBlockChecksum checksum;
                for (const auto& buffer : data.GetBlocks().GetBuffers()) {
                    checksum.Extend(buffer.Data(), buffer.Size());
                }
                response.SetChecksum(checksum.GetValue());
            }

            return response;
        });
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

void TDiskAgentState::AcquireDevices(
    const TVector<TString>& uuids,
    const TString& sessionId,
    TInstant now,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    const TString& diskId,
    ui32 volumeGeneration)
{
    auto error = DeviceClient->AcquireDevices(
        uuids,
        sessionId,
        now,
        accessMode,
        mountSeqNumber,
        diskId,
        volumeGeneration);

    if (HasError(error)) {
        ythrow TServiceError(error);
    }
}

void TDiskAgentState::ReleaseDevices(
    const TVector<TString>& uuids,
    const TString& sessionId,
    const TString& diskId,
    ui32 volumeGeneration)
{
    auto error = DeviceClient->ReleaseDevices(
        uuids,
        sessionId,
        diskId,
        volumeGeneration);

    if (HasError(error)) {
        ythrow TServiceError(error);
    }
}

void TDiskAgentState::DisableDevice(const TString& uuid)
{
    auto d = Devices.FindPtr(uuid);
    if (d) {
        d->Disabled = true;
        d->Stats->OnError();
    }
}

bool TDiskAgentState::IsDeviceDisabled(const TString& uuid) const
{
    auto d = Devices.FindPtr(uuid);
    return d ? d->Disabled : false;
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

}   // namespace NCloud::NBlockStore::NStorage
