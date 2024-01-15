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
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/spdk/iface/target.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_common/monitoring_utils.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/string/join.h>
#include <util/system/fs.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

using TInitializeResult = TDiskAgentState::TInitializeResult;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxRequestSize = 128*1024*1024;  // TODO

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
    ui32 blockSize)
{
    auto bytesCount = CalculateBytesCount(req, blockSize);
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

    auto sglistOrError = SgListNormalize(GetSgList(req), blockSize);
    if (HasError(sglistOrError)) {
        Y_DEBUG_ABORT_UNLESS(false);
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
    , Log(Logging->CreateLog("BLOCKSTORE_DISK_AGENT"))
    , RdmaServer(std::move(rdmaServer))
    , NvmeManager(std::move(nvmeManager))
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

    for (auto& kv: Devices) {
        devices.push_back(kv.second.Config);
    }

    return devices;
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
                        MaxRequestSize,
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
            AgentConfig,
            StorageProvider,
            NvmeManager)
        .Apply([=] (auto future) {
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
                        MaxRequestSize,
                        ioTimeout,
                        AgentConfig->GetShutdownTimeout()),
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

void TDiskAgentState::InitRdmaTarget(TRdmaTargetConfig rdmaTargetConfig)
{
    if (RdmaServer) {
        THashMap<TString, TStorageAdapterPtr> devices;

        auto endpoint = AgentConfig->GetRdmaTarget().GetEndpoint();

        if (endpoint.GetHost().empty()) {
            endpoint.SetHost(FQDNHostName());
        }

        for (auto& [uuid, state]: Devices) {
            state.Config.MutableRdmaEndpoint()->CopyFrom(endpoint);
            devices.emplace(uuid, state.StorageAdapter);
        }

        RdmaTarget = CreateRdmaTarget(
            endpoint,
            std::move(rdmaTargetConfig),
            Logging,
            RdmaServer,
            DeviceClient,
            std::move(devices));

        RdmaTarget->Start();
    }
}

TFuture<TInitializeResult> TDiskAgentState::Initialize(
    TRdmaTargetConfig rdmaTargetConfig)
{
    auto future = Spdk ? InitSpdkStorage() : InitAioStorage();

    return future.Subscribe(
        [this, rdmaTargetConfig = std::move(rdmaTargetConfig)](auto) mutable
        {
            TVector<TString> uuids(Reserve(Devices.size()));
            for (const auto& x: Devices) {
                uuids.push_back(x.first);
            }

            DeviceClient = std::make_shared<TDeviceClient>(
                AgentConfig->GetReleaseInactiveSessionsTimeout(),
                std::move(uuids),
                Logging->CreateLog("BLOCKSTORE_DISK_AGENT"));

            InitRdmaTarget(std::move(rdmaTargetConfig));

            RestoreSessions(*DeviceClient);
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

TFuture<NProto::TReadDeviceBlocksResponse> TDiskAgentState::Read(
    TInstant now,
    NProto::TReadDeviceBlocksRequest request)
{
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
        request.GetBlockSize());

    return result.Apply(
        [] (auto future) {
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
        request.GetHeaders().GetClientId(),
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
        now,
        MakeIntrusive<TCallContext>(),
        std::move(writeRequest),
        request.GetBlockSize());

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
        request.GetBlockSize(),
        ESysRequestType::ZeroDeviceBlocks
    );

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
        ReportAcquiredDiskEraseAttempt();

        ythrow TServiceError(E_INVALID_STATE)
            << "Device " << uuid.Quote()
            << " already acquired by client " << sessionInfo.Id;
    }

    if (AgentConfig->GetDeviceEraseMethod() == NProto::DEVICE_ERASE_METHOD_NONE) {
        return MakeFuture(NProto::TError());
    }

    auto onDeviceSecureErased =
        [weakRdmaTarget = std::weak_ptr<IRdmaTarget>(RdmaTarget),
         uuid](const TFuture<NProto::TError>& future)
    {
        if (HasError(future.GetValue())) {
            return;
        }
        // The device has been secure erased and now a new client can use it.
        if (auto rdmaTarget = weakRdmaTarget.lock()) {
            rdmaTarget->DeviceSecureErased(uuid);
        }
    };

    return device.StorageAdapter
        ->EraseDevice(AgentConfig->GetDeviceEraseMethod())
        .Subscribe(std::move(onDeviceSecureErased));
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
    readRequest->SetStartIndex(request.GetStartIndex());
    readRequest->SetBlocksCount(request.GetBlocksCount());

    auto result = device.StorageAdapter->ReadBlocks(
        now,
        MakeIntrusive<TCallContext>(),
        std::move(readRequest),
        request.GetBlockSize());

    return result.Apply(
        [] (auto future) {
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

void TDiskAgentState::AcquireDevices(
    const TVector<TString>& uuids,
    const TString& clientId,
    TInstant now,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    const TString& diskId,
    ui32 volumeGeneration)
{
    CheckError(DeviceClient->AcquireDevices(
        uuids,
        clientId,
        now,
        accessMode,
        mountSeqNumber,
        diskId,
        volumeGeneration));

    UpdateSessionCache(*DeviceClient);
}

void TDiskAgentState::ReleaseDevices(
    const TVector<TString>& uuids,
    const TString& clientId,
    const TString& diskId,
    ui32 volumeGeneration)
{
    CheckError(DeviceClient->ReleaseDevices(
        uuids,
        clientId,
        diskId,
        volumeGeneration));

    UpdateSessionCache(*DeviceClient);
}

void TDiskAgentState::DisableDevice(const TString& uuid)
{
    DeviceClient->DisableDevice(uuid);
    ReportDisabledDeviceError(uuid);
}

void TDiskAgentState::EnableDevice(const TString& uuid)
{
    DeviceClient->EnableDevice(uuid);
}

bool TDiskAgentState::IsDeviceDisabled(const TString& uuid) const
{
    return DeviceClient->IsDeviceDisabled(uuid);
}

void TDiskAgentState::ReportDisabledDeviceError(const TString& uuid) {
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

void TDiskAgentState::UpdateSessionCache(TDeviceClient& client) const
{
    const auto path = AgentConfig->GetCachedSessionsPath();

    if (path.empty()) {
        STORAGE_INFO("Session cache is not configured.");
        return;
    }

    try {
        auto sessions = client.GetSessions();

        NProto::TDiskAgentDeviceSessionCache proto;
        proto.MutableSessions()->Assign(
            std::make_move_iterator(sessions.begin()),
            std::make_move_iterator(sessions.end())
        );

        const TString tmpPath {path + ".tmp"};

        SerializeToTextFormat(proto, tmpPath);

        if (!NFs::Rename(tmpPath, path)) {
            const auto ec = errno;
            ythrow TServiceError {MAKE_SYSTEM_ERROR(ec)} << strerror(ec);
        }
    } catch (...) {
        STORAGE_ERROR("Can't update session cache: " << CurrentExceptionMessage());
        ReportDiskAgentSessionCacheUpdateError();
    }
}

void TDiskAgentState::RestoreSessions(TDeviceClient& client) const
{
    const auto path = AgentConfig->GetCachedSessionsPath();

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

            const auto error = client.AcquireDevices(
                uuids,
                session.GetClientId(),
                TInstant::MicroSeconds(session.GetLastActivityTs()),
                session.GetReadOnly()
                    ? NProto::VOLUME_ACCESS_READ_ONLY
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

}   // namespace NCloud::NBlockStore::NStorage
