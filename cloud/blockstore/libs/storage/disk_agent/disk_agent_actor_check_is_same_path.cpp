
#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_generator.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_scanner.h>

#include <contrib/ydb/core/base/appdata_fwd.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {
using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeDeviceChangedError(
    const TString& path,
    const TString& reason)
{
    return MakeError(
        E_INVALID_STATE,
        Sprintf("device %s changed: %s", path.c_str(), reason.c_str()));
}

TString DescribePaths(const THashMap<TString, ui64>& pathToGeneration)
{
    TStringBuilder sb;
    for (const auto& [path, generation]: pathToGeneration) {
        sb << path << ":" << generation << " ";
    }
    return sb;
}

class TCheckIsSameDiskActor: public TActorBootstrapped<TCheckIsSameDiskActor>
{
private:
    const TActorId Owner;
    const TVector<TString> Paths;
    const TDiskAgentConfigPtr AgentConfig;
    TVector<NProto::TDeviceConfig> Devices;
    NNvme::INvmeManagerPtr NvmeManager;

    TVector<NProto::TFileDeviceArgs> Files;

    ILoggingServicePtr LoggingService;

public:
    TCheckIsSameDiskActor(
            TActorId owner,
            TVector<TString> paths,
            TDiskAgentConfigPtr agentConfig,
            TVector<NProto::TDeviceConfig> devices,
            NNvme::INvmeManagerPtr nvmeManager,
            ILoggingServicePtr loggingService)
        : Owner(owner)
        , Paths(std::move(paths))
        , AgentConfig(std::move(agentConfig))
        , Devices(std::move(devices))
        , NvmeManager(std::move(nvmeManager))
        , LoggingService(std::move(loggingService))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto error = FillFileDeviceArgs();
        if (HasError(error)) {
            // device is broken, not config mismatch
            ReplyAndDie(ctx, {});
            return;
        }

        if (auto error = CheckDevicesCountAndFileSizes(); HasError(error)) {
            ReplyAndDie(ctx, error);
            return;
        }

        error = CheckSerialNumber(ctx);
        ReplyAndDie(ctx, error);
    }

private:
    NProto::TError CheckSerialNumber(const TActorContext& ctx)
    {
        THashMap<TString, TString> pathToSerialNumber;
        for (const auto& path: Paths) {
            auto [serialNumber, error] = NvmeManager->GetSerialNumber(path);
            if (HasError(error)) {
                LOG_INFO(
                    ctx,
                    TBlockStoreComponents::DISK_AGENT_WORKER,
                    "Failed to get serial number for device %s: %s",
                    path.Quote().c_str(),
                    error.GetMessage().c_str());
            }

            pathToSerialNumber[path] = serialNumber;
        }

        for (const auto& device: Devices) {
            const auto& path = device.GetDeviceName();
            if (auto* serialNumber = pathToSerialNumber.FindPtr(path);
                serialNumber && *serialNumber &&
                device.GetSerialNumber() != *serialNumber)
            {
                return MakeDeviceChangedError(path, "serial number mismatch");
            }
        }

        return {};
    }

    NProto::TError FillFileDeviceArgs()
    {
        if (!AgentConfig->GetFileDevices().empty()) {
            Files.insert(
                Files.end(),
                AgentConfig->GetFileDevices().begin(),
                AgentConfig->GetFileDevices().end());
            return {};
        }
        TDeviceGenerator gen{
            LoggingService->CreateLog("BLOCKSTORE_DISK_AGENT"),
            AgentConfig->GetAgentId()};

        if (auto error = FindDevices(
                AgentConfig->GetStorageDiscoveryConfig(),
                std::ref(gen));
            HasError(error))
        {
            return error;
        }

        Files = gen.ExtractResult();
        return {};
    }

    NProto::TError CheckDevicesCountAndFileSizes()
    {
        THashMap<TString, i64> pathToFileSize;
        for (const auto& path: Paths) {
            pathToFileSize[path] = GetFileLength(path);
        }

        THashMap<TString, NProto::TFileDeviceArgs> uuidToFileDeviceArgs;
        for (const auto& file: Files) {
            if (!pathToFileSize.FindPtr(file.GetPath())) {
                continue;
            }
            uuidToFileDeviceArgs[file.GetDeviceId()] = file;
        }

        THashMap<TString, NProto::TDeviceConfig> uuidToDeviceConfigExpected;
        for (const auto& device: Devices) {
            if (!pathToFileSize.FindPtr(device.GetDeviceName())) {
                continue;
            }
            uuidToDeviceConfigExpected[device.GetDeviceUUID()] = device;
        }

        if (uuidToDeviceConfigExpected.size() != uuidToFileDeviceArgs.size()) {
            MakeError(
                E_INVALID_STATE,
                Sprintf("device changed: device count mismatch"));
        }

        for (const auto& [uuid, device]: uuidToDeviceConfigExpected) {
            const auto& path = device.GetDeviceName();

            auto* fileDeviceArgs = uuidToFileDeviceArgs.FindPtr(uuid);
            if (!fileDeviceArgs) {
                return MakeDeviceChangedError(
                    path,
                    Sprintf("device with uuid %s not found", uuid.c_str()));
            }

            const ui32 blockSize = fileDeviceArgs->GetBlockSize();

            if (device.GetPhysicalOffset() != fileDeviceArgs->GetOffset()) {
                return MakeDeviceChangedError(path, "offset mismatch");
            }

            if (device.GetBlockSize() != blockSize) {
                return MakeDeviceChangedError(path, "block size mismatch");
            }

            auto actualLength = pathToFileSize[path];

            ui64 len = fileDeviceArgs->GetFileSize();
            if (!len && actualLength > 0) {
                len = actualLength;
            } else if (!len) {
                continue;
            }

            if (device.GetBlocksCount() != len / blockSize) {
                return MakeDeviceChangedError(path, "size mismatch");
            }

            if (actualLength <= 0) {
                continue;
            }

            if (fileDeviceArgs->GetOffset() && fileDeviceArgs->GetFileSize() &&
                (static_cast<ui64>(actualLength) <
                 fileDeviceArgs->GetOffset() + fileDeviceArgs->GetFileSize()))
            {
                return MakeDeviceChangedError(path, "file size mismatch");
            }
        }

        return {};
    }

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
    {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvCheckIsSamePathResult>(
                error);
        NCloud::Send(ctx, Owner, std::move(response));
        Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::CheckIsSamePath(
    const NActors::TActorContext& ctx,
    TVector<TString> paths)
{
    if (!paths) {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvCheckIsSamePathResult>();
        NCloud::Send(ctx, ctx.SelfID, std::move(response));
        return;
    }

    THashSet<TString> pathsSet(paths.begin(), paths.end());
    TVector<NProto::TDeviceConfig> devices =
        State->GetAllDevicesForPaths(pathsSet);

    auto actorToCheck = std::make_unique<TCheckIsSameDiskActor>(
        ctx.SelfID,
        std::move(paths),
        AgentConfig,
        std::move(devices),
        NvmeManager,
        Logging);

    ctx.Register(
        actorToCheck.release(),
        TMailboxType::HTSwap,
        AppData()->IOPoolId);
}

void TDiskAgentActor::HandleCheckIsSamePathResult(
    const TEvDiskAgentPrivate::TEvCheckIsSamePathResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(PendingAttachPathRequest);
    if (!PendingAttachPathRequest) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Unexpected attach disk request");
        return;
    }

    if (HasError(ev->Get()->Error)) {
        ReportPathConfigChangedAfterStart(FormatError(ev->Get()->Error));
        auto response = std::make_unique<TEvDiskAgent::TEvAttachPathResponse>(
            ev->Get()->Error);

        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to attach paths [%s]: %s",
            DescribePaths(PendingAttachPathRequest->PathToGenerationToAttach)
                .c_str(),
            FormatError(ev->Get()->Error).c_str());
        NCloud::Reply(
            ctx,
            *PendingAttachPathRequest->RequestInfo,
            std::move(response));
        PendingAttachPathRequest.reset();
        return;
    }

    THashMap<TString, TFuture<IStoragePtr>> storageFutures;
    for (auto& [path, _]: PendingAttachPathRequest->PathToGenerationToAttach) {
        auto storagesToAdd = State->AttachPath(path);
        storageFutures.insert(storagesToAdd.begin(), storagesToAdd.end());
    }

    TVector<TFuture<IStoragePtr>> futures;
    futures.reserve(storageFutures.size());
    for (auto& [_, future]: storageFutures) {
        futures.push_back(future);
    }

    WaitAll(futures).Subscribe(
        [actorSystem = ctx.ActorSystem(),
         replyTo = ctx.SelfID,
         uuidToFuture = std::move(storageFutures)](auto) mutable
        {
            THashMap<TString, TResultOrError<IStoragePtr>> deviceOpenResults;
            for (auto& [uuid, future]: uuidToFuture) {
                deviceOpenResults.try_emplace(uuid, ResultOrError(future));
            }

            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathAttached>(
                    std::move(deviceOpenResults));
            actorSystem->Send(replyTo, response.release());
        });
}

}   // namespace NCloud::NBlockStore::NStorage
