#include "disk_agent_actor.h"

#include "journalled_device.h"

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/journalled_device/journalled_device.h>
#include <cloud/storage/core/libs/journalled_device_tcp_server/server.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NJournalled;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr NProto::EVolumeAccessMode DefaultAccessMode =
    NProto::VOLUME_ACCESS_READ_WRITE;
constexpr ui64 DefaultMountSeqNumber = 0;
constexpr ui64 DefaultVolumeGeneration = 0;

////////////////////////////////////////////////////////////////////////////////

void CopyHeaders(
    NProto::THeaders& dst,
    const NCloud::NProto::TDeviceRequestHeaders& src)
{
    dst.SetClientId(src.GetClientId());
    dst.SetRequestTimeout(src.GetRequestTimeout());
}

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceHandler final: public IServerBackend
{
private:
    TActorSystem* ActorSystem = nullptr;
    const TActorId DiskAgentActorId;
    const THashMap<TString, IJournalledDevicePtr> Devices;

public:
    TJournalledDeviceHandler(
        TActorSystem* actorSystem,
        const TActorId& diskAgentActorId,
        THashMap<TString, IJournalledDevicePtr> devices)
        : ActorSystem(actorSystem)
        , DiskAgentActorId(diskAgentActorId)
        , Devices(std::move(devices))
    {}

    // IServerBackend

    [[nodiscard]] auto AcquireDevices(
        TInstant now,
        NCloud::NProto::TAcquireDevicesRequest request)
        -> TFuture<NCloud::NProto::TAcquireDevicesResponse> final
    {
        Y_UNUSED(now);

        auto ev = std::make_unique<TEvDiskAgent::TEvAcquireDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());
        ev->Record.SetAccessMode(DefaultAccessMode);
        ev->Record.SetMountSeqNumber(DefaultMountSeqNumber);
        ev->Record.SetDiskId(request.GetHeaders().GetClientId());
        ev->Record.SetVolumeGeneration(DefaultVolumeGeneration);

        auto future = ActorSystem->Ask<TEvDiskAgent::TEvAcquireDevicesResponse>(
            DiskAgentActorId,
            THolder(ev.release()));

        return future.Apply(
            [](const auto& future)
            {
                NCloud::NProto::TAcquireDevicesResponse response;
                const auto& ev = future.GetValue();
                response.MutableError()->CopyFrom(ev->Record.GetError());

                return response;
            });
    }

    [[nodiscard]] auto ReleaseDevices(
        TInstant now,
        NCloud::NProto::TReleaseDevicesRequest request)
        -> TFuture<NCloud::NProto::TReleaseDevicesResponse> final
    {
        Y_UNUSED(now);

        auto promise = NewPromise<NCloud::NProto::TReleaseDevicesResponse>();

        auto ev = std::make_unique<TEvDiskAgent::TEvReleaseDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());

        auto future = ActorSystem->Ask<TEvDiskAgent::TEvReleaseDevicesResponse>(
            DiskAgentActorId,
            THolder(ev.release()));

        return future.Apply(
            [](const auto& future)
            {
                NCloud::NProto::TReleaseDevicesResponse response;
                const auto& ev = future.GetValue();
                response.MutableError()->CopyFrom(ev->Record.GetError());

                return response;
            });
    }

    [[nodiscard]] auto ReadPages(
        TInstant now,
        NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        auto [device, error] = GetDevice(request.GetDeviceUUID());
        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        return device->ReadPages(now, std::move(request));
    }

    [[nodiscard]] auto WriteLogRecord(
        TInstant now,
        NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> final
    {
        auto [device, error] = GetDevice(request.GetDeviceUUID());
        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        }

        return device->WriteLogRecord(now, std::move(request));
    }

    [[nodiscard]] auto ReadJournalTail(
        TInstant now,
        NCloud::NProto::TReadJournalTailRequest request)
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> final
    {
        auto [device, error] = GetDevice(request.GetDeviceUUID());
        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadJournalTailResponse>(
                TErrorResponse(error));
        }

        return device->ReadJournalTail(now, std::move(request));
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        TInstant now,
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        auto [device, error] = GetDevice(request.GetDeviceUUID());
        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>(
                TErrorResponse(error));
        }

        return device->AdvanceLsnLowWatermark(now, std::move(request));
    }

private:
    TResultOrError<IJournalledDevicePtr> GetDevice(
        const TString& deviceUUID) const
    {
        if (deviceUUID.empty()) {
            return MakeError(E_ARGUMENT, "empty device UUID");
        }

        auto* device = Devices.FindPtr(deviceUUID);
        if (!device) {
            return MakeError(E_NOT_FOUND, TStringBuilder()
                << "Device " << deviceUUID.Quote() << " not found");
        }

        return *device;
    }
};

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress CreateNetworkAddress(TStringBuf s)
{
    TStringBuf hostRef;
    TStringBuf portRef;
    s.RSplit(':', hostRef, portRef);

    return {
        hostRef ? TString(hostRef).c_str() : nullptr,
        FromString<ui16>(portRef)};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::StartJournalledDeviceTcpServer(
    const NActors::TActorContext& ctx,
    THashMap<TString, IJournalledDevicePtr> devices)
{
    if (AgentConfig->GetJournalledDeviceTcpServerListenAddress().empty()) {
        return;
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Starting journaled device TCP server on "
            << AgentConfig->GetJournalledDeviceTcpServerListenAddress().Quote()
            << "...");

    try {
        const TNetworkAddress listenAddress = CreateNetworkAddress(
            AgentConfig->GetJournalledDeviceTcpServerListenAddress());

        Executor = TExecutor::Create("JD");
        Executor->Start();

        JournalledDeviceTcpServer = NJournalled::CreateServer(
            listenAddress,
            Logging,
            Executor,
            std::make_shared<TJournalledDeviceHandler>(
                TActivationContext::ActorSystem(),
                ctx.SelfID,
                std::move(devices)));

        JournalledDeviceTcpServer->Start();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Journalled device TCP server started on "
                << AgentConfig->GetJournalledDeviceTcpServerListenAddress()
                       .Quote());

    } catch (...) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Could not start journalled device TCP server"
                << ": " << CurrentExceptionMessage());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
