#include "device_integrity_check_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/random/fast.h>

#include <optional>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum EWakeupTag : ui64
{
    WAKEUP_TAG_HEALTH_CHECK = 1,
    WAKEUP_TAG_PARTLABEL_CHECK = 2,
};

////////////////////////////////////////////////////////////////////////////////

enum class EDeviceHealthStatus
{
    Healthy,
    Broken,
    Unknown,
};

EDeviceHealthStatus GetHealthStatus(EWellKnownResultCodes code)
{
    switch (code) {
        case EWellKnownResultCodes::S_OK:
            return EDeviceHealthStatus::Healthy;
        case EWellKnownResultCodes::E_ARGUMENT:
        case EWellKnownResultCodes::E_CANCELLED:
        case EWellKnownResultCodes::E_REJECTED:
            return EDeviceHealthStatus::Unknown;
        default:
            return EDeviceHealthStatus::Broken;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDeviceIntegrityCheckActor
    : public TActorBootstrapped<TDeviceIntegrityCheckActor>
{
private:
    const TActorId DiskAgent;
    const TVector<NProto::TDeviceConfig> Devices;
    const TDuration HealthCheckDelay;
    const TDuration PartlabelCheckInterval;

    TVector<EDeviceHealthStatus> DevicesHealth;
    THashMap<TString, TString> SymlinkSnapshot;
    std::optional<TFastRng<ui64>> Rng;

    int PendingRequests = 0;

public:
    TDeviceIntegrityCheckActor(
        const TActorId& diskAgent,
        TVector<NProto::TDeviceConfig> devices,
        TDuration healthCheckDelay,
        TDuration partlabelCheckInterval);

    void Bootstrap(const TActorContext& ctx);

private:
    void CheckDevicesHealth(const TActorContext& ctx);
    void CheckPartlabels();

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadDeviceBlocksResponse(
        const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDeviceIntegrityCheckActor::TDeviceIntegrityCheckActor(
    const TActorId& diskAgent,
    TVector<NProto::TDeviceConfig> devices,
    TDuration healthCheckDelay,
    TDuration partlabelCheckInterval)
    : DiskAgent{diskAgent}
    , Devices(std::move(devices))
    , HealthCheckDelay(healthCheckDelay)
    , PartlabelCheckInterval(partlabelCheckInterval)
    , DevicesHealth(Devices.size(), EDeviceHealthStatus::Healthy)
{}

void TDeviceIntegrityCheckActor::Bootstrap(const TActorContext& ctx)
{
    Rng.emplace(ctx.Now().GetValue());

    for (const auto& device: Devices) {
        TFsPath path(device.GetDeviceName());
        if (path.IsSymlink()) {
            SymlinkSnapshot.emplace(
                device.GetDeviceName(),
                path.ReadLink().GetPath());
        }
    }

    Become(&TThis::StateWork);
    ctx.Schedule(
        HealthCheckDelay,
        new TEvents::TEvWakeup(EWakeupTag::WAKEUP_TAG_HEALTH_CHECK));
    ctx.Schedule(
        PartlabelCheckInterval,
        new TEvents::TEvWakeup(EWakeupTag::WAKEUP_TAG_PARTLABEL_CHECK));

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT_WORKER,
        "Device Integrity Check Actor started. Devices: " << Devices.size());
}

void TDeviceIntegrityCheckActor::CheckPartlabels()
{
    for (const auto& [configPath, expected]: SymlinkSnapshot) {
        TString current;
        TFsPath path(configPath);
        if (path.IsSymlink()) {
            current = path.ReadLink().GetPath();
        }
        if (current != expected) {
            ReportDiskAgentDevicePartlabelMismatch(
                "Partlabel may point to a different physical disk",
                {{"path", configPath},
                 {"expected", expected},
                 {"actual", current}});
        }
    }
}

void TDeviceIntegrityCheckActor::CheckDevicesHealth(const TActorContext& ctx)
{
    for (size_t i = 0; i < Devices.size(); ++i) {
        const auto& device = Devices[i];
        auto request =
            std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
        auto& rec = request->Record;
        rec.MutableHeaders()->SetClientId(TString(CheckHealthClientId));
        rec.SetDeviceUUID(device.GetDeviceUUID());
        rec.SetStartIndex(Rng->Uniform(device.GetBlocksCount()));
        rec.SetBlockSize(device.GetBlockSize());
        rec.SetBlocksCount(1);

        LOG_DEBUG_S(
            ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
            "Checking device: " << rec.DebugString());

        ctx.Send(DiskAgent, request.release(), TEventFlags{}, i);
        ++PendingRequests;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDeviceIntegrityCheckActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TDeviceIntegrityCheckActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    switch (ev->Get()->Tag) {
        case EWakeupTag::WAKEUP_TAG_HEALTH_CHECK:
            CheckDevicesHealth(ctx);
            break;
        case EWakeupTag::WAKEUP_TAG_PARTLABEL_CHECK:
            CheckPartlabels();
            ctx.Schedule(
                PartlabelCheckInterval,
                new TEvents::TEvWakeup(EWakeupTag::WAKEUP_TAG_PARTLABEL_CHECK));
            break;
        default:
            break;
    }
}

void TDeviceIntegrityCheckActor::HandleReadDeviceBlocksResponse(
    const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const size_t deviceIndex = ev->Cookie;
    Y_DEBUG_ABORT_UNLESS(
        deviceIndex < DevicesHealth.size(),
        "Invalid device index");
    const auto& deviceUUID = Devices[deviceIndex].GetDeviceUUID();

    auto currentHealth = GetHealthStatus(
        static_cast<EWellKnownResultCodes>(msg->GetError().GetCode()));
    auto lastHealth = DevicesHealth[deviceIndex];

    // Device has changed state only if reads status changed from healthy to
    // broken or vice versa. Ignore the "unknown" state.
    const bool deviceHealthChanged =
        currentHealth != lastHealth &&
        currentHealth != EDeviceHealthStatus::Unknown;

    if (currentHealth != EDeviceHealthStatus::Unknown) {
        // We save only the "healthy" and "broken" states. This allows us not to
        // trigger when transitions with "unknown" state occur.
        DevicesHealth[deviceIndex] = currentHealth;
    }

    switch (currentHealth) {
        case EDeviceHealthStatus::Healthy: {
            LOG_TRACE_S(
                ctx,
                TBlockStoreComponents::DISK_AGENT_WORKER,
                "Everything fine!");
            if (deviceHealthChanged) {
                LOG_WARN_S(
                    ctx,
                    TBlockStoreComponents::DISK_AGENT_WORKER,
                    "A miracle happened, the device " << deviceUUID.Quote()
                                                      << " was healed.");
            }
            break;
        }
        case EDeviceHealthStatus::Broken: {
            auto priority = deviceHealthChanged ? NActors::NLog::PRI_ERROR
                                                : NActors::NLog::PRI_INFO;
            LOG_LOG_S(
                ctx,
                priority,
                TBlockStoreComponents::DISK_AGENT_WORKER,
                "The device " << deviceUUID.Quote() << " broke down. "
                              << FormatError(msg->GetError()));
            break;
        }
        case EDeviceHealthStatus::Unknown: {
            LOG_DEBUG_S(
                ctx,
                TBlockStoreComponents::DISK_AGENT_WORKER,
                "Got error when reading from the device "
                    << deviceUUID.Quote() << ". "
                    << FormatError(msg->GetError()));
            break;
        }
    }

    if (--PendingRequests == 0) {
        ctx.Schedule(
            HealthCheckDelay,
            new TEvents::TEvWakeup(EWakeupTag::WAKEUP_TAG_HEALTH_CHECK));
    }
}

STFUNC(TDeviceIntegrityCheckActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvDiskAgent::TEvReadDeviceBlocksResponse,
            HandleReadDeviceBlocksResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_AGENT_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IActor> CreateDeviceIntegrityCheckActor(
    const TActorId& diskAgent,
    TVector<NProto::TDeviceConfig> devices,
    TDuration healthCheckDelay,
    TDuration partlabelCheckInterval)
{
    return std::make_unique<TDeviceIntegrityCheckActor>(
        diskAgent,
        std::move(devices),
        healthCheckDelay,
        partlabelCheckInterval);
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
