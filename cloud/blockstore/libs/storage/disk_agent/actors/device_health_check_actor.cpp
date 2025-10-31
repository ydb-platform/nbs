#include "device_health_check_actor.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/random/fast.h>

#include <optional>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

namespace {

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

class TDeviceHealthCheckActor
    : public TActorBootstrapped<TDeviceHealthCheckActor>
{
private:
    const TActorId DiskAgent;
    const TVector<NProto::TDeviceConfig> Devices;
    const TDuration HealthCheckDelay;

    TVector<EDeviceHealthStatus> DevicesHealth;
    std::optional<TFastRng<ui64>> Rng;

    int PendingRequests = 0;

public:
    TDeviceHealthCheckActor(
        const TActorId& diskAgent,
        TVector<NProto::TDeviceConfig> devices,
        TDuration healthCheckDelay);

    void Bootstrap(const TActorContext& ctx);

private:
    void ScheduleHealthCheck(const TActorContext& ctx);
    void CheckDevicesHealth(const TActorContext& ctx);

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

TDeviceHealthCheckActor::TDeviceHealthCheckActor(
        const TActorId& diskAgent,
        TVector<NProto::TDeviceConfig> devices,
        TDuration healthCheckDelay)
    : DiskAgent{diskAgent}
    , Devices(std::move(devices))
    , HealthCheckDelay(healthCheckDelay)
    , DevicesHealth(Devices.size(), EDeviceHealthStatus::Healthy)
{}

void TDeviceHealthCheckActor::Bootstrap(const TActorContext& ctx)
{
    Rng.emplace(ctx.Now().GetValue());

    Become(&TThis::StateWork);
    ScheduleHealthCheck(ctx);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT_WORKER,
        "Device Health Check Actor started. Devices: " << Devices.size());
}

void TDeviceHealthCheckActor::ScheduleHealthCheck(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_AGENT_WORKER,
        "Schedule health check");

    ctx.Schedule(HealthCheckDelay, new TEvents::TEvWakeup());
}

void TDeviceHealthCheckActor::CheckDevicesHealth(const TActorContext& ctx)
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

void TDeviceHealthCheckActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TDeviceHealthCheckActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    CheckDevicesHealth(ctx);
}

void TDeviceHealthCheckActor::HandleReadDeviceBlocksResponse(
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
        ScheduleHealthCheck(ctx);
    }
}

STFUNC(TDeviceHealthCheckActor::StateWork)
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

std::unique_ptr<IActor> CreateDeviceHealthCheckActor(
    const TActorId& diskAgent,
    TVector<NProto::TDeviceConfig> devices,
    TDuration healthCheckDelay)
{
    return std::make_unique<TDeviceHealthCheckActor>(
        diskAgent,
        std::move(devices),
        healthCheckDelay);
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
