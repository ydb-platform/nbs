#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <util/random/fast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NThreading;

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

NProto::TDeviceStats CalcDelta(
    const NProto::TDeviceStats& cur,
    const NProto::TDeviceStats& prev)
{
    NProto::TDeviceStats delta;

    delta.SetDeviceUUID(cur.GetDeviceUUID());
    delta.SetDeviceName(cur.GetDeviceName());
    delta.SetBytesRead(cur.GetBytesRead() - prev.GetBytesRead());
    delta.SetNumReadOps(cur.GetNumReadOps() - prev.GetNumReadOps());
    delta.SetBytesWritten(cur.GetBytesWritten() - prev.GetBytesWritten());
    delta.SetNumWriteOps(cur.GetNumWriteOps() - prev.GetNumWriteOps());
    delta.SetErrors(cur.GetErrors() - prev.GetErrors());

    auto& buckets = *delta.MutableHistogramBuckets();
    buckets.Reserve(TStorageIoStats::BUCKETS_COUNT);

    auto it = prev.GetHistogramBuckets().begin();

    for (const auto& curBucket: cur.GetHistogramBuckets()) {
        auto& deltaBucket = *buckets.Add();
        deltaBucket = curBucket;
        if (it != prev.GetHistogramBuckets().end()
                && it->GetValue() == curBucket.GetValue())
        {
            deltaBucket.SetCount(curBucket.GetCount() - it->GetCount());
            ++it;
        }
    }

    return delta;
}

NProto::TAgentStats CalcDelta(
    const NProto::TAgentStats& cur,
    const NProto::TAgentStats& prev)
{
    NProto::TAgentStats delta;
    delta.SetInitErrorsCount(
        cur.GetInitErrorsCount() - prev.GetInitErrorsCount());

    THashMap<TString, NProto::TDeviceStats> prevDevStats;
    for (const auto& d: prev.GetDeviceStats()) {
        prevDevStats[d.GetDeviceUUID()] = d;
    }

    for (const auto& d: cur.GetDeviceStats()) {
        auto it = prevDevStats.find(d.GetDeviceUUID());
        if (it == prevDevStats.end()) {
            *delta.AddDeviceStats() = d;
        } else {
            *delta.AddDeviceStats() = CalcDelta(d, it->second);
        }
    }

    return delta;
}

////////////////////////////////////////////////////////////////////////////////

class TStatsActor
    : public TActorBootstrapped<TStatsActor>
{
private:
    const TActorId Owner;
    const TVector<NProto::TDeviceConfig> Devices;

    TVector<EDeviceHealthStatus> DevicesHealth;

    NProto::TAgentStats PrevStats = {};
    NProto::TAgentStats CurStats = {};

    TFastRng32 Rng;

public:
    explicit TStatsActor(const TActorId& owner, const TVector<NProto::TDeviceConfig>& devices);

    void Bootstrap(const TActorContext& ctx);

private:
    void ScheduleUpdateStats(const TActorContext& ctx);
    void CheckDevicesHealth(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleCollectStatsResponse(
        const TEvDiskAgentPrivate::TEvCollectStatsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUpdateAgentStatsResponse(
        const TEvDiskRegistry::TEvUpdateAgentStatsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadDeviceBlocksResponse(
        const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TStatsActor::TStatsActor(
        const TActorId& owner,
        const TVector<NProto::TDeviceConfig>& devices)
    : Owner(owner)
    , Devices(devices)
    , DevicesHealth(Devices.size(), EDeviceHealthStatus::Healthy)
    , Rng(12345, 0)
{}

void TStatsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    ScheduleUpdateStats(ctx);
}

void TStatsActor::ScheduleUpdateStats(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_AGENT_WORKER, "Schedule update stats");

    auto request = std::make_unique<TEvents::TEvWakeup>();

    ctx.ExecutorThread.Schedule(
        UpdateCountersInterval,
        new IEventHandle(ctx.SelfID, ctx.SelfID, request.release()));
}

void TStatsActor::CheckDevicesHealth(const TActorContext& ctx)
{
    for (size_t i = 0; i < Devices.size(); ++i) {
        const auto& device = Devices[i];
        auto request =
            std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
        auto& rec = request->Record;
        rec.MutableHeaders()->SetClientId(TString(CheckHealthClientId));
        rec.MutableHeaders()->SetRequestId(ctx.Now().MicroSeconds());
        rec.SetDeviceUUID(device.GetDeviceUUID());
        rec.SetStartIndex(Rng.Uniform(device.GetBlocksCount()));
        rec.SetBlockSize(device.GetBlockSize());
        rec.SetBlocksCount(1);

        LOG_DEBUG_S(
            ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
            "Checking device: " + rec.DebugString());

        ctx.Send(Owner, request.release(), TEventFlags{}, i);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TStatsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TStatsActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_AGENT_WORKER, "Collect stats");

    CheckDevicesHealth(ctx);
    NCloud::Send<TEvDiskAgentPrivate::TEvCollectStatsRequest>(ctx, Owner);
}

void TStatsActor::HandleCollectStatsResponse(
    const TEvDiskAgentPrivate::TEvCollectStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_WARN_S(ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
            "Collect stats failed: " << FormatError(msg->GetError()));

        ScheduleUpdateStats(ctx);

        return;
    }

    CurStats = std::move(msg->Stats);

    auto request = std::make_unique<TEvDiskRegistry::TEvUpdateAgentStatsRequest>();

    auto& stats = *request->Record.MutableAgentStats();

    stats = CalcDelta(CurStats, PrevStats);
    stats.SetNodeId(ctx.SelfID.NodeId());

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_AGENT_WORKER, "Update stats");
    ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());
}

void TStatsActor::HandleUpdateAgentStatsResponse(
    const TEvDiskRegistry::TEvUpdateAgentStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_WARN_S(ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
            "Update stats failed: " << FormatError(msg->GetError()));
    } else {
        PrevStats = CurStats;
    }

    ScheduleUpdateStats(ctx);
}

void TStatsActor::HandleReadDeviceBlocksResponse(
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
}

STFUNC(TStatsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvDiskAgentPrivate::TEvCollectStatsResponse,
            HandleCollectStatsResponse);

        HFunc(TEvDiskRegistry::TEvUpdateAgentStatsResponse,
            HandleUpdateAgentStatsResponse);

        HFunc(TEvDiskAgent::TEvReadDeviceBlocksResponse,
            HandleReadDeviceBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_AGENT_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::ScheduleUpdateStats(const TActorContext& ctx)
{
    if (!StatsActor) {
        StatsActor = NCloud::Register<TStatsActor>(
            ctx,
            ctx.SelfID,
            AgentConfig->GetDeviceHealthCheckDisabled()
                ? TVector<NProto::TDeviceConfig>{}
                : State->GetDevices());
    }
}

void TDiskAgentActor::HandleCollectStats(
    const TEvDiskAgentPrivate::TEvCollectStatsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto* actorSystem = ctx.ActorSystem();
    auto replyFrom = ctx.SelfID;

    auto reply = [=] (auto r) mutable {
        auto response = std::make_unique<TEvDiskAgentPrivate::TEvCollectStatsResponse>(
            std::move(r));

        actorSystem->Send(
            new IEventHandle(
                requestInfo->Sender,
                replyFrom,
                response.release(),
                0,          // flags
                requestInfo->Cookie));
    };

    State->CheckIOTimeouts(ctx.Now());

    auto result = State->CollectStats();

    result.Subscribe([=] (auto future) mutable {
        try {
            reply(future.ExtractValue());
        } catch (...) {
            reply(MakeError(E_FAIL, CurrentExceptionMessage()));
        }
    });
}

}   // namespace NCloud::NBlockStore::NStorage
