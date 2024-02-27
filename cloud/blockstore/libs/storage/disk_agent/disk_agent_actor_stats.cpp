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

    NProto::TAgentStats PrevStats = {};
    NProto::TAgentStats CurStats = {};

    TVector<NProto::TDeviceConfig> Devices;
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

TStatsActor::TStatsActor(const TActorId& owner, const TVector<NProto::TDeviceConfig>& devices)
    : Owner(owner)
    , Devices(devices)
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
        new IEventHandle(ctx.SelfID, ctx.SelfID, request.get()));

    request.release();
}

void TStatsActor::CheckDevicesHealth(const TActorContext& ctx)
{
    for (const auto& device: Devices) {
        auto request = std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
        auto& rec = request->Record;
        rec.MutableHeaders()->SetClientId(TString(CheckHealthClientId));
        rec.SetDeviceUUID(device.GetDeviceUUID());
        rec.SetStartIndex(Rng.Uniform(device.GetBlocksCount()));
        rec.SetBlockSize(device.GetBlockSize());
        rec.SetBlocksCount(1);

        LOG_DEBUG_S(
            ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
            "Checking device: " + rec.DebugString());

        ctx.Send(Owner, request.release());
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

    if (!HasError(msg->GetError())) {
        LOG_TRACE_S(ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
            "Everything fine!");
        return;
    }

    if (msg->GetError().GetCode() == E_REJECTED) {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
            "Got error from reading device blocks: " << FormatError(msg->GetError()));
        return;
    }

    LOG_WARN_S(ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
        "Broken device found: " << FormatError(msg->GetError()));
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
