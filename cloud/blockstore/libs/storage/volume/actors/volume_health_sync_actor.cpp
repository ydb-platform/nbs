#include "volume_health_sync_actor.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/log.h>

#include <util/random/random.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TVolumeHealthSyncActor::TVolumeHealthSyncActor(
    const TActorId& owner,
    TString diskId,
    TChildLogTitle logTitle,
    TDuration initialBackoff,
    TDuration maxBackoff)
    : Owner(owner)
    , DiskId(std::move(diskId))
    , LogTitle(std::move(logTitle))
    , InitialBackoff(initialBackoff)
    , MaxBackoff(maxBackoff)
    , Backoff(initialBackoff, maxBackoff)
{}

void TVolumeHealthSyncActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    Y_UNUSED(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeHealthSyncActor::SendUpdate(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvUpdateVolumeHealthRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetVolumeHealth(DesiredHealth);
    request->Record.MutableHeaders()->SetVolumeRequestId(DesiredSeqNo);

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request),
        DesiredSeqNo);

    ScheduleRetry(ctx);
}

void TVolumeHealthSyncActor::ScheduleRetry(const TActorContext& ctx)
{
    const auto delay = Backoff.GetDelayAndIncrease();
    const auto jitterMicros = delay.MicroSeconds() / 2
                                  ? RandomNumber<ui64>(delay.MicroSeconds() / 2)
                                  : 0;
    const auto total = delay + TDuration::MicroSeconds(jitterMicros);

    ++WakeupCookie;
    ctx.Schedule(total, new TEvents::TEvWakeup(WakeupCookie));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeHealthSyncActor::HandleSetDesiredHealth(
    const TEvVolumePrivate::TEvSetDesiredVolumeHealth::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->SeqNo <= DesiredSeqNo) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s SetDesiredVolumeHealth ignored: incoming seqNo=%" PRIu64
            " <= current desired=%" PRIu64,
            LogTitle.GetWithTime().c_str(),
            msg->SeqNo,
            DesiredSeqNo);
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s New desired volume health: %s seqNo=%" PRIu64,
        LogTitle.GetWithTime().c_str(),
        msg->VolumeHealth == NProto::VOLUME_HEALTH_HEALTHY ? "healthy"
                                                           : "unhealthy",
        msg->SeqNo);

    DesiredHealth = msg->VolumeHealth;
    DesiredSeqNo = msg->SeqNo;
    Backoff.Reset();
    SendUpdate(ctx);
}

void TVolumeHealthSyncActor::HandleUpdateVolumeHealthResponse(
    const TEvDiskRegistry::TEvUpdateVolumeHealthResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Cookie != DesiredSeqNo) {
        return;
    }

    const auto& error = ev->Get()->Record.GetError();
    if (HasError(error) && GetErrorKind(error) == EErrorKind::ErrorRetriable) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s UpdateVolumeHealth retriable error (seqNo=%" PRIu64
            "): %s; awaiting backoff",
            LogTitle.GetWithTime().c_str(),
            DesiredSeqNo,
            FormatError(error).c_str());
        return;
    }

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s UpdateVolumeHealth final error (seqNo=%" PRIu64 "): %s",
            LogTitle.GetWithTime().c_str(),
            DesiredSeqNo,
            FormatError(error).c_str());
    }

    LastConfirmedSeqNo = DesiredSeqNo;
    Backoff.Reset();
    ++WakeupCookie;
}

void TVolumeHealthSyncActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Get()->Tag != WakeupCookie) {
        return;
    }
    if (LastConfirmedSeqNo >= DesiredSeqNo) {
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Retrying UpdateVolumeHealth seqNo=%" PRIu64,
        LogTitle.GetWithTime().c_str(),
        DesiredSeqNo);

    SendUpdate(ctx);
}

void TVolumeHealthSyncActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TVolumeHealthSyncActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(
            TEvVolumePrivate::TEvSetDesiredVolumeHealth,
            HandleSetDesiredHealth);
        HFunc(
            TEvDiskRegistry::TEvUpdateVolumeHealthResponse,
            HandleUpdateVolumeHealthResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
