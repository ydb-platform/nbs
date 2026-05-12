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
    TString diskId,
    ui32 generation,
    TChildLogTitle logTitle,
    TDuration initialBackoff,
    TDuration maxBackoff)
    : DiskId(std::move(diskId))
    , SeqNoGen(TCompositeId::FromGeneration(generation))
    , LogTitle(std::move(logTitle))
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
    DesiredHealth = msg->VolumeHealth;
    DesiredSeqNo = SeqNoGen.Advance();
    Backoff.Reset();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s New desired volume health: %s seqNo=%" PRIu64,
        LogTitle.GetWithTime().c_str(),
        NProto::EVolumeHealth_Name(DesiredHealth).c_str(),
        DesiredSeqNo);

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
