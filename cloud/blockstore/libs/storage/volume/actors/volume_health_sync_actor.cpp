#include "volume_health_sync_actor.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

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
    TBackoffDelayProvider delayProvider)
    : TActor(&TThis::StateWork)
    , DiskId(std::move(diskId))
    , SeqNoGen(TCompositeId::FromGeneration(generation))
    , LogTitle(std::move(logTitle))
    , DelayProvider(std::move(delayProvider))
{}

////////////////////////////////////////////////////////////////////////////////

TInstant TVolumeHealthSyncActor::CalcRequestDeadline(TInstant now) const
{
    const auto delay = DelayProvider.GetDelay();
    const auto jitterMicros = delay.MicroSeconds() / 2
                                  ? RandomNumber<ui64>(delay.MicroSeconds() / 2)
                                  : 0;
    const auto total = delay + TDuration::MicroSeconds(jitterMicros);
    return total.ToDeadLine(now);
}

void TVolumeHealthSyncActor::SendUpdate(
    const TActorContext& ctx,
    TUpdate update)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Send UpdateVolumeHealth request: %s seqNo=%" PRIu64,
        LogTitle.GetWithTime().c_str(),
        NProto::EVolumeHealth_Name(update.Health).c_str(),
        update.SeqNo);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvUpdateVolumeHealthRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetVolumeHealth(update.Health);
    request->Record.MutableHeaders()->SetVolumeRequestId(update.SeqNo);

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request),
        update.SeqNo);

    ctx.Schedule(update.Deadline, new TEvents::TEvWakeup(update.SeqNo));
}

void TVolumeHealthSyncActor::CompleteUpdate(
    const TActorContext& ctx,
    ui64 seqNo,
    const NProto::TError& error)
{
    if (!UpdateInProgress || seqNo != UpdateInProgress->SeqNo) {
        return;
    }

    if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s UpdateVolumeHealth retriable error (seqNo=%" PRIu64 "): %s",
            LogTitle.GetWithTime().c_str(),
            seqNo,
            FormatError(error).c_str());

        if (error.GetCode() == E_TIMEOUT) {
            DelayProvider.IncreaseDelay();
        }

        UpdateInProgress->Deadline = CalcRequestDeadline(ctx.Now());
        SendUpdate(ctx, *UpdateInProgress);
        return;
    }

    const auto logLevel =
        HasError(error) ? NActors::NLog::PRI_ERROR : NActors::NLog::PRI_INFO;
    LOG_LOG(
        ctx,
        logLevel,
        TBlockStoreComponents::VOLUME,
        "%s UpdateVolumeHealth %s (seqNo=%" PRIu64 "): %s",
        LogTitle.GetWithTime().c_str(),
        HasError(error) ? "failed" : "succeeded",
        seqNo,
        FormatError(error).c_str());

    UpdateInProgress.reset();
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeHealthSyncActor::HandleUpdateVolumeHealth(
    const TEvDiskRegistry::TEvUpdateVolumeHealthRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto health = ev->Get()->Record.GetVolumeHealth();
    const ui64 seqNo = SeqNoGen.GetValue();
    SeqNoGen.Advance();
    DelayProvider.Reset();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Sync volume health with Disk Registry: %s seqNo=%" PRIu64,
        LogTitle.GetWithTime().c_str(),
        NProto::EVolumeHealth_Name(health).c_str(),
        seqNo);

    UpdateInProgress = TUpdate{
        .Health = health,
        .SeqNo = seqNo,
        .Deadline = CalcRequestDeadline(ctx.Now()),
    };
    SendUpdate(ctx, *UpdateInProgress);
}

void TVolumeHealthSyncActor::HandleUpdateVolumeHealthResponse(
    const TEvDiskRegistry::TEvUpdateVolumeHealthResponse::TPtr& ev,
    const TActorContext& ctx)
{
    CompleteUpdate(ctx, ev->Cookie, ev->Get()->Record.GetError());
}

void TVolumeHealthSyncActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    if (!UpdateInProgress || ev->Get()->Tag != UpdateInProgress->SeqNo) {
        return;
    }
    if (UpdateInProgress->Deadline > ctx.Now()) {
        return;
    }

    CompleteUpdate(
        ctx,
        UpdateInProgress->SeqNo,
        MakeError(E_TIMEOUT, "timed out"));
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
            TEvDiskRegistry::TEvUpdateVolumeHealthRequest,
            HandleUpdateVolumeHealth);
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
