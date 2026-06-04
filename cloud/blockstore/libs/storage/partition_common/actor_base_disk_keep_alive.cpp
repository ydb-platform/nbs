#include "actor_base_disk_keep_alive.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TBaseDiskKeepAliveActor::TBaseDiskKeepAliveActor(
    TString baseDiskId,
    TDuration keepAliveInterval,
    TChildLogTitle logTitle)
    : BaseDiskId(std::move(baseDiskId))
    , KeepAliveInterval(keepAliveInterval)
    , LogTitle(std::move(logTitle))
{
    Y_DEBUG_ABORT_UNLESS(BaseDiskId);
    Y_DEBUG_ABORT_UNLESS(KeepAliveInterval);
}

void TBaseDiskKeepAliveActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_COMMON,
        "%s BaseDiskKeepAlive started for interval=%s",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(KeepAliveInterval).c_str());

    // Send the first ping immediately and arm the periodic timer. The timer is
    // self-sustaining: every wakeup re-arms it, so the cadence does not depend
    // on ping responses (see HandlePingResponse).
    SendPing(ctx);
    SchedulePing(ctx);
}

void TBaseDiskKeepAliveActor::SendPing(const TActorContext& ctx)
{
    if (PingInFlight) {
        const auto outstanding = ctx.Now() - LastPingSentAt;
        if (outstanding < KeepAliveInterval) {
            // The previous ping is still expected to be answered shortly. Skip
            // this one.
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::PARTITION_COMMON,
                "%s BaseDiskKeepAlive skipping overlapping ping",
                LogTitle.GetWithTime().c_str());
            return;
        }

        // The previous ping has been outstanding for at least one interval -
        // the response was most likely lost. To avoid the keep-alive message
        // from getting stuck, will send a new ping.
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_COMMON,
            "%s BaseDiskKeepAlive previous ping lost after "
            "%s, sending a new one",
            LogTitle.GetWithTime().c_str(),
            FormatDuration(outstanding).c_str());
    }

    auto request = std::make_unique<TEvVolumeProxy::TEvKeepAliveRequest>(BaseDiskId);

    PingInFlight = true;
    LastPingSentAt = ctx.Now();

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TBaseDiskKeepAliveActor::SchedulePing(const TActorContext& ctx)
{
    ctx.Schedule(KeepAliveInterval, new TEvents::TEvWakeup());
}

void TBaseDiskKeepAliveActor::HandleKeepAliveResponse(
    const TEvVolumeProxy::TEvKeepAliveResponse::TPtr& ev,
    const TActorContext& ctx)
{
    PingInFlight = false;

    const auto& error = ev->Get()->GetError();
    if (HasError(error)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_COMMON,
            "%s BaseDiskKeepAlive ping failed: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());
    }

    // The next ping is driven by the self-sustaining timer, not by this
    // response - do not schedule here to keep a single timer chain.
}

void TBaseDiskKeepAliveActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SendPing(ctx);
    SchedulePing(ctx);
}

void TBaseDiskKeepAliveActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_COMMON,
        "%s BaseDiskKeepAlive stopping",
        LogTitle.GetWithTime().c_str());

    Die(ctx);
}

STFUNC(TBaseDiskKeepAliveActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvVolumeProxy::TEvKeepAliveResponse, HandleKeepAliveResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_COMMON,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
