#include "bootstrapper.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/bootstrapper.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/hive.h>
#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/tablet/tablet_setup.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/utility.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void IgnoreEvent(const TAutoPtr<T>& ev, const TActorContext& ctx)
{
    const auto* event = ev->Get();

    LOG_DEBUG(ctx, TBlockStoreComponents::BOOTSTRAPPER,
        "Ignored event: (0x%08X) %s",
        ev->GetTypeRewrite(),
        event->ToStringHeader().data());
}

////////////////////////////////////////////////////////////////////////////////

class TBootstrapperActor final
    : public TActor<TBootstrapperActor>
{
private:
    const TBootstrapperConfig Config;
    const TActorId Owner;
    const TTabletStorageInfoPtr TabletStorageInfo;
    const TTabletSetupInfoPtr TabletSetupInfo;

    TActorId TabletSys;
    TActorId TabletUser;
    ui32 Generation = 0;

    // state and backoff timeout for tablet restart
    ui32 BootAttempts = 0;
    TDuration Timeout;

public:
    TBootstrapperActor(
        const TBootstrapperConfig& config,
        const TActorId& owner,
        TTabletStorageInfoPtr tabletStorageInfo,
        TTabletSetupInfoPtr tabletSetupInfo);

private:
    void StartTablet(const TActorContext& ctx);
    void StopTablet(const TActorContext& ctx);

    void ReportStatus(
        const TActorContext& ctx,
        TEvBootstrapper::ETabletStatus status,
        TString message = {});

    void ReportStatusAndDie(
        const TActorContext& ctx,
        TEvBootstrapper::ETabletStatus status,
        TString message = {});

    bool ShouldRestartTablet(TEvTablet::TEvTabletDead::EReason reason) const
    {
        if (Config.SuggestedGeneration) {
            return false; // never restart when we have specific generation
        }
        return Config.RestartAlways
            || reason <= TEvTablet::TEvTabletDead::ReasonBootReservedValue;
    }

    bool BootAttemptsExceeded() const
    {
        return Config.BootAttemptsThreshold
            && BootAttempts >= Config.BootAttemptsThreshold;
    }

private:
    STFUNC(StateInit);
    STFUNC(StateBoot);
    STFUNC(StateWork);
    STFUNC(StateWait);
    STFUNC(StateZombie);

    void HandleStart(
        const TEvBootstrapper::TEvStart::TPtr& ev,
        const TActorContext& ctx);

    void HandleStop(
        const TEvBootstrapper::TEvStop::TPtr& ev,
        const TActorContext& ctx);

    void HandleRestored(
        const TEvTablet::TEvRestored::TPtr& ev,
        const TActorContext& ctx);

    void HandleTabletDead(
        const TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx);

    void HandleTabletDead_Unexpected(
        const TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeUp(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    template <class TEventPtr>
    void Forward(TEventPtr& ev);
};

////////////////////////////////////////////////////////////////////////////////

TBootstrapperActor::TBootstrapperActor(
        const TBootstrapperConfig& config,
        const TActorId& owner,
        TTabletStorageInfoPtr tabletStorageInfo,
        TTabletSetupInfoPtr tabletSetupInfo)
    : TActor(&TThis::StateInit)
    , Config(config)
    , Owner(owner)
    , TabletStorageInfo(std::move(tabletStorageInfo))
    , TabletSetupInfo(std::move(tabletSetupInfo))
{}

void TBootstrapperActor::StartTablet(const TActorContext& ctx)
{
    LOG_INFO(ctx, TBlockStoreComponents::BOOTSTRAPPER,
        "[%lu] Starting tablet",
        TabletStorageInfo->TabletID);

    if (TabletSys) {
        ReportStatus(
            ctx,
            TEvBootstrapper::STARTED,
            "Tablet already started");
        return;
    }

    ui32 suggestedGeneration = Config.SuggestedGeneration;
    TabletSys = TabletSetupInfo->Tablet(
        TabletStorageInfo.Get(),
        SelfId(),
        ctx,
        suggestedGeneration);

    Become(&TThis::StateBoot);
}

void TBootstrapperActor::StopTablet(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::BOOTSTRAPPER,
        "[%lu] Stopping tablet",
        TabletStorageInfo->TabletID);

    if (!TabletSys) {
        ReportStatusAndDie(
            ctx,
            TEvBootstrapper::STOPPED,
            "Tablet already stopped");
        return;
    }

    NCloud::Send<TEvents::TEvPoisonPill>(ctx, TabletSys);

    Become(&TThis::StateZombie);
}

void TBootstrapperActor::ReportStatus(
    const TActorContext& ctx,
    TEvBootstrapper::ETabletStatus status,
    TString message)
{
    auto msg = std::make_unique<TEvBootstrapper::TEvStatus>(
        TabletStorageInfo->TabletID,
        TabletSys,
        TabletUser,
        status,
        std::move(message));

    NCloud::Send(ctx, Owner, std::move(msg));
}

void TBootstrapperActor::ReportStatusAndDie(
    const TActorContext& ctx,
    TEvBootstrapper::ETabletStatus status,
    TString message)
{
    LOG_INFO(ctx, TBlockStoreComponents::BOOTSTRAPPER,
        "[%lu] Exiting tablet (reason: %s)",
        TabletStorageInfo->TabletID,
        message.data());

    ReportStatus(ctx, status, std::move(message));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TBootstrapperActor::HandleStart(
    const TEvBootstrapper::TEvStart::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(ev->Sender == Owner);
    StartTablet(ctx);
}

void TBootstrapperActor::HandleStop(
    const TEvBootstrapper::TEvStop::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(ev->Sender == Owner);
    StopTablet(ctx);
}

void TBootstrapperActor::HandleRestored(
    const TEvTablet::TEvRestored::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_ABORT_UNLESS(TabletStorageInfo->TabletID == msg->TabletID,
        "Tablet IDs mismatch: %lu vs %lu",
        TabletStorageInfo->TabletID,
        msg->TabletID);

    TabletUser = msg->UserTabletActor;
    Generation = msg->Generation;

    BootAttempts = 0;
    Timeout = TDuration::Zero();

    LOG_INFO(ctx, TBlockStoreComponents::BOOTSTRAPPER,
        "[%lu] Tablet started (gen: %u, system: %s, user: %s)",
        msg->TabletID,
        Generation,
        ToString(TabletSys).data(),
        ToString(TabletUser).data());

    ReportStatus(ctx, TEvBootstrapper::STARTED);

    Become(&TThis::StateWork);
}

void TBootstrapperActor::HandleTabletDead(
    const TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_ABORT_UNLESS(TabletStorageInfo->TabletID == msg->TabletID,
        "Tablet IDs mismatch: %lu vs %lu",
        TabletStorageInfo->TabletID,
        msg->TabletID);

    ReportStatusAndDie(
        ctx,
        TEvBootstrapper::STOPPED,
        "Tablet stopped");
}

void TBootstrapperActor::HandleTabletDead_Unexpected(
    const TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_ABORT_UNLESS(TabletStorageInfo->TabletID == msg->TabletID,
        "Tablet IDs mismatch: %lu vs %lu",
        TabletStorageInfo->TabletID,
        msg->TabletID);

    ++BootAttempts;
    LOG_ERROR(ctx, TBlockStoreComponents::BOOTSTRAPPER,
        "[%lu] Tablet failed: %s (gen: %u, attempts: %u, threshold: %u)",
        msg->TabletID,
        TEvTablet::TEvTabletDead::Str(msg->Reason),
        msg->Generation,
        BootAttempts,
        Config.BootAttemptsThreshold);

    TabletSys = {};
    TabletUser = {};

    if (ShouldRestartTablet(msg->Reason)) {
        // try tablet restarts upon boot as it may take some time for system to come up
        if (!BootAttemptsExceeded()) {
            Timeout = ClampVal(
                Timeout + Config.CoolDownTimeout,
                Config.CoolDownTimeout,
                Config.CoolDownTimeout * 5);

            LOG_DEBUG(ctx, TBlockStoreComponents::BOOTSTRAPPER,
                "[%lu] Wait before restart (timeout: %s)",
                TabletStorageInfo->TabletID,
                ToString(Timeout).data());

            Become(&TThis::StateWait);
            ctx.Schedule(Timeout, new TEvents::TEvWakeup());
            return;
        }
    }

    TEvBootstrapper::ETabletStatus status;
    switch (msg->Reason) {
        case TEvTablet::TEvTabletDead::ReasonBootRace:
            status = TEvBootstrapper::RACE;
            break;
        case TEvTablet::TEvTabletDead::ReasonBootSuggestOutdated:
            status = TEvBootstrapper::SUGGEST_OUTDATED;
            break;
        default:
            status = TEvBootstrapper::FAILED;
            break;
    }

    ReportStatusAndDie(
        ctx,
        status,
        TEvTablet::TEvTabletDead::Str(msg->Reason));
}

void TBootstrapperActor::HandleWakeUp(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    StartTablet(ctx);
}

template <class TEventPtr>
void TBootstrapperActor::Forward(TEventPtr& ev)
{
    IActor::Forward(ev, Owner);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TBootstrapperActor::StateInit)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvBootstrapper::TEvStart, HandleStart);
        HFunc(TEvBootstrapper::TEvStop, HandleStop);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::BOOTSTRAPPER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TBootstrapperActor::StateBoot)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvBootstrapper::TEvStart, HandleStart);
        HFunc(TEvBootstrapper::TEvStop, HandleStop);

        HFunc(TEvTablet::TEvRestored, HandleRestored);
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead_Unexpected);
        HFunc(TEvTablet::TEvReady, IgnoreEvent);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::BOOTSTRAPPER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TBootstrapperActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvBootstrapper::TEvStart, HandleStart);
        HFunc(TEvBootstrapper::TEvStop, HandleStop);

        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead_Unexpected);
        HFunc(TEvTablet::TEvReady, IgnoreEvent);

        default:
            Forward(ev);
            break;
    }
}

STFUNC(TBootstrapperActor::StateWait)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvBootstrapper::TEvStart, HandleStart);
        HFunc(TEvBootstrapper::TEvStop, HandleStop);

        HFunc(TEvents::TEvWakeup, HandleWakeUp);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::BOOTSTRAPPER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TBootstrapperActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTablet::TEvRestored, IgnoreEvent);
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);

        HFunc(TEvents::TEvWakeup, IgnoreEvent);
        HFunc(TEvTablet::TEvReady, IgnoreEvent);

        default:
            Forward(ev);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateBootstrapper(
    const TBootstrapperConfig& config,
    const TActorId& owner,
    TTabletStorageInfoPtr tabletStorageInfo,
    TTabletSetupInfoPtr tabletSetupInfo)
{
    return std::make_unique<TBootstrapperActor>(
        config,
        owner,
        std::move(tabletStorageInfo),
        std::move(tabletSetupInfo));
}

}   // namespace NCloud::NBlockStore::NStorage
