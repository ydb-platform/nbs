#include "volume_session_actor.h"

#include "service.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/volume.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/core/tablet/tablet_setup.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStartVolumeActor final
    : public TActorBootstrapped<TStartVolumeActor>
{
private:
    enum class EPendingRequest
    {
        NONE,
        LOCK,
        BOOT_EXTERNAL,
        START,
        READY,
        WAKEUP,
        UNLOCK,
        STOP
    };

private:
    const TActorId SessionActorId;
    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ITraceSerializerPtr TraceSerializer;
    const NServer::IEndpointEventHandlerPtr EndpointEventHandler;
    const NRdma::IClientPtr RdmaClient;
    const TString DiskId;
    const ui64 VolumeTabletId;

    EPendingRequest PendingRequest = EPendingRequest::NONE;

    // Becomes true once shutdown process initiated
    bool Stopping = false;
    NProto::TError Error;

    // Tablet storage info received from Hive
    TTabletStorageInfoPtr VolumeTabletStorageInfo;

    // Becomes true once tablet lock is acquired
    bool VolumeTabletLocked = false;

    // Tablet actors managed by the mounting process
    ui32 VolumeGeneration = 0;
    TActorId VolumeSysActor;
    TActorId VolumeUserActor;

    // Duration between reboot attempts
    TDuration RebootSleepDuration;

public:
    TStartVolumeActor(
        const TActorId& sessionActorId,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ITraceSerializerPtr traceSerializer,
        NServer::IEndpointEventHandlerPtr endpointEventHandler,
        NRdma::IClientPtr rdmaClient,
        TString diskId,
        ui64 volumeTabletId = 0);

    void Bootstrap(const TActorContext& ctx);

private:
    void LockTablet(const TActorContext& ctx);
    void UnlockTablet(const TActorContext& ctx);

    void ScheduleReboot(const TActorContext& ctx, bool delay = true);

    void BootExternal(const TActorContext& ctx);

    void StartTablet(const TActorContext& ctx);
    void StopTablet(const TActorContext& ctx);

    void WaitReady(const TActorContext& ctx);

    void StartShutdown(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ContinueShutdown(const TActorContext& ctx);

    TString FormatPendingRequest() const;

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleLockTabletResponse(
        const TEvHiveProxy::TEvLockTabletResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUnlockTabletResponse(
        const TEvHiveProxy::TEvUnlockTabletResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleTabletLockLost(
        const TEvHiveProxy::TEvTabletLockLost::TPtr& ev,
        const TActorContext& ctx);

    void HandleBootExternalResponse(
        const TEvHiveProxy::TEvBootExternalResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleTabletRestored(
        const TEvTablet::TEvRestored::TPtr& ev,
        const TActorContext& ctx);

    void HandleTabletDead(
        const TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx);

    void HandleWaitReadyResponse(
        const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TStartVolumeActor::TStartVolumeActor(
        const TActorId& sessionActorId,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ITraceSerializerPtr traceSerializer,
        NServer::IEndpointEventHandlerPtr endpointEventHandler,
        NRdma::IClientPtr rdmaClient,
        TString diskId,
        ui64 volumeTabletId)
    : SessionActorId(sessionActorId)
    , Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , TraceSerializer(std::move(traceSerializer))
    , EndpointEventHandler(std::move(endpointEventHandler))
    , RdmaClient(std::move(rdmaClient))
    , DiskId(std::move(diskId))
    , VolumeTabletId(volumeTabletId)
{}

void TStartVolumeActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    LockTablet(ctx);
}

////////////////////////////////////////////////////////////////////////////////
// LockTablet

void TStartVolumeActor::LockTablet(const TActorContext& ctx)
{
    if (VolumeTabletLocked) {
        // Tablet is already locked, move to the next phase
        BootExternal(ctx);
        return;
    }

    if (PendingRequest == EPendingRequest::LOCK) {
        LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Tablet is already being locked",
            VolumeTabletId);
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Acquiring tablet lock",
        VolumeTabletId);

    PendingRequest = EPendingRequest::LOCK;

    NCloud::Send<TEvHiveProxy::TEvLockTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        0,  // cookie
        VolumeTabletId);
}

void TStartVolumeActor::HandleLockTabletResponse(
    const TEvHiveProxy::TEvLockTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_ABORT_UNLESS(PendingRequest == EPendingRequest::LOCK);
    PendingRequest = EPendingRequest::NONE;

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Failed to acquire tablet lock: %s",
            VolumeTabletId,
            FormatError(error).data());

        StartShutdown(ctx, error);
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Successfully acquired tablet lock",
        VolumeTabletId);

    VolumeTabletLocked = true;

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    BootExternal(ctx);
}

void TStartVolumeActor::HandleTabletLockLost(
    const TEvHiveProxy::TEvTabletLockLost::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    VolumeTabletLocked = false;

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    const auto* msg = ev->Get();

    LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Tablet lock has been lost with error: %s",
        VolumeTabletId,
        FormatError(msg->Error).data());

    auto error = MakeError(E_REJECTED, "Tablet lock has been lost");
    StartShutdown(ctx, error);
}

////////////////////////////////////////////////////////////////////////////////
// UnlockTablet

void TStartVolumeActor::UnlockTablet(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(VolumeTabletLocked);

    if (PendingRequest == EPendingRequest::UNLOCK) {
        LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Tablet is already being unlocked",
            VolumeTabletId);
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Releasing tablet lock",
        VolumeTabletId);

    PendingRequest = EPendingRequest::UNLOCK;

    NCloud::Send<TEvHiveProxy::TEvUnlockTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        0,  // cookie
        VolumeTabletId);
}

void TStartVolumeActor::HandleUnlockTabletResponse(
    const TEvHiveProxy::TEvUnlockTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (PendingRequest == EPendingRequest::UNLOCK) {
        PendingRequest = EPendingRequest::NONE;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Tablet lock has been released",
        VolumeTabletId);

    VolumeTabletLocked = false;

    Y_ABORT_UNLESS(Stopping);
    ContinueShutdown(ctx);
}

////////////////////////////////////////////////////////////////////////////////
// ScheduleReboot

void TStartVolumeActor::ScheduleReboot(const TActorContext& ctx, bool delay)
{
    if (delay) {
        // Wait for some time and restart mount process
        auto coolDownIncrement = Config->GetTabletRebootCoolDownIncrement();
        auto maxCoolDownDuration = Config->GetTabletRebootCoolDownMax();

        RebootSleepDuration = ClampVal(
            RebootSleepDuration + coolDownIncrement,
            coolDownIncrement,
            maxCoolDownDuration);
    } else {
        RebootSleepDuration = TDuration::Zero();
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Sleeping for %s before rebooting tablet",
        VolumeTabletId,
        ToString(RebootSleepDuration).data());

    PendingRequest = EPendingRequest::WAKEUP;

    ctx.Schedule(RebootSleepDuration, new TEvents::TEvWakeup());
}

////////////////////////////////////////////////////////////////////////////////
// BootExternal

void TStartVolumeActor::BootExternal(const TActorContext& ctx)
{
    if (PendingRequest == EPendingRequest::BOOT_EXTERNAL) {
        LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] External boot is already requested for tablet");
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Requesting external boot for tablet",
        VolumeTabletId);

    PendingRequest = EPendingRequest::BOOT_EXTERNAL;

    NCloud::Send<TEvHiveProxy::TEvBootExternalRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        0,  // cookie
        VolumeTabletId);
}

void TStartVolumeActor::HandleBootExternalResponse(
    const TEvHiveProxy::TEvBootExternalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (PendingRequest == EPendingRequest::BOOT_EXTERNAL) {
        PendingRequest = EPendingRequest::NONE;
    }

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] External boot request failed: %s",
            VolumeTabletId,
            FormatError(error).data());

        ScheduleReboot(ctx);
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Successfully confirmed external boot with generation %u",
        VolumeTabletId,
        msg->SuggestedGeneration);

    if (msg->SuggestedGeneration > VolumeGeneration) {
        VolumeGeneration = msg->SuggestedGeneration;
    }
    VolumeTabletStorageInfo = msg->StorageInfo;
    Y_ABORT_UNLESS(VolumeTabletStorageInfo->TabletID == VolumeTabletId,
        "Tablet IDs mismatch: %lu != %lu",
        VolumeTabletStorageInfo->TabletID,
        VolumeTabletId);

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    StartTablet(ctx);
}

////////////////////////////////////////////////////////////////////////////////
// StartTablet

void TStartVolumeActor::StartTablet(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(!VolumeSysActor);
    Y_ABORT_UNLESS(!VolumeUserActor);

    if (PendingRequest == EPendingRequest::START) {
        LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Tablet is already being started",
            VolumeTabletId);
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Starting tablet (gen: %u)",
        VolumeTabletId,
        VolumeGeneration);

    const auto* appData = AppData(ctx);

    auto config = Config;
    auto diagnosticsConfig = DiagnosticsConfig;
    auto profileLog = ProfileLog;
    auto blockDigestGenerator = BlockDigestGenerator;
    auto traceSerializer = TraceSerializer;
    auto endpointEventHandler = EndpointEventHandler;
    auto rdmaClient = RdmaClient;

    auto factory = [=] (const TActorId& owner, TTabletStorageInfo* storage) {
        Y_ABORT_UNLESS(storage->TabletType == TTabletTypes::BlockStoreVolume);
        auto actor = CreateVolumeTablet(
            owner,
            storage,
            config,
            diagnosticsConfig,
            profileLog,
            blockDigestGenerator,
            traceSerializer,
            rdmaClient,
            endpointEventHandler,
            EVolumeStartMode::MOUNTED);
        return actor.release();
    };

    auto setupInfo = MakeIntrusive<TTabletSetupInfo>(
        factory,
        TMailboxType::ReadAsFilled,
        appData->UserPoolId,
        TMailboxType::ReadAsFilled,
        appData->SystemPoolId);

    VolumeSysActor = setupInfo->Tablet(
        VolumeTabletStorageInfo.Get(),
        SelfId(),
        ctx,
        VolumeGeneration);

    PendingRequest = EPendingRequest::START;
}

void TStartVolumeActor::HandleTabletRestored(
    const TEvTablet::TEvRestored::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_ABORT_UNLESS(msg->TabletID == VolumeTabletId,
        "Tablet IDs mismatch: %lu != %lu",
        msg->TabletID,
        VolumeTabletId);

    if (ev->Sender != VolumeSysActor) {
        // This message is from an unexpected boot attempt
        // Ignore and kill the wrong system tablet
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Unexpected TEvRestored from %s (expected %s)",
            VolumeTabletId,
            ToString(ev->Sender).data(),
            ToString(VolumeSysActor).data());

        NCloud::Send<TEvents::TEvPoisonPill>(ctx, ev->Sender);
        return;
    }

    if (PendingRequest == EPendingRequest::START) {
        PendingRequest = EPendingRequest::NONE;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Tablet restored (gen: %u, system: %s, user: %s)",
        VolumeTabletId,
        VolumeGeneration,
        ToString(VolumeSysActor).data(),
        ToString(VolumeUserActor).data());

    VolumeGeneration = msg->Generation;
    VolumeUserActor = msg->UserTabletActor;

    // Reset reboot sleep duration since tablet booted successfully
    RebootSleepDuration = TDuration::Zero();

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    WaitReady(ctx);
}

void TStartVolumeActor::HandleTabletDead(
    const TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_ABORT_UNLESS(msg->TabletID == VolumeTabletId,
        "Tablet IDs mismatch: %lu != %lu",
        msg->TabletID,
        VolumeTabletId);

    if (ev->Sender != VolumeSysActor) {
        // This message is from an unexpected boot attempt
        // Ignore and kill the wrong system tablet
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Unexpected TEvTabletDead from %s (expected %s)",
            VolumeTabletId,
            ToString(ev->Sender).data(),
            ToString(VolumeSysActor).data());

        NCloud::Send<TEvents::TEvPoisonPill>(ctx, ev->Sender);
        return;
    }

    if (PendingRequest == EPendingRequest::STOP) {
        PendingRequest = EPendingRequest::NONE;
    }

    if (PendingRequest == EPendingRequest::START) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Tablet boot failed during actor starting",
            VolumeTabletId);

        PendingRequest = EPendingRequest::NONE;
    }

    VolumeSysActor = {};
    VolumeUserActor = {};

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    // Notify service about tablet status
    auto error = MakeError(E_REJECTED, TEvTablet::TEvTabletDead::Str(msg->Reason));

    LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Tablet failed: %s",
        VolumeTabletId,
        FormatError(error).data());

    NCloud::Send<TEvServicePrivate::TEvVolumeTabletStatus>(
        ctx,
        SessionActorId,
        0,  // cookie
        error);

    bool delay = true;
    switch (msg->Reason) {
        case TEvTablet::TEvTabletDead::ReasonBootRace:
            // Avoid unnecessary delays
            delay = false;
            break;
        case TEvTablet::TEvTabletDead::ReasonBootSuggestOutdated:
            // Avoid unnecessary delays
            delay = false;
            ++VolumeGeneration;
            break;
        default:
            break;
    }

    ScheduleReboot(ctx, delay);
}

////////////////////////////////////////////////////////////////////////////////
// StopTablet

void TStartVolumeActor::StopTablet(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(VolumeSysActor);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Sending PoisonPill to %s",
        VolumeTabletId,
        ToString(VolumeSysActor).data());

    PendingRequest = EPendingRequest::STOP;

    NCloud::Send<TEvents::TEvPoisonPill>(ctx, VolumeSysActor);
}

////////////////////////////////////////////////////////////////////////////////
// WaitReady

void TStartVolumeActor::WaitReady(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Sending WaitReady to %s",
        VolumeTabletId,
        ToString(VolumeUserActor).data());

    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(DiskId);

    PendingRequest = EPendingRequest::READY;

    NCloud::Send(ctx, VolumeUserActor, std::move(request));
}

void TStartVolumeActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (PendingRequest != EPendingRequest::READY) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Start volume %s actor: ignoring wait ready response, "
            "was not pending it, pending request is %s",
            DiskId.Quote().data(),
            FormatPendingRequest().data());
        return;
    }

    if (ev->Sender != VolumeUserActor) {
        // Ignore unexpected response
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Unexpected TEvWaitReadyResponse from %s (expected %s)",
            VolumeTabletId,
            ToString(ev->Sender).data(),
            ToString(VolumeUserActor).data());
        return;
    }

    PendingRequest = EPendingRequest::NONE;

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] WaitReady request failed: %s",
            VolumeTabletId,
            FormatError(error).data());

        StartShutdown(ctx, error);
        return;
    }

    // Notify service about tablet status
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Received WaitReady response",
        VolumeTabletId);

    const auto& volume = msg->Record.GetVolume();
    Y_ABORT_UNLESS(volume.GetDiskId() == DiskId);

    NCloud::Send<TEvServicePrivate::TEvVolumeTabletStatus>(
        ctx,
        SessionActorId,
        0,  // cookie
        VolumeTabletId,
        volume,
        VolumeUserActor);
}

////////////////////////////////////////////////////////////////////////////////
// Shutdown

void TStartVolumeActor::StartShutdown(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    Stopping = true;
    Error = error;

    ContinueShutdown(ctx);
}

void TStartVolumeActor::ContinueShutdown(const TActorContext& ctx)
{
    if (PendingRequest == EPendingRequest::START) {
        // wait until the tablet is started, then stop it
        return;
    }

    if (PendingRequest == EPendingRequest::LOCK) {
        // wait until lock response is received, then unlock properly
        return;
    }

    // perform steps to shutdown
    if (VolumeSysActor) {
        StopTablet(ctx);
        return;
    }

    if (VolumeTabletLocked) {
        UnlockTablet(ctx);
        return;
    }

    // notify service
    auto msg = std::make_unique<TEvServicePrivate::TEvStartVolumeActorStopped>(
        Error);

    NCloud::Send(ctx, SessionActorId, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

TString TStartVolumeActor::FormatPendingRequest() const
{
    switch (PendingRequest) {
        case EPendingRequest::NONE:
            return "None";
        case EPendingRequest::LOCK:
            return "Lock tablet";
        case EPendingRequest::BOOT_EXTERNAL:
            return "Boot external";
        case EPendingRequest::START:
            return "Start volume";
        case EPendingRequest::READY:
            return "Wait ready";
        case EPendingRequest::WAKEUP:
            return "Wakeup";
        case EPendingRequest::UNLOCK:
            return "Unlock";
        case EPendingRequest::STOP:
            return "Stop";
        default:
            return TStringBuilder()
                << "Unknown: " << static_cast<size_t>(PendingRequest);
    }
}

void TStartVolumeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    if (Stopping) {
        return;
    }

    Y_UNUSED(ev);

    NProto::TError error;
    if (PendingRequest == EPendingRequest::LOCK ||
        PendingRequest == EPendingRequest::BOOT_EXTERNAL ||
        PendingRequest == EPendingRequest::START ||
        PendingRequest == EPendingRequest::READY)
    {
        error = MakeError(E_INVALID_STATE, "Volume stopped while starting");
    }

    StartShutdown(ctx, error);
}

void TStartVolumeActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (PendingRequest != EPendingRequest::WAKEUP) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Start volume %s actor: ignoring wakeup, was not pending it, "
            "pending request is %s",
            DiskId.Quote().data(),
            FormatPendingRequest().data());
        return;
    }

    PendingRequest = EPendingRequest::NONE;

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    LockTablet(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TStartVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvLocal::TEvTabletMetrics);

        HFunc(TEvHiveProxy::TEvLockTabletResponse, HandleLockTabletResponse);
        HFunc(TEvHiveProxy::TEvBootExternalResponse, HandleBootExternalResponse);
        HFunc(TEvHiveProxy::TEvTabletLockLost, HandleTabletLockLost);
        HFunc(TEvHiveProxy::TEvUnlockTabletResponse, HandleUnlockTabletResponse);

        HFunc(TEvTablet::TEvRestored, HandleTabletRestored);
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        IgnoreFunc(TEvTablet::TEvReady);

        HFunc(TEvVolume::TEvWaitReadyResponse, HandleWaitReadyResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::HandleStartVolumeRequest(
    const TEvServicePrivate::TEvStartVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& diskId = VolumeInfo->DiskId;

    if (!StartVolumeActor) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Starting volume %s locally",
            diskId.Quote().data());

        CurrentRequest = START_REQUEST;
        StartVolumeActor = NCloud::Register<TStartVolumeActor>(
            ctx,
            SelfId(),
            Config,
            DiagnosticsConfig,
            ProfileLog,
            BlockDigestGenerator,
            TraceSerializer,
            EndpointEventHandler,
            RdmaClient,
            diskId,
            TabletId);
        return;
    }

    if (VolumeInfo->State == TVolumeInfo::STOPPING) {
        auto response = std::make_unique<TEvServicePrivate::TEvStartVolumeResponse>(
            MakeError(E_REJECTED, TStringBuilder()
                << "Volume " << diskId << " is being stopped"));
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            FormatError(response->GetError()));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (VolumeInfo->State == TVolumeInfo::STARTED) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Volume is already started");
        auto response = std::make_unique<TEvServicePrivate::TEvStartVolumeResponse>(
            *VolumeInfo->VolumeInfo);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
}

void TVolumeSessionActor::HandleVolumeTabletStatus(
    const TEvServicePrivate::TEvVolumeTabletStatus::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& diskId = VolumeInfo->DiskId;

    if (!msg->VolumeActor) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Volume %s failed: %s",
            VolumeInfo->TabletId,
            diskId.Quote().data(),
            ToString(msg->Error).data());

        VolumeInfo->VolumeActor = {};
        VolumeInfo->SetFailed(msg->Error);
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Volume %s started",
        msg->TabletId,
        diskId.Quote().data());

    VolumeInfo->SetStarted(msg->TabletId, msg->VolumeInfo, msg->VolumeActor);

    if (MountRequestActor) {
        auto response = std::make_unique<TEvServicePrivate::TEvStartVolumeResponse>(
            *VolumeInfo->VolumeInfo);
        NCloud::Send(ctx, MountRequestActor, std::move(response));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
