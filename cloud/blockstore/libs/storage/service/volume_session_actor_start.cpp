#include "volume_session_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/volume/volume.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/format.h>

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

    bool Ready = false;
    NProto::TVolume Volume;
    TEvTablet::TEvTabletDead::EReason VolumeTabletDeadReason;

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

    TLogTitle LogTitle{VolumeTabletId, DiskId, GetCycleCount()};

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

    void DescribeVolume(const TActorContext& ctx);

    void ScheduleReboot(const TActorContext& ctx, bool delay = true);

    void BootExternal(const TActorContext& ctx);

    void StartTablet(const TActorContext& ctx);
    void StopTablet(const TActorContext& ctx);

    void WaitReady(const TActorContext& ctx);

    void StartShutdown(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ContinueShutdown(const TActorContext& ctx);

    void SendVolumeTabletStatus(const TActorContext& ctx);
    void SendVolumeTabletDeadErrorAndScheduleReboot(const TActorContext& ctx);

    TString FormatPendingRequest() const;

private:
    STFUNC(StateWork);

    void HandleStartVolumeRequest(
        const TEvServicePrivate::TEvStartVolumeRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleLockTabletResponse(
        const TEvHiveProxy::TEvLockTabletResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
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
        LOG_TRACE(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Tablet is already being locked",
            LogTitle.GetWithTime().c_str());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Acquiring tablet lock",
        LogTitle.GetWithTime().c_str());

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
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Failed to acquire tablet lock: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        StartShutdown(ctx, error);
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Successfully acquired tablet lock",
        LogTitle.GetWithTime().c_str());

    VolumeTabletLocked = true;

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    if (Ready) {
        SendVolumeTabletStatus(ctx);
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

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Tablet lock has been lost with error: %s",
        LogTitle.GetWithTime().c_str(),
        FormatError(msg->Error).c_str());

    if (!Config->GetDoNotStopVolumeTabletOnLockLost()) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Stop volume tablet on lock lost",
            LogTitle.GetWithTime().c_str());

        auto error = MakeError(E_REJECTED, "Tablet lock has been lost");
        StartShutdown(ctx, error);
        return;
    }

    // Check if volume is not destroyed.
    DescribeVolume(ctx);
}

void TStartVolumeActor::DescribeVolume(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Sending describe request for volume",
        LogTitle.GetWithTime().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(DiskId));
}

void TStartVolumeActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->GetStatus() ==
        MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist))
    {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Volume is destroyed",
            LogTitle.GetWithTime().c_str());

        StartShutdown(ctx);
        return;
    }

    if (msg->GetStatus() == NKikimrScheme::StatusSuccess) {
        auto volumeTabletId = msg->
            PathDescription.
            GetBlockStoreVolumeDescription().
            GetVolumeTabletId();

        if (volumeTabletId != VolumeTabletId) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::SERVICE,
                "%s Volume TabletId was changed to %lu",
                LogTitle.GetWithTime().c_str(),
                volumeTabletId);

            StartShutdown(ctx);
            return;
        }
    }

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }
}

void TStartVolumeActor::HandleStartVolumeRequest(
    const TEvServicePrivate::TEvStartVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!VolumeTabletLocked) {
        LockTablet(ctx);
        return;
    }

    SendVolumeTabletStatus(ctx);
}

////////////////////////////////////////////////////////////////////////////////
// UnlockTablet

void TStartVolumeActor::UnlockTablet(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(VolumeTabletLocked);

    if (PendingRequest == EPendingRequest::UNLOCK) {
        LOG_TRACE(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Tablet is already being unlocked",
            LogTitle.GetWithTime().c_str());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Releasing tablet lock",
        LogTitle.GetWithTime().c_str());

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

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Tablet lock has been released",
        LogTitle.GetWithTime().c_str());

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

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Sleeping for %s before rebooting tablet",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(RebootSleepDuration).c_str());

    PendingRequest = EPendingRequest::WAKEUP;

    ctx.Schedule(RebootSleepDuration, new TEvents::TEvWakeup());
}

////////////////////////////////////////////////////////////////////////////////
// BootExternal

void TStartVolumeActor::BootExternal(const TActorContext& ctx)
{
    if (PendingRequest == EPendingRequest::BOOT_EXTERNAL) {
        LOG_TRACE(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s External boot is already requested for tablet",
            LogTitle.GetWithTime().c_str());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Requesting external boot for volume tablet",
        LogTitle.GetWithTime().c_str());

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
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s External boot request failed: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        ScheduleReboot(ctx);
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Successfully confirmed external boot with generation %u",
        LogTitle.GetWithTime().c_str(),
        msg->SuggestedGeneration);

    if (msg->SuggestedGeneration > VolumeGeneration) {
        VolumeGeneration = msg->SuggestedGeneration;
        LogTitle.SetGeneration(VolumeGeneration);
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
        LOG_TRACE(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Tablet is already being started",
            LogTitle.GetWithTime().c_str());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Starting volume tablet",
        LogTitle.GetWithTime().c_str());

    const auto* appData = AppData(ctx);

    auto config = Config;
    auto diagnosticsConfig = DiagnosticsConfig;
    auto profileLog = ProfileLog;
    auto blockDigestGenerator = BlockDigestGenerator;
    auto traceSerializer = TraceSerializer;
    auto endpointEventHandler = EndpointEventHandler;
    auto rdmaClient = RdmaClient;

    auto factory =
        [config,
         diagnosticsConfig,
         profileLog,
         blockDigestGenerator,
         traceSerializer,
         rdmaClient,
         endpointEventHandler,
         diskId = DiskId](const TActorId& owner, TTabletStorageInfo* storage)
    {
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
            EVolumeStartMode::MOUNTED,
            diskId);
        return actor.release();
    };

    auto setupInfo = MakeIntrusive<TTabletSetupInfo>(
        std::move(factory),
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
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Unexpected TEvRestored from %s (expected %s)",
            LogTitle.GetWithTime().c_str(),
            ToString(ev->Sender).data(),
            ToString(VolumeSysActor).data());

        NCloud::Send<TEvents::TEvPoisonPill>(ctx, ev->Sender);
        return;
    }

    if (PendingRequest == EPendingRequest::START) {
        PendingRequest = EPendingRequest::NONE;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Tablet restored (gen: %u, system: %s, user: %s)",
        LogTitle.GetWithTime().c_str(),
        VolumeGeneration,
        ToString(VolumeSysActor).data(),
        ToString(VolumeUserActor).data());

    VolumeGeneration = msg->Generation;
    VolumeUserActor = msg->UserTabletActor;

    LogTitle.SetGeneration(VolumeGeneration);

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
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Unexpected TEvTabletDead from %s (expected %s)",
            LogTitle.GetWithTime().c_str(),
            ToString(ev->Sender).c_str(),
            ToString(VolumeSysActor).c_str());

        NCloud::Send<TEvents::TEvPoisonPill>(ctx, ev->Sender);
        return;
    }

    if (PendingRequest == EPendingRequest::STOP) {
        PendingRequest = EPendingRequest::NONE;
    }

    if (PendingRequest == EPendingRequest::START) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Tablet boot failed during actor starting",
            LogTitle.GetWithTime().c_str());

        PendingRequest = EPendingRequest::NONE;
    }

    Ready = false;
    VolumeSysActor = {};
    VolumeUserActor = {};
    VolumeTabletDeadReason = msg->Reason;

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    SendVolumeTabletDeadErrorAndScheduleReboot(ctx);
}

////////////////////////////////////////////////////////////////////////////////
// StopTablet

void TStartVolumeActor::StopTablet(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(VolumeSysActor);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Sending PoisonPill to %s",
        LogTitle.GetWithTime().c_str(),
        ToString(VolumeSysActor).data());

    PendingRequest = EPendingRequest::STOP;

    NCloud::Send<TEvents::TEvPoisonPill>(ctx, VolumeSysActor);
}

////////////////////////////////////////////////////////////////////////////////
// WaitReady

void TStartVolumeActor::WaitReady(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Sending WaitReady to %s",
        LogTitle.GetWithTime().c_str(),
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
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Start volume actor, ignoring wait ready response, "
            "was not pending it, pending request is %s",
            LogTitle.GetWithTime().c_str(),
            FormatPendingRequest().c_str());
        return;
    }

    if (ev->Sender != VolumeUserActor) {
        // Ignore unexpected response
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Unexpected TEvWaitReadyResponse from %s (expected %s)",
            LogTitle.GetWithTime().c_str(),
            ToString(ev->Sender).c_str(),
            ToString(VolumeUserActor).c_str());
        return;
    }

    PendingRequest = EPendingRequest::NONE;

    if (Stopping) {
        ContinueShutdown(ctx);
        return;
    }

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s WaitReady request failed: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        StartShutdown(ctx, error);
        return;
    }

    // Notify service about tablet status
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Received WaitReady response",
        LogTitle.GetWithTime().c_str());

    const auto& volume = msg->Record.GetVolume();
    Y_ABORT_UNLESS(volume.GetDiskId() == DiskId);

    Ready = true;
    Volume = std::move(volume);

    SendVolumeTabletStatus(ctx);
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

void TStartVolumeActor::SendVolumeTabletStatus(const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(Ready);

    NCloud::Send<TEvServicePrivate::TEvVolumeTabletStatus>(
        ctx,
        SessionActorId,
        0,  // cookie
        VolumeTabletId,
        Volume,
        VolumeUserActor);
}

void TStartVolumeActor::SendVolumeTabletDeadErrorAndScheduleReboot(
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(!Ready);

    auto error = MakeError(E_REJECTED, TEvTablet::TEvTabletDead::Str(VolumeTabletDeadReason));

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Tablet failed: %s",
        LogTitle.GetWithTime().c_str(),
        FormatError(error).c_str());

    NCloud::Send<TEvServicePrivate::TEvVolumeTabletStatus>(
        ctx,
        SessionActorId,
        0,  // cookie
        error);

    bool delay = true;
    switch (VolumeTabletDeadReason) {
        case TEvTablet::TEvTabletDead::ReasonBootRace:
            // Avoid unnecessary delays
            delay = false;
            break;
        case TEvTablet::TEvTabletDead::ReasonBootSuggestOutdated:
            // Avoid unnecessary delays
            delay = false;
            ++VolumeGeneration;
            LogTitle.SetGeneration(VolumeGeneration);
            break;
        default:
            break;
    }

    ScheduleReboot(ctx, delay);
}

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
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Start volume actor: ignoring wakeup, was not pending it, "
            "pending request is %s",
            LogTitle.GetWithTime().c_str(),
            FormatPendingRequest().c_str());
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

        HFunc(
            TEvServicePrivate::TEvStartVolumeRequest,
            HandleStartVolumeRequest);

        HFunc(TEvHiveProxy::TEvLockTabletResponse, HandleLockTabletResponse);
        HFunc(TEvHiveProxy::TEvBootExternalResponse, HandleBootExternalResponse);
        HFunc(TEvHiveProxy::TEvTabletLockLost, HandleTabletLockLost);
        HFunc(TEvHiveProxy::TEvUnlockTabletResponse, HandleUnlockTabletResponse);

        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeVolumeResponse);

        HFunc(TEvTablet::TEvRestored, HandleTabletRestored);
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        IgnoreFunc(TEvTablet::TEvReady);

        HFunc(TEvVolume::TEvWaitReadyResponse, HandleWaitReadyResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
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
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Starting volume locally",
            LogTitle.GetWithTime().c_str());

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
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(response->GetError()).c_str());
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (VolumeInfo->State == TVolumeInfo::STARTED) {
        // If volume is already started then start volume actor should only
        // update hive lock.
        auto request = std::make_unique<TEvServicePrivate::TEvStartVolumeRequest>(
            TabletId);

        NCloud::Send(ctx, StartVolumeActor, std::move(request));
        return;
    }
}

void TVolumeSessionActor::HandleVolumeTabletStatus(
    const TEvServicePrivate::TEvVolumeTabletStatus::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (!msg->VolumeActor) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Volume start failed: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(msg->Error).c_str());

        VolumeInfo->VolumeActor = {};
        VolumeInfo->SetFailed(msg->Error);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Volume started",
        LogTitle.GetWithTime().c_str());

    VolumeInfo->SetStarted(msg->TabletId, msg->VolumeInfo, msg->VolumeActor);

    if (MountRequestActor) {
        auto response = std::make_unique<TEvServicePrivate::TEvStartVolumeResponse>(
            *VolumeInfo->VolumeInfo);
        NCloud::Send(ctx, MountRequestActor, std::move(response));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
