#include "shadow_disk_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString MakeShadowDiskClientId(
    const TString& sourceDiskClientId,
    bool readOnlyMount)
{
    if (readOnlyMount || sourceDiskClientId.empty()) {
        return TString(ShadowDiskClientId);
    }
    return sourceDiskClientId;
}

TString GetDeviceUUIDs(const TDevices& devices)
{
    TString result;
    for (const auto& device: devices) {
        if (result.empty()) {
            result += device.GetDeviceUUID();
        } else {
            result += ", " + device.GetDeviceUUID();
        }
    }
    return result;
}

bool CheckDeviceUUIDsIdentical(
    const TDevices& described,
    const TDevices& acquired)
{
    TSet<TString> acquiredDevices;
    for (const auto& d: acquired) {
        acquiredDevices.insert(d.GetDeviceUUID());
    }

    return AllOf(
        described,
        [&](const auto& d)
        { return acquiredDevices.contains(d.GetDeviceUUID()); });
}

///////////////////////////////////////////////////////////////////////////////

class TAcquireShadowDiskActor
    : public NActors::TActorBootstrapped<TAcquireShadowDiskActor>
{
private:
    const TString ShadowDiskId;
    const TShadowDiskActor::EAcquireReason AcquireReason;
    const bool ReadOnlyMount;
    const TDuration TotalTimeout;
    const TActorId ParentActor;
    const TString RwClientId;
    const ui64 MountSeqNumber = 0;
    const ui32 Generation = 0;

    TInstant AcquireStartedAt = {};
    // The list of devices received via the describe request.
    // This is necessary to check that all disk devices have been acquired.
    TDevices ShadowDiskDevices;
    TDevices AcquiredShadowDiskDevices;
    bool WaitingForDiskToBeReleased = false;

    // Delay provider when retrying describe and acquire requests to disk
    // registry.
    TBackoffDelayProvider RetryDelayProvider;

public:
    TAcquireShadowDiskActor(
        TStorageConfigPtr config,
        TString shadowDiskId,
        TDevices shadowDiskDevices,
        TShadowDiskActor::EAcquireReason acquireReason,
        bool readOnlyMount,
        bool areWritesToSourceBlocked,
        TString rwClientId,
        ui64 mountSeqNumber,
        ui32 generation,
        TActorId parentActor);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReleaseShadowDisk(const NActors::TActorContext& ctx, bool force);
    void DescribeShadowDisk(const NActors::TActorContext& ctx);
    void AcquireShadowDisk(const NActors::TActorContext& ctx);

    std::unique_ptr<TEvDiskRegistry::TEvDescribeDiskRequest>
    MakeDescribeDiskRequest() const;

    std::unique_ptr<TEvDiskRegistry::TEvAcquireDiskRequest>
    MakeAcquireDiskRequest() const;

    std::unique_ptr<TEvDiskRegistry::TEvReleaseDiskRequest>
    MakeReleaseDiskRequest() const;

    void HandleDiskRegistryError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error,
        std::unique_ptr<IEventHandle> retryEvent,
        const TString& actionName);

    void MaybeReady(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(Work);

    void HandleDescribeDiskResponse(
        const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAcquireDiskResponse(
        const TEvDiskRegistry::TEvAcquireDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReleaseDiskResponse(
        const TEvDiskRegistry::TEvReleaseDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

///////////////////////////////////////////////////////////////////////////////

TAcquireShadowDiskActor::TAcquireShadowDiskActor(
        TStorageConfigPtr config,
        TString shadowDiskId,
        TDevices shadowDiskDevices,
        TShadowDiskActor::EAcquireReason acquireReason,
        bool readOnlyMount,
        bool areWritesToSourceBlocked,
        TString rwClientId,
        ui64 mountSeqNumber,
        ui32 generation,
        TActorId parentActor)
    : ShadowDiskId(std::move(shadowDiskId))
    , AcquireReason(acquireReason)
    , ReadOnlyMount(readOnlyMount)
    , TotalTimeout(
          areWritesToSourceBlocked
              ? config->GetMaxAcquireShadowDiskTotalTimeoutWhenBlocked()
              : config->GetMaxAcquireShadowDiskTotalTimeoutWhenNonBlocked())
    , ParentActor(parentActor)
    , RwClientId(std::move(rwClientId))
    , MountSeqNumber(mountSeqNumber)
    , Generation(generation)
    , ShadowDiskDevices(std::move(shadowDiskDevices))
    , RetryDelayProvider(
          areWritesToSourceBlocked
              ? config->GetMinAcquireShadowDiskRetryDelayWhenBlocked()
              : config->GetMinAcquireShadowDiskRetryDelayWhenNonBlocked(),
          areWritesToSourceBlocked
              ? config->GetMaxAcquireShadowDiskRetryDelayWhenBlocked()
              : config->GetMaxAcquireShadowDiskRetryDelayWhenNonBlocked())
{
    if (AcquireReason != TShadowDiskActor::EAcquireReason::FirstAcquire) {
        STORAGE_CHECK_PRECONDITION(!ShadowDiskDevices.empty());
    }
}

void TAcquireShadowDiskActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::Work);

    AcquireStartedAt = ctx.Now();

    ReleaseShadowDisk(ctx, false);
    DescribeShadowDisk(ctx);
    AcquireShadowDisk(ctx);

    ctx.Schedule(TotalTimeout, new TEvents::TEvWakeup());
}

void TAcquireShadowDiskActor::ReleaseShadowDisk(
    const NActors::TActorContext& ctx,
    bool force)
{
    if (AcquireReason ==
            TShadowDiskActor::EAcquireReason::PeriodicalReAcquire &&
        !force)
    {
        WaitingForDiskToBeReleased = false;
        return;
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Releasing shadow disk " << ShadowDiskId.Quote());

    WaitingForDiskToBeReleased = true;
    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        MakeReleaseDiskRequest());
}

void TAcquireShadowDiskActor::DescribeShadowDisk(
    const NActors::TActorContext& ctx)
{
    if (!ShadowDiskDevices.empty()) {
        // We will not describe devices if this is not the first acquire and we
        // already know them.
        return;
    }
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Describing shadow disk " << ShadowDiskId.Quote());

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        MakeDescribeDiskRequest());
}

void TAcquireShadowDiskActor::AcquireShadowDisk(
    const NActors::TActorContext& ctx)
{
    if (WaitingForDiskToBeReleased) {
        return;
    }

    if (AcquireReason != TShadowDiskActor::EAcquireReason::PeriodicalReAcquire)
    {
        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Acquiring shadow disk " << ShadowDiskId.Quote() << " by clientId "
                                     << RwClientId.Quote() << " with timeout "
                                     << TotalTimeout.ToString() << " for "
                                     << (ReadOnlyMount ? "read" : "write"));
    }

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        MakeAcquireDiskRequest());
}

std::unique_ptr<TEvDiskRegistry::TEvDescribeDiskRequest>
TAcquireShadowDiskActor::MakeDescribeDiskRequest() const
{
    auto request = std::make_unique<TEvDiskRegistry::TEvDescribeDiskRequest>();
    request->Record.SetDiskId(ShadowDiskId);
    return request;
}

std::unique_ptr<TEvDiskRegistry::TEvAcquireDiskRequest>
TAcquireShadowDiskActor::MakeAcquireDiskRequest() const
{
    auto request = std::make_unique<TEvDiskRegistry::TEvAcquireDiskRequest>();
    request->Record.SetDiskId(ShadowDiskId);
    request->Record.MutableHeaders()->SetClientId(RwClientId);
    request->Record.SetAccessMode(
        ReadOnlyMount ? NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_ONLY
                      : NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_WRITE);
    request->Record.SetMountSeqNumber(MountSeqNumber);
    request->Record.SetVolumeGeneration(Generation);
    return request;
}

std::unique_ptr<TEvDiskRegistry::TEvReleaseDiskRequest>
TAcquireShadowDiskActor::MakeReleaseDiskRequest() const
{
    auto request = std::make_unique<TEvDiskRegistry::TEvReleaseDiskRequest>();

    request->Record.SetDiskId(ShadowDiskId);
    request->Record.MutableHeaders()->SetClientId(TString(AnyWriterClientId));
    request->Record.SetVolumeGeneration(Generation);
    return request;
}

void TAcquireShadowDiskActor::HandleDiskRegistryError(
    const NActors::TActorContext& ctx,
    const NProto::TError& error,
    std::unique_ptr<IEventHandle> retryEvent,
    const TString& actionName)
{
    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Can't " << actionName << " shadow disk " << ShadowDiskId.Quote()
                 << " Error: " << FormatError(error));

    const TInstant timeoutElapsedAt = AcquireStartedAt + TotalTimeout;

    // An ErrorSession can be received in response to a disk acquiring request.
    // In this case, we can try to acquire again, hoping that the old session
    // has ended.
    const bool retriableError =
        GetErrorKind(error) == EErrorKind::ErrorRetriable;
    const bool sessionError = GetErrorKind(error) == EErrorKind::ErrorSession;
    const bool canRetry =
        (retriableError || sessionError) && timeoutElapsedAt > ctx.Now();

    if (canRetry) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Will soon retry " << actionName << " shadow disk "
                               << ShadowDiskId.Quote()
                               << " Error: " << FormatError(error)
                               << " with delay " << RetryDelayProvider.GetDelay().ToString());

        if (sessionError) {
            ReleaseShadowDisk(ctx, true);
        }

        TActivationContext::Schedule(
            RetryDelayProvider.GetDelayAndIncrease(),
            std::move(retryEvent),
            nullptr);
    } else {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Will not retry " << actionName << " shadow disk "
                              << ShadowDiskId.Quote()
                              << " Error: " << FormatError(error));
        ReplyAndDie(ctx, error);
    }
}

void TAcquireShadowDiskActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvVolumePrivate::TEvShadowDiskAcquired>(error);
    if (!HasError(error)) {
        response->Devices.Swap(&ShadowDiskDevices);
        response->ClientId = RwClientId;
    }

    NCloud::Send(
        ctx,
        ParentActor,
        std::move(response),
        static_cast<ui64>(AcquireReason));

    Die(ctx);
}

STFUNC(TAcquireShadowDiskActor::Work)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvDescribeDiskResponse,
            HandleDescribeDiskResponse);
        HFunc(
            TEvDiskRegistry::TEvAcquireDiskResponse,
            HandleAcquireDiskResponse);
        HFunc(
            TEvDiskRegistry::TEvReleaseDiskResponse,
            HandleReleaseDiskResponse);
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TAcquireShadowDiskActor::HandleDescribeDiskResponse(
    const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;
    if (HasError(record.GetError())) {
        HandleDiskRegistryError(
            ctx,
            record.GetError(),
            std::make_unique<IEventHandle>(
                MakeDiskRegistryProxyServiceId(),
                ctx.SelfID,
                MakeDescribeDiskRequest().release()),
            "describe");
        return;
    }

    ShadowDiskDevices.Swap(record.MutableDevices());
    MaybeReady(ctx);
}

void TAcquireShadowDiskActor::HandleAcquireDiskResponse(
    const TEvDiskRegistry::TEvAcquireDiskResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;

    if (HasError(record.GetError())) {
        HandleDiskRegistryError(
            ctx,
            record.GetError(),
            std::make_unique<IEventHandle>(
                MakeDiskRegistryProxyServiceId(),
                ctx.SelfID,
                MakeAcquireDiskRequest().release()),
            "acquire");
        return;
    }

    AcquiredShadowDiskDevices.Swap(record.MutableDevices());
    MaybeReady(ctx);
}

void TAcquireShadowDiskActor::HandleReleaseDiskResponse(
    const TEvDiskRegistry::TEvReleaseDiskResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;

    if (HasError(record.GetError())) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Shadow disk " << ShadowDiskId.Quote() << " release error: "
                           << FormatError(record.GetError()));
    }

    WaitingForDiskToBeReleased = false;
    AcquireShadowDisk(ctx);
}

void TAcquireShadowDiskActor::HandleWakeup(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_ERROR_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Acquire timeout. Shadow disk " << ShadowDiskId.Quote());

    ReplyAndDie(ctx, MakeError(E_TIMEOUT));
}

void TAcquireShadowDiskActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    NCloud::Reply(ctx, *ev, std::make_unique<TEvents::TEvPoisonTaken>());
    ReplyAndDie(ctx, MakeError(E_REJECTED));
}

void TAcquireShadowDiskActor::MaybeReady(const NActors::TActorContext& ctx)
{
    bool gotDescribeAndAcquireResponses =
        !ShadowDiskDevices.empty() && !AcquiredShadowDiskDevices.empty();
    if (!gotDescribeAndAcquireResponses) {
        return;
    }

    // Check all shadow disk devices have been acquired.
    if (!CheckDeviceUUIDsIdentical(
            ShadowDiskDevices,
            AcquiredShadowDiskDevices))
    {
        HandleDiskRegistryError(
            ctx,
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "The acquired devices are not identical to described. "
                       "Described ["
                    << GetDeviceUUIDs(ShadowDiskDevices) << "] acquired ["
                    << GetDeviceUUIDs(AcquiredShadowDiskDevices) << "]"),
            std::make_unique<IEventHandle>(
                MakeDiskRegistryProxyServiceId(),
                ctx.SelfID,
                MakeAcquireDiskRequest().release(),
                0,   // flags
                static_cast<ui64>(
                    TShadowDiskActor::EAcquireReason::FirstAcquire)),
            "acquire");
        AcquiredShadowDiskDevices.Clear();
        return;
    };

    if (AcquireReason != TShadowDiskActor::EAcquireReason::PeriodicalReAcquire)
    {
        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Acquired shadow disk " << ShadowDiskId.Quote() << " by clientId "
                                    << RwClientId.Quote());
    }

    ReplyAndDie(ctx, MakeError(S_OK));
}

}   // namespace

///////////////////////////////////////////////////////////////////////////////

TShadowDiskActor::TShadowDiskActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticConfig,
        NRdma::IClientPtr rdmaClient,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TString sourceDiskClientId,
        ui64 mountSeqNumber,
        ui32 generation,
        TNonreplicatedPartitionConfigPtr srcConfig,
        TActorId volumeActorId,
        TActorId srcActorId,
        const TActiveCheckpointInfo& checkpointInfo)
    : TNonreplicatedPartitionMigrationCommonActor(
          static_cast<IMigrationOwner*>(this),
          config,
          std::move(diagnosticConfig),
          srcConfig->GetName(),
          srcConfig->GetBlockCount(),
          srcConfig->GetBlockSize(),
          std::move(profileLog),
          std::move(digestGenerator),
          checkpointInfo.ProcessedBlockCount,
          MakeShadowDiskClientId(
              sourceDiskClientId,
              checkpointInfo.ShadowDiskState == EShadowDiskState::Ready),
          volumeActorId,
          config->GetMaxShadowDiskFillIoDepth(),
          volumeActorId,
          EDirectCopyPolicy::CanUse)
    , RdmaClient(std::move(rdmaClient))
    , SrcConfig(std::move(srcConfig))
    , CheckpointId(checkpointInfo.CheckpointId)
    , ShadowDiskId(checkpointInfo.ShadowDiskId)
    , MountSeqNumber(mountSeqNumber)
    , Generation(generation)
    , VolumeActorId(volumeActorId)
    , SrcActorId(srcActorId)
    , SourceDiskClientId(std::move(sourceDiskClientId))
    , ProcessedBlockCount(checkpointInfo.ProcessedBlockCount)
{
    STORAGE_CHECK_PRECONDITION(
        checkpointInfo.Data == ECheckpointData::DataPresent);

    switch (checkpointInfo.ShadowDiskState) {
        case EShadowDiskState::None:
            STORAGE_CHECK_PRECONDITION(
                checkpointInfo.ShadowDiskState != EShadowDiskState::None);
            break;
        case EShadowDiskState::New:
            State = EActorState::WaitAcquireForPrepareStart;
            break;
        case EShadowDiskState::Preparing:
            State = checkpointInfo.ProcessedBlockCount == 0
                        ? EActorState::WaitAcquireForPrepareStart
                        : EActorState::WaitAcquireForPrepareContinue;
            break;
        case EShadowDiskState::Ready:
            State = EActorState::WaitAcquireForRead;
            break;
        case EShadowDiskState::Error:
            State = EActorState::Error;
            break;
    }
}

TShadowDiskActor::~TShadowDiskActor() = default;

void TShadowDiskActor::OnBootstrap(const NActors::TActorContext& ctx)
{
    PoisonPillHelper.TakeOwnership(ctx, SrcActorId);
    AcquireShadowDisk(ctx, EAcquireReason::FirstAcquire);
}

bool TShadowDiskActor::OnMessage(
    const TActorContext& ctx,
    TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolumePrivate::TEvShadowDiskAcquired,
            HandleShadowDiskAcquired);

        HFunc(TEvVolume::TEvReacquireDisk, HandleReacquireDisk);
        HFunc(TEvVolume::TEvRdmaUnavailable, HandleRdmaUnavailable);
        HFunc(
            TEvVolumePrivate::TEvUpdateShadowDiskStateResponse,
            HandleUpdateShadowDiskStateResponse);
        HFunc(TEvService::TEvGetChangedBlocksRequest, HandleGetChangedBlocks);

        // Read request.
        HFunc(
            TEvService::TEvReadBlocksRequest,
            HandleReadBlocks<TEvService::TReadBlocksMethod>);
        HFunc(
            TEvService::TEvReadBlocksLocalRequest,
            HandleReadBlocks<TEvService::TReadBlocksLocalMethod>);

        IgnoreFunc(TEvVolumePrivate::TEvDeviceTimedOutRequest);

        // Write/zero request.
        case TEvService::TEvWriteBlocksRequest::EventType: {
            return HandleWriteZeroBlocks<TEvService::TWriteBlocksMethod>(
                *reinterpret_cast<TEvService::TEvWriteBlocksRequest::TPtr*>(
                    &ev),
                ctx);
        }
        case TEvService::TEvWriteBlocksLocalRequest::EventType: {
            return HandleWriteZeroBlocks<TEvService::TWriteBlocksLocalMethod>(
                *reinterpret_cast<
                    TEvService::TEvWriteBlocksLocalRequest::TPtr*>(&ev),
                ctx);
        }
        case TEvService::TEvZeroBlocksRequest::EventType: {
            return HandleWriteZeroBlocks<TEvService::TZeroBlocksMethod>(
                *reinterpret_cast<TEvService::TEvZeroBlocksRequest::TPtr*>(&ev),
                ctx);
        }

        // ClientId changed event.
        case TEvVolume::TEvRWClientIdChanged::EventType: {
            return HandleRWClientIdChanged(
                *reinterpret_cast<TEvVolume::TEvRWClientIdChanged::TPtr*>(&ev),
                ctx);
        }

        case NActors::TEvents::TEvWakeup::EventType: {
            return HandleWakeup(
                *reinterpret_cast<NActors::TEvents::TEvWakeup::TPtr*>(&ev),
                ctx);
        }

        default:
            // Message processing by the base class is required.
            return false;
    }

    // We get here if we have processed an incoming message. And its processing
    // by the base class is not required.
    return true;
}

void TShadowDiskActor::OnMigrationProgress(
    const NActors::TActorContext& ctx,
    ui64 migrationIndex)
{
    using EReason = TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

    ProcessedBlockCount = migrationIndex;

    auto request =
        std::make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
            CheckpointId,
            EReason::FillProgressUpdate,
            migrationIndex);

    NCloud::Send(ctx, VolumeActorId, std::move(request));
}

void TShadowDiskActor::OnMigrationFinished(const NActors::TActorContext& ctx)
{
    using EReason = TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

    ProcessedBlockCount = SrcConfig->GetBlockCount();

    auto request =
        std::make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
            CheckpointId,
            EReason::FillCompleted,
            ProcessedBlockCount);

    NCloud::Send(ctx, VolumeActorId, std::move(request));
}

void TShadowDiskActor::OnMigrationError(const NActors::TActorContext& ctx)
{
    using EReason = TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

    auto request =
        std::make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
            CheckpointId,
            EReason::FillError,
            ProcessedBlockCount);

    NCloud::Send(ctx, VolumeActorId, std::move(request));
}

void TShadowDiskActor::AcquireShadowDisk(
    const NActors::TActorContext& ctx,
    EAcquireReason acquireReason)
{
    if (State == EActorState::Error) {
        return;
    }

    switch (acquireReason) {
        case EAcquireReason::FirstAcquire: {
            STORAGE_CHECK_PRECONDITION(WaitingForAcquire());
            STORAGE_CHECK_PRECONDITION(DstActorId == TActorId());
            STORAGE_CHECK_PRECONDITION(AcquireActorId == TActorId());
        } break;
        case EAcquireReason::PeriodicalReAcquire: {
            STORAGE_CHECK_PRECONDITION(!WaitingForAcquire());
            STORAGE_CHECK_PRECONDITION(DstActorId != TActorId());

            if (AcquireActorId != TActorId()) {
                return;
            }
        } break;
        case EAcquireReason::ForcedReAcquire: {
            STORAGE_CHECK_PRECONDITION(!WaitingForAcquire());
            STORAGE_CHECK_PRECONDITION(DstActorId != TActorId());

            if (ForcedReAcquireInProgress) {
                return;
            }
            ForcedReAcquireInProgress = true;
        } break;
    }

    CurrentShadowDiskClientId =
        MakeShadowDiskClientId(SourceDiskClientId, ReadOnlyMount());
    AcquireActorId = NCloud::Register(
        ctx,
        std::make_unique<TAcquireShadowDiskActor>(
            GetConfig(),
            ShadowDiskId,
            ShadowDiskDevices,
            acquireReason,
            ReadOnlyMount(),
            AreWritesToSrcDiskImpossible(),
            CurrentShadowDiskClientId,
            MountSeqNumber,
            Generation,
            SelfId()));
    PoisonPillHelper.TakeOwnership(ctx, AcquireActorId);
}

void TShadowDiskActor::HandleShadowDiskAcquired(
    const TEvVolumePrivate::TEvShadowDiskAcquired::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    PoisonPillHelper.ReleaseOwnership(ctx, ev->Sender);
    if (AcquireActorId == ev->Sender) {
        AcquireActorId = {};
    }

    auto* msg = ev->Get();
    auto acquireReason = static_cast<EAcquireReason>(ev->Cookie);

    if (acquireReason == EAcquireReason::ForcedReAcquire) {
        ForcedReAcquireInProgress = false;
    }

    if (HasError(msg->Error)) {
        if (msg->Error.GetCode() == E_NOT_FOUND ||
            acquireReason != EAcquireReason::PeriodicalReAcquire)
        {
            SetErrorState(ctx);
        }
        return;
    }

    if (acquireReason == EAcquireReason::FirstAcquire) {
        CreateShadowDiskPartitionActor(ctx, msg->Devices);
    }
}

void TShadowDiskActor::CreateShadowDiskConfig()
{
    STORAGE_CHECK_PRECONDITION(!ShadowDiskDevices.empty());

    TNonreplicatedPartitionConfig::TVolumeInfo volumeInfo{
        .CreationTs = TInstant(),
        .MediaKind =
            GetCheckpointShadowDiskType(SrcConfig->GetVolumeInfo().MediaKind),
        .EncryptionMode = SrcConfig->GetVolumeInfo().EncryptionMode};

    TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfigInitParams
        params{
            ShadowDiskDevices,
            volumeInfo,
            ShadowDiskId,
            SrcConfig->GetBlockSize(),
            SelfId(),   // need to handle TEvRdmaUnavailable, TEvReacquireDisk
        };
    params.MuteIOErrors = true;
    params.IOMode = ReadOnlyMount() ? NProto::VOLUME_IO_ERROR_READ_ONLY
                                    : NProto::VOLUME_IO_OK;

    DstConfig =
        std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));
}

void TShadowDiskActor::CreateShadowDiskPartitionActor(
    const NActors::TActorContext& ctx,
    const TDevices& acquiredShadowDiskDevices)
{
    using EReason = TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

    STORAGE_CHECK_PRECONDITION(WaitingForAcquire());
    STORAGE_CHECK_PRECONDITION(DstActorId == TActorId());

    ShadowDiskDevices = acquiredShadowDiskDevices;

    CreateShadowDiskConfig();

    DstActorId = NCloud::Register(
        ctx,
        CreateNonreplicatedPartition(
            GetConfig(),
            GetDiagnosticsConfig(),
            DstConfig,
            VolumeActorId,   // send stat to volume directly.
            RdmaClient));
    PoisonPillHelper.TakeOwnership(ctx, DstActorId);

    if (State == EActorState::WaitAcquireForRead) {
        // Ready to serve checkpoint reads.
        State = EActorState::CheckpointReady;
    } else {
        STORAGE_CHECK_PRECONDITION(
            State == EActorState::WaitAcquireForPrepareStart ||
            State == EActorState::WaitAcquireForPrepareContinue);

        // Ready to fill shadow disk with data.
        State = EActorState::Preparing;
        InitWork(
            ctx,
            SrcActorId,
            SrcActorId,
            DstActorId,
            true,   // takeOwnershipOverActors
            std::make_unique<TMigrationTimeoutCalculator>(
                GetConfig()->GetMaxShadowDiskFillBandwidth(),
                GetConfig()->GetExpectedDiskAgentSize(),
                DstConfig));
        StartWork(ctx);

        // Persist state.
        NCloud::Send(
            ctx,
            VolumeActorId,
            std::make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
                CheckpointId,
                EReason::FillProgressUpdate,
                ProcessedBlockCount));
    }

    STORAGE_CHECK_PRECONDITION(Acquired());
    SchedulePeriodicalReAcquire(ctx);
}

void TShadowDiskActor::SchedulePeriodicalReAcquire(const TActorContext& ctx)
{
    ctx.Schedule(
        GetConfig()->GetClientRemountPeriod(),
        new TEvents::TEvWakeup(EShadowDiskWakeupReason::SDWR_REACQUIRE));
}

void TShadowDiskActor::SetErrorState(const NActors::TActorContext& ctx)
{
    using EReason = TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

    State = EActorState::Error;

    // Persist state.
    NCloud::Send(
        ctx,
        VolumeActorId,
        std::make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
            CheckpointId,
            EReason::FillError,
            ProcessedBlockCount));
}

bool TShadowDiskActor::CanJustForwardWritesToSrcDisk() const
{
    return State == EActorState::CheckpointReady ||
           State == EActorState::Error ||
           State == EActorState::WaitAcquireForPrepareStart;
}

bool TShadowDiskActor::AreWritesToSrcDiskForbidden() const
{
    return State == EActorState::WaitAcquireForPrepareContinue;
}

bool TShadowDiskActor::AreWritesToSrcDiskImpossible() const
{
    return AreWritesToSrcDiskForbidden() ||
           (ForcedReAcquireInProgress && State == EActorState::Preparing);
}

bool TShadowDiskActor::WaitingForAcquire() const
{
    return State == EActorState::WaitAcquireForPrepareStart ||
           State == EActorState::WaitAcquireForPrepareContinue ||
           State == EActorState::WaitAcquireForRead;
}

bool TShadowDiskActor::Acquired() const
{
    return State == EActorState::Preparing ||
           State == EActorState::CheckpointReady;
}

bool TShadowDiskActor::ReadOnlyMount() const
{
    STORAGE_CHECK_PRECONDITION(State != EActorState::Error);

    return State == EActorState::WaitAcquireForRead ||
           State == EActorState::CheckpointReady;
}

template <typename TMethod>
void TShadowDiskActor::HandleReadBlocks(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;
    const auto& checkpointId = record.GetCheckpointId();

    if (checkpointId.empty() || checkpointId != CheckpointId) {
        // Forward read request to Source partition.
        ForwardRequestToSrcPartition<TMethod>(ev, ctx);
        return;
    }

    if (State != EActorState::CheckpointReady) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<typename TMethod::TResponse>(MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Can't read from checkpoint " << CheckpointId.Quote()
                    << " while the data is being filled in.")));
        return;
    }

    // Remove checkpointId and read checkpoint data from shadow disk.
    record.SetCheckpointId(TString());
    record.MutableHeaders()->SetClientId(CurrentShadowDiskClientId);
    ForwardRequestToShadowPartition<TMethod>(ev, ctx);
}

template <typename TMethod>
bool TShadowDiskActor::HandleWriteZeroBlocks(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (CanJustForwardWritesToSrcDisk()) {
        ForwardRequestToSrcPartition<TMethod>(ev, ctx);
        return true;
    }

    if (AreWritesToSrcDiskForbidden()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<typename TMethod::TResponse>(MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Can't write to source disk while shadow disk "
                    << ShadowDiskId.Quote() << " not ready yet.")));
        return true;
    }

    // Migration is currently in progress. It is necessary to forward write/zero
    // requests to the source and shadow disk with the base class
    // TNonreplicatedPartitionMigrationCommonActor
    return false;
}

template <typename TMethod>
void TShadowDiskActor::ForwardRequestToSrcPartition(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, SrcActorId);
}

template <typename TMethod>
void TShadowDiskActor::ForwardRequestToShadowPartition(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (DstActorId == NActors::TActorId()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<typename TMethod::TResponse>(
                MakeError(E_REJECTED, "shadow disk partition not ready yet")));
        return;
    }

    if (State != EActorState::CheckpointReady) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<typename TMethod::TResponse>(
                MakeError(E_REJECTED, "shadow disk fill in progress")));
        return;
    }

    ForwardMessageToActor(ev, ctx, DstActorId);
}

void TShadowDiskActor::HandleUpdateShadowDiskStateResponse(
    const TEvVolumePrivate::TEvUpdateShadowDiskStateResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    switch (msg->NewState) {
        case EShadowDiskState::None:
        case EShadowDiskState::New: {
            STORAGE_CHECK_PRECONDITION(
                msg->NewState != EShadowDiskState::New &&
                msg->NewState != EShadowDiskState::None);
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::VOLUME,
                "State of shadow disk " << ShadowDiskId.Quote()
                                        << " unexpectedly changed to "
                                        << ToString(msg->NewState));
            State = EActorState::Error;
        } break;
        case EShadowDiskState::Preparing: {
            LOG_INFO_S(
                ctx,
                TBlockStoreComponents::VOLUME,
                "State of shadow disk "
                    << ShadowDiskId.Quote() << " changed to "
                    << ToString(msg->NewState)
                    << ", processed block count: " << msg->ProcessedBlockCount);
            if (State != EActorState::Preparing) {
                State = EActorState::Preparing;
            }
            STORAGE_CHECK_PRECONDITION(Acquired());
        } break;
        case EShadowDiskState::Ready: {
            LOG_INFO_S(
                ctx,
                TBlockStoreComponents::VOLUME,
                "State of shadow disk " << ShadowDiskId.Quote()
                                        << " changed to "
                                        << ToString(msg->NewState));
            State = EActorState::CheckpointReady;

            if (CurrentShadowDiskClientId !=
                MakeShadowDiskClientId(SourceDiskClientId, ReadOnlyMount()))
            {
                // Need to reacquire with ShadowDiskClientId.
                AcquireShadowDisk(ctx, EAcquireReason::ForcedReAcquire);
            }
            STORAGE_CHECK_PRECONDITION(Acquired());
        } break;
        case EShadowDiskState::Error: {
            LOG_WARN_S(
                ctx,
                TBlockStoreComponents::VOLUME,
                "State of shadow disk " << ShadowDiskId.Quote()
                                        << " changed to "
                                        << ToString(msg->NewState));
            State = EActorState::Error;
        } break;
    }
}

bool TShadowDiskActor::HandleWakeup(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    switch (ev->Get()->Tag) {
        case EShadowDiskWakeupReason::SDWR_REACQUIRE:
            AcquireShadowDisk(ctx, EAcquireReason::PeriodicalReAcquire);
            SchedulePeriodicalReAcquire(ctx);
            return true;
    }

    return false;
}

void TShadowDiskActor::HandleRdmaUnavailable(
    const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, VolumeActorId);
}

void TShadowDiskActor::HandleReacquireDisk(
    const TEvVolume::TEvReacquireDisk::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    // If an E_BS_INVALID_SESSION error occurred while working with the shadow
    // disk, the shadow disk partition sends a TEvReacquireDisk message. In this
    // case, we try to acquire shadow disk again.

    // If we are in prepare state, this means that writing errors to the
    // shadow disk will lead to blocking of the source disk. In that case, we
    // are trying to reacquire during short period MaxBlockingTotalTimeout.

    // If we are in ready state, this means that writing errors to the
    // shadow disk will not lead to blocking of the source disk. In that case,
    // we are trying to reacquire during long period MaxNonBlockingTotalTimeout.

    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Got TEvReacquireDisk for shadow disk " << ShadowDiskId.Quote());

    switch (State) {
        case EActorState::WaitAcquireForPrepareStart:
        case EActorState::WaitAcquireForPrepareContinue:
        case EActorState::WaitAcquireForRead:
        case EActorState::Error:
            break;
        case EActorState::Preparing:
        case EActorState::CheckpointReady: {
            AcquireShadowDisk(ctx, EAcquireReason::ForcedReAcquire);
        } break;
    }
}

bool TShadowDiskActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Changed clientId for disk "
            << SrcConfig->GetName().Quote() << ", shadow disk "
            << ShadowDiskId.Quote() << " from " << SourceDiskClientId.Quote()
            << " to " << ev->Get()->RWClientId);

    SourceDiskClientId = ev->Get()->RWClientId;

    // Notify the source partition about the new clientId.
    NCloud::Send(
        ctx,
        SrcActorId,
        std::make_unique<TEvVolume::TEvRWClientIdChanged>(SourceDiskClientId));

    // Somehow we got into an error state. There is no need to reacquire the disk.
    if (State == EActorState::Error) {
        return true;
    }

    // Reacquire shadow disk with new clientId.
    const bool clientChanged =
        CurrentShadowDiskClientId !=
        MakeShadowDiskClientId(SourceDiskClientId, ReadOnlyMount());
    if (clientChanged || !WaitingForAcquire()) {
        AcquireShadowDisk(ctx, EAcquireReason::ForcedReAcquire);
    }

    // It is necessary to handle the EvRWClientIdChanged message in the base
    // class TNonreplicatedPartitionMigrationCommonActor too.
    ev->Get()->RWClientId =
        MakeShadowDiskClientId(SourceDiskClientId, ReadOnlyMount());
    return false;
}

void TShadowDiskActor::HandleGetChangedBlocks(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->Record.GetHighCheckpointId() != CheckpointId) {
        ForwardMessageToActor(ev, ctx, SrcActorId);
        return;
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "GetChangedBlocks for shadow disk " << ShadowDiskId.Quote());

    if (State != EActorState::CheckpointReady) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Shadow disk: "
                << ShadowDiskId.Quote()
                << " Can't GetChangedBlocks when shadow disk is not ready.");

        auto response =
            std::make_unique<TEvService::TEvGetChangedBlocksResponse>();

        if (State == EActorState::Error) {
            *response->Record.MutableError() = MakeError(
                E_INVALID_STATE,
                "Can't GetChangedBlocks when shadow disk is broken.");
        } else {
            *response->Record.MutableError() = MakeError(
                E_REJECTED,
                "Can't GetChangedBlocks when shadow disk is not ready.");
        }

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>();
    auto range = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());
    response->Record.SetMask(GetNonZeroBlocks(range));

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
