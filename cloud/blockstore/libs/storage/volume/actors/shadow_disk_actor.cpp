#include "shadow_disk_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TEvent>
void ForwardMessageToActor(
    TEvent& ev,
    const NActors::TActorContext& ctx,
    TActorId destActor)
{
    NActors::TActorId nondeliveryActor = ev->GetForwardOnNondeliveryRecipient();
    auto message = std::make_unique<IEventHandle>(
        destActor,
        ev->Sender,
        ev->ReleaseBase().Release(),
        ev->Flags,
        ev->Cookie,
        ev->Flags & NActors::IEventHandle::FlagForwardOnNondelivery
            ? &nondeliveryActor
            : nullptr);
    ctx.Send(std::move(message));
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
    if (described.size() != acquired.size()) {
        return false;
    }

    for (int i = 0; i < described.size(); ++i) {
        if (described[i].GetDeviceUUID() != acquired[i].GetDeviceUUID()) {
            return false;
        }
    }
    return true;
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
    const ui64 MountSeqNumber = 0;
    const ui32 Generation = 0;
    const TString ShadowDiskClientId = "shadow-disk-client-id";

    TInstant AcquireStartedAt = {};
    // The list of devices received via the describe request.
    // This is necessary to check that all disk devices have been acquired.
    TDevices ShadowDiskDevices;
    TDevices AcquiredShadowDiskDevices;

    // Delay provider when retrying describe and acquire requests to disk
    // registry.
    TBackoffDelayProvider RetryDelayProvider;

public:
    TAcquireShadowDiskActor(
        const TStorageConfigPtr config,
        TString shadowDiskId,
        const TDevices& shadowDiskDevices,
        TShadowDiskActor::EAcquireReason acquireReason,
        bool readOlyMount,
        bool isWritesToSourceBlocked,
        ui64 mountSeqNumber,
        ui32 generation,
        TActorId parentActor);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeShadowDisk(const NActors::TActorContext& ctx);
    void AcquireShadowDisk(const NActors::TActorContext& ctx);

    std::unique_ptr<TEvDiskRegistry::TEvDescribeDiskRequest>
    MakeDescribeDiskRequest() const;
    std::unique_ptr<TEvDiskRegistry::TEvAcquireDiskRequest>
    MakeAcquireDiskRequest() const;

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

    void HandleDescribeDiskRequestUndelivery(
        const TEvDiskRegistry::TEvDescribeDiskRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAcquireDiskRequestUndelivery(
        const TEvDiskRegistry::TEvAcquireDiskRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

///////////////////////////////////////////////////////////////////////////////

TAcquireShadowDiskActor::TAcquireShadowDiskActor(
        const TStorageConfigPtr config,
        TString shadowDiskId,
        const TDevices& shadowDiskDevices,
        TShadowDiskActor::EAcquireReason acquireReason,
        bool readOnlyMount,
        bool isWritesToSourceBlocked,
        ui64 mountSeqNumber,
        ui32 generation,
        TActorId parentActor)
    : ShadowDiskId(std::move(shadowDiskId))
    , AcquireReason(acquireReason)
    , ReadOnlyMount(readOnlyMount)
    , TotalTimeout(
          isWritesToSourceBlocked
              ? config->GetMaxAcquireShadowDiskTotalTimeoutWhenBlocked()
              : config->GetMaxAcquireShadowDiskTotalTimeoutWhenNonBlocked())
    , ParentActor(parentActor)
    , MountSeqNumber(mountSeqNumber)
    , Generation(generation)
    , ShadowDiskDevices(shadowDiskDevices)
    , RetryDelayProvider(
          isWritesToSourceBlocked
              ? config->GetMinAcquireShadowDiskRetryDelayWhenBlocked()
              : config->GetMinAcquireShadowDiskRetryDelayWhenNonBlocked(),
          isWritesToSourceBlocked
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

    DescribeShadowDisk(ctx);
    AcquireShadowDisk(ctx);

    AcquireStartedAt = ctx.Now();
    ctx.Schedule(TotalTimeout, new TEvents::TEvWakeup());
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

    NCloud::SendWithUndeliveryTracking(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        MakeDescribeDiskRequest());
}

void TAcquireShadowDiskActor::AcquireShadowDisk(
    const NActors::TActorContext& ctx)
{
    if (AcquireReason != TShadowDiskActor::EAcquireReason::PeriodicalReAcquire)
    {
        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Acquiring shadow disk " << ShadowDiskId.Quote() << " with timeout "
                                     << TotalTimeout.ToString());
    }

    NCloud::SendWithUndeliveryTracking(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        MakeAcquireDiskRequest());
}

auto TAcquireShadowDiskActor::MakeDescribeDiskRequest() const
    -> std::unique_ptr<TEvDiskRegistry::TEvDescribeDiskRequest>
{
    auto request = std::make_unique<TEvDiskRegistry::TEvDescribeDiskRequest>();
    request->Record.SetDiskId(ShadowDiskId);
    return request;
}

auto TAcquireShadowDiskActor::MakeAcquireDiskRequest() const
    -> std::unique_ptr<TEvDiskRegistry::TEvAcquireDiskRequest>
{
    auto request = std::make_unique<TEvDiskRegistry::TEvAcquireDiskRequest>();
    request->Record.SetDiskId(ShadowDiskId);
    request->Record.MutableHeaders()->SetClientId(ShadowDiskClientId);
    request->Record.SetAccessMode(
        ReadOnlyMount ? NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_ONLY
                      : NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_WRITE);
    request->Record.SetMountSeqNumber(MountSeqNumber);
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

    const bool canRetry = GetErrorKind(error) == EErrorKind::ErrorRetriable &&
                          timeoutElapsedAt > ctx.Now();

    if (canRetry) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Will soon retry " << actionName << " shadow disk "
                               << ShadowDiskId.Quote()
                               << " Error: " << FormatError(error)
                               << " with delay " << RetryDelayProvider.GetDelay().ToString());

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
        std::make_unique<TEvDiskRegistry::TEvAcquireDiskResponse>(error);
    if (!HasError(response->GetError())) {
        response->Record.MutableDevices()->Swap(&AcquiredShadowDiskDevices);
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
            TEvDiskRegistry::TEvDescribeDiskRequest,
            HandleDescribeDiskRequestUndelivery);
        HFunc(
            TEvDiskRegistry::TEvAcquireDiskRequest,
            HandleAcquireDiskRequestUndelivery);
        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
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

void TAcquireShadowDiskActor::HandleDescribeDiskRequestUndelivery(
    const TEvDiskRegistry::TEvDescribeDiskRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    DescribeShadowDisk(ctx);
}

void TAcquireShadowDiskActor::HandleAcquireDiskRequestUndelivery(
    const TEvDiskRegistry::TEvAcquireDiskRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
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
            "Acquired shadow disk " << ShadowDiskId.Quote());
    }

    ReplyAndDie(ctx, MakeError(S_OK));
}

}   // namespace

///////////////////////////////////////////////////////////////////////////////

TShadowDiskActor::TShadowDiskActor(
        TStorageConfigPtr config,
        NRdma::IClientPtr rdmaClient,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TString rwClientId,
        ui64 mountSeqNumber,
        ui32 generation,
        TNonreplicatedPartitionConfigPtr srcConfig,
        TActorId volumeActorId,
        TActorId srcActorId,
        const TActiveCheckpointInfo& checkpointInfo)
    : TNonreplicatedPartitionMigrationCommonActor(
          static_cast<IMigrationOwner*>(this),
          config,
          srcConfig->GetName(),
          srcConfig->GetBlockCount(),
          srcConfig->GetBlockSize(),
          std::move(profileLog),
          std::move(digestGenerator),
          checkpointInfo.ProcessedBlockCount,
          std::move(rwClientId),
          volumeActorId)
    , Config(std::move(config))
    , RdmaClient(std::move(rdmaClient))
    , SrcConfig(std::move(srcConfig))
    , CheckpointId(checkpointInfo.CheckpointId)
    , ShadowDiskId(checkpointInfo.ShadowDiskId)
    , MountSeqNumber(mountSeqNumber)
    , Generation(generation)
    , VolumeActorId(volumeActorId)
    , SrcActorId(srcActorId)
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
            TEvDiskRegistry::TEvAcquireDiskResponse,
            HandleAcquireDiskResponse);
        HFunc(
            TEvNonreplPartitionPrivate::TEvUpdateCounters,
            HandleUpdateCounters);
        HFunc(
            TEvVolume::TEvDiskRegistryBasedPartitionCounters,
            HandleShadowDiskCounters);
        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvVolume::TEvReacquireDisk, HandleReacquireDisk);
        HFunc(TEvVolume::TEvRdmaUnavailable, HandleRdmaUnavailable);
        HFunc(
            TEvVolumePrivate::TEvUpdateShadowDiskStateResponse,
            HandleUpdateShadowDiskStateResponse);

        // Read request.
        HFunc(
            TEvService::TEvReadBlocksRequest,
            HandleReadBlocks<TEvService::TReadBlocksMethod>);
        HFunc(
            TEvService::TEvReadBlocksLocalRequest,
            HandleReadBlocks<TEvService::TReadBlocksLocalMethod>);

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

        default:
            // Message processing by the base class is required.
            return false;
    }

    // We get here if we have processed an incoming message. And its processing
    // by the base class is not required.
    return true;
}

TDuration TShadowDiskActor::CalculateMigrationTimeout()
{
    STORAGE_CHECK_PRECONDITION(TimeoutCalculator);

    if (TimeoutCalculator) {
        return TimeoutCalculator->CalculateTimeout(GetNextProcessingRange());
    }
    return TDuration::Seconds(1);
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
            migrationIndex,
            SrcConfig->GetBlockCount());

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
            ProcessedBlockCount,
            SrcConfig->GetBlockCount());

    NCloud::Send(ctx, VolumeActorId, std::move(request));
}

void TShadowDiskActor::OnMigrationError(const NActors::TActorContext& ctx)
{
    using EReason = TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

    auto request =
        std::make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
            CheckpointId,
            EReason::FillError,
            ProcessedBlockCount,
            SrcConfig->GetBlockCount());

    NCloud::Send(ctx, VolumeActorId, std::move(request));
}

void TShadowDiskActor::AcquireShadowDisk(
    const NActors::TActorContext& ctx,
    EAcquireReason acquireReason)
{
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

    AcquireActorId = NCloud::Register(
        ctx,
        std::make_unique<TAcquireShadowDiskActor>(
            Config,
            ShadowDiskId,
            ShadowDiskDevices,
            acquireReason,
            ReadOnlyMount(),
            AreWritesToSrcDiskImpossible(),
            MountSeqNumber,
            Generation,
            SelfId()));

    PoisonPillHelper.TakeOwnership(ctx, AcquireActorId);
}

void TShadowDiskActor::HandleAcquireDiskResponse(
    const TEvDiskRegistry::TEvAcquireDiskResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    PoisonPillHelper.ReleaseOwnership(ctx, ev->Sender);
    if (AcquireActorId == ev->Sender) {
        AcquireActorId = {};
    }

    auto* msg = ev->Get();
    auto& record = msg->Record;
    auto acquireReason = static_cast<EAcquireReason>(ev->Cookie);

    if (acquireReason == EAcquireReason::ForcedReAcquire) {
        ForcedReAcquireInProgress = false;
    }

    if (HasError(record.GetError())) {
        if (acquireReason != EAcquireReason::PeriodicalReAcquire) {
            SetErrorState(ctx);
        }
        return;
    }

    if (acquireReason == EAcquireReason::FirstAcquire) {
        CreateShadowDiskPartitionActor(ctx, record.GetDevices());
    }
}

void TShadowDiskActor::CreateShadowDiskConfig()
{
    STORAGE_CHECK_PRECONDITION(!ShadowDiskDevices.empty());

    TNonreplicatedPartitionConfig::TVolumeInfo volumeInfo{
        TInstant(),
        GetCheckpointShadowDiskType(SrcConfig->GetVolumeInfo().MediaKind)};

    DstConfig = std::make_shared<TNonreplicatedPartitionConfig>(
        ShadowDiskDevices,
        ReadOnlyMount() ? NProto::VOLUME_IO_ERROR_READ_ONLY
                        : NProto::VOLUME_IO_OK,
        ShadowDiskId,
        SrcConfig->GetBlockSize(),
        volumeInfo,
        SelfId(),   // need to handle TEvRdmaUnavailable, TEvReacquireDisk
        true,       // muteIOErrors
        false,      // markBlocksUsed
        THashSet<TString>(),   // freshDeviceIds
        TDuration(),           // maxTimedOutDeviceStateDuration
        false,                 // maxTimedOutDeviceStateDurationOverridden
        true                   // useSimpleMigrationBandwidthLimiter
    );

    TimeoutCalculator.emplace(
        Config->GetMaxShadowDiskFillBandwidth(),
        Config->GetExpectedDiskAgentSize(),
        DstConfig);
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
            Config,
            DstConfig,
            SelfId(),
            RdmaClient));
    PoisonPillHelper.TakeOwnership(ctx, DstActorId);

    if (State == EActorState::WaitAcquireForRead) {
        STORAGE_CHECK_PRECONDITION(Acquired());

        // Ready to serve checkpoint reads.
        State = EActorState::CheckpointReady;
    } else {
        STORAGE_CHECK_PRECONDITION(
            State == EActorState::WaitAcquireForPrepareStart ||
            State == EActorState::WaitAcquireForPrepareContinue);

        // Ready to fill shadow disk with data.
        State = EActorState::Preparing;
        STORAGE_CHECK_PRECONDITION(Acquired());

        TNonreplicatedPartitionMigrationCommonActor::InitWork(
            ctx,
            SrcActorId,
            DstActorId);
        TNonreplicatedPartitionMigrationCommonActor::StartWork(ctx);

        // Persist state.
        NCloud::Send(
            ctx,
            VolumeActorId,
            std::make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
                CheckpointId,
                EReason::FillProgressUpdate,
                GetNextProcessingRange().Start,
                SrcConfig->GetBlockCount()));
    }

    SchedulePeriodicalReAcquire(ctx);
}

void TShadowDiskActor::SchedulePeriodicalReAcquire(const TActorContext& ctx)
{
    ctx.Schedule(Config->GetClientRemountPeriod(), new TEvents::TEvWakeup());
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
            ProcessedBlockCount,
            SrcConfig->GetBlockCount()));
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
    return AreWritesToSrcDiskForbidden() || ForcedReAcquireInProgress;
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
            State = EActorState::Preparing;
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

void TShadowDiskActor::HandleUpdateCounters(
    const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);

    // Block sending statistics counters from the base class by processing the
    // TEvUpdateCounters message ourselves.
}

void TShadowDiskActor::HandleShadowDiskCounters(
    const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);

    // TODO. Do we need to count the statistics of the shadow disk in the source
    // disk?
}

void TShadowDiskActor::HandleWakeup(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    AcquireShadowDisk(ctx, EAcquireReason::PeriodicalReAcquire);
    SchedulePeriodicalReAcquire(ctx);
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
        case EActorState::Error: {
            // If we are waiting for disk acquire, or in error state, then we
            // should not receive TEvReacquireDisk message.
            STORAGE_CHECK_PRECONDITION(false);
        } break;
        case EActorState::Preparing:
        case EActorState::CheckpointReady: {
            AcquireShadowDisk(ctx, EAcquireReason::ForcedReAcquire);
        } break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
