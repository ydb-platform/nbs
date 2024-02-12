#include "shadow_disk_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

constexpr auto MinBlockedRetryDelay = TDuration::MilliSeconds(250);
constexpr auto MaxBlockedRetryDelay = TDuration::Seconds(1);

constexpr auto MinNonBlockedRetryDelay = TDuration::Seconds(1);
constexpr auto MaxNonBlockedRetryDelay = TDuration::Seconds(10);

constexpr auto MaxBlockingTotalTimeout = TDuration::Seconds(5);
constexpr auto MaxNonBlockingTotalTimeout = TDuration::Seconds(600);

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

}   // namespace

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
    , TimeoutCalculator(Config, SrcConfig)
    , BlockingDelays(MinBlockedRetryDelay, MaxBlockedRetryDelay)
    , NonBlockingDelays(MinNonBlockedRetryDelay, MaxNonBlockedRetryDelay)
{
    Y_DEBUG_ABORT_UNLESS(checkpointInfo.Data == ECheckpointData::DataPresent);

    switch (checkpointInfo.ShadowDiskState) {
        case EShadowDiskState::None:
            Y_DEBUG_ABORT_UNLESS(false);
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
    DescribeShadowDisk(ctx);
    AcquireShadowDisk(ctx, EAcquireReason::FirstAcquire);
}

bool TShadowDiskActor::OnMessage(
    const TActorContext& ctx,
    TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvDescribeDiskResponse,
            HandleDescribeDiskResponse);
        HFunc(
            TEvDiskRegistry::TEvAcquireDiskResponse,
            HandleAcquireDiskResponse);
        HFunc(TEvVolume::TEvReacquireDisk, HandleReacquireDisk);
        HFunc(TEvVolume::TEvRdmaUnavailable, HandleRdmaUnavailable);
        HFunc(
            TEvVolumePrivate::TEvUpdateShadowDiskStateResponse,
            HandleUpdateShadowDiskStateResponse);
        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);

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
        } break;
        case TEvService::TEvWriteBlocksLocalRequest::EventType: {
            return HandleWriteZeroBlocks<TEvService::TWriteBlocksLocalMethod>(
                *reinterpret_cast<
                    TEvService::TEvWriteBlocksLocalRequest::TPtr*>(&ev),
                ctx);
        } break;
        case TEvService::TEvZeroBlocksRequest::EventType: {
            return HandleWriteZeroBlocks<TEvService::TZeroBlocksMethod>(
                *reinterpret_cast<TEvService::TEvZeroBlocksRequest::TPtr*>(&ev),
                ctx);
        } break;

        default:
            // Message processing by the base class is required.
            return false;
            break;
    }

    // We get here if we have processed an incoming message. And its processing
    // by the base class is not required.
    return true;
}

TDuration TShadowDiskActor::CalculateMigrationTimeout()
{
    return TimeoutCalculator.CalculateTimeout(GetNextProcessingRange());
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

void TShadowDiskActor::DescribeShadowDisk(const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(WaitingForAcquire());
    Y_DEBUG_ABORT_UNLESS(DstActorId == TActorId());

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Describing shadow disk " << ShadowDiskId.Quote());

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        MakeDescribeDiskRequest());
}

void TShadowDiskActor::AcquireShadowDisk(
    const NActors::TActorContext& ctx,
    EAcquireReason acquireReason)
{
    if (acquireReason == EAcquireReason::FirstAcquire) {
        Y_DEBUG_ABORT_UNLESS(WaitingForAcquire());
        Y_DEBUG_ABORT_UNLESS(DstActorId == TActorId());
        Y_DEBUG_ABORT_UNLESS(AcquiredShadowDiskDevices.empty());
    } else {
        Y_DEBUG_ABORT_UNLESS(!WaitingForAcquire());
        Y_DEBUG_ABORT_UNLESS(DstActorId != TActorId());
        Y_DEBUG_ABORT_UNLESS(!ShadowDiskDevices.empty());
        Y_DEBUG_ABORT_UNLESS(!AcquiredShadowDiskDevices.empty());
    }

    const bool canSkipAcquireError =
        acquireReason == EAcquireReason::PeriodicalReAcquire;

    if (!canSkipAcquireError) {
        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Acquiring shadow disk " << ShadowDiskId.Quote());

        if (AcquireStartedAt == TInstant::Zero()) {
            // We are starting not a periodic, but an important disk acquiring.
            // Therefore, we set up a timer to track the operation timeout and
            // transition to an error state.
            AcquireStartedAt = ctx.Now();
            ctx.Schedule(
                IsWritesToSrcDiskPossible() ? MaxNonBlockingTotalTimeout
                                            : MaxBlockingTotalTimeout,
                new TEvents::TEvWakeup(
                    static_cast<ui64>(EWakeupReason::AcquireTimeout)));
        }
    }

    if (acquireReason == EAcquireReason::ForcedReAcquire) {
        ForcedReAcquireInProgress = true;
    }

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        MakeAcquireDiskRequest(),
        static_cast<ui64>(acquireReason));
}

auto TShadowDiskActor::MakeDescribeDiskRequest() const
    -> std::unique_ptr<TEvDiskRegistry::TEvDescribeDiskRequest>
{
    auto request = std::make_unique<TEvDiskRegistry::TEvDescribeDiskRequest>();
    request->Record.SetDiskId(ShadowDiskId);
    return request;
}

auto TShadowDiskActor::MakeAcquireDiskRequest() const
    -> std::unique_ptr<TEvDiskRegistry::TEvAcquireDiskRequest>
{
    auto request = std::make_unique<TEvDiskRegistry::TEvAcquireDiskRequest>();
    request->Record.SetDiskId(ShadowDiskId);
    request->Record.MutableHeaders()->SetClientId(ShadowDiskClientId);
    request->Record.SetAccessMode(
        ReadOnlyMount() ? NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_ONLY
                        : NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_WRITE);
    request->Record.SetMountSeqNumber(MountSeqNumber);
    request->Record.SetVolumeGeneration(Generation);
    return request;
}

void TShadowDiskActor::HandleDescribeDiskResponse(
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

    MaybeCreateShadowDiskPartitionActor(ctx);
}

void TShadowDiskActor::HandleAcquireDiskResponse(
    const TEvDiskRegistry::TEvAcquireDiskResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;
    auto acquireReason = static_cast<EAcquireReason>(ev->Cookie);

    if (HasError(record.GetError())) {
        const bool canSkipAcquireError =
            acquireReason == EAcquireReason::PeriodicalReAcquire;

        if (canSkipAcquireError) {
            SchedulePeriodicalReAcquire(ctx);
            return;
        }

        HandleDiskRegistryError(
            ctx,
            record.GetError(),
            std::make_unique<IEventHandle>(
                MakeDiskRegistryProxyServiceId(),
                ctx.SelfID,
                MakeAcquireDiskRequest().release(),
                0,   // flags
                static_cast<ui64>(acquireReason)),
            "acquire");
        return;
    }

    switch (acquireReason) {
        case EAcquireReason::FirstAcquire: {
            AcquiredShadowDiskDevices.Swap(record.MutableDevices());
            MaybeCreateShadowDiskPartitionActor(ctx);
        } break;

        case EAcquireReason::PeriodicalReAcquire:
        case EAcquireReason::ForcedReAcquire: {
            OnShadowDiskAcquiringCompleted(ctx, acquireReason);
        } break;
    }
}

void TShadowDiskActor::HandleDiskRegistryError(
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

    const TInstant timeoutElapsedAt =
        IsWritesToSrcDiskPossible()
            ? AcquireStartedAt + MaxNonBlockingTotalTimeout
            : AcquireStartedAt + MaxBlockingTotalTimeout;

    const bool canRetry = GetErrorKind(error) == EErrorKind::ErrorRetriable &&
                          timeoutElapsedAt > ctx.Now();

    if (canRetry) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Will soon retry " << actionName << " shadow disk "
                               << ShadowDiskId.Quote()
                               << " Error: " << FormatError(error));

        TActivationContext::Schedule(
            IsWritesToSrcDiskPossible()
                ? NonBlockingDelays.GetDelayAndIncrease()
                : BlockingDelays.GetDelayAndIncrease(),
            TAutoPtr(retryEvent.release()));
    } else {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Will not retry " << actionName << " shadow disk "
                              << ShadowDiskId.Quote()
                              << " Error: " << FormatError(error));
        SetErrorState(ctx);
    }
}

void TShadowDiskActor::MaybeCreateShadowDiskPartitionActor(
    const NActors::TActorContext& ctx)
{
    using EReason = TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

    Y_DEBUG_ABORT_UNLESS(WaitingForAcquire());

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
                static_cast<ui64>(EAcquireReason::FirstAcquire)),
            "acquire");
        AcquiredShadowDiskDevices.Clear();
        return;
    };

    TNonreplicatedPartitionConfig::TVolumeInfo volumeInfo{
        TInstant(),
        GetCheckpointShadowDiskType(SrcConfig->GetVolumeInfo().MediaKind)};

    auto nonreplicatedConfig = std::make_shared<TNonreplicatedPartitionConfig>(
        AcquiredShadowDiskDevices,
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
        false                  // useSimpleMigrationBandwidthLimiter
    );

    DstActorId = NCloud::Register(
        ctx,
        CreateNonreplicatedPartition(
            Config,
            nonreplicatedConfig,
            VolumeActorId,
            RdmaClient));
    PoisonPillHelper.TakeOwnership(ctx, DstActorId);

    if (State == EActorState::WaitAcquireForRead) {
        // Ready to serve checkpoint reads.
        State = EActorState::CheckpointReady;
        Y_DEBUG_ABORT_UNLESS(Acquired());
    } else {
        Y_DEBUG_ABORT_UNLESS(
            State == EActorState::WaitAcquireForPrepareStart ||
            State == EActorState::WaitAcquireForPrepareContinue);

        // Ready to fill shadow disk with data.
        State = EActorState::Preparing;
        Y_DEBUG_ABORT_UNLESS(Acquired());

        TNonreplicatedPartitionMigrationCommonActor::StartWork(
            ctx,
            SrcActorId,
            DstActorId);

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

    OnShadowDiskAcquiringCompleted(ctx, EAcquireReason::FirstAcquire);
}

void TShadowDiskActor::OnShadowDiskAcquiringCompleted(
    const NActors::TActorContext& ctx,
    EAcquireReason acquireReason)
{
    if (acquireReason != EAcquireReason::PeriodicalReAcquire) {
        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Acquired shadow disk " << ShadowDiskId.Quote());
    }

    ForcedReAcquireInProgress = false;
    AcquireStartedAt = {};
    BlockingDelays.Reset();
    NonBlockingDelays.Reset();

    SchedulePeriodicalReAcquire(ctx);
}

void TShadowDiskActor::SchedulePeriodicalReAcquire(const TActorContext& ctx)
{
    if (!PeriodicalReAcquireShadowDiskScheduled) {
        ctx.Schedule(
            Config->GetClientRemountPeriod(),
            new TEvents::TEvWakeup(
                static_cast<ui64>(EWakeupReason::PeriodicalReAcquire)));
    }
    PeriodicalReAcquireShadowDiskScheduled = true;
}

void TShadowDiskActor::OnPeriodicalReAcquire(const NActors::TActorContext& ctx)
{
    AcquireShadowDisk(ctx, EAcquireReason::PeriodicalReAcquire);
    SchedulePeriodicalReAcquire(ctx);
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

void TShadowDiskActor::OnMaybeAcquireTimeout(const NActors::TActorContext& ctx)
{
    if (!WaitingForAcquire()) {
        return;
    }

    LOG_ERROR_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Acquire timeout. Shadow disk " << ShadowDiskId.Quote());

    SetErrorState(ctx);
}

bool TShadowDiskActor::CanJustForwardWritesToSrcDisk() const
{
    return State == EActorState::CheckpointReady ||
           State == EActorState::Error ||
           State == EActorState::WaitAcquireForPrepareStart;
}

bool TShadowDiskActor::IsWritesToSrcDiskForbidden() const
{
    return State == EActorState::WaitAcquireForPrepareContinue;
}

bool TShadowDiskActor::IsWritesToSrcDiskPossible() const
{
    return IsWritesToSrcDiskForbidden() || ForcedReAcquireInProgress;
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
    Y_DEBUG_ABORT_UNLESS(State != EActorState::Error);

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

    if (IsWritesToSrcDiskForbidden()) {
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
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::VOLUME,
                "State of shadow disk " << ShadowDiskId.Quote()
                                        << " unexpected changed to "
                                        << ToString(msg->NewState));
            State = EActorState::Error;
            Y_DEBUG_ABORT_UNLESS(false);
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
            Y_DEBUG_ABORT_UNLESS(Acquired());
        } break;
        case EShadowDiskState::Ready: {
            LOG_INFO_S(
                ctx,
                TBlockStoreComponents::VOLUME,
                "State of shadow disk " << ShadowDiskId.Quote()
                                        << " changed to "
                                        << ToString(msg->NewState));
            State = EActorState::CheckpointReady;
            Y_DEBUG_ABORT_UNLESS(Acquired());
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

void TShadowDiskActor::HandleWakeup(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto reason = static_cast<EWakeupReason>(ev->Get()->Tag);

    switch (reason) {
        case EWakeupReason::AcquireTimeout: {
            OnMaybeAcquireTimeout(ctx);
        } break;
        case EWakeupReason::PeriodicalReAcquire: {
            OnPeriodicalReAcquire(ctx);
        } break;
    }
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
    // case, we try acquire shadow disk again.

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
            Y_DEBUG_ABORT_UNLESS(false);
        } break;
        case EActorState::Preparing:
        case EActorState::CheckpointReady: {
            AcquireShadowDisk(ctx, EAcquireReason::ForcedReAcquire);
        } break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
