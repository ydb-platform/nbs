#include "volume_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/volume/actors/shadow_disk_actor.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;
using namespace NPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDescr
{
    ui64 TabletId = 0;
    TActorId ActorId;
    bool DiskRegistryBasedDisk = false;
};

using TPartitionDescrs = TVector<TPartitionDescr>;

////////////////////////////////////////////////////////////////////////////////

using TCreateCheckpointMethod = TEvService::TCreateCheckpointMethod;
using TDeleteCheckpointMethod = TEvService::TDeleteCheckpointMethod;
using TDeleteCheckpointDataMethod = TEvVolume::TDeleteCheckpointDataMethod;
using TGetChangedBlocksMethod = TEvService::TGetChangedBlocksMethod;
using TGetCheckpointStatusMethod = TEvService::TGetCheckpointStatusMethod;

const char* GetCheckpointRequestName(ECheckpointRequestType reqType) {
    switch (reqType) {
        case ECheckpointRequestType::Delete:
            return TDeleteCheckpointMethod::Name;
        case ECheckpointRequestType::DeleteData:
            return TDeleteCheckpointDataMethod::Name;
        case ECheckpointRequestType::Create:
            return TCreateCheckpointMethod::Name;
        case ECheckpointRequestType::CreateWithoutData:
            return "CreateWithoutData";
    }
}

ECheckpointType ProtoCheckpointType2CheckpointType(
    bool isLight,
    NProto::ECheckpointType checkpointType)
{
    static const THashMap<NProto::ECheckpointType, ECheckpointType> proto2enum{
        {NProto::ECheckpointType::NORMAL, ECheckpointType::Normal},
        {NProto::ECheckpointType::LIGHT, ECheckpointType::Light},
        {NProto::ECheckpointType::WITHOUT_DATA, ECheckpointType::Normal}};

    if (isLight) {
        // TODO(NBS-4531): remove this after dm release
        checkpointType = NProto::ECheckpointType::LIGHT;
    }

    return proto2enum.Value(checkpointType, ECheckpointType::Normal);
}

ECheckpointRequestType ProtoCheckpointType2Create(
    bool isLight,
    NProto::ECheckpointType checkpointType)
{
    static const THashMap<NProto::ECheckpointType, ECheckpointRequestType>
        proto2enum{
            {NProto::ECheckpointType::NORMAL, ECheckpointRequestType::Create},
            {NProto::ECheckpointType::LIGHT, ECheckpointRequestType::Create},
            {NProto::ECheckpointType::WITHOUT_DATA,
             ECheckpointRequestType::CreateWithoutData}};

    if (isLight) {
        // TODO(NBS-4531): remove this after dm release
        checkpointType = NProto::ECheckpointType::LIGHT;
    }

    return proto2enum.Value(checkpointType, ECheckpointRequestType::Create);
}

NProto::ECheckpointStatus GetCheckpointStatus(const TActiveCheckpointInfo& checkpointInfo)
{
    switch (checkpointInfo.Type) {
        case ECheckpointType::Normal:
            break;
        case ECheckpointType::Light:
            return NProto::ECheckpointStatus::READY;
    }

    switch (checkpointInfo.Data) {
        case ECheckpointData::DataPresent:
            break;
        case ECheckpointData::DataDeleted:
            return NProto::ECheckpointStatus::ERROR;
    }

    if (!checkpointInfo.IsShadowDiskBased()) {
        return NProto::ECheckpointStatus::READY;
    }

    switch (checkpointInfo.ShadowDiskState) {
        case EShadowDiskState::None:
        case EShadowDiskState::New:
        case EShadowDiskState::Preparing:
            return NProto::ECheckpointStatus::NOT_READY;
        case EShadowDiskState::Ready:
            return NProto::ECheckpointStatus::READY;
        case EShadowDiskState::Error:
            return NProto::ECheckpointStatus::ERROR;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TCheckpointActor final
    : public TActorBootstrapped<TCheckpointActor<TMethod>>
{
private:
    using TThis = TCheckpointActor<TMethod>;
    using TBase = TActor<TThis>;

private:
    const TRequestInfoPtr RequestInfo;
    const ui64 RequestId;
    const TString DiskId;
    const TString CheckpointId;
    const ui64 VolumeTabletId;
    const TActorId VolumeActorId;
    const TPartitionDescrs PartitionDescrs;
    const TRequestTraceInfo TraceInfo;
    const ECheckpointRequestType RequestType;
    const bool CreateCheckpointShadowDisk;

    ui32 DrainResponses = 0;
    ui32 Responses = 0;
    NProto::TError Error;

    TVector<TCallContextPtr> ChildCallContexts;

public:
    TCheckpointActor(
        TRequestInfoPtr requestInfo,
        ui64 requestId,
        TString diskId,
        TString checkpointId,
        ui64 volumeTabletId,
        TActorId volumeActorId,
        TPartitionDescrs partitionDescrs,
        TRequestTraceInfo traceInfo,
        ECheckpointRequestType requestType,
        bool createCheckpointShadowDisk);

    void Bootstrap(const TActorContext& ctx);
    void Drain(const TActorContext& ctx);
    void DoAction(const TActorContext& ctx);
    void UpdateCheckpointRequest(
        const TActorContext& ctx,
        bool completed,
        std::optional<TString> error,
        TString shadowDiskId);
    void ReplyAndDie(const TActorContext& ctx);

private:
    void DoActionForDiskRegistryBasedPartition(
        const TActorContext& ctx,
        const TPartitionDescr& partition);
    void DoActionForBlobStorageBasedPartition(
        const TActorContext& ctx,
        const TPartitionDescr& partition,
        ui32 partitionIndex);
    void ForkTraces(TCallContextPtr callContext);
    void JoinTraces(ui32 cookie);

private:
    STFUNC(StateDrain);
    STFUNC(StateDoAction);
    STFUNC(StateUpdateCheckpointRequest);

    void HandleResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAllocateCheckpointResponse(
        const TEvDiskRegistry::TEvAllocateCheckpointResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDeallocateCheckpointResponse(
        const TEvDiskRegistry::TEvDeallocateCheckpointResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleDrainResponse(
        const TEvPartition::TEvDrainResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDrainUndelivery(
        const TEvPartition::TEvDrainRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleUpdateCheckpointRequestResponse(
        const TEvVolumePrivate::TEvUpdateCheckpointRequestResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TCheckpointActor<TMethod>::TCheckpointActor(
        TRequestInfoPtr requestInfo,
        ui64 requestId,
        TString diskId,
        TString checkpointId,
        ui64 volumeTabletId,
        TActorId volumeActorId,
        TPartitionDescrs partitionDescrs,
        TRequestTraceInfo traceInfo,
        ECheckpointRequestType requestType,
        bool createCheckpointShadowDisk)
    : RequestInfo(std::move(requestInfo))
    , RequestId(requestId)
    , DiskId(std::move(diskId))
    , CheckpointId(std::move(checkpointId))
    , VolumeTabletId(volumeTabletId)
    , VolumeActorId(volumeActorId)
    , PartitionDescrs(std::move(partitionDescrs))
    , TraceInfo(std::move(traceInfo))
    , RequestType(requestType)
    , CreateCheckpointShadowDisk(createCheckpointShadowDisk)
    , ChildCallContexts(Reserve(PartitionDescrs.size()))
{}

template <typename TMethod>
void TCheckpointActor<TMethod>::Drain(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& x: PartitionDescrs) {
        LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Sending Drain request to partition %lu",
            VolumeTabletId,
            x.TabletId);

        const auto selfId = TBase::SelfId();
        auto request = std::make_unique<TEvPartition::TEvDrainRequest>();

        ForkTraces(request->CallContext);

        auto event = std::make_unique<IEventHandle>(
            x.ActorId,
            selfId,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            cookie++,
            &selfId
        );

        ctx.Send(event.release());
    }

    TBase::Become(&TThis::StateDrain);
}

template <typename TMethod>
void TCheckpointActor<TMethod>::UpdateCheckpointRequest(
    const TActorContext& ctx,
    bool completed,
    std::optional<TString> error,
    TString shadowDiskId)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Sending UpdateCheckpointRequest to volume",
        VolumeTabletId);

    const auto selfId = TBase::SelfId();

    auto requestInfo = CreateRequestInfo(
        selfId,
        0,  // cookie
        RequestInfo->CallContext
    );

    auto request =
        std::make_unique<TEvVolumePrivate::TEvUpdateCheckpointRequestRequest>(
            std::move(requestInfo),
            RequestId,
            completed,
            std::move(error),
            std::move(shadowDiskId));

    auto event = std::make_unique<IEventHandle>(
        VolumeActorId,
        selfId,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,  // cookie
        &selfId
    );

    ctx.Send(event.release());

    TBase::Become(&TThis::StateUpdateCheckpointRequest);
}

template <typename TMethod>
void TCheckpointActor<TMethod>::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>(Error);

    if (TraceInfo.IsTraced) {
        TraceInfo.TraceSerializer->BuildTraceInfo(
            *response->Record.MutableTrace(),
            RequestInfo->CallContext->LWOrbit,
            TraceInfo.ReceiveTime,
            GetCycleCount());
    }

    LWTRACK(
        ResponseSent_Volume,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    TBase::Die(ctx);
}

template <typename TMethod>
void TCheckpointActor<TMethod>::ForkTraces(TCallContextPtr callContext)
{
    auto& cc = RequestInfo->CallContext;
    if (!cc->LWOrbit.Fork(callContext->LWOrbit)) {
        LWTRACK(ForkFailed, cc->LWOrbit, TMethod::Name, cc->RequestId);
    }

    ChildCallContexts.push_back(callContext);
}

template <typename TMethod>
void TCheckpointActor<TMethod>::JoinTraces(ui32 cookie)
{
    if (cookie < ChildCallContexts.size()) {
        if (ChildCallContexts[cookie]) {
            auto& cc = RequestInfo->CallContext;
            cc->LWOrbit.Join(ChildCallContexts[cookie]->LWOrbit);
            ChildCallContexts[cookie].Reset();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TCheckpointActor<TMethod>::HandleDrainResponse(
    const TEvPartition::TEvDrainResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    JoinTraces(ev->Cookie);

    if (FAILED(msg->GetStatus())) {
        Error = msg->GetError();

        NCloud::Send(
            ctx,
            VolumeActorId,
            std::make_unique<TEvents::TEvPoisonPill>()
        );

        ReplyAndDie(ctx);
        return;
    }

    if (++DrainResponses == PartitionDescrs.size()) {
        ChildCallContexts.clear();
        DoAction(ctx);
    }
}

template <typename TMethod>
void TCheckpointActor<TMethod>::HandleDrainUndelivery(
    const TEvPartition::TEvDrainRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(
        E_REJECTED,
        "failed to deliver drain request to some partitions"
    );

    NCloud::Send(
        ctx,
        VolumeActorId,
        std::make_unique<TEvents::TEvPoisonPill>()
    );

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TCheckpointActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    JoinTraces(ev->Cookie);

    if (FAILED(msg->GetStatus())) {
        Error = msg->GetError();

        NCloud::Send(
            ctx,
            VolumeActorId,
            std::make_unique<TEvents::TEvPoisonPill>()
        );

        ReplyAndDie(ctx);
        return;
    }

    if (++Responses == PartitionDescrs.size()) {
        // All the underlying actors have sent a successful status, we mark the
        // request as completed.
        UpdateCheckpointRequest(
            ctx,
            true,           // Completed
            std::nullopt,   // Error
            {}              // ShadowDiskId
        );
    }
}

template <typename TMethod>
void TCheckpointActor<TMethod>::HandleAllocateCheckpointResponse(
    const TEvDiskRegistry::TEvAllocateCheckpointResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    JoinTraces(ev->Cookie);

    bool success = !HasError(msg->Record);
    if (!success) {
        Error = msg->GetError();

        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "For disk %s could not create shadow disk for checkpoint %s, "
            "error: %s",
            DiskId.Quote().c_str(),
            CheckpointId.Quote().c_str(),
            FormatError(Error).c_str());
    }

    UpdateCheckpointRequest(
        ctx,
        true,   // Completed
        success ? std::nullopt : std::optional<TString>(FormatError(Error)),
        success ? msg->Record.GetShadowDiskId() : "ERROR");
}

template <typename TMethod>
void TCheckpointActor<TMethod>::HandleDeallocateCheckpointResponse(
    const TEvDiskRegistry::TEvDeallocateCheckpointResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    JoinTraces(ev->Cookie);

    if (FAILED(msg->GetStatus())) {
        Error = msg->GetError();

        NCloud::Send(
            ctx,
            VolumeActorId,
            std::make_unique<TEvents::TEvPoisonPill>());

        ReplyAndDie(ctx);
        return;
    }

    UpdateCheckpointRequest(
        ctx,
        true,           // Completed
        std::nullopt,   // Error
        {}              // ShadowDiskId
    );
}

template <typename TMethod>
void TCheckpointActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(
        E_REJECTED,
        "failed to deliver checkpoint request to some partitions"
    );

    NCloud::Send(
        ctx,
        VolumeActorId,
        std::make_unique<TEvents::TEvPoisonPill>()
    );

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TCheckpointActor<TMethod>::HandleUpdateCheckpointRequestResponse(
    const TEvVolumePrivate::TEvUpdateCheckpointRequestResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx);
}

template <typename TMethod>
void TCheckpointActor<TMethod>::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(
        E_REJECTED,
        "tablet is shutting down"
    );

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
STFUNC(TCheckpointActor<TMethod>::StateDrain)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPartition::TEvDrainResponse, HandleDrainResponse);
        HFunc(TEvPartition::TEvDrainRequest, HandleDrainUndelivery);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

template <typename TMethod>
STFUNC(TCheckpointActor<TMethod>::StateDoAction)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);
        HFunc(
            TEvDiskRegistry::TEvAllocateCheckpointResponse,
            HandleAllocateCheckpointResponse);
        HFunc(
            TEvDiskRegistry::TEvDeallocateCheckpointResponse,
            HandleDeallocateCheckpointResponse);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

template <typename TMethod>
STFUNC(TCheckpointActor<TMethod>::StateUpdateCheckpointRequest)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolumePrivate::TEvUpdateCheckpointRequestResponse,
            HandleUpdateCheckpointRequestResponse
        );
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TCheckpointActor<TMethod>::DoAction(const TActorContext& ctx)
{
    ui32 partitionIndex = 0;
    for (const auto& x: PartitionDescrs) {
        if (x.DiskRegistryBasedDisk) {
            Y_DEBUG_ABORT_UNLESS(PartitionDescrs.size() == 1);

            DoActionForDiskRegistryBasedPartition(ctx, x);
        } else {
            DoActionForBlobStorageBasedPartition(ctx, x, partitionIndex);
        }
        ++partitionIndex;
    }
}

template <typename TMethod>
void TCheckpointActor<TMethod>::DoActionForDiskRegistryBasedPartition(
    const TActorContext& ctx,
    const TPartitionDescr& partition)
{
    Y_UNUSED(partition);

    if (!CheckpointId) {
        Error = MakeError(E_ARGUMENT, "empty checkpoint name");
        UpdateCheckpointRequest(ctx, false, "empty checkpoint name", {});
        return;
    }

    if (!CreateCheckpointShadowDisk) {
        // It is not required to send requests to the actor, we reject
        // zero/write requests in TVolume (see CanExecuteWriteRequest).
        UpdateCheckpointRequest(ctx, true, std::nullopt, {});
        return;
    }

    if constexpr (std::is_same_v<TMethod, TCreateCheckpointMethod>) {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvAllocateCheckpointRequest>();
        request->Record.SetSourceDiskId(DiskId);
        request->Record.SetCheckpointId(CheckpointId);

        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "AllocateCheckpointDiskRequest: %s",
            request->Record.Utf8DebugString().Quote().c_str());

        NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
    }
    if constexpr (
        std::is_same_v<TMethod, TDeleteCheckpointDataMethod> ||
        std::is_same_v<TMethod, TDeleteCheckpointMethod>)
    {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvDeallocateCheckpointRequest>();
        request->Record.SetSourceDiskId(DiskId);
        request->Record.SetCheckpointId(CheckpointId);

        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "DeallocateCheckpointDiskRequest: %s",
            request->Record.Utf8DebugString().Quote().c_str());

        NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
    }

    TBase::Become(&TThis::StateDoAction);
}

template <typename TMethod>
void TCheckpointActor<TMethod>::DoActionForBlobStorageBasedPartition(
    const TActorContext& ctx,
    const TPartitionDescr& partition,
    ui32 partitionIndex)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Sending %s request to partition %lu",
        VolumeTabletId,
        TMethod::Name,
        partition.TabletId);

    const auto selfId = TBase::SelfId();
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->Record.SetCheckpointId(CheckpointId);
    if constexpr (std::is_same_v<TMethod, TCreateCheckpointMethod>) {
        request->Record.SetCheckpointType(
            RequestType == ECheckpointRequestType::CreateWithoutData
                ? NProto::ECheckpointType::WITHOUT_DATA
                : NProto::ECheckpointType::NORMAL
        );
    }

    ForkTraces(request->CallContext);

    auto event = std::make_unique<IEventHandle>(
        partition.ActorId,
        selfId,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        partitionIndex,
        &selfId
    );

    ctx.Send(event.release());

    TBase::Become(&TThis::StateDoAction);
}

template <typename TMethod>
void TCheckpointActor<TMethod>::Bootstrap(const TActorContext& ctx)
{
    DoAction(ctx);
}

template <>
void TCheckpointActor<TCreateCheckpointMethod>::Bootstrap(
    const TActorContext& ctx)
{
    // if this volume is a single-partition blobstorage-based disk, draining is
    // not needed.
    const bool skipDrain = (PartitionDescrs.size() == 1) &&
                           (PartitionDescrs[0].DiskRegistryBasedDisk == false);

    if (skipDrain) {
        DoAction(ctx);
    } else {
        Drain(ctx);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::CreateCheckpointLightRequest(
    const TActorContext& ctx,
    ui64 requestId,
    const TString& checkpointId)
{
    const auto type = State->GetCheckpointStore().GetCheckpointType(checkpointId);

    if (type && *type == ECheckpointType::Light) {
        LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s: light checkpoint %s already exists",
            TabletID(),
            TCreateCheckpointMethod::Name,
            checkpointId.Quote().c_str());
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s created light checkpoint %s",
            TabletID(),
            TCreateCheckpointMethod::Name,
            checkpointId.Quote().c_str());

        State->CreateCheckpointLight(checkpointId);
    }

    const TCheckpointRequestInfo* requestInfo =
        CheckpointRequests.FindPtr(requestId);

    ExecuteTx<TUpdateCheckpointRequest>(
        ctx,
        requestInfo ? requestInfo->RequestInfo : nullptr,
        requestId,
        true,        // Completed
        TString(),   // ShadowDiskId
        EShadowDiskState::None,
        TString()   // Error
    );
}

void TVolumeActor::DeleteCheckpointLightRequest(
    const TActorContext& ctx,
    ui64 requestId,
    const TString& checkpointId)
{
    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] %s deleted light checkpoint %s",
        TabletID(),
        TCreateCheckpointMethod::Name,
        checkpointId.Quote().c_str());

    State->DeleteCheckpointLight(checkpointId);

    const TCheckpointRequestInfo* requestInfo =
        CheckpointRequests.FindPtr(requestId);

    ExecuteTx<TUpdateCheckpointRequest>(
        ctx,
        requestInfo ? requestInfo->RequestInfo : nullptr,
        requestId,
        true,        // Completed
        TString(),   // ShadowDiskId
        EShadowDiskState::None,
        TString()   // Error
    );
}

void TVolumeActor::GetChangedBlocksForLightCheckpoints(
    const TGetChangedBlocksMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto replyError = [&](NProto::TError error)
    {
        TString errorMsg = TStringBuilder()
            << "[%lu] %s for light checkpoints failed, request parameters are:"
            << " LowCheckpointId: " << msg->Record.GetLowCheckpointId() << ","
            << " HighCheckpointId: " << msg->Record.GetHighCheckpointId() << ","
            << " StartIndex: " << msg->Record.GetStartIndex() << ","
            << " BlocksCount: " << msg->Record.GetBlocksCount() << ","
            << " error message: " << error.GetMessage();
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            errorMsg.c_str(),
            TabletID(),
            TGetChangedBlocksMethod::Name);

        NCloud::Reply(ctx, *requestInfo,
            std::make_unique<TGetChangedBlocksMethod::TResponse>(std::move(error)));
    };

    if (!msg->Record.GetHighCheckpointId().empty()) {
        const auto highCheckpointType = State->GetCheckpointStore().GetCheckpointType(
                msg->Record.GetHighCheckpointId());

        if (!highCheckpointType || *highCheckpointType != ECheckpointType::Light){
            replyError(MakeError(E_ARGUMENT, "High light checkpoint does not exist"));
            return;
        }
    }

    if (!msg->Record.GetLowCheckpointId().empty()) {
        const auto lowCheckpointType = State->GetCheckpointStore().GetCheckpointType(
                msg->Record.GetLowCheckpointId());

        if (!lowCheckpointType || *lowCheckpointType != ECheckpointType::Light){
            replyError(MakeError(E_ARGUMENT, "Low light checkpoint does not exist"));
            return;
        }
    }

    if (msg->Record.GetBlocksCount() == 0) {
        replyError(MakeError(E_ARGUMENT, "Empty block range is forbidden for GetChangedBlocks"));
        return;
    }

    TString mask;

    auto error = State->FindDirtyBlocksBetweenLightCheckpoints(
        msg->Record.GetLowCheckpointId(),
        msg->Record.GetHighCheckpointId(),
        TBlockRange64::WithLength(
            msg->Record.GetStartIndex(),
            msg->Record.GetBlocksCount()),
        &mask);

    if (!SUCCEEDED(error.GetCode())) {
        replyError(error);
        return;
    }

    auto response = std::make_unique<TGetChangedBlocksMethod::TResponse>();
    *response->Record.MutableMask() = std::move(mask);

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

void TVolumeActor::ReplyErrorOnNormalGetChangedBlocksRequestForDiskRegistryBasedDisk(
    const TGetChangedBlocksMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response = std::make_unique<TGetChangedBlocksMethod::TResponse>();
    auto requestInfo =
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TString errorMsg = TStringBuilder()
        << "[%lu] %s: " << "Disk registry based disks can not handle"
        << " GetChangedBlocks requests for normal checkpoints,"
        << " Checkpoints requested:"
        << " LowCheckpointId: " << msg->Record.GetLowCheckpointId() << ","
        << " HighCheckpointId: " << msg->Record.GetHighCheckpointId();

    LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
        errorMsg.c_str(),
        TabletID(),
        TGetChangedBlocksMethod::Name);

    *response->Record.MutableError() = MakeError(E_ARGUMENT, errorMsg);
    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

template <>
bool TVolumeActor::HandleRequest<TCreateCheckpointMethod>(
    const TActorContext& ctx,
    const TCreateCheckpointMethod::TRequest::TPtr& ev,
    ui64 volumeRequestId,
    bool isTraced,
    ui64 traceTs)
{
    Y_UNUSED(volumeRequestId);

    const auto& msg = *ev->Get();

    auto requestInfo = CreateRequestInfo<TCreateCheckpointMethod>(
        ev->Sender,
        ev->Cookie,
        msg.CallContext);

    AddTransaction(*requestInfo);

    const auto& checkpointRequest = State->GetCheckpointStore().CreateNew(
        msg.Record.GetCheckpointId(),
        ctx.Now(),
        ProtoCheckpointType2Create(
            msg.Record.GetIsLight(),
            msg.Record.GetCheckpointType()),
        ProtoCheckpointType2CheckpointType(
            msg.Record.GetIsLight(),
            msg.Record.GetCheckpointType()));
    ExecuteTx<TSaveCheckpointRequest>(
        ctx,
        std::move(requestInfo),
        checkpointRequest.RequestId,
        isTraced,
        traceTs);

    return true;
}

template <>
bool TVolumeActor::HandleRequest<TDeleteCheckpointMethod>(
    const TActorContext& ctx,
    const TDeleteCheckpointMethod::TRequest::TPtr& ev,
    ui64 volumeRequestId,
    bool isTraced,
    ui64 traceTs)
{
    Y_UNUSED(volumeRequestId);

    const auto& msg = *ev->Get();

    auto requestInfo = CreateRequestInfo<TDeleteCheckpointMethod>(
        ev->Sender,
        ev->Cookie,
        msg.CallContext);

    AddTransaction(*requestInfo);

    auto& checkpointStore = State->GetCheckpointStore();
    const auto& checkpointId = msg.Record.GetCheckpointId();
    const auto type = checkpointStore.GetCheckpointType(checkpointId);

    const auto& checkpointRequest = checkpointStore.CreateNew(
        checkpointId,
        ctx.Now(),
        ECheckpointRequestType::Delete,
        type.value_or(ECheckpointType::Normal));
    ExecuteTx<TSaveCheckpointRequest>(
        ctx,
        std::move(requestInfo),
        checkpointRequest.RequestId,
        isTraced,
        traceTs);

    return true;
}

template <>
bool TVolumeActor::HandleRequest<TDeleteCheckpointDataMethod>(
    const TActorContext& ctx,
    const TDeleteCheckpointDataMethod::TRequest::TPtr& ev,
    ui64 volumeRequestId,
    bool isTraced,
    ui64 traceTs)
{
    Y_UNUSED(volumeRequestId);

    const auto& msg = *ev->Get();

    auto requestInfo = CreateRequestInfo<TDeleteCheckpointDataMethod>(
        ev->Sender,
        ev->Cookie,
        msg.CallContext);

    AddTransaction(*requestInfo);

    const auto& checkpointRequest = State->GetCheckpointStore().CreateNew(
        msg.Record.GetCheckpointId(),
        ctx.Now(),
        ECheckpointRequestType::DeleteData,
        ECheckpointType::Normal);
    ExecuteTx<TSaveCheckpointRequest>(
        ctx,
        std::move(requestInfo),
        checkpointRequest.RequestId,
        isTraced,
        traceTs);

    return true;
}

template <>
bool TVolumeActor::HandleRequest<TGetCheckpointStatusMethod>(
    const TActorContext& ctx,
    const TGetCheckpointStatusMethod::TRequest::TPtr& ev,
    ui64 volumeRequestId,
    bool isTraced,
    ui64 traceTs)
{
    Y_UNUSED(volumeRequestId);
    Y_UNUSED(isTraced);
    Y_UNUSED(traceTs);

    const auto& msg = *ev->Get();
    const auto& checkpointId = msg.Record.GetCheckpointId();

    auto reply =
        [&](const NProto::TError& error, NProto::ECheckpointStatus status)
    {
        auto response =
            std::make_unique<TGetCheckpointStatusMethod::TResponse>(error);
        response->Record.SetCheckpointStatus(status);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Get checkpoint status for checkpoint %s",
        TabletID(),
        checkpointId.Quote().c_str());

    auto checkpoint = State->GetCheckpointStore().GetCheckpoint(checkpointId);
    if (!checkpoint) {
        reply(
            MakeError(E_NOT_FOUND, "Checkpoint not found"),
            NProto::ECheckpointStatus::ERROR);
        return true;
    }

    reply(MakeError(S_OK), GetCheckpointStatus(*checkpoint));
    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleUpdateCheckpointRequest(
    const TEvVolumePrivate::TEvUpdateCheckpointRequestRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    EShadowDiskState shadowDiskState = EShadowDiskState::None;
    if (!msg->Completed || msg->Error) {
        shadowDiskState = EShadowDiskState::Error;
    } else if (msg->ShadowDiskId) {
        shadowDiskState = EShadowDiskState::New;
    } else {
        shadowDiskState = EShadowDiskState::None;
    }

    ExecuteTx<TUpdateCheckpointRequest>(
        ctx,
        std::move(msg->RequestInfo),
        msg->RequestId,
        msg->Completed,
        msg->ShadowDiskId,
        shadowDiskState,
        msg->Error.value_or(""));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::ProcessNextCheckpointRequest(const TActorContext& ctx)
{
    // only one simultaneous checkpoint request is supported, other requests
    // should wait
    if (State->GetCheckpointStore().IsRequestInProgress()) {
        return;
    }

    // there is no FIFO guarantee for requests sent via TPartitionRequestActor
    // and requests forwarded via TVolumeActor => we can start checkpoint
    // creation only if there are no TPartitionRequestActor-based requests
    // in flight currently
    if (MultipartitionWriteAndZeroRequestsInProgress) {
        return;
    }

    ui64 readyToExecuteRequestId = 0;
    // nothing to do
    if (!State->GetCheckpointStore().HasRequestToExecute(
            &readyToExecuteRequestId))
    {
        return;
    }

    const TCheckpointRequest& request =
        State->GetCheckpointStore().GetRequestById(readyToExecuteRequestId);

    TCheckpointRequestInfo emptyRequestInfo(
        CreateRequestInfo(
            SelfId(),
            0,   // cookie
            MakeIntrusive<TCallContext>()),
        false,
        GetCycleCount());
    const TCheckpointRequestInfo* requestInfo =
        CheckpointRequests.FindPtr(readyToExecuteRequestId);
    if (!requestInfo) {
        requestInfo = &emptyRequestInfo;
    }

    TPartitionDescrs partitionDescrs;
    for (const auto& p: State->GetPartitions()) {
        partitionDescrs.push_back({p.TabletId, p.Owner, false});

        LOG_TRACE(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Forward %s request to partition: %lu %s",
            TabletID(),
            GetCheckpointRequestName(request.ReqType),
            p.TabletId,
            ToString(p.Owner).data()
        );
    }

    if (auto actorId = State->GetDiskRegistryBasedPartitionActor()) {
        ui64 tabletId = 0;
        partitionDescrs.push_back({tabletId, actorId, true});

        LOG_TRACE(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%lu] Forward %s request to nonrepl partition: %s",
            TabletID(),
            GetCheckpointRequestName(request.ReqType),
            ToString(actorId).c_str());
    }

    State->GetCheckpointStore().SetCheckpointRequestInProgress(
        readyToExecuteRequestId);

    TActorId actorId;
    switch (request.ReqType) {
        case ECheckpointRequestType::Create:
        case ECheckpointRequestType::CreateWithoutData: {
            if (request.Type == ECheckpointType::Light) {
                return CreateCheckpointLightRequest(
                    ctx,
                    request.RequestId,
                    request.CheckpointId);
            }
            actorId =
                NCloud::Register<TCheckpointActor<TCreateCheckpointMethod>>(
                    ctx,
                    requestInfo->RequestInfo,
                    request.RequestId,
                    State->GetDiskId(),
                    request.CheckpointId,
                    TabletID(),
                    SelfId(),
                    std::move(partitionDescrs),
                    TRequestTraceInfo(
                        requestInfo->IsTraced,
                        requestInfo->TraceTs,
                        TraceSerializer),
                    request.ReqType,
                    Config->GetUseShadowDisksForNonreplDiskCheckpoints());
            break;
        }
        case ECheckpointRequestType::Delete: {
            if (request.Type == ECheckpointType::Light) {
                return DeleteCheckpointLightRequest(
                    ctx,
                    request.RequestId,
                    request.CheckpointId);
            }
            actorId =
                NCloud::Register<TCheckpointActor<TDeleteCheckpointMethod>>(
                    ctx,
                    requestInfo->RequestInfo,
                    request.RequestId,
                    State->GetDiskId(),
                    request.CheckpointId,
                    TabletID(),
                    SelfId(),
                    std::move(partitionDescrs),
                    TRequestTraceInfo(
                        requestInfo->IsTraced,
                        requestInfo->TraceTs,
                        TraceSerializer),
                    request.ReqType,
                    Config->GetUseShadowDisksForNonreplDiskCheckpoints());
            break;
        }
        case ECheckpointRequestType::DeleteData: {
            actorId =
                NCloud::Register<TCheckpointActor<TDeleteCheckpointDataMethod>>(
                    ctx,
                    requestInfo->RequestInfo,
                    request.RequestId,
                    State->GetDiskId(),
                    request.CheckpointId,
                    TabletID(),
                    SelfId(),
                    std::move(partitionDescrs),
                    TRequestTraceInfo(
                        requestInfo->IsTraced,
                        requestInfo->TraceTs,
                        TraceSerializer),
                    request.ReqType,
                    Config->GetUseShadowDisksForNonreplDiskCheckpoints());
            break;
        }
        default:
            Y_ABORT();
    }

    Actors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareSaveCheckpointRequest(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TSaveCheckpointRequest& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteSaveCheckpointRequest(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TSaveCheckpointRequest& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);

    // The request in memory has the Received status.
    // We save this request to the database with the Saved status.
    // After receiving confirmation of the transaction,
    //  we will raise the status in memory to Saved too.
    auto checkpointRequestCopy =
        State->GetCheckpointStore().GetRequestById(args.RequestId);
    Y_DEBUG_ABORT_UNLESS(
        checkpointRequestCopy.State == ECheckpointRequestState::Received);
    checkpointRequestCopy.State = ECheckpointRequestState::Saved;

    db.WriteCheckpointRequest(checkpointRequestCopy);
}

void TVolumeActor::CompleteSaveCheckpointRequest(
    const TActorContext& ctx,
    TTxVolume::TSaveCheckpointRequest& args)
{
    const auto& checkpointRequest =
        State->GetCheckpointStore().GetRequestById(args.RequestId);

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] CheckpointRequest %lu %s saved",
        TabletID(),
        checkpointRequest.RequestId,
        checkpointRequest.CheckpointId.Quote().c_str());

    State->GetCheckpointStore().SetCheckpointRequestSaved(args.RequestId);

    CheckpointRequests.insert(
        {args.RequestId, {args.RequestInfo, args.IsTraced, args.TraceTs}});

    RemoveTransaction(*args.RequestInfo);
    ProcessNextCheckpointRequest(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::CanExecuteWriteRequest() const
{
    const bool isCheckpointBeingCreated =
        State->GetCheckpointStore().IsCheckpointBeingCreated();

    // If our volume is DiskRegistry-based, write requests are allowed only if
    // there are no active checkpoints
    if (State->GetDiskRegistryBasedPartitionActor()) {
        const bool checkpointExistsOrCreatingNow =
            State->GetCheckpointStore().DoesCheckpointBlockingWritesExist() ||
            isCheckpointBeingCreated;

        return !checkpointExistsOrCreatingNow;
    }

    // If our volume is BlobStorage-based, write requests are allowed only if we
    // have only one partition or there is no checkpoint creation request in
    // flight
    const bool isSinglePartition = State->GetPartitions().size() == 1;
    return isSinglePartition || !isCheckpointBeingCreated;
}

bool TVolumeActor::PrepareUpdateCheckpointRequest(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateCheckpointRequest& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateCheckpointRequest(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateCheckpointRequest& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    db.UpdateCheckpointRequest(
        args.RequestId,
        args.Completed,
        args.ShadowDiskId,
        args.ShadowDiskState,
        args.ErrorMessage.value_or(""));
}

void TVolumeActor::CompleteUpdateCheckpointRequest(
    const TActorContext& ctx,
    TTxVolume::TUpdateCheckpointRequest& args)
{
    const TCheckpointRequest& request =
        State->GetCheckpointStore().GetRequestById(args.RequestId);

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] CheckpointRequest %lu %s marked: %s disk: %s",
        TabletID(),
        request.RequestId,
        request.CheckpointId.Quote().c_str(),
        args.Completed ? "completed" : "rejected",
        args.ShadowDiskId.Quote().c_str());

    const bool needToDestroyShadowActor =
        (request.ReqType == ECheckpointRequestType::Delete ||
         request.ReqType == ECheckpointRequestType::DeleteData) &&
         State->GetCheckpointStore().HasShadowActor(request.CheckpointId);

    if (needToDestroyShadowActor) {
        auto checkpointInfo =
            State->GetCheckpointStore().GetCheckpoint(request.CheckpointId);
        if (checkpointInfo && checkpointInfo->ShadowDiskId) {
            DoUnregisterVolume(ctx, checkpointInfo->ShadowDiskId);
        }
    }

    State->SetCheckpointRequestFinished(
        request,
        args.Completed,
        args.ShadowDiskId,
        args.ShadowDiskState);
    CheckpointRequests.erase(request.RequestId);

    const bool needToCreateShadowActor =
        request.ReqType == ECheckpointRequestType::Create &&
        request.ShadowDiskState != EShadowDiskState::Error &&
        !request.ShadowDiskId.Empty() &&
        !State->GetCheckpointStore().HasShadowActor(request.CheckpointId);

    if (needToDestroyShadowActor || needToCreateShadowActor) {
        RestartDiskRegistryBasedPartition(ctx);
    }

    if (request.Type == ECheckpointType::Light) {
        const bool needReply =
            args.RequestInfo && SelfId() != args.RequestInfo->Sender;
        if (needReply) {
            if (request.ReqType == ECheckpointRequestType::Create) {
                NCloud::Reply(
                    ctx,
                    *args.RequestInfo,
                    std::make_unique<TCreateCheckpointMethod::TResponse>());
            } else if (request.ReqType == ECheckpointRequestType::Delete) {
                NCloud::Reply(
                    ctx,
                    *args.RequestInfo,
                    std::make_unique<TDeleteCheckpointMethod::TResponse>());
            }
        }
    } else {
        NCloud::Reply(
            ctx,
            *args.RequestInfo,
            std::make_unique<TEvVolumePrivate::TEvUpdateCheckpointRequestResponse>()
        );
        Actors.erase(args.RequestInfo->Sender);
    }

    ProcessNextCheckpointRequest(ctx);
}

bool TVolumeActor::PrepareUpdateShadowDiskState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateShadowDiskState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateShadowDiskState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateShadowDiskState& args)
{
    Y_UNUSED(ctx);

    Y_DEBUG_ABORT_UNLESS(
        args.ShadowDiskState >= EShadowDiskState::None &&
        args.ShadowDiskState <= EShadowDiskState::Error);

    TVolumeDatabase db(tx.DB);
    db.UpdateShadowDiskState(
        args.RequestId,
        args.ProcessedBlockCount,
        args.ShadowDiskState);
}

void TVolumeActor::CompleteUpdateShadowDiskState(
    const TActorContext& ctx,
    TTxVolume::TUpdateShadowDiskState& args)
{
    const TCheckpointRequest& request =
        State->GetCheckpointStore().GetRequestById(args.RequestId);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] For checkpoint %s shadow disk %s state updated. Processed "
        "blocks: %lu, state: %s",
        TabletID(),
        request.CheckpointId.Quote().c_str(),
        request.ShadowDiskId.Quote().c_str(),
        args.ProcessedBlockCount,
        ToString(args.ShadowDiskState).c_str());

    State->GetCheckpointStore().SetShadowDiskState(
        request.CheckpointId,
        args.ShadowDiskState,
        args.ProcessedBlockCount);

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateResponse>(
            args.ShadowDiskState,
            args.ProcessedBlockCount));
}

void TVolumeActor::HandleUpdateShadowDiskState(
    const TEvVolumePrivate::TEvUpdateShadowDiskStateRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    using EReason = TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto checkpointInfo =
        State->GetCheckpointStore().GetCheckpoint(msg->CheckpointId);

    auto reply = [&](EShadowDiskState newState)
    {
        auto response = std::make_unique<
            TEvVolumePrivate::TEvUpdateShadowDiskStateResponse>(
            newState,
            msg->ProcessedBlockCount);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if (!checkpointInfo) {
        reply(EShadowDiskState::Error);
        return;
    }

    auto currentShadowDiskState = checkpointInfo->ShadowDiskState;
    if (currentShadowDiskState == EShadowDiskState::Error) {
        reply(EShadowDiskState::Error);
        return;
    }

    EShadowDiskState newShadowDiskState = EShadowDiskState::None;

    switch (msg->Reason) {
        case EReason::FillProgressUpdate: {
            if (currentShadowDiskState == EShadowDiskState::Ready) {
                reply(EShadowDiskState::Ready);
                return;
            }
            newShadowDiskState = EShadowDiskState::Preparing;
        } break;
        case EReason::FillCompleted: {
            newShadowDiskState = EShadowDiskState::Ready;
        } break;
        case EReason::FillError: {
            newShadowDiskState = EShadowDiskState::Error;
        } break;
    }
    Y_DEBUG_ABORT_UNLESS(newShadowDiskState != EShadowDiskState::None);

    ExecuteTx<TUpdateShadowDiskState>(
        ctx,
        std::move(requestInfo),
        checkpointInfo->RequestId,
        newShadowDiskState,
        msg->ProcessedBlockCount);
}

}   // namespace NCloud::NBlockStore::NStorage
