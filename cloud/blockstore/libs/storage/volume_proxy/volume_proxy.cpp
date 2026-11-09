#include "volume_proxy.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/api/tablet_proxy.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/lwtrace/shuttle.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>

#include <memory>
#include <type_traits>
#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NCloud::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool IsDataPlaneMethod =
    std::is_same_v<TMethod, TEvVolume::TDescribeBlocksMethod> ||
    std::is_same_v<TMethod, TEvService::TReadBlocksMethod> ||
    std::is_same_v<TMethod, TEvService::TWriteBlocksMethod> ||
    std::is_same_v<TMethod, TEvService::TZeroBlocksMethod> ||
    std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod> ||
    std::is_same_v<TMethod, TEvService::TWriteBlocksLocalMethod>;

template <typename TRequest>
ui64 GetExternalVolumeTabletId(const TRequest& request)
{
    if constexpr (requires { request.Record.GetExternalVolumeTabletId(); }) {
        return request.Record.GetExternalVolumeTabletId();
    }
    return 0;
}

// VolumeProxy discovers the volume specified in the DiskId field of request,
// establishes a connection to the tablet of this volume and redirects the
// request to it. TabletProxy keeps the pipe to the volume tablet and ensures
// that the requests is sent to the tablet. When pipe to volume breaks the
// response "E_REJECTED Tablet is dead" sent to client. If the client stops
// sending messages to the volume, the connection is terminated after a
// PipeInactivityTimeout (1 minute).
//
// Additionally, VolumeProxy tracks the list of base disks tablets. The
// connection to the base disks occurs even if the describe returned an error,
// since the tablet id is already known.

class TVolumeProxyActor final
    : public TActor<TVolumeProxyActor>
{
    enum EVolumeInfoState
    {
        RESOLVING = 0,
        RESOLVED = 1,
        FAILED = 2,
    };

    struct TVolumeInfo
    {
        // Numeric volume info ID. It is used to find the volume info by cookie.
        const ui64 Id;

        // ID of the disk that the user is sending requests to. It may differ
        // from the actual disk that serves the requests if the disk was
        // copied, since a suffix is added to the name of the copied disk.
        const TString DiskId;

        // Id of the disk that was found by the SchemeShard describe.
        // Note: If the disk was a copy the name of the real disk differs
        // from the DiskId.
        TString RealDiskId;

        // An exact name match is required. It is forbidden to forward to a
        // copied disk that has a name with a suffix.
        const bool RequireExactDiskIdMatch = false;

        EVolumeInfoState State = RESOLVING;
        // The tablet comes from the base disk mapping, not from the describe.
        bool IsBaseDisk = false;
        ui64 TabletId = 0;

        // An error that is returned for all requests of a failed volume info.
        NProto::TError Error;

        // Requests waiting for the volume info to be resolved.
        TDeque<IEventHandlePtr> Requests;

        // The time of the last request or keep-alive. It is used to drop
        // unused volume infos.
        TInstant LastActivity;

        TLogTitle LogTitle;

        TVolumeInfo(
            ui64 id,
            TString diskId,
            bool requireExactDiskIdMatch,
            bool temporaryServer)
            : Id(id)
            , DiskId(std::move(diskId))
            , RequireExactDiskIdMatch(requireExactDiskIdMatch)
            , LogTitle(
                  GetCycleCount(),
                  TLogTitle::TVolumeProxy{
                      .DiskId = DiskId,
                      .TemporaryServer = temporaryServer})
        {}
    };

    // TActiveRequest contains an IEventHandle without embedded message,
    // it only contains information about the event type and the sender.
    // This is enough to response "E_REJECTED Tablet is dead". And it doesn't
    // waste any extra memory.
    struct TActiveRequest
    {
        // Zero for a request routed by its ExternalVolumeTabletId.
        const ui64 VolumeInfoId;
        IEventHandlePtr Request;
        TCallContextPtr CallContext;
        ui64 SendTime;

        TActiveRequest(
                ui64 volumeInfoId,
                IEventHandlePtr request,
                TCallContextPtr callContext,
                ui64 sendTime)
            : VolumeInfoId(volumeInfoId)
            , Request(std::move(request))
            , CallContext(std::move(callContext))
            , SendTime(sendTime)
        {}
    };

    using TActiveRequestMap = THashMap<ui64, TActiveRequest>;

    // A base disk ping waiting for the answer of the tablet proxy.
    struct TBaseDiskPingRequest
    {
        TActorId Sender;
        ui64 Cookie = 0;
        // Zero if there is only the base disk mapping.
        ui64 VolumeInfoId = 0;
    };

private:
    const TDuration InactivityTimeout;
    const ITraceSerializerPtr TraceSerializer;
    const bool TemporaryServer = false;

    ui64 VolumeInfoIdGenerator = 0;

    THashMap<ui64, TVolumeInfo> VolumeInfoById;
    // Mapping of logical DiskId to the volume info. The DiskId of the real
    // disk that serves the requests may differ and have the suffix "-copy".
    THashMap<TString, TVolumeInfo*> VolumeInfoByDiskId;
    // Mapping of the DiskId to the volume info with exact DiskId match.
    THashMap<TString, TVolumeInfo*> VolumeInfoByRealDiskId;

    struct TBaseTabletId
    {
        const ui64 TabletId = 0;
        int RefCount = 0;
    };

    THashMap<TString, TBaseTabletId> BaseDiskIdToTabletId;

    ui64 RequestId = 0;
    TActiveRequestMap ActiveRequests;
    THashMap<ui64, TBaseDiskPingRequest> BaseDiskPingRequests;

public:
    TVolumeProxyActor(
        TDuration inactivityTimeout,
        ITraceSerializerPtr traceSerializer,
        bool temporaryServer);

private:
    TVolumeInfo* FindVolumeInfo(const TString& diskId, bool exactDiskIdMatch);
    TVolumeInfo& CreateVolumeInfo(const TString& diskId, bool exactDiskIdMatch);
    void EraseVolumeInfo(TVolumeInfo* volumeInfo);

    template <typename TMethod>
    void ResolveVolumeInfo(const TActorContext& ctx, TVolumeInfo& volumeInfo);

    void CompleteVolumeInfo(
        const TActorContext& ctx,
        TVolumeInfo& volumeInfo,
        ui64 tabletId,
        const TString& path,
        const TString& realDiskId);

    void FailVolumeInfo(TVolumeInfo& volumeInfo, NProto::TError error);

    void ProcessPendingRequests(TVolumeInfo& volumeInfo);

    void PostponeRequest(TVolumeInfo& volumeInfo, IEventHandlePtr ev);

    TVolumeInfo* GetVolumeInfoById(ui64 id);

    template <typename TMethod>
    void ForwardRequest(
        const TActorContext& ctx,
        ui64 volumeInfoId,
        ui64 tabletId,
        const TLogTitle& logTitle,
        const typename TMethod::TRequest::TPtr& ev);

    void DescribeVolume(const TActorContext& ctx, TVolumeInfo& volumeInfo);

    void ScheduleActivityCheck(
        const TActorContext& ctx,
        TVolumeInfo& volumeInfo,
        TDuration delay);

private:
    STFUNC(StateWork);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleDisconnected(
        const TEvTabletProxy::TEvDisconnected::TPtr& ev,
        const TActorContext& ctx);

    void HandleBaseDiskDescribeResponse(
        TVolumeInfo* volumeInfo,
        const NProto::TError& error,
        const TActorContext& ctx);

    void HandleDescribeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    template <typename TMethod>
    void HandleRequest(
        const TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    template <typename TMethod>
    void HandleResponse(
        const TActorContext& ctx,
        const typename TMethod::TResponse::TPtr& ev);

    bool HandleRequests(STFUNC_SIG);
    bool LogLateMessage(ui32 evType, const TActorContext& ctx);

    void HandleMapBaseDiskIdToTabletId(
        const TEvVolume::TEvMapBaseDiskIdToTabletId::TPtr& ev,
        const TActorContext& ctx);

    void HandleClearBaseDiskIdToTabletIdMapping(
        const TEvVolume::TEvClearBaseDiskIdToTabletIdMapping::TPtr& ev,
        const TActorContext& ctx);

    void HandlePingRequest(
        const TEvVolumeProxy::TEvKeepAliveRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandlePingResponse(
        const TEvTabletProxy::TEvKeepAliveResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TVolumeProxyActor::TVolumeProxyActor(
    TDuration inactivityTimeout,
    ITraceSerializerPtr traceSerializer,
    bool temporaryServer)
    : TActor(&TThis::StateWork)
    , InactivityTimeout(inactivityTimeout)
    , TraceSerializer(std::move(traceSerializer))
    , TemporaryServer(temporaryServer)
{
}

TVolumeProxyActor::TVolumeInfo* TVolumeProxyActor::FindVolumeInfo(
    const TString& diskId,
    bool exactDiskIdMatch)
{
    if (exactDiskIdMatch) {
        if (TVolumeInfo** volumeInfo =
                VolumeInfoByRealDiskId.FindPtr(diskId))
        {
            return *volumeInfo;
        }
    } else {
        if (TVolumeInfo** volumeInfo = VolumeInfoByDiskId.FindPtr(diskId)) {
            return *volumeInfo;
        }
    }

    return nullptr;
}

TVolumeProxyActor::TVolumeInfo& TVolumeProxyActor::CreateVolumeInfo(
    const TString& diskId,
    bool exactDiskIdMatch)
{
    const ui64 volumeInfoId = ++VolumeInfoIdGenerator;

    auto [it, inserted] = VolumeInfoById.emplace(
        volumeInfoId,
        TVolumeInfo(volumeInfoId, diskId, exactDiskIdMatch, TemporaryServer));

    if (exactDiskIdMatch) {
        VolumeInfoByRealDiskId[diskId] = &it->second;
    } else {
        VolumeInfoByDiskId[diskId] = &it->second;
    }

    return it->second;
}

void TVolumeProxyActor::EraseVolumeInfo(TVolumeInfo* volumeInfo)
{
    // Different DiskIds can resolve to the same RealDiskId. The mapping can
    // belong to another volume info that still uses it.
    auto removeFromMap =
        [volumeInfo](auto& map, const auto& key)
    {
        auto it = map.find(key);
        if (it != map.end() && it->second == volumeInfo) {
            map.erase(it);
        }
    };

    removeFromMap(VolumeInfoByDiskId, volumeInfo->DiskId);
    removeFromMap(VolumeInfoByDiskId, volumeInfo->RealDiskId);
    removeFromMap(VolumeInfoByRealDiskId, volumeInfo->DiskId);
    removeFromMap(VolumeInfoByRealDiskId, volumeInfo->RealDiskId);

    VolumeInfoById.erase(volumeInfo->Id);
}

template <typename TMethod>
void TVolumeProxyActor::ResolveVolumeInfo(
    const TActorContext& ctx,
    TVolumeInfo& volumeInfo)
{
    if (auto* baseDisk = BaseDiskIdToTabletId.FindPtr(volumeInfo.DiskId)) {
        if (!baseDisk->TabletId) {
            ReportVolumeProxyInvalidState(
                "Base disk tablet id is not set",
                {{"VolumeInfoId", volumeInfo.Id},
                 {"DiskId", volumeInfo.DiskId}});

            FailVolumeInfo(
                volumeInfo,
                MakeError(E_INVALID_STATE, "Base disk tablet id is not set"));
            return;
        }

        volumeInfo.TabletId = baseDisk->TabletId;
        volumeInfo.IsBaseDisk = true;

        if constexpr (IsDataPlaneMethod<TMethod>) {
            HandleBaseDiskDescribeResponse(&volumeInfo, {}, ctx);
            return;
        }
    }

    DescribeVolume(ctx, volumeInfo);
}

void TVolumeProxyActor::CompleteVolumeInfo(
    const TActorContext& ctx,
    TVolumeInfo& volumeInfo,
    ui64 tabletId,
    const TString& path,
    const TString& realDiskId)
{
    volumeInfo.LogTitle.SetTabletId(tabletId);
    volumeInfo.RealDiskId = realDiskId;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME_PROXY,
        "%s Resolved TabletID for volume %s with path %s",
        volumeInfo.LogTitle.GetWithTime().c_str(),
        volumeInfo.RealDiskId.Quote().c_str(),
        path.Quote().c_str());

    if (!VolumeInfoByDiskId.contains(volumeInfo.RealDiskId)) {
        VolumeInfoByDiskId[volumeInfo.RealDiskId] = &volumeInfo;
    }

    if (!VolumeInfoByRealDiskId.contains(volumeInfo.RealDiskId)) {
        VolumeInfoByRealDiskId[volumeInfo.RealDiskId] = &volumeInfo;
    }

    volumeInfo.State = RESOLVED;
    volumeInfo.TabletId = tabletId;
    volumeInfo.LastActivity = ctx.Now();
    ScheduleActivityCheck(ctx, volumeInfo, InactivityTimeout);

    ProcessPendingRequests(volumeInfo);
}

void TVolumeProxyActor::FailVolumeInfo(
    TVolumeInfo& volumeInfo,
    NProto::TError error)
{
    volumeInfo.State = FAILED;
    volumeInfo.Error = std::move(error);

    // Cancel all pending requests.
    ProcessPendingRequests(volumeInfo);

    EraseVolumeInfo(&volumeInfo);
}

void TVolumeProxyActor::ProcessPendingRequests(TVolumeInfo& volumeInfo)
{
    auto requests = std::move(volumeInfo.Requests);

    for (auto& ev: requests) {
        TAutoPtr<IEventHandle> handle(ev.release());
        Receive(handle);
    }
}

void TVolumeProxyActor::PostponeRequest(
    TVolumeInfo& volumeInfo,
    IEventHandlePtr ev)
{
    volumeInfo.Requests.emplace_back(std::move(ev));
}

TVolumeProxyActor::TVolumeInfo* TVolumeProxyActor::GetVolumeInfoById(ui64 id)
{
    return VolumeInfoById.FindPtr(id);
}

template <typename TMethod>
void TVolumeProxyActor::ForwardRequest(
    const TActorContext& ctx,
    ui64 volumeInfoId,
    ui64 tabletId,
    const TLogTitle& logTitle,
    const typename TMethod::TRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::VOLUME_PROXY,
        "%s Forward request to volume",
        logTitle.GetWithTime().c_str());

    ui64 requestId = ++RequestId;

    TraceSerializer->BuildTraceRequest(
        *msg->Record.MutableHeaders()->MutableInternal()->MutableTrace(),
        msg->CallContext->LWOrbit);

    auto event = std::make_unique<IEventHandle>(
        ev->Recipient,
        SelfId(),
        ev->ReleaseBase().Release(),
        0,          // flags
        requestId  // cookie
    );

    ActiveRequests.emplace(
        requestId,
        TActiveRequest(
            volumeInfoId,
            IEventHandlePtr(ev.Release()),
            msg->CallContext,
            GetCycleCount()));

    LWTRACK(
        RequestSentPipe,
        msg->CallContext->LWOrbit,
        TMethod::Name,
        msg->CallContext->RequestId);

    ForwardToTablet(tabletId, std::move(event));
}

void TVolumeProxyActor::DescribeVolume(
    const TActorContext& ctx,
    TVolumeInfo& volumeInfo)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME_PROXY,
        "%s Query describe volume",
        volumeInfo.LogTitle.GetWithTime().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
            volumeInfo.DiskId,
            volumeInfo.RequireExactDiskIdMatch),
        volumeInfo.Id);
}

void TVolumeProxyActor::ScheduleActivityCheck(
    const TActorContext& ctx,
    TVolumeInfo& volumeInfo,
    TDuration delay)
{
    ctx.Schedule(delay, new TEvents::TEvWakeup(volumeInfo.Id));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeProxyActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    TVolumeInfo* volumeInfo = GetVolumeInfoById(ev->Get()->Tag);
    if (!volumeInfo) {
        // already erased, nothing to do
        return;
    }

    const TInstant deadline = volumeInfo->LastActivity + InactivityTimeout;
    if (ctx.Now() < deadline) {
        ScheduleActivityCheck(ctx, *volumeInfo, deadline - ctx.Now());
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME_PROXY,
        "%s Remove unused volume info",
        volumeInfo->LogTitle.GetWithTime().c_str());

    EraseVolumeInfo(volumeInfo);
}

void TVolumeProxyActor::HandleDisconnected(
    const TEvTabletProxy::TEvDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    auto it = ActiveRequests.find(ev->Cookie);
    if (it == ActiveRequests.end()) {
        return;
    }

    const ui64 volumeInfoId = it->second.VolumeInfoId;
    TAutoPtr<IEventHandle> handle(it->second.Request.release());
    ActiveRequests.erase(it);
    Receive(handle);

    if (auto* volumeInfo = GetVolumeInfoById(volumeInfoId)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME_PROXY,
            "%s Connection to volume failed: %s",
            volumeInfo->LogTitle.GetWithTime().c_str(),
            FormatError(ev->Get()->GetError()).c_str());

        EraseVolumeInfo(volumeInfo);
    }
}

void TVolumeProxyActor::HandleBaseDiskDescribeResponse(
    TVolumeInfo* volumeInfo,
    const NProto::TError& error,
    const TActorContext& ctx)
{
    if (error.GetCode() == MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME_PROXY,
            "%s Could not resolve path for base disk volume. Error: %s",
            volumeInfo->LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        FailVolumeInfo(*volumeInfo, error);
        return;
    }

    CompleteVolumeInfo(
        ctx,
        *volumeInfo,
        volumeInfo->TabletId,
        "PartitionConfig",
        volumeInfo->DiskId);
}

void TVolumeProxyActor::HandleDescribeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TVolumeInfo* volumeInfo = GetVolumeInfoById(ev->Cookie);
    if (!volumeInfo) {
        return;
    }

    const auto& error = msg->GetError();

    if (volumeInfo->IsBaseDisk) {
        HandleBaseDiskDescribeResponse(volumeInfo, error, ctx);
        return;
    }

    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME_PROXY,
            "%s Could not resolve path for volume. Error: %s",
            volumeInfo->LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        FailVolumeInfo(*volumeInfo, error);
        return;
    }

    const auto& pathDescr = msg->PathDescription;
    const auto& volumeDescr = pathDescr.GetBlockStoreVolumeDescription();
    const auto& realDiskId = volumeDescr.GetName();

    if (volumeInfo->RequireExactDiskIdMatch &&
        volumeInfo->DiskId != realDiskId)
    {
        ReportLogicalDiskIdMismatch(
            {{"DiskId", volumeInfo->DiskId}, {"RealDiskid", realDiskId}});

        FailVolumeInfo(
            *volumeInfo,
            MakeError(
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist),
                TStringBuilder() << "Copy " << realDiskId.Quote()
                                 << " of the disk "
                                 << volumeInfo->DiskId.Quote()
                                 << " was found, when an exact match of the "
                                    "DiskId was required. Pretend that we "
                                    "haven't found anything."));
        return;
    }

    CompleteVolumeInfo(
        ctx,
        *volumeInfo,
        volumeDescr.GetVolumeTabletId(),
        msg->Path,
        realDiskId);
}

template <typename TMethod>
void TVolumeProxyActor::HandleRequest(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev)
{
    // ActiveRequests contains IEventHandles without embedded messages,
    // only information about event type and sender. When the pipe fails
    // before the response, we re-send the IEventHandle from ActiveRequests
    // to ourself, enabling standard path for message processing.
    // So we need to handle the cases when message is re-sent or just came from
    // outside. We have to to check if message buffer is present
    // otherwise it is safe to use Get() to retrieve actual message.
    if (!ev->HasBuffer() && !ev->HasEvent()) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeTabletIsDeadError(E_REJECTED, __LOCATION__));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto* msg = ev->Get();

    const TString& diskId = GetDiskId(*msg);

    if (const ui64 tabletId = GetExternalVolumeTabletId(*msg)) {
        const TLogTitle logTitle(
            GetCycleCount(),
            TLogTitle::TVolumeProxy{
                .DiskId = diskId,
                .TemporaryServer = TemporaryServer,
                .TabletId = tabletId});
        ForwardRequest<TMethod>(ctx, 0, tabletId, logTitle, ev);
        return;
    }

    const bool exactDiskIdMatch =
        msg->Record.GetHeaders().GetExactDiskIdMatch();
    TVolumeInfo* volumeInfo = FindVolumeInfo(diskId, exactDiskIdMatch);
    if (!volumeInfo) {
        volumeInfo = &CreateVolumeInfo(diskId, exactDiskIdMatch);
        PostponeRequest(*volumeInfo, IEventHandlePtr(ev.Release()));
        ResolveVolumeInfo<TMethod>(ctx, *volumeInfo);
        return;
    }

    switch (volumeInfo->State) {
        case RESOLVING: {
            PostponeRequest(*volumeInfo, IEventHandlePtr(ev.Release()));
            break;
        }
        case RESOLVED: {
            volumeInfo->LastActivity = ctx.Now();
            ForwardRequest<TMethod>(
                ctx,
                volumeInfo->Id,
                volumeInfo->TabletId,
                volumeInfo->LogTitle,
                ev);
            break;
        }
        case FAILED: {
            auto response = std::make_unique<typename TMethod::TResponse>(
                volumeInfo->Error);

            NCloud::Reply(ctx, *ev, std::move(response));
            break;
        }
    }
}

template <typename TMethod>
void TVolumeProxyActor::HandleResponse(
    const TActorContext& ctx,
    const typename TMethod::TResponse::TPtr& ev)
{
    auto it = ActiveRequests.find(ev->Cookie);
    if (it == ActiveRequests.end()) {
        // The request was already answered on its disconnect notice
        LogLateMessage(ev->GetTypeRewrite(), ctx);
        return;
    }

    auto* msg = ev->Get();

    if (it->second.CallContext->LWOrbit.HasShuttles()) {
        TraceSerializer->HandleTraceInfo(
            msg->Record.GetHeaders().GetTrace(),
            it->second.CallContext->LWOrbit,
            it->second.SendTime,
            GetCycleCount());
        msg->Record.MutableHeaders()->ClearTrace();
    }

    if constexpr (RequiresThrottling<TMethod>) {
        NProto::TThrottlerInfo& throttler =
            *msg->Record.MutableHeaders()->MutableThrottler();
        const ui64 throttlerDelay = throttler.GetDelay();
        it->second.CallContext->AddTime(
            EProcessingStage::Postponed,
            TDuration::MicroSeconds(throttlerDelay));
        throttler.SetDelay(0);
        it->second.CallContext->SetPossiblePostponeDuration(TDuration::Zero());

        it->second.CallContext->AddTime(
            EProcessingStage::Shaping,
            TDuration::MicroSeconds(throttler.GetShapingDelay()));
        throttler.SetShapingDelay(0);
    }

    LWTRACK(
        ResponseReceivedPipe,
        it->second.CallContext->LWOrbit,
        TMethod::Name,
        it->second.CallContext->RequestId);

    // forward response to the caller
    std::unique_ptr<IEventHandle> event;
    if (ev->HasEvent()) {
        event = std::make_unique<IEventHandle>(
            it->second.Request->Sender,
            ev->Sender,
            ev->ReleaseBase().Release(),
            ev->Flags,
            it->second.Request->Cookie);
    } else {
        event = std::make_unique<IEventHandle>(
            ev->Type,
            ev->Flags,
            it->second.Request->Sender,
            ev->Sender,
            ev->ReleaseChainBuffer(),
            it->second.Request->Cookie);
    }

    // The volume info can be gone by now: dropped as unused or on a disconnect.
    if (TVolumeInfo* volumeInfo = GetVolumeInfoById(it->second.VolumeInfoId)) {
        volumeInfo->LastActivity = ctx.Now();
    }

    ctx.Send(event.release());
    ActiveRequests.erase(it);
}

void TVolumeProxyActor::HandleMapBaseDiskIdToTabletId(
    const TEvVolume::TEvMapBaseDiskIdToTabletId::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();
    auto [it, inserted] = BaseDiskIdToTabletId.try_emplace(
        msg->BaseDiskId,
        TBaseTabletId{.TabletId = msg->BaseTabletId, .RefCount = 1});
    if (!inserted) {
        ++it->second.RefCount;
    }
}

void TVolumeProxyActor::HandleClearBaseDiskIdToTabletIdMapping(
    const TEvVolume::TEvClearBaseDiskIdToTabletIdMapping::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();
    auto itr = BaseDiskIdToTabletId.find(msg->BaseDiskId);
    if (itr != BaseDiskIdToTabletId.end()) {
        --itr->second.RefCount;
        if (itr->second.RefCount == 0) {
            BaseDiskIdToTabletId.erase(itr);
        }
    }
}

void TVolumeProxyActor::HandlePingRequest(
    const TEvVolumeProxy::TEvKeepAliveRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    // The base disk is known from the mapping of an overlay partition or from
    // the volume info resolved for an earlier request.
    ui64 tabletId = 0;
    ui64 volumeInfoId = 0;
    if (TVolumeInfo* volumeInfo = FindVolumeInfo(msg->DiskId, false)) {
        if (volumeInfo->State == RESOLVED) {
            tabletId = volumeInfo->TabletId;
            volumeInfoId = volumeInfo->Id;
        }
    }
    if (const auto* baseDisk = BaseDiskIdToTabletId.FindPtr(msg->DiskId)) {
        if (baseDisk->TabletId) {
            tabletId = baseDisk->TabletId;
        }
    }

    if (!tabletId) {
        auto response = std::make_unique<TEvVolumeProxy::TEvKeepAliveResponse>(
            MakeError(E_INVALID_STATE, "Connection is not established"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    // The tablet proxy owns the pipe and tells whether there is one to keep.
    const ui64 requestId = ++RequestId;
    BaseDiskPingRequests.emplace(
        requestId,
        TBaseDiskPingRequest{
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .VolumeInfoId = volumeInfoId});

    NCloud::Send(
        ctx,
        MakeTabletProxyServiceId(),
        std::make_unique<TEvTabletProxy::TEvKeepAliveRequest>(tabletId),
        requestId);
}

void TVolumeProxyActor::HandlePingResponse(
    const TEvTabletProxy::TEvKeepAliveResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto it = BaseDiskPingRequests.find(ev->Cookie);
    if (it == BaseDiskPingRequests.end()) {
        return;
    }

    const TBaseDiskPingRequest request = it->second;
    BaseDiskPingRequests.erase(it);

    const auto& error = ev->Get()->GetError();
    if (!HasError(error)) {
        if (TVolumeInfo* volumeInfo = GetVolumeInfoById(request.VolumeInfoId)) {
            volumeInfo->LastActivity = ctx.Now();
        }
    }

    NCloud::Reply(
        ctx,
        request,
        std::make_unique<TEvVolumeProxy::TEvKeepAliveResponse>(error));
}

void TVolumeProxyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto error = MakeError(E_REJECTED, "Volume proxy is stopping");
    TVector<IEventHandlePtr> requests;
    for (auto& [id, volumeInfo]: VolumeInfoById) {
        volumeInfo.State = FAILED;
        volumeInfo.Error = error;
        for (auto& request: volumeInfo.Requests) {
            requests.push_back(std::move(request));
        }
        volumeInfo.Requests.clear();
    }

    for (auto& request: requests) {
        TAutoPtr<IEventHandle> handle(request.release());
        Receive(handle);
    }

    for (auto& [requestId, request]: std::exchange(ActiveRequests, {})) {
        TAutoPtr<IEventHandle> handle(request.Request.release());
        Receive(handle);
    }

    for (auto& [requestId, request]: std::exchange(BaseDiskPingRequests, {})) {
        NCloud::Reply(
            ctx,
            request,
            std::make_unique<TEvVolumeProxy::TEvKeepAliveResponse>(error));
    }

    Die(ctx);
}

bool TVolumeProxyActor::HandleRequests(STFUNC_SIG)
{
    auto ctx(ActorContext());
#define BLOCKSTORE_HANDLE_METHOD(name, ns)                                     \
    case ns::TEv##name##Request::EventType: {                                  \
        auto* x = reinterpret_cast<ns::TEv##name##Request::TPtr*>(&ev);        \
        HandleRequest<ns::T##name##Method>(ctx, *x);                           \
        break;                                                                 \
    }                                                                          \
    case ns::TEv##name##Response::EventType: {                                 \
        auto* x = reinterpret_cast<ns::TEv##name##Response::TPtr*>(&ev);       \
        HandleResponse<ns::T##name##Method>(ctx, *x);                          \
        break;                                                                 \
    }                                                                          \
// BLOCKSTORE_HANDLE_METHOD

    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_HANDLE_METHOD, TEvVolume)
        BLOCKSTORE_VOLUME_REQUESTS_FWD_SERVICE(BLOCKSTORE_HANDLE_METHOD, TEvService)

        default:
            return false;
    }

    return true;

#undef BLOCKSTORE_HANDLE_METHOD
}

bool TVolumeProxyActor::LogLateMessage(ui32 evType, const TActorContext& ctx)
{
#define BLOCKSTORE_LOG_MESSAGE(name, ns)                                       \
    case ns::TEv##name##Request::EventType: {                                  \
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME_PROXY,                    \
            "Late request : (0x%08X) %s request",                              \
            evType,                                                            \
            #name);                                                            \
        break;                                                                 \
    }                                                                          \
    case ns::TEv##name##Response::EventType: {                                 \
        LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME_PROXY,                    \
          "Late response : (0x%08X) %s response",                              \
          evType,                                                              \
          #name);                                                              \
        break;                                                                 \
    }                                                                          \
// BLOCKSTORE_LOG_MESSAGE

    switch (evType) {
        BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_LOG_MESSAGE, TEvVolume)
        BLOCKSTORE_VOLUME_REQUESTS_FWD_SERVICE(BLOCKSTORE_LOG_MESSAGE, TEvService)

        default:
            return false;
    }

    return true;

#undef BLOCKSTORE_LOG_MESSAGE
}


STFUNC(TVolumeProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletProxy::TEvDisconnected, HandleDisconnected);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvVolume::TEvMapBaseDiskIdToTabletId,
            HandleMapBaseDiskIdToTabletId);
        HFunc(
            TEvVolume::TEvClearBaseDiskIdToTabletIdMapping,
            HandleClearBaseDiskIdToTabletIdMapping);

        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeResponse);
        HFunc(TEvVolumeProxy::TEvKeepAliveRequest, HandlePingRequest);
        HFunc(TEvTabletProxy::TEvKeepAliveResponse, HandlePingResponse);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::VOLUME_PROXY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateVolumeProxy(
    TStorageConfigConstPtr config,
    ITraceSerializerPtr traceSerializer,
    bool temporaryServer)
{
    return std::make_unique<TVolumeProxyActor>(
        config->GetVolumeProxyPipeInactivityTimeout(),
        std::move(traceSerializer),
        temporaryServer);
}

}   // namespace NCloud::NBlockStore::NStorage
