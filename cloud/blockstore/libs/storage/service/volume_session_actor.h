#pragma once

#include "public.h"

#include "service_counters.h"
#include "service_events_private.h"
#include "service_state.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/volume_proxy/volume_proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TVolumeSessionActor final
    : public NActors::TActorBootstrapped<TVolumeSessionActor>
{
private:
    enum EVolumeRequest
    {
        NONE,
        START_REQUEST,
        STOP_REQUEST
    };

    struct TMountRequestProcResult
    {
        TMaybe<NProto::TError> Error;
        bool MountOptionsChanged = false;
    };

private:
    const TVolumeInfoPtr VolumeInfo;
    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ITraceSerializerPtr TraceSerializer;
    const NServer::IEndpointEventHandlerPtr EndpointEventHandler;
    const NRdma::IClientPtr RdmaClient;
    const std::shared_ptr<NKikimr::TTabletCountersBase> Counters;
    const TSharedServiceCountersPtr SharedCounters;
    const TString InitialClientId;
    const bool TemporaryServer;

    TLogTitle LogTitle{
        GetCycleCount(),
        TLogTitle::TSession{
            .SessionId = VolumeInfo->SessionId,
            .DiskId = VolumeInfo->DiskId,
            .TemporaryServer = TemporaryServer}};

    TQueue<NActors::IEventHandlePtr> MountUnmountRequests;

    bool IsClientsCheckScheduled = false;

    NActors::TActorId VolumeClient;
    ui64 TabletId = 0;

    NActors::TActorId StartVolumeActor;
    NActors::TActorId MountRequestActor;
    NActors::TActorId UnmountRequestActor;

    ui64 LastPipeResetTick = 0;

    EVolumeRequest CurrentRequest = NONE;

    bool ShuttingDown = false;
    NProto::TError ShuttingDownError;

public:
    TVolumeSessionActor(
        TVolumeInfoPtr volumeInfo,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ITraceSerializerPtr traceSerializer,
        NServer::IEndpointEventHandlerPtr endpointEventHandler,
        NRdma::IClientPtr rdmaClient,
        std::shared_ptr<NKikimr::TTabletCountersBase> counters,
        TSharedServiceCountersPtr sharedCounters,
        TString clientId,
        bool temporaryServer)
        : VolumeInfo(std::move(volumeInfo))
        , Config(std::move(config))
        , DiagnosticsConfig(std::move(diagnosticsConfig))
        , ProfileLog(std::move(profileLog))
        , BlockDigestGenerator(std::move(blockDigestGenerator))
        , TraceSerializer(std::move(traceSerializer))
        , EndpointEventHandler(std::move(endpointEventHandler))
        , RdmaClient(std::move(rdmaClient))
        , Counters(std::move(counters))
        , SharedCounters(std::move(sharedCounters))
        , InitialClientId(std::move(clientId))
        , TemporaryServer(temporaryServer)
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void DescribeVolume(const NActors::TActorContext& ctx);

    void RemoveInactiveClients(const NActors::TActorContext& ctx);

    void ScheduleInactiveClientsRemoval(const NActors::TActorContext& ctx);

    void ReceiveNextMountOrUnmountRequest(const NActors::TActorContext& ctx);

    TMountRequestProcResult ProcessMountRequest(
        const NActors::TActorContext& ctx,
        const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
        ui64 tick);

    template <typename TProtoRequest>
    void AddClientToVolume(
        const NActors::TActorContext& ctx,
        const TProtoRequest& mountRequest,
        ui64 mountTick);

    void LogNewClient(
        const NActors::TActorContext& ctx,
        const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
        ui64 tick);

    void SendUnmountVolumeResponse(
        const NActors::TActorContext& ctx,
        const TEvService::TEvUnmountVolumeRequest::TPtr& ev,
        NProto::TError error = {});

    void NotifyAndDie(const NActors::TActorContext& ctx);

    // This function should be called only when we need to fail pending
    // requests and terminate this actor. Error codes like S_OK or S_ALREADY are
    // not allowed
    void FailPendingRequestsAndDie(
        const NActors::TActorContext& ctx,
        NProto::TError error);

private:
    STFUNC(StateDescribe);
    STFUNC(StateWork);

    void HandleInternalMountVolume(
        const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleInactiveClientsTimeout(
        const TEvServicePrivate::TEvInactiveClientsTimeout::TPtr& ev,
        const NActors::TActorContext& ctx);

    void PostponeMountVolume(
        const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleMountRequestProcessed(
        const TEvServicePrivate::TEvMountRequestProcessed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void PostponeChangeVolumeBindingRequest(
        const TEvService::TEvChangeVolumeBindingRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUnmountVolume(
        const TEvService::TEvUnmountVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void PostponeUnmountVolume(
        const TEvService::TEvUnmountVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUnmountRequestProcessed(
        const TEvServicePrivate::TEvUnmountRequestProcessed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleVolumePipeReset(
        const TEvServicePrivate::TEvVolumePipeReset::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleStartVolumeRequest(
        const TEvServicePrivate::TEvStartVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleStopVolumeRequest(
        const TEvServicePrivate::TEvStopVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleVolumeTabletStatus(
        const TEvServicePrivate::TEvVolumeTabletStatus::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleStartVolumeActorStopped(
        const TEvServicePrivate::TEvStartVolumeActorStopped::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChangeVolumeBindingRequest(
        const TEvService::TEvChangeVolumeBindingRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateAddClientActor(
    std::unique_ptr<TEvVolume::TEvAddClientRequest> request,
    TRequestInfoPtr requestInfo,
    TDuration timeout,
    TDuration backoffTimeoutIncrement,
    NActors::TActorId volumeProxy);

NActors::IActorPtr CreateWaitReadyActor(
    std::unique_ptr<TEvVolume::TEvWaitReadyRequest> request,
    TRequestInfoPtr requestInfo,
    TDuration timeout,
    TDuration backoffTimeoutIncrement,
    NActors::TActorId volumeProxy);

}   // namespace NCloud::NBlockStore::NStorage
