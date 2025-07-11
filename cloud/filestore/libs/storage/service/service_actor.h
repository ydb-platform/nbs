#pragma once

#include "public.h"

#include "service_private.h"
#include "service_state.h"

#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/request_info.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NProto {
    class TProfileLogRequestInfo;
}   // namespace NCloud::NFileStore::NProto

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TStorageServiceActor final
    : public NActors::TActorBootstrapped<TStorageServiceActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const IProfileLogPtr ProfileLog;
    const ITraceSerializerPtr TraceSerializer;
    const NCloud::NStorage::IStatsFetcherPtr StatsFetcher;

    std::unique_ptr<TStorageServiceState> State;
    ui64 ProxyCounter = 0;

    IRequestStatsRegistryPtr StatsRegistry;
    THashMap<ui64, TInFlightRequest> InFlightRequests;

    NMonitoring::TDynamicCounters::TCounterPtr CpuWaitCounter;

    NMonitoring::TDynamicCounters::TCounterPtr TotalFileSystemCount;
    NMonitoring::TDynamicCounters::TCounterPtr TotalTabletCount;

    NMonitoring::TDynamicCounters::TCounterPtr HddFileSystemCount;
    NMonitoring::TDynamicCounters::TCounterPtr HddTabletCount;

    NMonitoring::TDynamicCounters::TCounterPtr SsdFileSystemCount;
    NMonitoring::TDynamicCounters::TCounterPtr SsdTabletCount;

    TInstant LastCpuWaitTs;

public:
    TStorageServiceActor(
        TStorageConfigPtr storageConfig,
        IRequestStatsRegistryPtr statsRegistry,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        NCloud::NStorage::IStatsFetcherPtr statsFetcher);
    ~TStorageServiceActor();

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterPages(const NActors::TActorContext& ctx);
    void RegisterCounters(const NActors::TActorContext& ctx);
    void ScheduleUpdateStats(const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo_Search(
        const NActors::NMon::TEvHttpInfo::TPtr& ev,
        const TString& filesystemId,
        const NActors::TActorContext& ctx);

    void HandleUpdateStats(
        const TEvServicePrivate::TEvUpdateStats::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ForwardRequest(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    template <typename TMethod>
    void ForwardRequestToShard(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev,
        ui32 shardNo);
    
    template <typename TMethod>
    TSessionInfo* GetAndValidateSession(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    template <typename TMethod>
    void ForwardXAttrRequest(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev,
        const TSessionInfo*);

    template <typename TMethod>
    void CompleteRequest(
        const NActors::TActorContext& ctx,
        const typename TMethod::TResponse::TPtr& ev);

    bool HandleRequests(STFUNC_SIG);

#define FILESTORE_DECLARE_REQUEST_RESPONSE(name, ns)                           \
    FILESTORE_IMPLEMENT_REQUEST(name, ns)                                      \
                                                                               \
    void Handle##name(                                                         \
        const ns::TEv##name##Response::TPtr& ev,                               \
        const NActors::TActorContext& ctx);                                    \

    FILESTORE_REMOTE_SERVICE(FILESTORE_DECLARE_REQUEST_RESPONSE, TEvService)
#undef FILESTORE_DECLARE_REQUEST_RESPONSE

    STFUNC(StateWork);

    void HandleRegisterLocalFileStore(
        const TEvService::TEvRegisterLocalFileStoreRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleUnregisterLocalFileStore(
        const TEvService::TEvUnregisterLocalFileStoreRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSessionCreated(
        const TEvServicePrivate::TEvSessionCreated::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleSessionDestroyed(
        const TEvServicePrivate::TEvSessionDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    std::pair<ui64, TInFlightRequest*> CreateInFlightRequest(
        const TRequestInfo& info,
        NProto::EStorageMediaKind media,
        IRequestStatsPtr requestStats,
        TInstant currentTs);

    TInFlightRequest* FindInFlightRequest(ui64 cookie);

    bool RemoveSession(
        const TString& sessionId,
        ui64 seqNo,
        const NActors::TActorContext& ctx);

    void RemoveSession(
        const TString& sessionId,
        const NActors::TActorContext& ctx);

    TResultOrError<TString> SelectShard(
        const NActors::TActorContext& ctx,
        const TString& sessionId,
        const ui64 seqNo,
        const bool disableMultiTabletForwarding,
        const TString& methodName,
        const ui64 requestId,
        const NProto::TFileStore& filestore,
        const ui32 shardNo) const;

private:
    // actions
    NActors::IActorPtr CreateDrainTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateGetStorageConfigFieldsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateGetStorageConfigActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateChangeStorageConfigActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateDescribeSessionsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateForcedOperationActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateForcedOperationStatusActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateReassignTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateConfigureShardsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateConfigureAsShardActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateWriteCompactionMapActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateUnsafeDeleteNodeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateUnsafeUpdateNodeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateUnsafeGetNodeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateGetStorageStatsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateListLocalFileStoresActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateRestartTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    NActors::IActorPtr CreateGetFileSystemTopologyActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

private:
    void RenderSessions(IOutputStream& out);
    void RenderLocalFileStores(IOutputStream& out);

    TString LogTag(
        const TString& fsId,
        const TString& clientId,
        const TString& sessionId,
        ui64 seqNo) const;
};

}   // namespace NCloud::NFileStore::NStorage
