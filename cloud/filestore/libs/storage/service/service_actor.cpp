#include "service_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

#define MAKE_PROXY_COOKIE(cookie)        ((cookie) | (1llu << 63))
#define VALIDATE_PROXY_COOKIE(cookie)    ((cookie) & (1llu << 63))

////////////////////////////////////////////////////////////////////////////////

TStorageServiceActor::TStorageServiceActor(
        TStorageConfigPtr storageConfig,
        IRequestStatsRegistryPtr statsRegistry,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        NCloud::NStorage::ICgroupStatsFetcherPtr cgroupStatsFetcher)
    : StorageConfig{std::move(storageConfig)}
    , ProfileLog{std::move(profileLog)}
    , TraceSerializer{std::move(traceSerializer)}
    , CgroupStatsFetcher(std::move(cgroupStatsFetcher))
    , State{std::make_unique<TStorageServiceState>()}
    , StatsRegistry{std::move(statsRegistry)}
{}

TStorageServiceActor::~TStorageServiceActor()
{
}

void TStorageServiceActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    RegisterPages(ctx);
    RegisterCounters(ctx);
    ScheduleUpdateStats(ctx);
}

void TStorageServiceActor::RegisterPages(const NActors::TActorContext& ctx)
{
    auto* appData = AppData(ctx);
    auto* mon = appData->Mon;
    if (mon) {
        auto* rootPage = mon->RegisterIndexPage("filestore", "FileStore");

        mon->RegisterActorPage(rootPage, "service", "Service",
            false, ctx.ExecutorThread.ActorSystem, SelfId());
    }

}

void TStorageServiceActor::RegisterCounters(const NActors::TActorContext& ctx)
{
    auto* appData = AppData(ctx);
    auto& counters = appData->Counters;
    auto rootGroup = counters->GetSubgroup("counters", "filestore");
    auto serverCounters = rootGroup->GetSubgroup("component", "server");

    CpuWait = serverCounters->GetCounter("CpuWait", false);
}

void TStorageServiceActor::ScheduleUpdateStats(const NActors::TActorContext& ctx)
{
    ctx.Schedule(
        UpdateStatsInterval,
        new TEvServicePrivate::TEvUpdateStats{});
}

std::pair<ui64, TInFlightRequest*> TStorageServiceActor::CreateInFlightRequest(
    const TRequestInfo& info,
    NProto::EStorageMediaKind media,
    IRequestStatsPtr requestStats,
    TInstant start)
{
    const ui64 cookie = MAKE_PROXY_COOKIE(++ProxyCounter);
    auto [it, inserted] = InFlightRequests.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(cookie),
        std::forward_as_tuple(
            info,
            ProfileLog,
            media,
            requestStats));

    Y_ABORT_UNLESS(inserted);
    it->second.Start(start);

    return std::make_pair(cookie, &it->second);
}

TInFlightRequest* TStorageServiceActor::FindInFlightRequest(ui64 cookie)
{
    if (!VALIDATE_PROXY_COOKIE(cookie)) {
        return nullptr;
    }

    return InFlightRequests.FindPtr(cookie);
}

TString TStorageServiceActor::LogTag(
    const TString& fsId,
    const TString& clientId,
    const TString& sessionId,
    ui64 seqNo) const
{
    return TStringBuilder() <<
        "[f:" << fsId << ']' <<
        "[c:" << clientId << ']' <<
        "[s:" << sessionId << ']' <<
        "[n:" << seqNo << ']';
}

////////////////////////////////////////////////////////////////////////////////

bool TStorageServiceActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
    #define FILESTORE_HANDLE_REQUEST_RESPONSE(name, ns) \
        FILESTORE_HANDLE_REQUEST(name, ns)              \
        FILESTORE_HANDLE_RESPONSE(name, ns)             \

        FILESTORE_SERVICE(FILESTORE_HANDLE_REQUEST_RESPONSE, TEvService)
        FILESTORE_SERVICE_REQUESTS_PRIVATE(FILESTORE_HANDLE_REQUEST_RESPONSE, TEvServicePrivate)
    #undef FILESTORE_HANDLE_REQUEST_RESPONSE

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(TEvService::TEvRegisterLocalFileStoreRequest, HandleRegisterLocalFileStore);
        HFunc(TEvService::TEvUnregisterLocalFileStoreRequest, HandleUnregisterLocalFileStore);

        HFunc(TEvServicePrivate::TEvSessionCreated, HandleSessionCreated);
        HFunc(TEvServicePrivate::TEvSessionDestroyed, HandleSessionDestroyed);
        HFunc(TEvServicePrivate::TEvUpdateStats, HandleUpdateStats);

        default:
            return false;
    }

    return true;
}

STFUNC(TStorageServiceActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE);
            }
            break;
    }
}

void TStorageServiceActor::HandleRegisterLocalFileStore(
    const TEvService::TEvRegisterLocalFileStoreRequest::TPtr& ev,
    const TActorContext&)
{
    auto* msg = ev->Get();
    if (State) {
        State->RegisterLocalFileStore(
            msg->FileStoreId,
            msg->TabletId,
            msg->Generation,
            std::move(msg->Config));
    }
}

void TStorageServiceActor::HandleUnregisterLocalFileStore(
    const TEvService::TEvUnregisterLocalFileStoreRequest::TPtr& ev,
    const TActorContext&)
{
    auto* msg = ev->Get();
    if (State) {
        State->UnregisterLocalFileStore(
            msg->FileStoreId,
            msg->Generation);
    }
}

}   // namespace NCloud::NFileStore::NStorage
