#include "service_actor.h"

#include <cloud/filestore/libs/storage/core/system_counters.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/mon/mon.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 RequestCookieBit = 63;

////////////////////////////////////////////////////////////////////////////////

TStorageServiceActor::TStorageServiceActor(
        TStorageConfigPtr storageConfig,
        IRequestStatsRegistryPtr statsRegistry,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        TSystemCountersPtr systemCounters,
        NCloud::NStorage::IStatsFetcherPtr statsFetcher)
    : StorageConfig{std::move(storageConfig)}
    , ProfileLog{std::move(profileLog)}
    , TraceSerializer{std::move(traceSerializer)}
    , SystemCounters(std::move(systemCounters))
    , StatsFetcher(std::move(statsFetcher))
    , State{std::make_unique<TStorageServiceState>()}
    , StatsRegistry{std::move(statsRegistry)}
    , InFlightRequests(MakeIntrusive<TInFlightRequestStorage>(ProfileLog))
{}

TStorageServiceActor::~TStorageServiceActor()
{
}

void TStorageServiceActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LastCpuWaitTs = ctx.Monotonic();
    ServiceState = NProto::SERVICE_STATE_RUNNING;

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
            false, ctx.ActorSystem(), SelfId());
    }
}

void TStorageServiceActor::RegisterCounters(const NActors::TActorContext& ctx)
{
    auto* appData = AppData(ctx);
    auto& counters = appData->Counters;
    auto rootGroup = counters->GetSubgroup("counters", "filestore");
    auto serverCounters = rootGroup->GetSubgroup("component", "server");

    CpuWaitCounter = serverCounters->GetCounter("CpuWait", false);

    auto serviceCounters = rootGroup->GetSubgroup("component", "service");
    TotalFileSystemCount = serviceCounters->GetCounter("FileSystemCount", false);
    TotalTabletCount = serviceCounters->GetCounter("TabletCount", false);
    InFlightRequestCount =
        serviceCounters->GetCounter("InFlightRequestCount", false);
    CompletedRequestCountWithLogData = serviceCounters->GetCounter(
        "CompletedRequestCountWithLogData",
        true);
    CompletedRequestCountWithError = serviceCounters->GetCounter(
        "CompletedRequestCountWithError",
        true);
    CompletedRequestCountWithoutErrorOrLogData = serviceCounters->GetCounter(
        "CompletedRequestCountWithoutErrorOrLogData",
        true);

    auto hddCounters = serviceCounters->GetSubgroup("type", "hdd");
    HddFileSystemCount = hddCounters->GetCounter("FileSystemCount", false);
    HddTabletCount = hddCounters->GetCounter("TabletCount", false);

    auto ssdCounters = serviceCounters->GetSubgroup("type", "ssd");
    SsdFileSystemCount = hddCounters->GetCounter("FileSystemCount", false);
    SsdTabletCount = hddCounters->GetCounter("TabletCount", false);
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
    return CreateInFlightRequest(
        info,
        media,
        {} /* checksumCalcInfo */,
        std::move(requestStats),
        start);
}

ui64 TStorageServiceActor::GenerateRequestCookie()
{
    return (++InFlightRequestCounter) | (1LLU << RequestCookieBit);
}

bool TStorageServiceActor::ValidateRequestCookie(ui64 cookie)
{
    return (cookie & (1LLU << RequestCookieBit)) != 0;
}

std::pair<ui64, TInFlightRequest*> TStorageServiceActor::CreateInFlightRequest(
    const TRequestInfo& info,
    NProto::EStorageMediaKind mediaKind,
    TChecksumCalcInfo checksumCalcInfo,
    IRequestStatsPtr requestStats,
    TInstant currentTs)
{
    const ui64 requestCookie = GenerateRequestCookie();
    auto* request = InFlightRequests->Register(
        info.Sender,
        info.Cookie,
        info.CallContext,
        mediaKind,
        std::move(checksumCalcInfo),
        std::move(requestStats),
        currentTs,
        requestCookie);

    return std::make_pair(requestCookie, request);
}

TInFlightRequest* TStorageServiceActor::FindInFlightRequest(ui64 requestCookie)
{
    if (!ValidateRequestCookie(requestCookie)) {
        return nullptr;
    }

    return InFlightRequests->Find(requestCookie);
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

        FILESTORE_REMOTE_SERVICE(FILESTORE_HANDLE_REQUEST_RESPONSE, TEvService)
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
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
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
            msg->IsShard,
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
