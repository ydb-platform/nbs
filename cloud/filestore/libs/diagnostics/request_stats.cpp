#include "request_stats.h"

#include "config.h"
#include "critical_events.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/user_counter.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/request_counters.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

TRequestCountersPtr MakeRequestCounters(ITimerPtr timer, TDynamicCounters& counters)
{
    auto requestCounters = std::make_shared<TRequestCounters>(
        std::move(timer),
        FileStoreRequestCount,
        [] (TRequestCounters::TRequestType t) -> const TString& {
            return GetFileStoreRequestName(static_cast<EFileStoreRequest>(t));
        },
        [] (TRequestCounters::TRequestType t) {
            auto rt = static_cast<EFileStoreRequest>(t);
            return rt == EFileStoreRequest::WriteData
                || rt == EFileStoreRequest::ReadData;
        },
        TRequestCounters::EOption::ReportDataPlaneHistogram |
        TRequestCounters::EOption::ReportControlPlaneHistogram
    );
    requestCounters->Register(counters);
    return requestCounters;
}

////////////////////////////////////////////////////////////////////////////////

class TRequestLogger
{
public:
    void LogStarted(TLog& Log, TCallContext& callContext) const
    {
        STORAGE_LOG(TLOG_RESOURCES, callContext.LogString() << " REQUEST");
    }

    void LogCompleted(
        TLog& Log,
        TCallContext& callContext,
        TDuration requestTime,
        EDiagnosticsErrorKind errorKind) const
    {
        ELogPriority logPriority;
        TStringBuf message;

        // TODO pass slow req threshold via diagnostic config
        if (requestTime >= TDuration::Seconds(10)) {
            logPriority = TLOG_WARNING;
            message = "request too slow";
        } else if (errorKind == EDiagnosticsErrorKind::ErrorFatal) {
            logPriority = TLOG_ERR;
            message = "request failed";
        } else {
            logPriority = TLOG_RESOURCES;
            message = "request completed";
        }

        STORAGE_LOG(logPriority, LogHeader(callContext)
            << " RESPONSE " << message
            << " (time: " << FormatDuration(requestTime)
            << ", size: " << FormatByteSize(callContext.RequestSize)
            << ", error: " << FormatError(callContext.Error)
            << ")");
    }

    virtual TString LogHeader(const TCallContext& callcontext) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TRequestStats final
    : public IRequestStats
    , IIncompleteRequestCollector
    , TRequestLogger
{
    TDynamicCountersPtr RootCounters;

    TRequestCountersPtr TotalCounters;
    TRequestCountersPtr SsdCounters;
    TRequestCountersPtr HddCounters;

    TMutex ProvidersLock;
    TVector<IIncompleteRequestProviderPtr> IncompleteRequestProviders;

    TRWMutex FsLock;
    THashMap<TString, NProto::EStorageMediaKind> FileSystems;

public:
    TRequestStats(
            TDynamicCountersPtr counters,
            ITimerPtr timer)
        : RootCounters(std::move(counters))
        , TotalCounters(MakeRequestCounters(timer, *RootCounters))
        , SsdCounters(MakeRequestCounters(timer, *RootCounters->GetSubgroup("type", "ssd")))
        , HddCounters(MakeRequestCounters(timer, *RootCounters->GetSubgroup("type", "hdd")))
    {
        auto revisionGroup = RootCounters->GetSubgroup("revision", GetFullVersionString());

        auto versionCounter = revisionGroup->GetCounter(
            "version",
            false);
        *versionCounter = 1;

        InitCriticalEventsCounter(RootCounters);
    }

    void RequestStarted(TCallContext& callContext) override
    {
        const auto requestType = static_cast<TRequestCounters::TRequestType>(callContext.RequestType);
        if (const auto media = GetMediaKind(callContext); media != NProto::STORAGE_MEDIA_DEFAULT) {
            GetCounters(media)->RequestStarted(requestType, callContext.RequestSize);
        }

        auto cycles = TotalCounters->RequestStarted(requestType, callContext.RequestSize);
        callContext.SetRequestStartedCycles(cycles);
    }

    void RequestCompleted(TCallContext& callContext) override
    {
        RequestCompleted(callContext, GetDiagnosticsErrorKind(callContext.Error));
    }

    void RequestStarted(TLog& log, TCallContext& callContext) override
    {
        RequestStarted(callContext);
        LogStarted(log, callContext);
    }

    void RequestCompleted(TLog& log, TCallContext& callContext) override
    {
        const auto errorKind = GetDiagnosticsErrorKind(callContext.Error);
        const auto requestTime = RequestCompleted(callContext, errorKind);
        LogCompleted(log, callContext, requestTime, errorKind);
    }

    void ResponseSent(TCallContext& callContext) override
    {
        Y_UNUSED(callContext);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        with_lock (ProvidersLock) {
            for (const auto& provider: IncompleteRequestProviders) {
                provider->Accept(*this);
            }
        }

        TotalCounters->UpdateStats(updatePercentiles);
        SsdCounters->UpdateStats(updatePercentiles);
        HddCounters->UpdateStats(updatePercentiles);
    }

    void RegisterIncompleteRequestProvider(IIncompleteRequestProviderPtr provider) override
    {
        TGuard g(ProvidersLock);
        IncompleteRequestProviders.push_back(std::move(provider));
    }

    void Reset() override
    {
        for (auto& p: IncompleteRequestProviders) {
            p.reset();
        }
    }

    void Collect(const TIncompleteRequest& req) override
    {
        const auto& executionTime = req.RequestTime;
        const auto& totalTime = req.RequestTime;

        TotalCounters->AddIncompleteStats(
            static_cast<TRequestCounters::TRequestType>(req.RequestType),
            executionTime,
            totalTime,
            ECalcMaxTime::ENABLE);
        if (req.MediaKind != NProto::STORAGE_MEDIA_DEFAULT) {
            GetCounters(req.MediaKind)->AddIncompleteStats(
                static_cast<TRequestCounters::TRequestType>(req.RequestType),
                executionTime,
                totalTime,
                ECalcMaxTime::ENABLE);
        }
    }

    TRequestCountersPtr GetTotalCounters() const
    {
        return TotalCounters;
    }

    TRequestCountersPtr GetCounters(NProto::EStorageMediaKind media) const
    {
        switch (media) {
            case NProto::STORAGE_MEDIA_SSD: return SsdCounters;
            default: return HddCounters;
        }
    }

    void SetFileSystemMediaKind(const TString& fileSystemId, NProto::EStorageMediaKind media)
    {
        TWriteGuard g(FsLock);
        FileSystems[fileSystemId] = media;
    }

private:
    NProto::EStorageMediaKind GetMediaKind(const TCallContext& callContext) const
    {
        NProto::EStorageMediaKind media = NProto::STORAGE_MEDIA_DEFAULT;
        TReadGuard g(FsLock);
        if (const auto it = FileSystems.find(callContext.FileSystemId); it != FileSystems.end()) {
            media = it->second;
        }

        return media;
    }

    TDuration RequestCompleted(
        const TCallContext& callContext,
        EDiagnosticsErrorKind errorKind) const
    {
        const auto requestType = static_cast<TRequestCounters::TRequestType>(callContext.RequestType);
        if (const auto media = GetMediaKind(callContext); media != NProto::STORAGE_MEDIA_DEFAULT) {
            GetCounters(media)->RequestCompleted(
                requestType,
                callContext.GetRequestStartedCycles(),
                {}, // postponedTime
                callContext.RequestSize,
                errorKind,
                NCloud::NProto::EF_NONE,
                callContext.Unaligned,
                ECalcMaxTime::ENABLE,
                0);
        }

        return TotalCounters->RequestCompleted(
            requestType,
            callContext.GetRequestStartedCycles(),
            {}, // postponedTime
            callContext.RequestSize,
            errorKind,
            NCloud::NProto::EF_NONE,
            callContext.Unaligned,
            ECalcMaxTime::ENABLE,
            0);
    }

    TString LogHeader(const TCallContext& callContext) const override
    {
        return callContext.LogString();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TUserMetadata
{
    TString CloudId;
    TString FolderId;
};

class TFileSystemStats final
    : public IRequestStats
    , IIncompleteRequestCollector
    , TRequestLogger
{
private:
    const TString FileSystemId;
    const TString ClientId;

    TRequestCountersPtr Counters;
    TMutex Lock;
    TVector<IIncompleteRequestProviderPtr> IncompleteRequestProviders;

    TUserMetadata UserMetadata;

public:
    TFileSystemStats(
            TString fileSystemId,
            TString clientId,
            ITimerPtr timer,
            TDynamicCountersPtr counters)
        : FileSystemId{std::move(fileSystemId)}
        , ClientId{std::move(clientId)}
        , Counters{MakeRequestCounters(std::move(timer), *counters)}
    {
    }

    void SetUserMetadata(TUserMetadata userMetadata)
    {
        UserMetadata = std::move(userMetadata);
    }

    const TUserMetadata& GetUserMetadata() const
    {
        return UserMetadata;
    }

    void Subscribe(TRequestCountersPtr subscriber)
    {
        Counters->Subscribe(std::move(subscriber));
    }

    void RequestStarted(TCallContext& callContext) override
    {
        callContext.SetRequestStartedCycles(Counters->RequestStarted(
            static_cast<TRequestCounters::TRequestType>(callContext.RequestType),
            callContext.RequestSize));
    }

    void RequestCompleted(TCallContext& callContext) override
    {
        RequestCompleted(callContext, GetDiagnosticsErrorKind(callContext.Error));
    }

    void RequestStarted(TLog& log, TCallContext& callContext) override
    {
        RequestStarted(callContext);
        LogStarted(log, callContext);
    }

    void RequestCompleted(TLog& log, TCallContext& callContext) override
    {
        const auto errorKind = GetDiagnosticsErrorKind(callContext.Error);
        const auto requestTime = RequestCompleted(callContext, errorKind);
        LogCompleted(log, callContext, requestTime, errorKind);
    }

    void ResponseSent(TCallContext& callContext) override
    {
        Y_UNUSED(callContext);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        with_lock (Lock) {
            for (auto& provider: IncompleteRequestProviders) {
                provider->Accept(*this);
            }
        }

        Counters->UpdateStats(updatePercentiles);
    }

    void RegisterIncompleteRequestProvider(IIncompleteRequestProviderPtr provider) override
    {
        TGuard g(Lock);
        IncompleteRequestProviders.push_back(std::move(provider));
    }

    void Collect(const TIncompleteRequest& req) override
    {
        Counters->AddIncompleteStats(
            static_cast<ui32>(req.RequestType),
            req.RequestTime,
            req.RequestTime,
            ECalcMaxTime::ENABLE);
    }

    void Reset() override
    {
        for (auto& provider: IncompleteRequestProviders) {
            provider.reset();
        }
    }

private:
    TDuration RequestCompleted(
        const TCallContext& callContext,
        EDiagnosticsErrorKind errorKind) const
    {
        return Counters->RequestCompleted(
            static_cast<TRequestCounters::TRequestType>(callContext.RequestType),
            callContext.GetRequestStartedCycles(),
            {}, // postponedTime
            callContext.RequestSize,
            errorKind,
            NCloud::NProto::EF_NONE,
            callContext.Unaligned,
            ECalcMaxTime::ENABLE,
            0);
    }

    TString LogHeader(const TCallContext& callContext) const override
    {
        return TStringBuilder()
            << callContext.LogString()
            << "[c:" << ClientId << "]";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestStatsStub final
    : public IRequestStats
{
public:
    void RequestStarted(TCallContext& callContext) override
    {
        Y_UNUSED(callContext);
    }

    void RequestCompleted(TCallContext& callContext) override
    {
        Y_UNUSED(callContext);
    }

    void RequestStarted(TLog& log, TCallContext& callContext) override
    {
        Y_UNUSED(log);
        Y_UNUSED(callContext);
    }

    void RequestCompleted(TLog& log, TCallContext& callContext) override
    {
        Y_UNUSED(log);
        Y_UNUSED(callContext);
    }

    void ResponseSent(TCallContext& callContext) override
    {
        Y_UNUSED(callContext);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);
    }

    void RegisterIncompleteRequestProvider(IIncompleteRequestProviderPtr provider) override
    {
        Y_UNUSED(provider);
    }

    void Reset() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TRequestStatsRegistry final
    : public IRequestStatsRegistry
{
private:
    struct TFileSystemStatsHolder
        : private TAtomicRefCount<TFileSystemStatsHolder>
    {
        std::shared_ptr<TFileSystemStats> Stats;

        explicit TFileSystemStatsHolder(std::shared_ptr<TFileSystemStats> stats)
            : Stats(std::move(stats))
        {}

        std::shared_ptr<TFileSystemStats> Acquire()
        {
            Ref();
            return Stats;
        }

        // Returns true, when ref_count == 0 and it's time to be destroyed.
        bool Release()
        {
            DecRef();
            return !RefCount();
        }
    };

    TDiagnosticsConfigPtr DiagnosticsConfig;
    NMonitoring::TDynamicCountersPtr RootCounters;
    ITimerPtr Timer;

    std::shared_ptr<TRequestStats> RequestStats;

    TDynamicCountersPtr FsCounters;
    TRWMutex Lock;
    THashMap<std::pair<TString, TString>, TFileSystemStatsHolder> StatsMap;
    std::shared_ptr<NUserCounter::IUserCounterSupplier> UserCounters;

public:
    TRequestStatsRegistry(
            TString component,
            TDiagnosticsConfigPtr diagnosticsConfig,
            NMonitoring::TDynamicCountersPtr rootCounters,
            ITimerPtr timer,
            std::shared_ptr<NUserCounter::IUserCounterSupplier> userCounters)
        : DiagnosticsConfig(std::move(diagnosticsConfig))
        , RootCounters(std::move(rootCounters))
        , Timer(std::move(timer))
        , UserCounters(std::move(userCounters))
    {
        auto totalCounters = RootCounters
            ->GetSubgroup("component", component);
        RequestStats = std::make_shared<TRequestStats>(std::move(totalCounters), Timer);

        FsCounters = RootCounters
            ->GetSubgroup("component", component + "_fs")
            ->GetSubgroup("host", "cluster");
    }

    IRequestStatsPtr GetFileSystemStats(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        std::pair<TString, TString> key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);

        auto it = StatsMap.find(key);
        if (it == StatsMap.end()) {
            auto counters = FsCounters
                ->GetSubgroup("filesystem", fileSystemId)
                ->GetSubgroup("client", clientId);

            auto stats = CreateRequestStats(
                fileSystemId,
                clientId,
                std::move(counters),
                Timer);
            it = StatsMap.emplace(key, stats).first;
            stats->Subscribe(RequestStats->GetTotalCounters());
        }

        return it->second.Acquire();
    }

    void SetFileSystemMediaKind(
        const TString& fileSystemId,
        const TString& clientId,
        NProto::EStorageMediaKind media) override
    {
        TReadGuard guard{Lock};
        auto it = StatsMap.find(std::make_pair(fileSystemId, clientId));
        Y_ENSURE(
            it != StatsMap.end(),
            "unable to find (filesystem, client) to set media kind:"
            << " (" << fileSystemId << ", " << clientId << ")");
        it->second.Stats->Subscribe(RequestStats->GetCounters(media));
        RequestStats->SetFileSystemMediaKind(fileSystemId, media);
    }

    void RegisterUserStats(
        const TString& cloudId,
        const TString& folderId,
        const TString& fileSystemId,
        const TString& clientId) override
    {
        std::pair<TString, TString> key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);
        if (auto it = StatsMap.find(key); it != StatsMap.end() && UserCounters) {

            it->second.Stats->SetUserMetadata(
                {.CloudId = cloudId, .FolderId = folderId});

            auto counters = FsCounters
                ->GetSubgroup("filesystem", fileSystemId)
                ->GetSubgroup("client", clientId);

            NUserCounter::RegisterFilestore(
                *UserCounters,
                cloudId,
                folderId,
                fileSystemId,
                clientId,
                counters);
        }
    }

    IRequestStatsPtr GetRequestStats() override
    {
        return RequestStats;
    }

    void Unregister(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        std::pair<TString, TString> key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);
        auto it = StatsMap.find(key);
        if (it == StatsMap.end()) {
            return;
        }

        if (it->second.Release()) {
            const auto& userMetadata = it->second.Stats->GetUserMetadata();
            NUserCounter::UnregisterFilestore(
                *UserCounters,
                userMetadata.CloudId,
                userMetadata.FolderId,
                fileSystemId,
                clientId);
            it->second.Stats->Reset();
            StatsMap.erase(it);
            FsCounters->GetSubgroup("filesystem", fileSystemId)
                ->RemoveSubgroup("client", clientId);
        }
    }

    void AddIncompleteRequest(const TIncompleteRequest& req) override
    {
        RequestStats->Collect(req);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        TReadGuard guard(Lock);
        for (auto& [_, holder]: StatsMap) {
            holder.Stats->UpdateStats(updatePercentiles);
        }

        RequestStats->UpdateStats(updatePercentiles);
    }

private:
    std::shared_ptr<TFileSystemStats> CreateRequestStats(
        TString fileSystemId,
        TString clientId,
        TDynamicCountersPtr counters,
        ITimerPtr timer) const
    {
        return std::make_shared<TFileSystemStats>(
            std::move(fileSystemId),
            std::move(clientId),
            std::move(timer),
            std::move(counters));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestStatsRegistryStub final
    : public IRequestStatsRegistry
{
public:
    TRequestStatsRegistryStub() = default;

    IRequestStatsPtr GetFileSystemStats(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);

        return std::make_shared<TRequestStatsStub>();
    }

    void SetFileSystemMediaKind(
        const TString& fileSystemId,
        const TString& clientId,
        NProto::EStorageMediaKind media) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(media);
    }

    void RegisterUserStats(
        const TString& cloudId,
        const TString& folderId,
        const TString& fileSystemId,
        const TString& clientId) override
    {
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
    }

    IRequestStatsPtr GetRequestStats() override
    {
        return std::make_shared<TRequestStatsStub>();
    }

    void Unregister(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
    }

    void AddIncompleteRequest(const TIncompleteRequest& request) override
    {
        Y_UNUSED(request);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestStatsRegistryPtr CreateRequestStatsRegistry(
    TString component,
    TDiagnosticsConfigPtr diagnosticsConfig,
    NMonitoring::TDynamicCountersPtr rootCounters,
    ITimerPtr timer,
    std::shared_ptr<NUserCounter::IUserCounterSupplier> userCounters)
{
    return std::make_shared<TRequestStatsRegistry>(
        std::move(component),
        std::move(diagnosticsConfig),
        std::move(rootCounters),
        std::move(timer),
        std::move(userCounters));
}

IRequestStatsRegistryPtr CreateRequestStatsRegistryStub()
{
    return std::make_shared<TRequestStatsRegistryStub>();
}

}   // namespace NCloud::NFileStore
