#include "request_stats.h"

#include "config.h"
#include "critical_events.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/user_counter.h>
#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/busy_idle_calculator.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/postpone_time_predictor.h>
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

bool IsReadWriteRequest(EFileStoreRequest rt)
{
    return rt == EFileStoreRequest::WriteData
        || rt == EFileStoreRequest::ReadData;
}

const auto REQUEST_COUNTERS_OPTIONS =
    TRequestCounters::EOption::ReportDataPlaneHistogram |
    TRequestCounters::EOption::ReportControlPlaneHistogram;

TRequestCountersPtr MakeRequestCounters(
    ITimerPtr timer,
    TDynamicCounters& counters,
    TRequestCounters::EOptions options,
    EHistogramCounterOptions histogramCounterOptions)
{
    auto requestCounters = std::make_shared<TRequestCounters>(
        std::move(timer),
        FileStoreRequestCount,
        [] (TRequestCounters::TRequestType t) -> const TString& {
            return GetFileStoreRequestName(static_cast<EFileStoreRequest>(t));
        },
        [] (TRequestCounters::TRequestType t) {
            return IsReadWriteRequest(static_cast<EFileStoreRequest>(t));
        },
        options,
        histogramCounterOptions
    );
    requestCounters->Register(counters);
    return requestCounters;
}

template <typename T>
requires requires { T::RequestType; }
TRequestCounters::TRequestType GetRequestType(const T& obj)
{
    return static_cast<TRequestCounters::TRequestType>(obj.RequestType);
}

////////////////////////////////////////////////////////////////////////////////

class TRequestLogger
{
private:
    const TDuration ExecutionTimeThreshold;
    const TDuration TotalTimeThreshold;

public:
    TRequestLogger(
            TDuration executionTimeThreshold,
            TDuration totalTimeThreshold)
        : ExecutionTimeThreshold(executionTimeThreshold)
        , TotalTimeThreshold(totalTimeThreshold)
    {}

    void LogStarted(TLog& Log, TCallContext& callContext) const
    {
        STORAGE_LOG(TLOG_RESOURCES, callContext.LogString() << " REQUEST");
    }

    void LogCompleted(
        TLog& Log,
        TCallContext& callContext,
        TDuration totalTime,
        EDiagnosticsErrorKind errorKind,
        const NCloud::NProto::TError& error) const
    {
        ELogPriority logPriority;
        TStringBuf message;

        const auto predictedTime = callContext.GetPossiblePostponeDuration();
        const auto postponedTime = callContext.Time(EProcessingStage::Postponed);
        const auto backoffTime = callContext.Time(EProcessingStage::Backoff);
        const auto executionTime = totalTime - postponedTime - backoffTime;

        if (executionTime >= ExecutionTimeThreshold
                || totalTime >= TotalTimeThreshold)
        {
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
            << "(total_time: " << FormatDuration(totalTime)
            << ", execution_time: " << FormatDuration(executionTime)
            << ", predicted_postponed_time: " << FormatDuration(predictedTime)
            << ", postponed_time: " << FormatDuration(postponedTime)
            << ", backoff_time: " << FormatDuration(backoffTime)
            << ", size: " << FormatByteSize(callContext.RequestSize)
            << ", error: " << FormatError(error) << ")");
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

    TBusyIdleTimeCalculatorDynamicCounters BusyIdleCalc;

    TMutex ProvidersLock;
    TVector<IIncompleteRequestProviderPtr> IncompleteRequestProviders;

    TRWMutex FsLock;
    THashMap<TString, NProto::EStorageMediaKind> FileSystems;

public:
    TRequestStats(
            TDynamicCountersPtr counters,
            ITimerPtr timer,
            TDuration executionTimeThreshold,
            TDuration totalTimeThreshold,
            EHistogramCounterOptions histogramCounterOptions)
        : TRequestLogger(executionTimeThreshold, totalTimeThreshold)
        , RootCounters(std::move(counters))
        , TotalCounters(MakeRequestCounters(
            timer,
            *RootCounters,
            REQUEST_COUNTERS_OPTIONS,
            histogramCounterOptions))
        , SsdCounters(MakeRequestCounters(
            timer,
            *RootCounters->GetSubgroup("type", "ssd"),
            REQUEST_COUNTERS_OPTIONS,
            histogramCounterOptions))
        , HddCounters(MakeRequestCounters(
            timer,
            *RootCounters->GetSubgroup("type", "hdd"),
            REQUEST_COUNTERS_OPTIONS,
            histogramCounterOptions))
    {
        auto revisionGroup =
            RootCounters->GetSubgroup("revision", GetFullVersionString());

        auto versionCounter =
            revisionGroup->GetCounter("version", false);
        *versionCounter = 1;

        InitCriticalEventsCounter(RootCounters);

        BusyIdleCalc.Register(RootCounters);
    }

    void RequestStarted(TCallContext& callContext) override
    {
        const auto requestType = GetRequestType(callContext);
        const auto media = GetMediaKind(callContext);
        if (media != NProto::STORAGE_MEDIA_DEFAULT) {
            GetCounters(media)->RequestStarted(
                requestType,
                callContext.RequestSize);
        }

        BusyIdleCalc.OnRequestStarted();

        const auto cycles =
            TotalCounters->RequestStarted(requestType, callContext.RequestSize);
        callContext.SetRequestStartedCycles(cycles);
    }

    void RequestCompleted(
        TCallContext& callContext,
        const NCloud::NProto::TError& error) override
    {
        RequestCompleted(
            callContext,
            GetDiagnosticsErrorKind(error));
    }

    void RequestStarted(TLog& log, TCallContext& callContext) override
    {
        RequestStarted(callContext);
        LogStarted(log, callContext);
    }

    void RequestCompleted(
        TLog& log,
        TCallContext& callContext,
        const NCloud::NProto::TError& error) override
    {
        const auto errorKind =
            GetDiagnosticsErrorKind(error);
        const auto requestTime = RequestCompleted(callContext, errorKind);
        LogCompleted(log, callContext, requestTime, errorKind, error);
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

        BusyIdleCalc.OnUpdateStats();
    }

    void RegisterIncompleteRequestProvider(
        IIncompleteRequestProviderPtr provider) override
    {
        TGuard g(ProvidersLock);
        IncompleteRequestProviders.push_back(std::move(provider));
    }

    void Reset() override
    {
        TGuard g(ProvidersLock);
        IncompleteRequestProviders.clear();
    }

    void Collect(const TIncompleteRequest& req) override
    {
        TotalCounters->AddIncompleteStats(
            GetRequestType(req),
            req.ExecutionTime,
            req.TotalTime,
            ECalcMaxTime::ENABLE);
        if (req.MediaKind != NProto::STORAGE_MEDIA_DEFAULT) {
            GetCounters(req.MediaKind)->AddIncompleteStats(
                GetRequestType(req),
                req.ExecutionTime,
                req.TotalTime,
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

    void SetFileSystemMediaKind(
        const TString& fileSystemId,
        NProto::EStorageMediaKind media)
    {
        TWriteGuard g(FsLock);
        FileSystems[fileSystemId] = media;
    }

private:
    NProto::EStorageMediaKind GetMediaKind(const TCallContext& ctx) const
    {
        NProto::EStorageMediaKind media = NProto::STORAGE_MEDIA_DEFAULT;

        TReadGuard g(FsLock);
        if (const auto it = FileSystems.find(ctx.FileSystemId);
                it != FileSystems.end())
        {
            media = it->second;
        }

        return media;
    }

    TDuration RequestCompleted(
        const TCallContext& callContext,
        EDiagnosticsErrorKind errorKind)
    {
        const auto type = GetRequestType(callContext);
        const auto media = GetMediaKind(callContext);
        const auto startedCycles = callContext.GetRequestStartedCycles();
        const auto postponedTime = callContext.Time(EProcessingStage::Postponed);
        const auto backoffTime = callContext.Time(EProcessingStage::Backoff);
        const auto waitTime = postponedTime + backoffTime;
        if (media != NProto::STORAGE_MEDIA_DEFAULT) {
            GetCounters(media)->RequestCompleted(
                type,
                startedCycles,
                waitTime,
                callContext.RequestSize,
                errorKind,
                NCloud::NProto::EF_NONE,
                callContext.Unaligned,
                ECalcMaxTime::ENABLE,
                0);
        }
        BusyIdleCalc.OnRequestCompleted();

        return TotalCounters->RequestCompleted(
            type,
            startedCycles,
            waitTime,
            callContext.RequestSize,
            errorKind,
            NCloud::NProto::EF_NONE,
            callContext.Unaligned,
            ECalcMaxTime::ENABLE,
            0).Time;
    }

    TString LogHeader(const TCallContext& callContext) const override
    {
        return callContext.LogString();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPostponeTimePredictorStats
{
    static constexpr TStringBuf COUNTER_LABEL = "MaxPredictedPostponeTime";

    TDynamicCountersPtr RootCounters;

    TDynamicCounters::TCounterPtr Counter;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxCalc;

public:
    TPostponeTimePredictorStats(
            TDynamicCountersPtr rootCounters,
            ITimerPtr timer)
        : RootCounters(std::move(rootCounters))
        , Counter(RootCounters->GetCounter(COUNTER_LABEL.data()))
        , MaxCalc(std::move(timer))
    {}

    void SetupStats(TDuration predictedDelay)
    {
        MaxCalc.Add(predictedDelay.MicroSeconds());
    }

    void UpdateStats()
    {
        *Counter = MaxCalc.NextValue();
    }

    void Unregister()
    {
        if (RootCounters) {
            RootCounters->RemoveCounter(COUNTER_LABEL.data());
        }
    }
};

struct TUserMetadata
{
    TString CloudId;
    TString FolderId;
};

class TFileSystemStats final
    : public IFileSystemStats
    , IIncompleteRequestCollector
    , TRequestLogger
{
private:
    const TString FileSystemId;
    const TString ClientId;
    const TString CloudId;
    const TString FolderId;

    TDynamicCountersPtr RootCounters;
    TRequestCountersPtr Counters;
    IPostponeTimePredictorPtr Predictor;
    TPostponeTimePredictorStats PredictorStats;

    TMutex Lock;
    TVector<IIncompleteRequestProviderPtr> IncompleteRequestProviders;
    TVector<IUpdateableStatsPtr> UpdateableStats;

    TUserMetadata UserMetadata;

public:
    TFileSystemStats(
            TString fileSystemId,
            TString clientId,
            TString cloudId,
            TString folderId,
            ITimerPtr timer,
            TDynamicCountersPtr counters,
            IPostponeTimePredictorPtr predictor,
            TDuration executionTimeThreshold,
            TDuration totalTimeThreshold,
            EHistogramCounterOptions histogramCounterOptions)
        : TRequestLogger{executionTimeThreshold, totalTimeThreshold}
        , FileSystemId{std::move(fileSystemId)}
        , ClientId{std::move(clientId)}
        , CloudId{std::move(cloudId)}
        , FolderId{std::move(folderId)}
        , RootCounters(std::move(counters))
        , Counters{MakeRequestCounters(
            timer,
            *RootCounters,
            REQUEST_COUNTERS_OPTIONS
                | TRequestCounters::EOption::LazyRequestInitialization,
            histogramCounterOptions)}
        , Predictor{std::move(predictor)}
        , PredictorStats{RootCounters, std::move(timer)}
    {}

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
            GetRequestType(callContext),
            callContext.RequestSize));

        PredictionStarted(callContext);
    }

    void RequestCompleted(
        TCallContext& callContext,
        const NCloud::NProto::TError& error) override
    {
        RequestCompleted(
            callContext,
            GetDiagnosticsErrorKind(error));

        PredictionCompleted(callContext);
    }

    void RequestStarted(TLog& log, TCallContext& callContext) override
    {
        RequestStarted(callContext);
        LogStarted(log, callContext);

        PredictionStarted(callContext);
    }

    void RequestCompleted(
        TLog& log,
        TCallContext& callContext,
        const NCloud::NProto::TError& error) override
    {
        const auto errorKind =
            GetDiagnosticsErrorKind(error);
        const auto requestTime = RequestCompleted(callContext, errorKind);
        LogCompleted(log, callContext, requestTime, errorKind, error);

        PredictionCompleted(callContext);
    }

    void ResponseSent(TCallContext& callContext) override
    {
        Y_UNUSED(callContext);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        TVector<IUpdateableStatsPtr> updateableStats;

        with_lock (Lock) {
            updateableStats = UpdateableStats;
            for (auto& provider: IncompleteRequestProviders) {
                provider->Accept(*this);
            }
        }

        Counters->UpdateStats(updatePercentiles);
        PredictorStats.UpdateStats();

        for (const auto& stats: updateableStats) {
            stats->UpdateStats(updatePercentiles);
        }
    }

    void RegisterIncompleteRequestProvider(
        IIncompleteRequestProviderPtr provider) override
    {
        TGuard g(Lock);
        IncompleteRequestProviders.push_back(std::move(provider));
    }

    void Collect(const TIncompleteRequest& req) override
    {
        Counters->AddIncompleteStats(
            GetRequestType(req),
            req.ExecutionTime,
            req.TotalTime,
            ECalcMaxTime::ENABLE);
    }

    void Reset() override
    {
        PredictorStats.Unregister();
        for (auto& provider: IncompleteRequestProviders) {
            provider.reset();
        }
    }

    TString GetCloudId() const {
        return CloudId;
    }

    TString GetFolderId() const {
        return FolderId;
    }

    NMonitoring::TDynamicCountersPtr GetModuleCounters(
        const TString& moduleName) override
    {
        return RootCounters->GetSubgroup("module", moduleName);
    }

    void RegisterUpdateableStats(IUpdateableStatsPtr stats) override
    {
        with_lock (Lock) {
            UpdateableStats.push_back(std::move(stats));
        }
    }

private:
    TDuration RequestCompleted(
        const TCallContext& callContext,
        EDiagnosticsErrorKind errorKind) const
    {
        const auto type = GetRequestType(callContext);
        const auto startedCycles = callContext.GetRequestStartedCycles();
        const auto postponedTime = callContext.Time(EProcessingStage::Postponed);
        const auto backoffTime = callContext.Time(EProcessingStage::Backoff);
        const auto waitTime = postponedTime + backoffTime;
        return Counters->RequestCompleted(
            type,
            startedCycles,
            waitTime,
            callContext.RequestSize,
            errorKind,
            NCloud::NProto::EF_NONE,
            callContext.Unaligned,
            ECalcMaxTime::ENABLE,
            0).Time;
    }

    void PredictionStarted(TCallContext& callContext)
    {
        if (IsReadWriteRequest(callContext.RequestType)) {
            const auto possibleDelay = Predictor->GetPossiblePostponeDuration();
            // TODO: Remove if condition after split RequestStats to Server and Service parts.
            if (possibleDelay > callContext.GetPossiblePostponeDuration()) {
                callContext.SetPossiblePostponeDuration(possibleDelay);
            }
            PredictorStats.SetupStats(possibleDelay);
        }
    }

    void PredictionCompleted(TCallContext& callContext)
    {
        if (IsReadWriteRequest(callContext.RequestType)) {
            const auto delay = callContext.Time(EProcessingStage::Postponed);
            Predictor->Register(delay);
            callContext.SetPossiblePostponeDuration(TDuration::Zero());
        }
    }

    TString LogHeader(const TCallContext& callContext) const override
    {
        return TStringBuilder()
            << callContext.LogString()
            << "[c:" << ClientId << "]";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileSystemStatsStub final
    : public IFileSystemStats
{
public:
    void RequestStarted(TCallContext& callContext) override
    {
        Y_UNUSED(callContext);
    }

    void RequestCompleted(
        TCallContext& callContext,
        const NCloud::NProto::TError& error) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(error);
    }

    void RequestStarted(TLog& log, TCallContext& callContext) override
    {
        Y_UNUSED(log);
        Y_UNUSED(callContext);
    }

    void RequestCompleted(
        TLog& log,
        TCallContext& callContext,
        const NCloud::NProto::TError& error) override
    {
        Y_UNUSED(log);
        Y_UNUSED(callContext);
        Y_UNUSED(error);
    }

    void ResponseSent(TCallContext& callContext) override
    {
        Y_UNUSED(callContext);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);
    }

    void RegisterIncompleteRequestProvider(
        IIncompleteRequestProviderPtr provider) override
    {
        Y_UNUSED(provider);
    }

    void Reset() override
    {}

    TDynamicCountersPtr GetModuleCounters(
        const TString& moduleName) override
    {
        Y_UNUSED(moduleName);
        return MakeIntrusive<TDynamicCounters>();
    }

    void RegisterUpdateableStats(IUpdateableStatsPtr stats) override
    {
        Y_UNUSED(stats);
    }
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

        RequestStats = std::make_shared<TRequestStats>(
            std::move(totalCounters),
            Timer,
            DiagnosticsConfig->GetSlowExecutionTimeRequestThreshold(),
            DiagnosticsConfig->GetSlowTotalTimeRequestThreshold(),
            DiagnosticsConfig->GetHistogramCounterOptions());

        FsCounters = RootCounters
            ->GetSubgroup("component", component + "_fs")
            ->GetSubgroup("host", "cluster");
    }

    IFileSystemStatsPtr GetFileSystemStats(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) override
    {
        std::pair<TString, TString> key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);

        auto it = StatsMap.find(key);
        if (it == StatsMap.end())
        {
            auto counters = FsCounters
                ->GetSubgroup("filesystem", fileSystemId)
                ->GetSubgroup("client", clientId)
                ->GetSubgroup("cloud", cloudId)
                ->GetSubgroup("folder", folderId);

            auto stats = CreateRequestStats(
                fileSystemId,
                clientId,
                cloudId,
                folderId,
                std::move(counters),
                Timer,
                DiagnosticsConfig->GetPostponeTimePredictorInterval(),
                DiagnosticsConfig->GetPostponeTimePredictorPercentage(),
                DiagnosticsConfig->GetPostponeTimePredictorMaxTime(),
                DiagnosticsConfig->GetSlowExecutionTimeRequestThreshold(),
                DiagnosticsConfig->GetSlowTotalTimeRequestThreshold(),
                DiagnosticsConfig->GetHistogramCounterOptions());
            it = StatsMap.emplace(key, stats).first;
            stats->Subscribe(RequestStats->GetTotalCounters());
        } else {
            UpdateCloudAndFolderIfNecessary(
                *it->second.Stats,
                fileSystemId,
                clientId,
                cloudId,
                folderId);
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
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) override
    {
        std::pair<TString, TString> key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);
        if (auto it = StatsMap.find(key); it != StatsMap.end() && UserCounters) {

            it->second.Stats->SetUserMetadata(
                {.CloudId = cloudId, .FolderId = folderId});

            auto counters = FsCounters
                ->GetSubgroup("filesystem", fileSystemId)
                ->GetSubgroup("client", clientId)
                ->GetSubgroup("cloud", cloudId)
                ->GetSubgroup("folder", folderId);

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

    void UpdateCloudAndFolder(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) override
    {
        std::pair<TString, TString> key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);

        auto it = StatsMap.find(key);
        if (it == StatsMap.end()) {
            return;
        }

        UpdateCloudAndFolderIfNecessary(
            *it->second.Stats,
            fileSystemId,
            clientId,
            cloudId,
            folderId);
    }

private:
    std::shared_ptr<TFileSystemStats> CreateRequestStats(
        TString fileSystemId,
        TString clientId,
        TString cloudId,
        TString folderId,
        TDynamicCountersPtr counters,
        ITimerPtr timer,
        TDuration delayWindowInterval,
        double delayWindowPercentage,
        TMaybe<TDuration> delayMaxTime,
        TDuration executionTimeThreshold,
        TDuration totalTimeThreshold,
        EHistogramCounterOptions histogramCounterOptions) const
    {
        auto predictor = CreatePostponeTimePredictor(
            timer,
            delayWindowInterval,
            delayWindowPercentage,
            delayMaxTime);

        return std::make_shared<TFileSystemStats>(
            std::move(fileSystemId),
            std::move(clientId),
            std::move(cloudId),
            std::move(folderId),
            std::move(timer),
            std::move(counters),
            std::move(predictor),
            executionTimeThreshold,
            totalTimeThreshold,
            histogramCounterOptions);
    }

    void UpdateCloudAndFolderIfNecessary(
        const TFileSystemStats& stats,
        const TString& fileSystemId,
        const TString& clientId,
        const TString& newCloudId,
        const TString& newFolderId)
    {
        const auto& currentCloudId = stats.GetCloudId();
        const auto& currentFolderId = stats.GetFolderId();

        if ((!newCloudId || newCloudId == currentCloudId) &&
            (!newFolderId || newFolderId == currentFolderId))
        {
            return;
        }

        auto fsCounters = FsCounters
            ->GetSubgroup("filesystem", fileSystemId)
            ->GetSubgroup("client", clientId)
            ->GetSubgroup("cloud", currentCloudId)
            ->GetSubgroup("folder", currentFolderId);

        FsCounters->RemoveSubgroupChain({
            {"filesystem", fileSystemId},
            {"client", clientId}
        });

        // GetSubgroup is used to create all neccessary subgroups
        FsCounters->GetSubgroup("filesystem", fileSystemId)
            ->GetSubgroup("client", clientId)
            ->GetSubgroup("cloud", newCloudId)
            ->GetSubgroup("folder", newFolderId);

        FsCounters
            ->GetSubgroup("filesystem", fileSystemId)
            ->GetSubgroup("client", clientId)
            ->GetSubgroup("cloud", newCloudId)
            ->ReplaceSubgroup("folder", newFolderId, fsCounters);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestStatsRegistryStub final
    : public IRequestStatsRegistry
{
public:
    TRequestStatsRegistryStub() = default;

    IFileSystemStatsPtr GetFileSystemStats(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);

        return std::make_shared<TFileSystemStatsStub>();
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
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
    }

    IRequestStatsPtr GetRequestStats() override
    {
        return std::make_shared<TFileSystemStatsStub>();
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

    void UpdateCloudAndFolder(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
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
