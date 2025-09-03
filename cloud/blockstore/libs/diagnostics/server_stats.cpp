#include "server_stats.h"

#include "config.h"
#include "dumpable.h"
#include "hostname.h"
#include "probes.h"
#include "profile_log.h"
#include "request_stats.h"
#include "volume_stats.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/postpone_time_predictor.h>

#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore {

using namespace NMonitoring;

LWTRACE_USING(BLOCKSTORE_SERVER_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Capitalize(TString str)
{
    if (str.empty()) {
        return str;
    }

    str[0] = ::toupper(str[0]);
    return str;
}

////////////////////////////////////////////////////////////////////////////////

class TServerStats
    : public IServerStats
{
    class TMonPage;

private:
    const IDumpablePtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const IRequestStatsPtr RequestStats;
    const IVolumeStatsPtr VolumeStats;
    const TString RequestInstanceId;

    TDynamicCountersPtr Counters;
    TExecutorCounters ExecutorCounters;
    TDynamicCounters::TCounterPtr EndpointCounters[NProto::EClientIpcType_ARRAYSIZE];

public:
    TServerStats(
        IDumpablePtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IMonitoringServicePtr monitoring,
        IProfileLogPtr profileLog,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        const TString& componentName,
        TString instanceId);

    TExecutorCounters::TExecutorScope StartExecutor() override;

    TDynamicCounters::TCounterPtr GetEndpointCounter(NProto::EClientIpcType ipcType) override;

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override;

    void UnmountVolume(
        const TString& diskId,
        const TString& clientId) override;

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override;

    ui32 GetBlockSize(const TString& diskId) const override;

    void PrepareMetricRequest(
        TMetricRequest& metricRequest,
        TString clientId,
        TString diskId,
        ui64 startIndex,
        ui64 requestBytes,
        bool unaligned) override;

    void RequestStarted(
        TLog& Log,
        TMetricRequest& req,
        TCallContext& callContext,
        const TString& message) override;

    void RequestAcquired(
        TMetricRequest& req,
        TCallContext& callContext) override;

    void RequestSent(
        TMetricRequest& req,
        TCallContext& callContext) override;

    void ResponseReceived(
        TMetricRequest& req,
        TCallContext& callContext) override;

    void ResponseSent(
        TMetricRequest& req,
        TCallContext& callContext) override;

    void RequestCompleted(
        TLog& Log,
        TMetricRequest& req,
        TCallContext& callContext,
        const NProto::TError& error) override;

    void RequestFastPathHit(
        const TString& diskId,
        const TString& clientId,
        EBlockStoreRequest requestType) override;

    void ReportException(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId) override;

    void ReportInfo(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId,
        const TString& message) override;

    void AddIncompleteRequest(
        TCallContext& callContext,
        IVolumeInfoPtr volumeInfo,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime time) override;

    void BatchCompleted(
        TMetricRequest& metricRequest,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override;

    void UpdateStats(bool updateIntervalFinished) override;

    void InitEndpointCounters();

private:
    void OutputHtml(IOutputStream& out, const IMonHttpRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

class TServerStats::TMonPage final
    : public THtmlMonPage
{
private:
    TServerStats& ServerStats;

public:
    TMonPage(TServerStats& serverStats, const TString& componentName)
        : THtmlMonPage(componentName, Capitalize(componentName), true)
        , ServerStats(serverStats)
    {}

    void OutputContent(IMonHttpRequest& request) override
    {
        ServerStats.OutputHtml(request.Output(), request);
    }
};

////////////////////////////////////////////////////////////////////////////////

TServerStats::TServerStats(
        IDumpablePtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IMonitoringServicePtr monitoring,
        IProfileLogPtr profileLog,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        const TString& componentName,
        TString requestInstanceId)
    : Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , ProfileLog(std::move(profileLog))
    , RequestStats(std::move(requestStats))
    , VolumeStats(std::move(volumeStats))
    , RequestInstanceId(std::move(requestInstanceId))
{
    auto counters = monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters = rootGroup->GetSubgroup("component", componentName);
    ExecutorCounters.Register(*Counters);

    for (ui32 i = 0; i < NProto::EClientIpcType_ARRAYSIZE; ++i) {
        auto ipcType = static_cast<NProto::EClientIpcType>(i);
        EndpointCounters[ipcType] = MakeIntrusive<NMonitoring::TCounterForPtr>();
    }

    auto rootPage = monitoring->RegisterIndexPage("blockstore", "BlockStore");
    static_cast<TIndexMonPage&>(*rootPage).Register(
        new TMonPage(*this, componentName));
}

void TServerStats::InitEndpointCounters()
{
    for (ui32 i = 0; i < NProto::EClientIpcType_ARRAYSIZE; ++i) {
        auto ipcType = static_cast<NProto::EClientIpcType>(i);
        auto ipcGroup = Counters->GetSubgroup("ipcType", GetIpcTypeString(ipcType));
        EndpointCounters[ipcType] = ipcGroup->GetCounter("EndpointCount");
    }
}

TExecutorCounters::TExecutorScope TServerStats::StartExecutor()
{
    return ExecutorCounters.StartExecutor();
}

TDynamicCounters::TCounterPtr TServerStats::GetEndpointCounter(
    NProto::EClientIpcType ipcType)
{
    if (ipcType >= NProto::EClientIpcType_ARRAYSIZE) {
        return nullptr;
    }
    return EndpointCounters[ipcType];
}

bool TServerStats::MountVolume(
    const NProto::TVolume& volume,
    const TString& clientId,
    const TString& instanceId)
{
    return VolumeStats->MountVolume(volume, clientId, instanceId);
}

void TServerStats::UnmountVolume(
    const TString& diskId,
    const TString& clientId)
{
    return VolumeStats->UnmountVolume(diskId, clientId);
}

void TServerStats::AlterVolume(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId)
{
    return VolumeStats->AlterVolume(diskId, cloudId, folderId);
}

ui32 TServerStats::GetBlockSize(const TString& diskId) const
{
    return VolumeStats->GetBlockSize(diskId);
}

void TServerStats::PrepareMetricRequest(
    TMetricRequest& metricRequest,
    TString clientId,
    TString diskId,
    ui64 startIndex,
    ui64 requestBytes,
    bool unaligned)
{
    metricRequest.ClientId = std::move(clientId);
    metricRequest.DiskId = std::move(diskId);
    metricRequest.StartIndex = startIndex;
    metricRequest.RequestBytes = requestBytes;
    metricRequest.Unaligned = unaligned;

    if (metricRequest.DiskId) {
        auto volumeInfo =
            VolumeStats->GetVolumeInfo(
                metricRequest.DiskId,
                metricRequest.ClientId);

        if (volumeInfo) {
            const auto& volume = volumeInfo->GetInfo();
            metricRequest.MediaKind = volume.GetStorageMediaKind();
            metricRequest.VolumeInfo = std::move(volumeInfo);
        }
    }
}

void TServerStats::RequestStarted(
    TLog& Log,
    TMetricRequest& req,
    TCallContext& callContext,
    const TString& message)
{
    auto logPriority = IsControlRequest(req.RequestType) ? TLOG_INFO : TLOG_RESOURCES;
    STORAGE_LOG(logPriority,
        TRequestInfo(
            req.RequestType,
            callContext.RequestId,
            req.DiskId,
            req.ClientId,
            RequestInstanceId)
        << " REQUEST " << message);

    LWTRACK(
        RequestStarted,
        callContext.LWOrbit,
        GetBlockStoreRequestName(req.RequestType),
        static_cast<ui32>(req.MediaKind),
        callContext.RequestId,
        req.DiskId,
        req.StartIndex,
        req.RequestBytes);

    req.RequestTimestamp = TInstant::Now();
    auto started = RequestStats->RequestStarted(
        req.MediaKind,
        req.RequestType,
        req.RequestBytes);
    callContext.SetRequestStartedCycles(started);

    if (req.VolumeInfo) {
        req.VolumeInfo->RequestStarted(req.RequestType, req.RequestBytes);
        if (IsReadWriteRequest(req.RequestType)) {
            callContext.SetPossiblePostponeDuration(
                req.VolumeInfo->GetPossiblePostponeDuration());
        }
    }
}

void TServerStats::RequestAcquired(
    TMetricRequest& req,
    TCallContext& callContext)
{
    LWTRACK(
        RequestAcquired,
        callContext.LWOrbit,
        GetBlockStoreRequestName(req.RequestType),
        callContext.RequestId,
        req.DiskId);
}

void TServerStats::RequestSent(
    TMetricRequest& req,
    TCallContext& callContext)
{
    LWTRACK(
        RequestSent,
        callContext.LWOrbit,
        GetBlockStoreRequestName(req.RequestType),
        callContext.RequestId,
        req.DiskId);
}

void TServerStats::ResponseReceived(
    TMetricRequest& req,
    TCallContext& callContext)
{
    LWTRACK(
        ResponseReceived,
        callContext.LWOrbit,
        GetBlockStoreRequestName(req.RequestType),
        callContext.RequestId,
        req.DiskId);
}

void TServerStats::ResponseSent(
    TMetricRequest& req,
    TCallContext& callContext)
{
    auto responseSentCycles = GetCycleCount();
    callContext.SetResponseSentCycles(responseSentCycles);
    LWTRACK(
        ResponseSent,
        callContext.LWOrbit,
        GetBlockStoreRequestName(req.RequestType),
        callContext.RequestId,
        req.DiskId);
}

void TServerStats::RequestCompleted(
    TLog& Log,
    TMetricRequest& req,
    TCallContext& callContext,
    const NProto::TError& error)
{
    const ui64 started = callContext.GetRequestStartedCycles();
    const auto postponedTime = callContext.Time(EProcessingStage::Postponed);
    const auto predictedTime = callContext.GetPossiblePostponeDuration();
    const auto backoffTime = callContext.Time(EProcessingStage::Backoff);
    const auto waitTime = postponedTime + backoffTime;
    const auto errorFlags = error.GetFlags();
    auto errorKind = GetDiagnosticsErrorKind(error);

    if (errorKind == EDiagnosticsErrorKind::ErrorRetriable &&
        callContext.GetSilenceRetriableErrors())
    {
        // Make error silent here to prevent it from showing in metrics.
        errorKind = EDiagnosticsErrorKind::ErrorSilent;
    }

    if (errorKind != EDiagnosticsErrorKind::Success && req.CellRequest) {
        // Hide inter-cell errors here to prevent them from showing in error metrics.
        errorKind = EDiagnosticsErrorKind::Success;
    }

    auto calcMaxTime = callContext.GetHasUncountableRejects()
                           ? ECalcMaxTime::DISABLE
                           : ECalcMaxTime::ENABLE;

    const auto responseSentCycles = callContext.GetResponseSentCycles();

    const auto requestTime = RequestStats->RequestCompleted(
        req.MediaKind,
        req.RequestType,
        started,
        waitTime,
        req.RequestBytes,
        errorKind,
        errorFlags,
        req.Unaligned,
        calcMaxTime,
        responseSentCycles);

    ui32 blockSize = DefaultBlockSize;

    TString maxTimeSuppressedMessage;
    if (req.VolumeInfo) {
        blockSize = req.VolumeInfo->GetInfo().GetBlockSize();

        req.VolumeInfo->RequestCompleted(
            req.RequestType,
            started,
            waitTime,
            req.RequestBytes,
            errorKind,
            errorFlags,
            req.Unaligned,
            responseSentCycles);

        if (calcMaxTime == ECalcMaxTime::DISABLE) {
            maxTimeSuppressedMessage = ", Warning! MaxTime calculation suppressed";
        }
    }

    if (ProfileLog) {
        IProfileLog::TRecord record;
        bool requestSet = false;

        if (IsReadWriteRequest(req.RequestType)) {
            if (req.RequestBytes) {
                record.Request = IProfileLog::TReadWriteRequest{
                    req.RequestType,
                    requestTime,
                    waitTime,
                    {
                        TBlockRange64::WithLength(
                            req.StartIndex,
                            req.RequestBytes / blockSize
                        )
                    },
                };
                requestSet = true;
            }
        } else if (req.RequestType == EBlockStoreRequest::MountVolume) {
            record.Request = IProfileLog::TMiscRequest{
                EBlockStoreRequest::MountVolume,
                requestTime,
            };
            requestSet = true;
        } else if (req.RequestType == EBlockStoreRequest::UnmountVolume) {
            record.Request = IProfileLog::TMiscRequest{
                EBlockStoreRequest::UnmountVolume,
                requestTime,
            };
            requestSet = true;
        }

        if (requestSet) {
            record.DiskId = req.DiskId;
            record.Ts = req.RequestTimestamp;

            ProfileLog->Write(std::move(record));
        }
    }

    auto execTime = requestTime - waitTime;

    LWTRACK(
        RequestCompleted,
        callContext.LWOrbit,
        GetBlockStoreRequestName(req.RequestType),
        callContext.RequestId,
        req.DiskId,
        req.RequestBytes,
        requestTime.MicroSeconds(),
        execTime.MicroSeconds(),
        error.GetCode());

    ELogPriority logPriority;
    TString message;
    const bool controlRequest = IsControlRequest(req.RequestType);

    if (execTime >= RequestTimeWarnThreshold) {
        logPriority = TLOG_WARNING;
        message = "request execution too slow";
    } else if (requestTime >= RequestTimeWarnThreshold) {
        logPriority = TLOG_WARNING;
        message = "request too slow";
    } else if (errorKind == EDiagnosticsErrorKind::ErrorFatal ||
               errorKind == EDiagnosticsErrorKind::ErrorAborted) {
        logPriority = TLOG_ERR;
        message = "request failed";
    } else if (
        controlRequest && errorKind == EDiagnosticsErrorKind::ErrorRetriable)
    {
        logPriority = TLOG_WARNING;
        message = "request rejected";
    } else {
        logPriority = controlRequest ? TLOG_INFO : TLOG_RESOURCES;
        message = "request completed";
    }

    if (req.CellRequest) {
        logPriority = TLOG_DEBUG;
    }

    STORAGE_LOG(logPriority,
        TRequestInfo(
            req.RequestType,
            callContext.RequestId,
            req.DiskId,
            req.ClientId,
            RequestInstanceId)
        << " RESPONSE"
        << " " << message
        << " (execution: " << FormatDuration(execTime)
        << ", postponed: " << FormatDuration(postponedTime)
        << ", predicted: " << FormatDuration(predictedTime)
        << ", backoff: " << FormatDuration(backoffTime)
        << ", size: " << FormatByteSize(req.RequestBytes)
        << ", unaligned: " << req.Unaligned
        << maxTimeSuppressedMessage
        << ", error: " << FormatError(error)
        << ")");
}

void TServerStats::RequestFastPathHit(
    const TString& diskId,
    const TString& clientId,
    EBlockStoreRequest requestType)
{
    RequestStats->RequestFastPathHit(requestType);

    auto volumeInfo = VolumeStats->GetVolumeInfo(diskId, clientId);
    if (volumeInfo) {
        volumeInfo->RequestFastPathHit(requestType);
    }
}

void TServerStats::ReportException(
    TLog& Log,
    EBlockStoreRequest requestType,
    ui64 requestId,
    const TString& diskId,
    const TString& clientId)
{
    STORAGE_ERROR(
        TRequestInfo(
            requestType,
            requestId,
            diskId,
            clientId,
            RequestInstanceId)
        << " exception in callback: " << CurrentExceptionMessage());
}

void TServerStats::ReportInfo(
    TLog& Log,
    EBlockStoreRequest requestType,
    ui64 requestId,
    const TString& diskId,
    const TString& clientId,
    const TString& message)
{
    STORAGE_INFO(
        TRequestInfo(
            requestType,
            requestId,
            diskId,
            clientId,
            RequestInstanceId)
        << " " << message);
}

void TServerStats::OutputHtml(IOutputStream& out, const IMonHttpRequest& request)
{
    Y_UNUSED(request);

    HTML(out) {
        if (DiagnosticsConfig) {
            TAG(TH3) {
                out << "<a href='"
                    << GetMonitoringNBSAlertsUrl(*DiagnosticsConfig)
                    << "'>NBS Alerts dashboard</a>";
            };

            TAG(TH3) {
                out << "<a href='"
                    << GetMonitoringNBSOverviewToTVUrl(*DiagnosticsConfig)
                    << "'>NBS overview To TV</a>";
            }
        }

        TAG(TH3) { out << "Config"; }
        Config->DumpHtml(out);

        if (DiagnosticsConfig) {
            DiagnosticsConfig->DumpHtml(out);
        }

        TAG(TH3) { out << "Counters"; }
        Counters->OutputHtml(out);
    }
}

void TServerStats::AddIncompleteRequest(
    TCallContext& callContext,
    IVolumeInfoPtr volumeInfo,
    NCloud::NProto::EStorageMediaKind mediaKind,
    EBlockStoreRequest requestType,
    TRequestTime time)
{
    auto calcMaxTime = callContext.GetHasUncountableRejects()
                           ? ECalcMaxTime::DISABLE
                           : ECalcMaxTime::ENABLE;

    RequestStats->AddIncompleteStats(
        mediaKind,
        requestType,
        time,
        calcMaxTime);

    if (volumeInfo) {
        volumeInfo->AddIncompleteStats(
            requestType,
            time);
    }
}

void TServerStats::BatchCompleted(
    TMetricRequest& req,
    ui64 count,
    ui64 bytes,
    ui64 errors,
    std::span<TTimeBucket> timeHist,
    std::span<TSizeBucket> sizeHist)
{
    RequestStats->BatchCompleted(
        req.MediaKind,
        req.RequestType,
        count,
        bytes,
        errors,
        timeHist,
        sizeHist);

    if (req.VolumeInfo) {
        req.VolumeInfo->BatchCompleted(
            req.RequestType,
            count,
            bytes,
            errors,
            timeHist,
            sizeHist);
    }
}

void TServerStats::UpdateStats(bool updateIntervalFinished)
{
    RequestStats->UpdateStats(updateIntervalFinished);

    VolumeStats->TrimVolumes();
    VolumeStats->UpdateStats(updateIntervalFinished);

    ExecutorCounters.UpdateStats();
}

////////////////////////////////////////////////////////////////////////////////

class TServerStatsStub final
    : public IServerStats
{
private:
    TExecutorCounters ExecutorCounters;
    TDynamicCounters::TCounterPtr CounterStub;

public:
    TServerStatsStub()
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();
        ExecutorCounters.Register(*counters);
        CounterStub = MakeIntrusive<NMonitoring::TCounterForPtr>();
    }

    TExecutorCounters::TExecutorScope StartExecutor() override
    {
        return ExecutorCounters.StartExecutor();
    }

    TDynamicCounters::TCounterPtr GetEndpointCounter(
        NProto::EClientIpcType ipcType) override
    {
        Y_UNUSED(ipcType);
        return CounterStub;
    }

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);

        return true;
    }

    void UnmountVolume(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(clientId);
        Y_UNUSED(diskId);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
    }

    ui32 GetBlockSize(const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return DefaultBlockSize;
    }

    void PrepareMetricRequest(
        TMetricRequest& metricRequest,
        TString clientId,
        TString diskId,
        ui64 startIndex,
        ui64 requestBytes,
        bool unaligned) override
    {
        Y_UNUSED(metricRequest);
        Y_UNUSED(clientId);
        Y_UNUSED(diskId);
        Y_UNUSED(startIndex);
        Y_UNUSED(requestBytes);
        Y_UNUSED(unaligned);
    }

    void RequestStarted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const TString& message) override
    {
        Y_UNUSED(log);
        Y_UNUSED(metricRequest);
        Y_UNUSED(message);

        callContext.SetRequestStartedCycles(GetCycleCount());
    }

    void RequestAcquired(
        TMetricRequest& metricRequest,
        TCallContext& callContext) override
    {
        Y_UNUSED(metricRequest);
        Y_UNUSED(callContext);
    }

    void RequestSent(
        TMetricRequest& metricRequest,
        TCallContext& callContext) override
    {
        Y_UNUSED(metricRequest);
        Y_UNUSED(callContext);
    }

    void ResponseReceived(
        TMetricRequest& metricRequest,
        TCallContext& callContext) override
    {
        Y_UNUSED(metricRequest);
        Y_UNUSED(callContext);
    }

    void ResponseSent(
        TMetricRequest& metricRequest,
        TCallContext& callContext) override
    {
        Y_UNUSED(metricRequest);
        Y_UNUSED(callContext);
    }

    void RequestCompleted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error) override
    {
        Y_UNUSED(log);
        Y_UNUSED(metricRequest);
        Y_UNUSED(callContext);
        Y_UNUSED(error);
    }

    void RequestFastPathHit(
        const TString& diskId,
        const TString& clientId,
        EBlockStoreRequest requestType) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
        Y_UNUSED(requestType);
    }

    void ReportException(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(Log);
        Y_UNUSED(requestType);
        Y_UNUSED(requestId);
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void ReportInfo(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId,
        const TString& message) override
    {
        Y_UNUSED(Log);
        Y_UNUSED(requestType);
        Y_UNUSED(requestId);
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
        Y_UNUSED(message);
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }

    void AddIncompleteRequest(
        TCallContext& callContext,
        IVolumeInfoPtr volumeInfo,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime time) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(volumeInfo);
        Y_UNUSED(requestType);
        Y_UNUSED(mediaKind);
        Y_UNUSED(time);
    }

    void BatchCompleted(
        TMetricRequest& metricRequest,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        Y_UNUSED(metricRequest);
        Y_UNUSED(count);
        Y_UNUSED(bytes);
        Y_UNUSED(errors);
        Y_UNUSED(timeHist);
        Y_UNUSED(sizeHist);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerStatsPtr CreateServerStats(
    IDumpablePtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IMonitoringServicePtr monitoring,
    IProfileLogPtr profileLog,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats)
{
    auto serverStats = std::make_shared<TServerStats>(
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(monitoring),
        std::move(profileLog),
        std::move(requestStats),
        std::move(volumeStats),
        "server",
        TString{});

    serverStats->InitEndpointCounters();

    return serverStats;
}

IServerStatsPtr CreateClientStats(
    IDumpablePtr config,
    IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    TString instanceId)
{
    return std::make_shared<TServerStats>(
        std::move(config),
        nullptr,
        std::move(monitoring),
        nullptr,
        std::move(requestStats),
        std::move(volumeStats),
        "client",
        std::move(instanceId));
}

IServerStatsPtr CreateServerStatsStub()
{
    return std::make_shared<TServerStatsStub>();
}

}   // namespace NCloud::NBlockStore
