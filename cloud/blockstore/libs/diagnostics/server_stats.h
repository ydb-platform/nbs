#pragma once

#include "public.h"

#include "latency_sli.h"

#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/metric_request.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/executor_counters.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>

#include <span>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IServerStats
    : public IStats
{
    virtual TExecutorCounters::TExecutorScope StartExecutor() = 0;

    virtual NMonitoring::TDynamicCounters::TCounterPtr
        GetEndpointCounter(NProto::EClientIpcType ipcType) = 0;

    virtual bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) = 0;

    virtual void UnmountVolume(
        const TString& diskId,
        const TString& clientId) = 0;

    virtual void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) = 0;

    virtual ui32 GetBlockSize(const TString& diskId) const = 0;

    virtual void PrepareMetricRequest(
        TMetricRequest& metricRequest,
        TString clientId,
        TString diskId,
        ui64 startIndex,
        ui64 requestBytes,
        bool unaligned) = 0;

    virtual void RequestStarted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const TString& message = {}) = 0;

    virtual void RequestAcquired(
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void RequestSent(
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void ResponseReceived(
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void ResponseSent(
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void RequestCompleted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error) = 0;

    virtual void RequestFastPathHit(
        const TString& diskId,
        const TString& clientId,
        EBlockStoreRequest requestType) = 0;

    virtual void ReportException(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId) = 0;

    virtual void ReportInfo(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId,
        const TString& message) = 0;

    virtual void AddIncompleteRequest(
        TCallContext& callContext,
        const TMetricRequest& metricRequest,
        TRequestTime time) = 0;

    using TTimeBucket = std::pair<TDuration, ui64>;
    using TSizeBucket = std::pair<ui64, ui64>;

    // An explicit endpoint rejection, before a legacy request is registered.
    // Call only where the original client cause is established locally.
    virtual void LatencyClientRejected(
        TMetricRequest& request, ui64 startedCycles,
        NProto::TLatencyDiagnostics::EExclusion origin)
    {
        Y_UNUSED(request);
        Y_UNUSED(startedCycles);
        Y_UNUSED(origin);
    }

    // Optional shadow supplement; the legacy batch contract is unchanged.
    virtual void LatencyBatchCompleted(
        TMetricRequest& request, const TLatencyCounts& counts,
        ELatencyBatchStatus status = ELatencyBatchStatus::CountsOnly)
    {
        Y_UNUSED(request);
        Y_UNUSED(counts);
        Y_UNUSED(status);
    }

    virtual TLatencyBatchResult UpdateLatencyBatch(
        TLatencyBatchTracker& tracker, const TLatencyBatch* batch)
    {
        Y_UNUSED(tracker);
        Y_UNUSED(batch);
        return {.Status = ELatencyBatchStatus::Disabled};
    }

    virtual void BatchCompleted(
        TMetricRequest& metricRequest,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IServerStatsPtr CreateServerStats(
    IDumpablePtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IMonitoringServicePtr monitoring,
    IProfileLogPtr profileLog,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats);

IServerStatsPtr CreateClientStats(
    IDumpablePtr config,
    IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    TString instanceId);

IServerStatsPtr CreateServerStatsStub();

}   // namespace NCloud::NBlockStore
