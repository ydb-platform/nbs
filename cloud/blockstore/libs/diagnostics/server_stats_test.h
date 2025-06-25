#pragma once

#include "public.h"

#include "server_stats.h"

namespace NCloud::NBlockStore {

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////

class TTestServerStats final
    : public IServerStats
{
private:
    IServerStatsPtr Stub = CreateServerStatsStub();

public:
    std::function<NMonitoring::TDynamicCounters::TCounterPtr(
        NProto::EClientIpcType ipcType)> GetEndpointCounterHandler
            = std::bind_front(&IServerStats::GetEndpointCounter, Stub.get());

    std::function<bool(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId)> MountVolumeHandler
            = std::bind_front(&IServerStats::MountVolume, Stub.get());

    std::function<void(
        const TString& diskId,
        const TString& clientId)> UnmountVolumeHandler
            = std::bind_front(&IServerStats::UnmountVolume, Stub.get());

    std::function<void(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId)> AlterVolumeHandler
            = std::bind_front(&IServerStats::AlterVolume, Stub.get());

    std::function<ui32(const TString& diskId)> GetBlockSizeHandler
        = std::bind_front(&IServerStats::GetBlockSize, Stub.get());

    std::function<void(
        TMetricRequest& metricRequest,
        TString clientId,
        TString diskId,
        ui64 startIndex,
        ui64 requestBytes,
        bool unaligned)> PrepareMetricRequestHandler
            = std::bind_front(&IServerStats::PrepareMetricRequest, Stub.get());

    std::function<void(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const TString& message)> RequestStartedHandler
            = std::bind_front(&IServerStats::RequestStarted, Stub.get());

    std::function<void(
        TMetricRequest& metricRequest,
        TCallContext& callContext)> RequestAcquiredHandler
            = std::bind_front(&IServerStats::RequestAcquired, Stub.get());

    std::function<void(
        TMetricRequest& metricRequest,
        TCallContext& callContext)> RequestSentHandler
            = std::bind_front(&IServerStats::RequestSent, Stub.get());

    std::function<void(
        TMetricRequest& metricRequest,
        TCallContext& callContext)> ResponseReceivedHandler
            = std::bind_front(&IServerStats::ResponseReceived, Stub.get());

    std::function<void(
        TMetricRequest& metricRequest,
        TCallContext& callContext)> ResponseSentHandler
            = std::bind_front(&IServerStats::ResponseSent, Stub.get());

    std::function<void(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error)> RequestCompletedHandler
            = std::bind_front(&IServerStats::RequestCompleted, Stub.get());

    std::function<void(
        const TString& diskId,
        const TString& clientId,
        EBlockStoreRequest requestType)> RequestFastPathHitHandler
            = std::bind_front(&IServerStats::RequestFastPathHit, Stub.get());

    std::function<void(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId)> ReportExceptionHandler
            = std::bind_front(&IServerStats::ReportException, Stub.get());

    std::function<void(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId,
        const TString& message)> ReportInfoHandler
            = std::bind_front(&IServerStats::ReportInfo, Stub.get());

    std::function<void(
        TCallContext& callContext,
        IVolumeInfoPtr volumeInfo,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime time)> AddIncompleteRequestHandler = std::bind_front(
            &IServerStats::AddIncompleteRequest,
            Stub.get());

    std::function<void(
        TMetricRequest& metricRequest,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist)> BatchCompletedHandler = std::bind_front(
            &IServerStats::BatchCompleted,
            Stub.get());

    std::function<void(bool updateIntervalFinished)> UpdateStatsHandler
        = std::bind_front(&IServerStats::UpdateStats, Stub.get());

    TExecutorCounters::TExecutorScope StartExecutor() override
    {
        return Stub->StartExecutor();
    }

    NMonitoring::TDynamicCounters::TCounterPtr
        GetEndpointCounter(NProto::EClientIpcType ipcType) override
    {
        return GetEndpointCounterHandler(ipcType);
    }

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override
    {
        return MountVolumeHandler(volume, clientId, instanceId);
    }

    void UnmountVolume(
        const TString& diskId,
        const TString& clientId) override
    {
        return UnmountVolumeHandler(diskId, clientId);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override
    {
        return AlterVolumeHandler(diskId, cloudId, folderId);
    }

    ui32 GetBlockSize(const TString& diskId) const override
    {
        return GetBlockSizeHandler(diskId);
    }

    void PrepareMetricRequest(
        TMetricRequest& metricRequest,
        TString clientId,
        TString diskId,
        ui64 startIndex,
        ui64 requestBytes,
        bool unaligned) override
    {
        PrepareMetricRequestHandler(
            metricRequest,
            std::move(clientId),
            std::move(diskId),
            startIndex,
            requestBytes,
            unaligned);
    }

    void RequestStarted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const TString& message) override
    {
        RequestStartedHandler(log, metricRequest, callContext, message);
    }

    void RequestAcquired(
        TMetricRequest& metricRequest,
        TCallContext& callContext) override
    {
        RequestAcquiredHandler(metricRequest, callContext);
    }

    void RequestSent(
        TMetricRequest& metricRequest,
        TCallContext& callContext) override
    {
        RequestSentHandler(metricRequest, callContext);
    }

    void ResponseReceived(
        TMetricRequest& metricRequest,
        TCallContext& callContext) override
    {
        ResponseReceivedHandler(metricRequest, callContext);
    }

    void ResponseSent(
        TMetricRequest& metricRequest,
        TCallContext& callContext) override
    {
        ResponseSentHandler(metricRequest, callContext);
    }

    void RequestCompleted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error) override
    {
        RequestCompletedHandler(log, metricRequest, callContext, error);
    }

    void RequestFastPathHit(
        const TString& diskId,
        const TString& clientId,
        EBlockStoreRequest requestType) override
    {
        RequestFastPathHitHandler(diskId, clientId, requestType);
    }

    void ReportException(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId) override
    {
        ReportExceptionHandler(
            Log,
            requestType,
            requestId,
            diskId,
            clientId);
    }

    void ReportInfo(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId,
        const TString& message) override
    {
        ReportInfoHandler(
            Log,
            requestType,
            requestId,
            diskId,
            clientId,
            message);
    }

    void AddIncompleteRequest(
        TCallContext& callContext,
        IVolumeInfoPtr volumeInfo,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime time) override
    {
        AddIncompleteRequestHandler(
            callContext,
            std::move(volumeInfo),
            mediaKind,
            requestType,
            time);
    }

    void BatchCompleted(
        TMetricRequest& metricRequest,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        BatchCompletedHandler(
            metricRequest,
            count,
            bytes,
            errors,
            timeHist,
            sizeHist);
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        UpdateStatsHandler(updateIntervalFinished);
    }
};

}   // namespace NCloud::NBlockStore
