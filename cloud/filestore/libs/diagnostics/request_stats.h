#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/user_counter.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/diagnostics/incomplete_request_processor.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/protos/media.pb.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IRequestStats
{
    virtual void RequestStarted(TCallContext& callContext) = 0;

    virtual void RequestCompleted(TCallContext& callContext) = 0;

    virtual void RequestStarted(TLog& log, TCallContext& callContext) = 0;

    virtual void RequestCompleted(TLog& log, TCallContext& callContext) = 0;

    virtual void ResponseSent(TCallContext& callContext) = 0;

    virtual void UpdateStats(bool updatePercentiles) = 0;

    virtual void RegisterIncompleteRequestProvider(IIncompleteRequestProviderPtr provider) = 0;

    virtual void Reset() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IRequestStatsRegistry
    : public IIncompleteRequestProcessor
{
    virtual IRequestStatsPtr GetRequestStats() = 0;

    virtual IRequestStatsPtr GetFileSystemStats(
        const TString& filesystem,
        const TString& client) = 0;

    virtual void SetFileSystemMediaKind(
        const TString& filesystem,
        const TString& client,
        NCloud::NProto::EStorageMediaKind media) = 0;

    virtual void RegisterUserStats(
        const TString& cloudId,
        const TString& folderId,
        const TString& fileSystemId,
        const TString& clientId) = 0;

    virtual void Unregister(
        const TString& fileSystemId,
        const TString& clientId) = 0;

    virtual void AddIncompleteRequest(const TIncompleteRequest& req) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRequestStatsRegistryPtr CreateRequestStatsRegistry(
    TString component,
    TDiagnosticsConfigPtr diagnosticsConfig,
    NMonitoring::TDynamicCountersPtr rootCounters,
    ITimerPtr timer,
    std::shared_ptr<NCloud::NStorage::NUserStats::TUserCounterSupplier> userCounters);

IRequestStatsRegistryPtr CreateRequestStatsRegistryStub();

}   // namespace NCloud::NFileStore
