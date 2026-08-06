#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/public.h>

#include <cloud/filestore/libs/service/public.h>

#include <cloud/filestore/public/api/protos/headers.pb.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/grpc/public.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/strbuf.h>

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public IStartable
    , public IIncompleteRequestProvider
{
};

////////////////////////////////////////////////////////////////////////////////

namespace NImpl {

void PrepareRequestHeaders(
    NCloud::NProto::ERequestSource source,
    TStringBuf peer,
    TStringBuf authToken,
    NProto::THeaders& headers);

}   // namespace NImpl

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    NMonitoring::TDynamicCountersPtr counters,
    IProfileLogPtr profileLog,
    NCloud::ISchedulerPtr scheduler,
    IFileStoreServicePtr service,
    ICertificateProviderPtr certificateProvider);

IServerPtr CreateServer(
    TServerConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    NMonitoring::TDynamicCountersPtr counters,
    NCloud::ISchedulerPtr scheduler,
    IEndpointManagerPtr service,
    ICertificateProviderPtr certificateProvider);

}   // namespace NCloud::NFileStore::NServer
