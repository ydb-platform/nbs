#pragma once

#include "public.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/nbd/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/endpoints/iface/public.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointManager
    : public IStartable
    , public IIncompleteRequestProvider
{
    virtual ~IEndpointManager() = default;

#define ENDPOINT_DECLARE_METHOD(name, ...)                                     \
    virtual NThreading::TFuture<NProto::T##name##Response> name(               \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> req) = 0;                    \
// ENDPOINT_DECLARE_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(ENDPOINT_DECLARE_METHOD)

#undef ENDPOINT_DECLARE_METHOD

    virtual NThreading::TFuture<void> RestoreEndpoints() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TEndpointManagerOptions
{
    NProto::TClientConfig ClientConfig;
    TString NbdSocketSuffix;
    TString NbdDevicePrefix = "/dev/nbd";
};

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateEndpointManager(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    IEndpointEventProxyPtr eventProxy,
    ISessionManagerPtr sessionManager,
    IEndpointStoragePtr endpointStorage,
    THashMap<NProto::EClientIpcType, IEndpointListenerPtr> listeners,
    NBD::IDeviceFactoryPtr nbdDeviceFactory,
    NBD::IErrorHandlerMapPtr errorHandlerMap,
    IBlockStorePtr service,
    TEndpointManagerOptions options);

bool AreSameStartEndpointRequests(
    const NProto::TStartEndpointRequest& left,
    const NProto::TStartEndpointRequest& right);

}   // namespace NCloud::NBlockStore::NServer
