#pragma once

#include "public.h"

#include "app_context.h"
#include "executor.h"
#include "server.h"
#include "vhost.h"

#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/intrlist.h>
#include <util/generic/string.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <memory>

namespace NCloud::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

// A vhost request in flight: the request itself plus everything needed to
// report it to the diagnostics.
struct TRequest
    : public TIntrusiveListItem<TRequest>
    , TAtomicRefCount<TRequest>
{
    const TVhostRequestPtr VhostRequest;
    const TCallContextPtr CallContext;
    TMetricRequest MetricRequest;

    std::atomic_flag Completed = 0;

    TRequest(ui64 requestId, TVhostRequestPtr vhostRequest)
        : VhostRequest(std::move(vhostRequest))
        , CallContext(MakeIntrusive<TCallContext>(requestId))
        , MetricRequest(VhostRequest->Type)
    {}
};

using TRequestPtr = TIntrusivePtr<TRequest>;

////////////////////////////////////////////////////////////////////////////////

// A single vhost block device exposed to the guest. Translates the vhost
// requests dispatched to it by its executor into the IDeviceHandler API and
// keeps track of the requests in flight.
class TEndpoint final
    : public IRequestProcessor
    , public std::enable_shared_from_this<TEndpoint>
{
private:
    TAppContext& AppCtx;
    const IDeviceHandlerPtr DeviceHandler;
    const TString SocketPath;
    const TStorageOptions Options;
    const ui32 SocketAccessMode;
    TExecutor* const Executor;
    IVhostDevicePtr VhostDevice;

    TIntrusiveList<TRequest> RequestsInFlight;
    TAdaptiveLock RequestsLock;

    std::atomic_flag Stopped = false;

public:
    TEndpoint(
        TAppContext& appCtx,
        IDeviceHandlerPtr deviceHandler,
        TString socketPath,
        const TStorageOptions& options,
        ui32 socketAccessMode,
        TExecutor* executor);

    // The cookie attached to every request dispatched through this endpoint's
    // vhost device.
    void* GetCookie()
    {
        return static_cast<IRequestProcessor*>(this);
    }

    TExecutor* GetExecutor() const
    {
        return Executor;
    }

    void SetVhostDevice(IVhostDevicePtr vhostDevice);

    NProto::TError Start();

    NThreading::TFuture<NProto::TError> Stop(bool deleteSocket);

    void Update(ui64 blocksCount);

    ui32 GetVhostQueuesCount() const
    {
        return Options.VhostQueuesCount;
    }

    size_t CollectRequests(const TIncompleteRequestsCollector& collector);

    void ProcessRequest(TVhostRequestPtr vhostRequest) override;

private:
    template <typename TMethod>
    void ProcessRequest(TRequestPtr request);

    TRequestPtr RegisterRequest(TVhostRequestPtr vhostRequest);

    void CompleteRequest(TRequest& request, const NProto::TError& error);

    void UnregisterRequest(TRequest& request);

    TVhostRequest::EResult GetResult(NProto::TError& error);
};

using TEndpointPtr = std::shared_ptr<TEndpoint>;

}   // namespace NCloud::NBlockStore::NVhost
