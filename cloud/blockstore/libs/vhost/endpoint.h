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
#include <util/generic/vector.h>
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
// requests dispatched to it by its executors into the IDeviceHandler API and
// keeps track of the requests in flight.
//
// The endpoint exposes VhostQueuesCount virtqueues to the guest. These are
// spread over the executors assigned to the endpoint (their number is
// requested via TStorageOptions::ThreadCount), so requests of a single
// endpoint may be processed by several threads simultaneously. All of them
// share one IDeviceHandler and therefore the same storage-wrapper chain, which
// is safe to call from multiple threads at once.
class TEndpoint final
    : public IRequestProcessor
    , public std::enable_shared_from_this<TEndpoint>
{
private:
    TAppContext& AppCtx;
    // Single device handler shared by all executors of this endpoint.
    const IDeviceHandlerPtr DeviceHandler;
    const TString SocketPath;
    const TStorageOptions Options;
    const ui32 SocketAccessMode;
    // Executors serving this endpoint. Kept here so that the endpoint's
    // lifetime governs the per-executor assignment counter.
    const TVector<TExecutor*> Executors;
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
        TVector<TExecutor*> executors);

    ~TEndpoint() override;

    // The cookie attached to every request dispatched through this endpoint's
    // vhost device.
    void* GetCookie()
    {
        return static_cast<IRequestProcessor*>(this);
    }

    void SetVhostDevice(IVhostDevicePtr vhostDevice);

    NProto::TError Start();

    NThreading::TFuture<NProto::TError> Stop(bool deleteSocket);

    void Update(ui64 blocksCount);

    size_t CollectRequests(const TIncompleteRequestsCollector& collector);

    // Processes a request dequeued from any of the endpoint's request queues,
    // i.e. this may be called concurrently from several executor threads.
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
