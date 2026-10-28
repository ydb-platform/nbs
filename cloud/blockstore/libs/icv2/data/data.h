#pragma once

#include <cloud/blockstore/libs/icv2/transport/request_channel.h>
#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TDataConfig
{
    // Bounds both serialized requests and the requested read/write payload.
    ui64 MaxRequestBytes = 4 * 1024 * 1024;
    ui32 MaxInflightRequests = 1024;
    // Reservations cover admitted work through storage completion or outgoing
    // result continuations. Transport buffers have their own channel limits.
    ui64 MaxInflightBytes = 64 * 1024 * 1024;
};

struct IDataEndpoint: public IBlockStore
{
    // Prepared channel belongs to the caller, who activates it after installing
    // handlers. Start is a no-op. Only local read/write and zero are supported.
    // Requests require a positive Headers.RequestTimeout in milliseconds.
    // Write data is copied into an event payload before returning; reads copy
    // validated response payloads to the caller. No automatic retries are made.
    // Results run on the channel completion executor. Continuations must not
    // throw, call Stop, or destroy the endpoint. RequestStop is callback-safe;
    // external Stop closes the channel and waits for admitted work/results.
    // Owner shutdown completes unpublished results with E_CANCELLED; admission
    // overload returns E_REJECTED. Closing does not cancel remote storage work.
    virtual void RequestStop() = 0;
};

using IDataEndpointPtr = std::shared_ptr<IDataEndpoint>;

TResultOrError<IDataEndpointPtr> CreateDataEndpoint(
    IRequestChannelPtr channel, ITimerPtr timer, TDataConfig config);

struct IDataTarget
{
    virtual ~IDataTarget() = default;

    // Install this handler before channel activation. Storage calls, decoding
    // and replies run on the supplied asynchronous executor. Received events
    // and buffers remain owned until the corresponding storage future ends.
    virtual void HandleRequest(
        const IRequestChannelPtr& channel,
        std::unique_ptr<NActors::IEventHandle> request) = 0;

    // Stops admitting work without cancelling storage operations already sent.
    virtual void RequestStop() = 0;
    // External thread only. First close the source channels and wait for their
    // callbacks. Then Stop waits for queued work, storage futures and replies.
    // Service and executor must remain operational until Stop returns. Neither
    // is stopped by the target. Owner must Stop before releasing the last
    // reference.
    virtual void Stop() = 0;
};

using IDataTargetPtr = std::shared_ptr<IDataTarget>;

TResultOrError<IDataTargetPtr> CreateDataTarget(
    IBlockStorePtr service, ITaskQueuePtr executor, TDataConfig config);

}   // namespace NCloud::NBlockStore::NIcV2
