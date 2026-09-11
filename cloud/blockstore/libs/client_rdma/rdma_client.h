#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaEndpointConfig
{
    TString Address;
    ui32 Port;
};

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateRdmaEndpointClient(
    ILoggingServicePtr logging,
    NCloud::NStorage::NRdma::IClientPtr client,
    IBlockStorePtr volumeClient,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config);

NThreading::TFuture<TResultOrError<IBlockStorePtr>> CreateRdmaEndpointClientAsync(
    ILoggingServicePtr logging,
    NCloud::NStorage::NRdma::IClientPtr client,
    IBlockStorePtr volumeClient,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config);

// Waits for the first connect: the future fails, and the endpoint is torn
// down, if it does not come up.
NThreading::TFuture<TResultOrError<IBlockStorePtr>> CreateRdmaDataEndpointAsync(
    ILoggingServicePtr logging,
    NCloud::NStorage::NRdma::IClientPtr client,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config);

// The handler is how the rdma client reports the endpoint state back; it may
// be empty when nobody listens. The endpoint is returned before it has
// connected, so a caller that has no handler cannot tell when it is usable.
TResultOrError<IBlockStorePtr> CreateRdmaDataEndpoint(
    ILoggingServicePtr logging,
    NCloud::NStorage::NRdma::IClientPtr client,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config,
    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler = nullptr);

}   // namespace NCloud::NBlockStore::NClient
