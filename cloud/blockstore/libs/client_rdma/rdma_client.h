#pragma once

#include "public.h"

#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

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
    NRdma::IClientPtr client,
    IBlockStorePtr volumeClient,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config);

IBlockStorePtr CreateRdmaDataEndpoint(
    ILoggingServicePtr logging,
    NRdma::IClientPtr client,
    NRdma::IClientEndpointPtr clientEndpoint,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue);

NThreading::TFuture<TResultOrError<IBlockStorePtr>> CreateRdmaEndpointClientAsync(
    ILoggingServicePtr logging,
    NRdma::IClientPtr client,
    IBlockStorePtr volumeClient,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config);

}   // namespace NCloud::NBlockStore::NClient
