#pragma once

#include <cloud/blockstore/config/grpc_client.pb.h>

#include <cloud/blockstore/libs/opentelemetry/iface/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

ITraceServiceClientPtr CreateTraceServiceClient(
    ILoggingServicePtr logging,
    NProto::TGrpcClientConfig config);

}   // namespace NCloud::NBlockStore
