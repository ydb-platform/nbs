#pragma once

#include <cloud/storage/core/config/grpc_client.pb.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/opentelemetry/iface/public.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

ITraceServiceClientPtr CreateTraceServiceClient(
    ILoggingServicePtr logging,
    NProto::TGrpcClientConfig config);

}   // namespace NCloud
