#pragma once

#include <cloud/blockstore/libs/kms/iface/public.h>

#include <cloud/storage/core/config/grpc_client.pb.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IKmsClientPtr CreateKmsClient(
    ILoggingServicePtr logging,
    ::NCloud::NProto::TGrpcClientConfig config);

}   // namespace NCloud::NBlockStore
