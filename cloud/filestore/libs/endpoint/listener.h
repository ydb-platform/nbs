#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/public/api/protos/endpoint.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IEndpoint
{
    virtual ~IEndpoint() = default;

    virtual NThreading::TFuture<NProto::TError> StartAsync() = 0;
    virtual NThreading::TFuture<void> StopAsync() = 0;
    virtual NThreading::TFuture<void> SuspendAsync() = 0;
    virtual NThreading::TFuture<NProto::TError> AlterAsync(
        bool isReadonly,
        ui64 mountSeqNumber) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IEndpointListener
{
    virtual ~IEndpointListener() = default;

    virtual IEndpointPtr CreateEndpoint(
        const NProto::TEndpointConfig& config) = 0;
};

}   // namespace NCloud::NFileStore
