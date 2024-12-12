#pragma once

#include "public.h"
#include "request.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/public/api/protos/endpoint.pb.h>
#include <cloud/filestore/public/api/protos/ping.pb.h>

#include <cloud/storage/core/protos/error.pb.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointManager
    : public IStartable
{
    virtual ~IEndpointManager() = default;

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    virtual NThreading::TFuture<NProto::T##name##Response> name(               \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) = 0;                \
// FILESTORE_DECLARE_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD

    virtual void Drain() = 0;

    virtual NThreading::TFuture<void> RestoreEndpoints() = 0;
};

}   // namespace NCloud::NFileStore
