#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ITraceServiceClient: public IStartable
{
    using TRequest =
        opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
    using TResponse = TResultOrError<
        opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse>;

    virtual NThreading::TFuture<TResponse> Export(
        TRequest traces,
        const TString& authToken) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITraceServiceClientPtr CreateTraceServiceClientStub();

}   // namespace NCloud
