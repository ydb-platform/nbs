#pragma once

#include "public.h"

#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TCompletedRequest
{
    NProto::EAction Action{};
    TDuration Elapsed;
    NProto::TError Error;

    TCompletedRequest() = default;

    TCompletedRequest(NProto::EAction action, TInstant start, NProto::TError error) noexcept
        : Action(action)
        , Elapsed(TInstant::Now() - start)
        , Error(std::move(error))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct IRequestGenerator
{
    virtual ~IRequestGenerator() = default;

    virtual bool HasNextRequest() = 0;
    virtual NThreading::TFuture<TCompletedRequest> ExecuteNextRequest() = 0;

    // With false collect request futures and process them in bulk
    // With true process every request future immediately after ExecuteNextRequest
    virtual bool ShouldImmediatelyProcessQueue()
    {
        return false;
    }

    virtual bool ShouldFailOnError()
    {
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateIndexRequestGenerator(
    NProto::TIndexLoadSpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers);

IRequestGeneratorPtr CreateDataRequestGenerator(
    NProto::TDataLoadSpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers);

IRequestGeneratorPtr CreateReplayRequestGeneratorFs(
    NProto::TReplaySpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers);

}   // namespace NCloud::NFileStore::NLoadTest
