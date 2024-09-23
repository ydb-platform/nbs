#pragma once

#include "public.h"

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/tools/testing/replay/protos/replay.pb.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NReplay {

////////////////////////////////////////////////////////////////////////////////

struct TCompletedRequest
{
    NProto::EAction Action{};
    TDuration Elapsed;
    NProto::TError Error;
    bool Stop{};
    TCompletedRequest() = default;

    TCompletedRequest(
        NProto::EAction action,
        TInstant start,
        NProto::TError error) noexcept
        : Action(action)
        , Elapsed(TInstant::Now() - start)
        , Error(std::move(error))
    {}
    TCompletedRequest(bool stop) noexcept
        : Stop{stop}
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct IRequestGenerator
{
    virtual ~IRequestGenerator() = default;

    virtual bool HasNextRequest() = 0;
    // virtual TInstant NextRequestAt() = 0;
    virtual NThreading::TFuture<TCompletedRequest> ExecuteNextRequest() = 0;

    virtual bool InstantProcessQueue()
    {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateReplayRequestGenerator(
    NProto::TReplaySpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers);

}   // namespace NCloud::NFileStore::NReplay
