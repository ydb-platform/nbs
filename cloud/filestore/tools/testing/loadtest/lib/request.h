#pragma once

#include "public.h"
#include "shm_client.h"

#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NLoadTest {

class TFileCreationLimiter
{
private:
    const ui64 MaxFileCount;
    std::atomic<ui64> ReservedFileCount = 0;

public:
    explicit TFileCreationLimiter(ui64 maxFileCount)
        : MaxFileCount(maxFileCount)
    {}

    bool TryReserve()
    {
        if (!MaxFileCount) {
            return true;
        }

        auto count = ReservedFileCount.load(std::memory_order_relaxed);
        while (count < MaxFileCount &&
               !ReservedFileCount.compare_exchange_weak(
                   count,
                   count + 1,
                   std::memory_order_relaxed))
        {}
        return count < MaxFileCount;
    }

    void Release()
    {
        if (MaxFileCount) {
            ReservedFileCount.fetch_sub(1, std::memory_order_relaxed);
        }
    }
};

using TFileCreationLimiterPtr = std::shared_ptr<TFileCreationLimiter>;

////////////////////////////////////////////////////////////////////////////////

struct TCompletedRequest
{
    NProto::EAction Action{};
    TDuration Elapsed;
    ui64 RequestBytes = 0;
    NProto::TError Error;

    TCompletedRequest() = default;

    TCompletedRequest(
            NProto::EAction action,
            TInstant start,
            NProto::TError error,
            ui64 requestBytes = 0) noexcept
        : Action(action)
        , Elapsed(TInstant::Now() - start)
        , RequestBytes(requestBytes)
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

    virtual bool ShouldFailOnError(const NProto::TError& error)
    {
        Y_UNUSED(error);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateIndexRequestGenerator(
    NProto::TIndexLoadSpec spec,
    ILoggingServicePtr logging,
    IFileStoreServicePtr client,
    NClient::ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers,
    TFileCreationLimiterPtr fileCreationLimiter);

IRequestGeneratorPtr CreateDataRequestGenerator(
    NProto::TDataLoadSpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers,
    TFileCreationLimiterPtr fileCreationLimiter);

IRequestGeneratorPtr CreateReplayRequestGeneratorFs(
    NProto::TReplaySpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers,
    TFileCreationLimiterPtr fileCreationLimiter);

IRequestGeneratorPtr CreateReplayRequestGeneratorGRPC(
    NProto::TReplaySpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers,
    TFileCreationLimiterPtr fileCreationLimiter);

IRequestGeneratorPtr CreateFastShardRequestGenerator(
    NProto::TFastShardLoadSpec spec,
    ui32 maxParallelism,
    ILoggingServicePtr logging,
    TFileCreationLimiterPtr fileCreationLimiter);

IRequestGeneratorPtr CreateDatashardLikeRequestGenerator(
    NProto::TDatashardLikeLoadSpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    IShmDataClientPtr dataClient,
    TString filesystemId,
    NProto::THeaders headers,
    TFileCreationLimiterPtr fileCreationLimiter);

}   // namespace NCloud::NFileStore::NLoadTest
