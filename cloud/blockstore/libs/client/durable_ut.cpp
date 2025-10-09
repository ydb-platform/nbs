#include "durable.h"

#include "client.h"
#include "config.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPostponedTimeTestService
    : public TTestService
{
    const size_t ThrottledRequests;
    size_t requestCount = 0;

    TPostponedTimeTestService(size_t throttledRequests)
        : ThrottledRequests(throttledRequests)
    {}

    NThreading::TFuture<NProto::TPingResponse> Ping(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TPingRequest> request) override
    {
        Y_UNUSED(request);
        ++requestCount;

        NProto::TPingResponse response;

        if (requestCount <= ThrottledRequests) {
            response = TErrorResponse {E_REJECTED, "Throttled"};
            callContext->AddTime(
                EProcessingStage::Postponed,
                TDuration::Seconds(2));
        }

        return MakeFuture(std::move(response));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDurableClientTest)
{
    Y_UNIT_TEST(WellKnownCodesEnumShouldMatch)
    {
        UNIT_ASSERT_EQUAL(
            S_OK,
            static_cast<ui32>(NProto::EWellKnownErrorCode::S_OK));
        UNIT_ASSERT_EQUAL(
            S_FALSE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::S_FALSE));
        UNIT_ASSERT_EQUAL(
            S_ALREADY,
            static_cast<ui32>(NProto::EWellKnownErrorCode::S_ALREADY));
        UNIT_ASSERT_EQUAL(
            E_FAIL,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FAIL));
        UNIT_ASSERT_EQUAL(
            E_ARGUMENT,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_ARGUMENT));
        UNIT_ASSERT_EQUAL(
            E_REJECTED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_REJECTED));
        UNIT_ASSERT_EQUAL(
            E_INVALID_STATE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_INVALID_STATE));
        UNIT_ASSERT_EQUAL(
            E_TIMEOUT,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_TIMEOUT));
        UNIT_ASSERT_EQUAL(
            E_NOT_FOUND,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_NOT_FOUND));
        UNIT_ASSERT_EQUAL(
            E_UNAUTHORIZED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_UNAUTHORIZED));
        UNIT_ASSERT_EQUAL(
            E_NOT_IMPLEMENTED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_NOT_IMPLEMENTED));
        UNIT_ASSERT_EQUAL(
            E_ABORTED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_ABORTED));
        UNIT_ASSERT_EQUAL(
            E_TRY_AGAIN,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_TRY_AGAIN));
        UNIT_ASSERT_EQUAL(
            E_IO,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_IO));
        UNIT_ASSERT_EQUAL(
            E_CANCELLED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_CANCELLED));
        UNIT_ASSERT_EQUAL(
            E_IO_SILENT,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_IO_SILENT));
        UNIT_ASSERT_EQUAL(
            E_RETRY_TIMEOUT,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_RETRY_TIMEOUT));
        UNIT_ASSERT_EQUAL(
            E_PRECONDITION_FAILED,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_PRECONDITION_FAILED));
        UNIT_ASSERT_EQUAL(
            E_GRPC_CANCELLED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_GRPC_CANCELLED));
        UNIT_ASSERT_EQUAL(
            E_GRPC_UNKNOWN,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_GRPC_UNKNOWN));
        UNIT_ASSERT_EQUAL(
            E_GRPC_INVALID_ARGUMENT,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_INVALID_ARGUMENT));
        UNIT_ASSERT_EQUAL(
            E_GRPC_DEADLINE_EXCEEDED,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_DEADLINE_EXCEEDED));
        UNIT_ASSERT_EQUAL(
            E_GRPC_NOT_FOUND,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_GRPC_NOT_FOUND));
        UNIT_ASSERT_EQUAL(
            E_GRPC_ALREADY_EXISTS,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_ALREADY_EXISTS));
        UNIT_ASSERT_EQUAL(
            E_GRPC_PERMISSION_DENIED,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_PERMISSION_DENIED));
        UNIT_ASSERT_EQUAL(
            E_GRPC_RESOURCE_EXHAUSTED,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_RESOURCE_EXHAUSTED));
        UNIT_ASSERT_EQUAL(
            E_GRPC_FAILED_PRECONDITION,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_FAILED_PRECONDITION));
        UNIT_ASSERT_EQUAL(
            E_GRPC_ABORTED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_GRPC_ABORTED));
        UNIT_ASSERT_EQUAL(
            E_GRPC_OUT_OF_RANGE,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_OUT_OF_RANGE));
        UNIT_ASSERT_EQUAL(
            E_GRPC_UNIMPLEMENTED,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_UNIMPLEMENTED));
        UNIT_ASSERT_EQUAL(
            E_GRPC_INTERNAL,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_GRPC_INTERNAL));
        UNIT_ASSERT_EQUAL(
            E_GRPC_UNAVAILABLE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_GRPC_UNAVAILABLE));
        UNIT_ASSERT_EQUAL(
            E_GRPC_DATA_LOSS,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_GRPC_DATA_LOSS));
        UNIT_ASSERT_EQUAL(
            E_GRPC_UNAUTHENTICATED,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_GRPC_UNAUTHENTICATED));
        UNIT_ASSERT_EQUAL(
            E_BS_INVALID_SESSION,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_BS_INVALID_SESSION));
        UNIT_ASSERT_EQUAL(
            E_BS_OUT_OF_SPACE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_BS_OUT_OF_SPACE));
        UNIT_ASSERT_EQUAL(
            E_BS_THROTTLED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_BS_THROTTLED));
        UNIT_ASSERT_EQUAL(
            E_BS_RESOURCE_EXHAUSTED,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_BS_RESOURCE_EXHAUSTED));
        UNIT_ASSERT_EQUAL(
            E_BS_DISK_ALLOCATION_FAILED,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_BS_DISK_ALLOCATION_FAILED));
        UNIT_ASSERT_EQUAL(
            E_BS_MOUNT_CONFLICT,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_BS_MOUNT_CONFLICT));
        UNIT_ASSERT_EQUAL(
            E_FS_IO,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_IO));
        UNIT_ASSERT_EQUAL(
            E_FS_PERM,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_PERM));
        UNIT_ASSERT_EQUAL(
            E_FS_NOENT,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NOENT));
        UNIT_ASSERT_EQUAL(
            E_FS_NXIO,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NXIO));
        UNIT_ASSERT_EQUAL(
            E_FS_ACCESS,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_ACCESS));
        UNIT_ASSERT_EQUAL(
            E_FS_EXIST,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_EXIST));
        UNIT_ASSERT_EQUAL(
            E_FS_XDEV,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_XDEV));
        UNIT_ASSERT_EQUAL(
            E_FS_NODEV,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NODEV));
        UNIT_ASSERT_EQUAL(
            E_FS_NOTDIR,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NOTDIR));
        UNIT_ASSERT_EQUAL(
            E_FS_ISDIR,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_ISDIR));
        UNIT_ASSERT_EQUAL(
            E_FS_INVAL,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_INVAL));
        UNIT_ASSERT_EQUAL(
            E_FS_FBIG,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_FBIG));
        UNIT_ASSERT_EQUAL(
            E_FS_NOSPC,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NOSPC));
        UNIT_ASSERT_EQUAL(
            E_FS_ROFS,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_ROFS));
        UNIT_ASSERT_EQUAL(
            E_FS_MLINK,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_MLINK));
        UNIT_ASSERT_EQUAL(
            E_FS_NAMETOOLONG,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NAMETOOLONG));
        UNIT_ASSERT_EQUAL(
            E_FS_NOTEMPTY,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NOTEMPTY));
        UNIT_ASSERT_EQUAL(
            E_FS_DQUOT,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_DQUOT));
        UNIT_ASSERT_EQUAL(
            E_FS_STALE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_STALE));
        UNIT_ASSERT_EQUAL(
            E_FS_REMOTE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_REMOTE));
        UNIT_ASSERT_EQUAL(
            E_FS_BADHANDLE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_BADHANDLE));
        UNIT_ASSERT_EQUAL(
            E_FS_NOTSUPP,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NOTSUPP));
        UNIT_ASSERT_EQUAL(
            E_FS_WOULDBLOCK,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_WOULDBLOCK));
        UNIT_ASSERT_EQUAL(
            E_FS_NOLCK,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_NOLCK));
        UNIT_ASSERT_EQUAL(
            E_FS_INVALID_SESSION,
            static_cast<ui32>(
                NProto::EWellKnownErrorCode::E_FS_INVALID_SESSION));
        UNIT_ASSERT_EQUAL(
            E_FS_OUT_OF_SPACE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_OUT_OF_SPACE));
        UNIT_ASSERT_EQUAL(
            E_FS_THROTTLED,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_FS_THROTTLED));
        UNIT_ASSERT_EQUAL(
            E_RDMA_UNAVAILABLE,
            static_cast<ui32>(NProto::EWellKnownErrorCode::E_RDMA_UNAVAILABLE));
    }

    Y_UNIT_TEST(ShouldRetryUndeliveredRequests)
    {
        auto client = std::make_shared<TTestService>();

        static constexpr size_t maxRequestsCount = 3;
        size_t requestsCount = 0;

        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);

                NProto::TPingResponse response;

                if (++requestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        auto config = std::make_shared<TClientAppConfig>();

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto future = durable->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        UNIT_ASSERT_EQUAL(requestsCount, maxRequestsCount);
    }

    Y_UNIT_TEST(ShouldUseNonRetriableListEnabledAndNotRetryListed)
    {
        auto client = std::make_shared<TTestService>();

        static constexpr size_t maxRequestsCount = 3;
        size_t requestsCount = 0;

        client->PingHandler = [&](std::shared_ptr<NProto::TPingRequest> request)
        {
            Y_UNUSED(request);

            NProto::TPingResponse response;

            if (++requestsCount < maxRequestsCount) {
                auto& error = *response.MutableError();
                error.SetCode(E_FAIL);
            }

            return MakeFuture(std::move(response));
        };

        auto protoConf = NProto::TClientAppConfig{};
        protoConf.MutableClientConfig()->SetEnableListBasedRetryRules(true);
        protoConf.MutableClientConfig()->AddNonRetriableErrorsForReliableMedia(
            NProto::EWellKnownErrorCode::E_FAIL);

        auto config = std::make_shared<TClientAppConfig>(protoConf);

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=)
        {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto future = durable->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(response));
        UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_FAIL);

        UNIT_ASSERT_EQUAL(requestsCount, 1);
    }

    Y_UNIT_TEST(ShouldUseNonRetriableListWhenEnabledAndNotRetryNeverRetriable)
    {
        auto client = std::make_shared<TTestService>();

        static constexpr size_t maxRequestsCount = 3;
        size_t requestsCount = 0;
        EWellKnownResultCodes errorToReturn = S_OK;

        client->PingHandler = [&](std::shared_ptr<NProto::TPingRequest> request)
        {
            Y_UNUSED(request);

            NProto::TPingResponse response;

            if (++requestsCount < maxRequestsCount) {
                auto& error = *response.MutableError();
                error.SetCode(errorToReturn);
            }

            return MakeFuture(std::move(response));
        };

        auto protoConf = NProto::TClientAppConfig{};
        protoConf.MutableClientConfig()->SetEnableListBasedRetryRules(true);

        auto config = std::make_shared<TClientAppConfig>(protoConf);

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=)
        {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        for (const auto errorCode: NeverRetriableErrors) {
            requestsCount = 0;
            errorToReturn = errorCode;

            auto future = durable->Ping(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TPingRequest>());
            const auto& response = future.GetValue(TDuration::Seconds(5));

            UNIT_ASSERT_EQUAL(response.GetError().GetCode(), errorCode);
            UNIT_ASSERT_EQUAL(requestsCount, 1);
        }
    }

    Y_UNIT_TEST(ShouldUseNonRetriableListWhenEnabledAndRetryNotListed)
    {
        auto client = std::make_shared<TTestService>();

        static constexpr size_t maxRequestsCount = 3;
        size_t requestsCount = 0;

        client->PingHandler = [&](std::shared_ptr<NProto::TPingRequest> request)
        {
            Y_UNUSED(request);

            NProto::TPingResponse response;

            if (++requestsCount < maxRequestsCount) {
                auto& error = *response.MutableError();
                error.SetCode(E_FAIL);
            }

            return MakeFuture(std::move(response));
        };

        auto protoConf = NProto::TClientAppConfig{};
        protoConf.MutableClientConfig()->SetEnableListBasedRetryRules(true);
        protoConf.MutableClientConfig()->AddNonRetriableErrorsForReliableMedia(
            NProto::EWellKnownErrorCode::E_REJECTED);

        auto config = std::make_shared<TClientAppConfig>(protoConf);

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=)
        {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto future = durable->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        UNIT_ASSERT_EQUAL(requestsCount, maxRequestsCount);
    }

    Y_UNIT_TEST(ShouldNotRetryNonRetriableRequests)
    {
        auto client = std::make_shared<TTestService>();

        size_t requestsCount = 0;

        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);
                ++requestsCount;

                NProto::TPingResponse response;

                auto& error = *response.MutableError();
                error.SetCode(E_FAIL);

                return MakeFuture(std::move(response));
            };

        auto config = std::make_shared<TClientAppConfig>();

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto future = durable->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(FAILED(response.GetError().GetCode()));

        UNIT_ASSERT_EQUAL(requestsCount, 1);
    }

    Y_UNIT_TEST(ShouldClosePreviousLocalRequestWhenRetry)
    {
        auto client = std::make_shared<TTestService>();

        std::unique_ptr<TGuardedSgList> prevSgList;

        static constexpr size_t maxRequestsCount = 3;
        size_t requestsCount = 0;

        client->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {

                UNIT_ASSERT(!prevSgList || !prevSgList->Acquire());
                prevSgList = std::make_unique<TGuardedSgList>(request->Sglist);
                UNIT_ASSERT(prevSgList->Acquire());

                NProto::TReadBlocksLocalResponse response;

                if (++requestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        auto config = std::make_shared<TClientAppConfig>();

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto future = durable->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksLocalRequest>());

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        UNIT_ASSERT_EQUAL(requestsCount, maxRequestsCount);
    }

    Y_UNIT_TEST(ShouldNotRetryClosedLocalRequests)
    {
        auto client = std::make_shared<TTestService>();

        auto config = std::make_shared<TClientAppConfig>();

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        {
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->Sglist.Close();

            auto future = durable->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(FAILED(response.GetError().GetCode()));
        }

        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->Sglist.Close();

            auto future = durable->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(FAILED(response.GetError().GetCode()));
        }
    }

    Y_UNIT_TEST(ShouldHandleInFlightRequestsAfterDurableClientStop)
    {
        auto promise = NewPromise<NProto::TPingResponse>();

        auto client = std::make_shared<TTestService>();
        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);
                return promise.GetFuture();
            };

        auto config = std::make_shared<TClientAppConfig>();

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        TFuture<NProto::TPingResponse> future;
        {
            auto durable = CreateDurableClient(
                config,
                client,
                std::move(policy),
                std::move(logging),
                std::move(timer),
                std::move(scheduler),
                std::move(requestStats),
                std::move(volumeStats));
            durable->Start();

            future = durable->Ping(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TPingRequest>());

            durable->Stop();
        }

        NProto::TPingResponse resp;
        auto& error = *resp.MutableError();
        error.SetCode(E_REJECTED);
        promise.SetValue(resp);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(response));
    }

    Y_UNIT_TEST(ShouldHandleWaitingRequestsAfterDurableClientStop)
    {
        auto timer = CreateCpuCycleTimer();
        auto scheduler = std::make_shared<TTestScheduler>();

        auto client = std::make_shared<TTestService>();
        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);

                NProto::TPingResponse response;
                auto& error = *response.MutableError();
                error.SetCode(E_REJECTED);

                return MakeFuture(std::move(response));
            };

        auto config = std::make_shared<TClientAppConfig>();

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        TFuture<NProto::TPingResponse> future;
        {
            auto durable = CreateDurableClient(
                config,
                client,
                std::move(policy),
                std::move(logging),
                std::move(timer),
                scheduler,
                std::move(requestStats),
                std::move(volumeStats));
            durable->Start();

            future = durable->Ping(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TPingRequest>());

            durable->Stop();
        }

        scheduler->RunAllScheduledTasks();

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(response));
    }

    Y_UNIT_TEST(ShouldCloneLocalRequests)
    {
        auto client = std::make_shared<TTestService>();

        static constexpr size_t maxRequestsCount = 3;
        size_t mountRequestsCount = 0;
        size_t unmountRequestsCount = 0;
        size_t readRequestsCount = 0;
        size_t writeRequestsCount = 0;
        size_t zeroRequestsCount = 0;

        TString diskId = "testDiskId";
        ui64 startIndex = 42;
        ui32 blocksCount = 7;

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, 'f'));

        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(diskId, request->GetDiskId());

                request->Clear();

                NProto::TMountVolumeResponse response;

                if (++mountRequestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        client->UnmountVolumeHandler =
            [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(diskId, request->GetDiskId());

                request->Clear();

                NProto::TUnmountVolumeResponse response;

                if (++unmountRequestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        client->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(diskId, request->GetDiskId());
                UNIT_ASSERT(request->GetStartIndex() == startIndex);
                UNIT_ASSERT(request->GetBlocksCount() == blocksCount);

                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                UNIT_ASSERT(guard.Get() == sglist);

                request->Clear();
                request->CommitId = 0;
                request->BlockSize = 0;
                request->Sglist = TGuardedSgList();

                NProto::TReadBlocksLocalResponse response;

                if (++readRequestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        client->WriteBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(diskId, request->GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(startIndex, request->GetStartIndex());
                UNIT_ASSERT_VALUES_EQUAL(blocksCount, request->BlocksCount);

                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                UNIT_ASSERT(guard.Get() == sglist);

                request->Clear();
                request->BlocksCount = 0;
                request->BlockSize = 0;
                request->Sglist = TGuardedSgList();

                NProto::TWriteBlocksLocalResponse response;

                if (++writeRequestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        client->ZeroBlocksHandler =
            [&] (std::shared_ptr<NProto::TZeroBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(diskId, request->GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(startIndex, request->GetStartIndex());
                UNIT_ASSERT_VALUES_EQUAL(blocksCount, request->GetBlocksCount());

                request->Clear();

                NProto::TZeroBlocksResponse response;

                if (++zeroRequestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        auto config = std::make_shared<TClientAppConfig>();

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        {
            auto request = std::make_shared<NProto::TMountVolumeRequest>();
            request->SetDiskId(diskId);

            auto future = durable->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto request = std::make_shared<NProto::TUnmountVolumeRequest>();
            request->SetDiskId(diskId);

            auto future = durable->UnmountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetDiskId(diskId);
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);
            request->BlockSize = DefaultBlockSize;
            request->Sglist = TGuardedSgList(sglist);

            auto future = durable->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetDiskId(diskId);
            request->SetStartIndex(startIndex);
            request->BlocksCount = blocksCount;
            request->BlockSize = DefaultBlockSize;
            request->Sglist = TGuardedSgList(sglist);

            auto future = durable->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetDiskId(diskId);
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);

            auto future = durable->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        UNIT_ASSERT_VALUES_EQUAL(mountRequestsCount, maxRequestsCount);
        UNIT_ASSERT_VALUES_EQUAL(unmountRequestsCount, maxRequestsCount);
        UNIT_ASSERT_VALUES_EQUAL(readRequestsCount, maxRequestsCount);
        UNIT_ASSERT_VALUES_EQUAL(writeRequestsCount, maxRequestsCount);
        UNIT_ASSERT_VALUES_EQUAL(zeroRequestsCount, maxRequestsCount);
    }

    Y_UNIT_TEST(ShouldNotCloneNonLocalRequests)
    {
        auto client = std::make_shared<TTestService>();

        static constexpr size_t maxRequestsCount = 3;
        size_t readRequestsCount = 0;
        size_t writeRequestsCount = 0;

        TString diskId = "testDiskId";
        ui64 startIndex = 42;
        ui32 blocksCount = 7;

        client->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                if (readRequestsCount == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(diskId, request->GetDiskId());
                    UNIT_ASSERT_VALUES_EQUAL(startIndex, request->GetStartIndex());
                    UNIT_ASSERT_VALUES_EQUAL(blocksCount, request->GetBlocksCount());
                } else {
                    UNIT_ASSERT(request->GetDiskId().empty());
                    UNIT_ASSERT_VALUES_EQUAL(0, request->GetStartIndex());
                    UNIT_ASSERT_VALUES_EQUAL(0, request->GetBlocksCount());
                }

                request->Clear();

                NProto::TReadBlocksResponse response;

                if (++readRequestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        client->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                if (writeRequestsCount == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(diskId, request->GetDiskId());
                    UNIT_ASSERT_VALUES_EQUAL(startIndex, request->GetStartIndex());
                    UNIT_ASSERT_VALUES_EQUAL(blocksCount, request->GetBlocks().GetBuffers().size());
                } else {
                    UNIT_ASSERT(request->GetDiskId().empty());
                    UNIT_ASSERT_VALUES_EQUAL(0, request->GetStartIndex());
                    UNIT_ASSERT(request->GetBlocks().GetBuffers().empty());
                }

                request->Clear();

                NProto::TWriteBlocksResponse response;

                if (++writeRequestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_REJECTED);
                }

                return MakeFuture(std::move(response));
            };

        auto config = std::make_shared<TClientAppConfig>();

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        {
            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            request->SetDiskId(diskId);
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);

            auto future = durable->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                request);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT(request->GetDiskId().empty());
            UNIT_ASSERT_VALUES_EQUAL(0, request->GetStartIndex());
            UNIT_ASSERT_VALUES_EQUAL(0, request->GetBlocksCount());
        }

        {
            auto request = std::make_shared<NProto::TWriteBlocksRequest>();
            request->SetDiskId(diskId);
            request->SetStartIndex(startIndex);
            ResizeIOVector(
                *request->MutableBlocks(),
                blocksCount,
                DefaultBlockSize);

            auto future = durable->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                request);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT(request->GetDiskId().empty());
            UNIT_ASSERT_VALUES_EQUAL(0, request->GetStartIndex());
            UNIT_ASSERT(request->GetBlocks().GetBuffers().empty());
        }

        UNIT_ASSERT_VALUES_EQUAL(readRequestsCount, maxRequestsCount);
        UNIT_ASSERT_VALUES_EQUAL(writeRequestsCount, maxRequestsCount);
    }

    Y_UNIT_TEST(ShouldCalculateCorrectTimeoutForYDBDisks)
    {
        NProto::TClientAppConfig configProto;
        auto& clientConfigProto = *configProto.MutableClientConfig();
        clientConfigProto.SetRetryTimeoutIncrement(2'000);
        clientConfigProto.SetYDBBasedDiskInitialRetryTimeout(1'000);
        clientConfigProto.SetConnectionErrorMaxRetryTimeout(4'000);
        // Should not be used in this test.
        clientConfigProto.SetDiskRegistryBasedDiskInitialRetryTimeout(500);
        auto config = std::make_shared<TClientAppConfig>(configProto);

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        {
            TRetryState state;

            auto spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(3), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(4), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(5), spec.Backoff);
        }

        {
            TRetryState state;

            auto spec = policy->ShouldRetry(state, MakeError(S_OK));
            UNIT_ASSERT(!spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), spec.Backoff);

            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(3), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(4), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(4), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(5), spec.Backoff);
        }
    }

    Y_UNIT_TEST(ShouldCalculateCorrectTimeoutForDiskRegistryBasedDisk)
    {
        NProto::TClientAppConfig configProto;
        auto& clientConfigProto = *configProto.MutableClientConfig();
        clientConfigProto.SetRetryTimeoutIncrement(3'000);
        clientConfigProto.SetConnectionErrorMaxRetryTimeout(7'000);
        clientConfigProto.SetDiskRegistryBasedDiskInitialRetryTimeout(2'000);
        // Should not be used in this test.
        clientConfigProto.SetYDBBasedDiskInitialRetryTimeout(500);
        auto config = std::make_shared<TClientAppConfig>(configProto);

        auto policy =
            CreateRetryPolicy(config, NProto::STORAGE_MEDIA_SSD_MIRROR3);

        {
            TRetryState state;

            auto spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(2), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(5), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(8), spec.Backoff);
        }

        {
            TRetryState state;

            auto spec = policy->ShouldRetry(state, MakeError(S_OK));
            UNIT_ASSERT(!spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), spec.Backoff);

            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(2), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(5), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), spec.Backoff);
        }
    }

    Y_UNIT_TEST(ShouldCalculateCorrectTimeoutForDefaultPolicy)
    {
        NProto::TClientAppConfig configProto;
        auto& clientConfigProto = *configProto.MutableClientConfig();
        clientConfigProto.SetRetryTimeoutIncrement(3'000);
        clientConfigProto.SetConnectionErrorMaxRetryTimeout(7'000);
        // Should not be used in this test.
        clientConfigProto.SetDiskRegistryBasedDiskInitialRetryTimeout(500);
        clientConfigProto.SetYDBBasedDiskInitialRetryTimeout(500);
        auto config = std::make_shared<TClientAppConfig>(configProto);

        auto policy = CreateRetryPolicy(config, std::nullopt);

        {
            TRetryState state;

            auto spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(3), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(6), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(9), spec.Backoff);
        }

        {
            TRetryState state;

            auto spec = policy->ShouldRetry(state, MakeError(S_OK));
            UNIT_ASSERT(!spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), spec.Backoff);

            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(3), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(6), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_GRPC_UNAVAILABLE));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), spec.Backoff);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(9), spec.Backoff);
        }
    }

    Y_UNIT_TEST(ShouldCalculateCorrectTimeoutForInstantRetryFlag)
    {
        NProto::TClientAppConfig configProto;
        auto& clientConfigProto = *configProto.MutableClientConfig();
        clientConfigProto.SetRetryTimeoutIncrement(3'000);
        clientConfigProto.SetConnectionErrorMaxRetryTimeout(7'000);
        // Should not be used in this test.
        clientConfigProto.SetDiskRegistryBasedDiskInitialRetryTimeout(500);
        clientConfigProto.SetYDBBasedDiskInitialRetryTimeout(500);
        auto config = std::make_shared<TClientAppConfig>(configProto);

        auto policy = CreateRetryPolicy(config, std::nullopt);
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_INSTANT_RETRIABLE);

        {
            TRetryState state;

            auto spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(3), spec.Backoff);
            UNIT_ASSERT(!state.DoneInstantRetry);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED, "", flags));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), spec.Backoff);
            UNIT_ASSERT(state.DoneInstantRetry);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(6), spec.Backoff);
            UNIT_ASSERT(state.DoneInstantRetry);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED, "", flags));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(9), spec.Backoff);
            UNIT_ASSERT(state.DoneInstantRetry);
        }

        {
            TRetryState state;

            auto spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(3), spec.Backoff);
            UNIT_ASSERT(!state.DoneInstantRetry);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(6), spec.Backoff);
            UNIT_ASSERT(!state.DoneInstantRetry);

            state.Retries++;
            spec = policy->ShouldRetry(
                state,
                MakeError(E_GRPC_UNAVAILABLE, "", flags));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), spec.Backoff);
            UNIT_ASSERT(state.DoneInstantRetry);

            state.Retries++;
            spec = policy->ShouldRetry(
                state,
                MakeError(E_GRPC_UNAVAILABLE, "", flags));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), spec.Backoff);
            UNIT_ASSERT(state.DoneInstantRetry);

            state.Retries++;
            spec = policy->ShouldRetry(state, MakeError(E_REJECTED, "", flags));
            UNIT_ASSERT(spec.ShouldRetry);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(9), spec.Backoff);
            UNIT_ASSERT(state.DoneInstantRetry);
        }
    }

    Y_UNIT_TEST(ShouldReturnNonRetriableErrorAfterRetryTimeout)
    {
        auto client = std::make_shared<TTestService>();

        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);

                return MakeFuture<NProto::TPingResponse>(
                    TErrorResponse(E_REJECTED));
            };

        NProto::TClientAppConfig clientAppConfig;
        auto& appConfig = *clientAppConfig.MutableClientConfig();
        appConfig.SetRetryTimeout(300);
        auto config = std::make_shared<TClientAppConfig>(clientAppConfig);

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            policy,
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto future = durable->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(
            EErrorKind::ErrorRetriable != GetErrorKind(response.GetError()),
            response.GetError());

        TRetryState retryState;
        auto retrySpec = policy->ShouldRetry(retryState, response.GetError());
        UNIT_ASSERT(!retrySpec.ShouldRetry);
    }

    Y_UNIT_TEST(ShouldKeepPostponedTimeDuringRetries)
    {
        auto client = std::make_shared<TPostponedTimeTestService>(4);

        NProto::TClientAppConfig configProto;
        auto& clientConfigProto = *configProto.MutableClientConfig();
        clientConfigProto.SetRetryTimeoutIncrement(1);
        auto config = std::make_shared<TClientAppConfig>(configProto);

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto callContext = MakeIntrusive<TCallContext>();

        auto future = durable->Ping(
            callContext,
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValueSync();
        UNIT_ASSERT(SUCCEEDED(response.GetError().GetCode()));

        auto expectedDelay = TDuration::Seconds(8);
        auto errorText = TStringBuilder() <<
            "Request was posponed for " <<
            callContext->Time(EProcessingStage::Postponed) <<
            " which is less than " <<
            expectedDelay;

        UNIT_ASSERT_GE_C(
            callContext->Time(EProcessingStage::Postponed), expectedDelay, errorText);
    }

    Y_UNIT_TEST(ShouldCountBackoffTimeAsPostponedForThrottledRequests)
    {
        auto client = std::make_shared<TTestService>();

        size_t requestCount = 0;

        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);

                NProto::TPingResponse response;
                if (!requestCount++) {
                    response = TErrorResponse {E_REJECTED, "Throttled"};
                }
                return MakeFuture(std::move(response));
            };

        NProto::TClientAppConfig configProto;
        auto& clientConfigProto = *configProto.MutableClientConfig();
        clientConfigProto.SetRetryTimeoutIncrement(
            TDuration::Seconds(2).MilliSeconds());
        clientConfigProto.SetDiskRegistryBasedDiskInitialRetryTimeout(
            TDuration::Seconds(1).MilliSeconds());
        auto config = std::make_shared<TClientAppConfig>(configProto);

        auto policy =
            CreateRetryPolicy(config, NProto::STORAGE_MEDIA_SSD_MIRROR3);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto callContext = MakeIntrusive<TCallContext>();

        auto future = durable->Ping(
            callContext,
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValueSync();
        UNIT_ASSERT(SUCCEEDED(response.GetError().GetCode()));

        const auto actualDelay = callContext->Time(EProcessingStage::Postponed);
        {
            constexpr auto expectedDelay = TDuration::Seconds(1);
            auto errorText = TStringBuilder()
                             << "Request was posponed for " << actualDelay
                             << " which is less than " << expectedDelay;
            UNIT_ASSERT_GE_C(actualDelay, expectedDelay, errorText);
        }

        {
            constexpr auto ExpectedDelay = TDuration::Seconds(2);
            auto errorText = TStringBuilder()
                             << "Request was posponed for " << actualDelay
                             << " which is more than " << ExpectedDelay;
            UNIT_ASSERT_LT_C(actualDelay, ExpectedDelay, errorText);
        }
    }

    Y_UNIT_TEST(ShouldNotCountBackoffTimeAsPostponedForNotThrottledRequests)
    {
        auto client = std::make_shared<TTestService>();

        size_t requestCount = 0;

        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);

                NProto::TPingResponse response;
                if (!requestCount++) {
                    response = TErrorResponse {E_REJECTED, "Some failure"};
                }
                return MakeFuture(std::move(response));
            };

        NProto::TClientAppConfig configProto;
        auto& clientConfigProto = *configProto.MutableClientConfig();
        clientConfigProto.SetRetryTimeoutIncrement(TDuration::Seconds(1).MilliSeconds());
        auto config = std::make_shared<TClientAppConfig>(configProto);

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto callContext = MakeIntrusive<TCallContext>();

        auto future = durable->Ping(
            callContext,
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValueSync();
        UNIT_ASSERT(SUCCEEDED(response.GetError().GetCode()));

        UNIT_ASSERT_VALUES_EQUAL(
            callContext->Time(EProcessingStage::Postponed), TDuration());
    }

    Y_UNIT_TEST(ShouldIncreaseRequestTimeoutOnRetry)
    {
        auto client = std::make_shared<TTestService>();

        static constexpr uint32_t expectedTimeoutsSec[] = {30, 70, 110, 120};
        static constexpr size_t maxRequestsCount = std::size(expectedTimeoutsSec);
        size_t requestsCount = 0;

        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                const auto& headers = request->GetHeaders();
                const auto requestTimeoutMsec = headers.GetRequestTimeout();
                UNIT_ASSERT_EQUAL(
                    requestTimeoutMsec,
                    expectedTimeoutsSec[requestsCount] * 1000);

                UNIT_ASSERT_VALUES_EQUAL(
                    requestsCount,
                    headers.GetRetryNumber());

                NProto::TPingResponse response;

                if (++requestsCount < maxRequestsCount) {
                    auto& error = *response.MutableError();
                    error.SetCode(E_TIMEOUT);
                }

                return MakeFuture(std::move(response));
            };

        NProto::TClientAppConfig configProto;
        auto& clientConfigProto = *configProto.MutableClientConfig();
        clientConfigProto.SetRequestTimeout(TDuration::Seconds(30).MilliSeconds());
        clientConfigProto.SetRequestTimeoutIncrementOnRetry(TDuration::Seconds(40).MilliSeconds());
        clientConfigProto.SetRequestTimeoutMax(TDuration::Minutes(2).MilliSeconds());
        auto config = std::make_shared<TClientAppConfig>(configProto);

        auto policy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_SCOPE_EXIT(=) {
            scheduler->Stop();
        };

        auto requestStats = CreateRequestStatsStub();
        auto volumeStats = CreateVolumeStatsStub();

        auto logging = CreateLoggingService("console");

        auto durable = CreateDurableClient(
            config,
            client,
            std::move(policy),
            std::move(logging),
            std::move(timer),
            std::move(scheduler),
            std::move(requestStats),
            std::move(volumeStats));

        auto request = std::make_shared<NProto::TPingRequest>();
        request->MutableHeaders()->SetRequestTimeout(
            TDuration::Seconds(30).MilliSeconds());
        auto future = durable->Ping(
            MakeIntrusive<TCallContext>(),
            request);

        const auto& response = future.GetValue(TDuration::Minutes(30));
        UNIT_ASSERT(!HasError(response));

        UNIT_ASSERT_EQUAL(requestsCount, maxRequestsCount);
    }
}

}   // namespace NCloud::NBlockStore::NClient
