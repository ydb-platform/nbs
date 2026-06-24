#include "executor.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

namespace NCloud {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

struct TTestRequest {};
using TTestResponse = TResultOrError<int>;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TExecutorTest)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto future = executor->Execute([] {
            return 42;
        });

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldWaitForFuture)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto request = NewPromise<TTestRequest>();
        auto response = NewPromise<TTestResponse>();

        auto future = executor->Execute([=] () mutable {
            request.SetValue({});

            auto resp = executor->ExtractResponse(response.GetFuture());
            UNIT_ASSERT(!HasError(resp));

            return resp.GetResult();
        });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({ 42 });

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldIgnoreFutureCompletionAfterCanceledWait)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto promise = NewPromise<void>();
        TCont* cont = nullptr;

        // Start waiting for an unresolved future. ResultOrError() will suspend
        // inside WaitForI() and keep a stack-local wait event alive only until
        // the wait is canceled.
        auto future = executor->Execute([&] {
            cont = RunningCont();
            UNIT_ASSERT(cont);
            return executor->ResultOrError(promise.GetFuture()).GetError();
        });

        // Cancel the wait before the promise is completed. This makes WaitForI()
        // return and destroys the stack-local wait event.
        executor->Execute([&] {
            UNIT_ASSERT(cont);
            cont->Cancel();
        });

        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            error.GetCode(),
            FormatError(error));

        // Now complete the promise. Its subscription still runs and schedules a
        // Signal() on the executor. This used to hit the already-destroyed event.
        promise.SetValue();

        // Pump the executor so the late scheduled callback is actually executed.
        executor->Execute([]{}).Wait();

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldExtractResponse)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto request = NewPromise<TTestRequest>();
        auto response = NewPromise<TTestResponse>();

        auto future = executor->Execute([=] () mutable {
            request.SetValue({});

            auto resp = executor->ExtractResponse(response.GetFuture());
            UNIT_ASSERT(!HasError(resp));

            return resp.GetResult();
        });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({ 42 });

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldGetResultOrError)
    {
        auto executor = TExecutor::Create("TEST");
        executor->Start();

        auto request = NewPromise<TTestRequest>();
        auto response = NewPromise<int>();

        auto future = executor->Execute([=] () mutable {
            request.SetValue({});

            auto resp = executor->ResultOrError(response.GetFuture());
            UNIT_ASSERT(!HasError(resp));

            return resp.GetResult();
        });

        request.GetFuture().GetValue(WaitTimeout);
        response.SetValue({ 42 });

        auto result = future.GetValue(WaitTimeout);
        UNIT_ASSERT(result == 42);

        executor->Stop();
    }
}

}   // namespace NCloud
