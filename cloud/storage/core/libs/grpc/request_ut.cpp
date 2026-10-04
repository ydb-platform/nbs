#include "request.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace NCloud::NStorage::NGrpc {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestHandler: public TRequestHandlerBase
{
    std::atomic<bool> Cancelled = false;

    void Process(bool ok) override
    {
        Y_UNUSED(ok);
    }

    void Cancel() override
    {
        Cancelled = true;
    }
};

inline constexpr TStringBuf TestEntityType = "Test";

using TTestRequestsInFlight = TRequestsInFlight<TTestHandler, TestEntityType>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestsInFlightTest)
{
    Y_UNIT_TEST(ShouldDrainInFlightRequestsBeforeShutdownReturns)
    {
        TTestRequestsInFlight requests;
        TTestHandler handler;

        UNIT_ASSERT(requests.Register(&handler));

        std::atomic<bool> shutdownReturned = false;
        std::thread shutdownThread([&]
        {
            requests.Shutdown();
            shutdownReturned = true;
        });

        while (!handler.Cancelled.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        const bool returnedEarly = shutdownReturned.load();

        // the request finishes its enqueue/processing and unregisters - only
        // now may Shutdown() return
        requests.Unregister(&handler);
        shutdownThread.join();

        UNIT_ASSERT(!returnedEarly);
        UNIT_ASSERT(shutdownReturned.load());
    }

    Y_UNIT_TEST(ShouldRejectRegisterAfterShutdown)
    {
        TTestRequestsInFlight requests;

        // no in-flight requests, so Shutdown() drains at once
        requests.Shutdown();

        TTestHandler handler;
        UNIT_ASSERT(!requests.Register(&handler));
    }
}

}   // namespace NCloud::NStorage::NGrpc
