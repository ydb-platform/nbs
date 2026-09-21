#include "request.h"

#include <cloud/storage/core/libs/common/verify.h>

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

using TTestRequestsInFlight = TRequestsInFlight<
    TTestHandler,
    TNoIdRequestsInFlightDiag<TWellKnownEntityTypes::SERVER>>;

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
        // A correct Shutdown() spins while the handler is registered, so it
        // cannot have returned here whatever the timing - this assert never
        // flakes. There is no way to directly observe "still inside Shutdown()",
        // so the 1s just gives a regressed, non-draining Shutdown() ample time
        // to wrongly return and be caught.
        std::this_thread::sleep_for(std::chrono::seconds(1));
        UNIT_ASSERT(!shutdownReturned.load());

        // the request finishes its enqueue/processing and unregisters - only
        // now may Shutdown() return
        requests.Unregister(&handler);
        shutdownThread.join();
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
