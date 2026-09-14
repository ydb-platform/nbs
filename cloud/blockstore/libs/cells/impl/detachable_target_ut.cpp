#include "detachable_target.h"

#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/service_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <atomic>
#include <thread>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRecordingTarget: public ITransportTarget
{
    TVector<IBlockStorePtr> Targets;
    std::atomic<ui32> Calls{0};

    void SetTarget(IBlockStorePtr target) override
    {
        Targets.push_back(std::move(target));
        Calls.fetch_add(1);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDetachableTargetTest)
{
    Y_UNIT_TEST(ShouldForwardUntilDetached)
    {
        auto recording = std::make_shared<TRecordingTarget>();
        auto detachable = CreateDetachableTarget(recording);

        auto first = std::make_shared<TTestService>();
        detachable->SetTarget(first);

        UNIT_ASSERT_VALUES_EQUAL(1, recording->Targets.size());
        UNIT_ASSERT(recording->Targets[0] == first);
    }

    Y_UNIT_TEST(ShouldStaySilentAfterDetach)
    {
        auto recording = std::make_shared<TRecordingTarget>();
        auto detachable = CreateDetachableTarget(recording);

        detachable->Detach();
        detachable->SetTarget(std::make_shared<TTestService>());

        UNIT_ASSERT_VALUES_EQUAL(0, recording->Targets.size());
    }

    Y_UNIT_TEST(ShouldReleaseTargetOnDetach)
    {
        auto recording = std::make_shared<TRecordingTarget>();
        std::weak_ptr<TRecordingTarget> weak = recording;

        auto detachable = CreateDetachableTarget(recording);
        recording.reset();

        UNIT_ASSERT_C(weak.lock(), "the target must be held until detached");

        detachable->Detach();
        UNIT_ASSERT_C(!weak.lock(), "detaching must release the target");
    }

    Y_UNIT_TEST(ShouldStopForwardingForOtherThreads)
    {
        auto recording = std::make_shared<TRecordingTarget>();
        auto detachable = CreateDetachableTarget(recording);

        std::atomic<bool> stop{false};
        std::thread writer(
            [&]
            {
                while (!stop.load()) {
                    detachable->SetTarget(
                        std::make_shared<TTestService>());
                }
            });

        Sleep(TDuration::MilliSeconds(10));

        detachable->Detach();
        const auto seen = recording->Calls.load();

        Sleep(TDuration::MilliSeconds(10));

        // a safety property: once Detach has returned, nothing reaches the
        // target again, however the two threads interleave
        UNIT_ASSERT_VALUES_EQUAL(seen, recording->Calls.load());

        stop.store(true);
        writer.join();
    }
}

}   // namespace NCloud::NBlockStore::NCells
