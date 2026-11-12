#include "address_resolver.h"

#include <cloud/storage/core/libs/common/task_queue.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TManualExecutor final: public ITaskQueue
{
public:
    TVector<ITaskPtr> Tasks;

    void Start() override
    {}

    void Stop() override
    {}

    void Enqueue(ITaskPtr task) override
    {
        Tasks.push_back(std::move(task));
    }

    void Run()
    {
        auto tasks = std::move(Tasks);
        for (const auto& task: tasks) {
            task->Execute();
        }
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TAddressResolverTest)
{
    Y_UNIT_TEST(QueuesLookupAndOwnsItsArguments)
    {
        auto executor = std::make_shared<TManualExecutor>();
        auto resolver = CreateAddressResolver(executor);
        TString host = "127.0.0.1";
        auto future = resolver->Resolve(host, 1234);
        host = "different-host";
        resolver.reset();
        UNIT_ASSERT(!future.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(executor->Tasks.size(), 1);
        executor->Run();
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        const auto& result = future.GetValue();
        UNIT_ASSERT_C(
            !HasError(result.GetError()),
            FormatError(result.GetError()));
        UNIT_ASSERT(!result.GetResult().empty());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResult().front().GetPort(), 1234);
    }

    Y_UNIT_TEST(ReturnsResolutionFailureAsError)
    {
        auto executor = std::make_shared<TManualExecutor>();
        auto resolver = CreateAddressResolver(executor);
        auto future = resolver->Resolve("::1::", 1234);
        UNIT_ASSERT(!future.IsReady());
        executor->Run();
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!future.HasException());
        UNIT_ASSERT_VALUES_EQUAL(
            future.GetValue().GetError().GetCode(),
            E_UNAVAILABLE);
    }
}

}   // namespace NCloud::NIcV2
