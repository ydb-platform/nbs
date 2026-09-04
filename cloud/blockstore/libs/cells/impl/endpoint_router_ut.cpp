#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCountingService: public TTestService
{
    ui32 ReadCount = 0;
    ui32 AllocateCount = 0;
    TPromise<NProto::TReadBlocksLocalResponse> Pending;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        ++AllocateCount;
        return nullptr;
    }

    TCountingService()
    {
        ReadBlocksLocalHandler =
            [this](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(request);
            ++ReadCount;
            return Pending.Initialized()
                       ? Pending.GetFuture()
                       : MakeFuture(NProto::TReadBlocksLocalResponse{});
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TReadBlocksLocalResponse> Read(const IBlockStorePtr& endpoint)
{
    return endpoint->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::make_shared<NProto::TReadBlocksLocalRequest>());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEndpointRouterTest)
{
    Y_UNIT_TEST(ShouldForwardRequestsToInitialTarget)
    {
        auto target = std::make_shared<TCountingService>();

        auto router = CreateEndpointRouter(target);
        Read(router);

        UNIT_ASSERT_VALUES_EQUAL(1, target->ReadCount);
    }

    Y_UNIT_TEST(ShouldForwardRequestsToNewTargetAfterSetTarget)
    {
        auto first = std::make_shared<TCountingService>();
        auto second = std::make_shared<TCountingService>();

        auto router = CreateEndpointRouter(first);
        router->SetTarget(second);
        Read(router);

        UNIT_ASSERT_VALUES_EQUAL(0, first->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(1, second->ReadCount);
    }

    Y_UNIT_TEST(ShouldAllocateBufferOnCurrentTarget)
    {
        auto target = std::make_shared<TCountingService>();

        auto router = CreateEndpointRouter(target);
        router->AllocateBuffer(4096);

        UNIT_ASSERT_VALUES_EQUAL(1, target->AllocateCount);
    }

    Y_UNIT_TEST(ShouldKeepReplacedTargetAliveUntilItsRequestsComplete)
    {
        auto promise = NewPromise<NProto::TReadBlocksLocalResponse>();

        auto first = std::make_shared<TCountingService>();
        first->Pending = promise;

        auto router = CreateEndpointRouter(first);
        auto future = Read(router);
        UNIT_ASSERT(!future.HasValue());

        router->SetTarget(std::make_shared<TCountingService>());

        std::weak_ptr<TCountingService> weakFirst = first;
        first.reset();

        UNIT_ASSERT_C(
            weakFirst.lock(),
            "replaced target was released while its request was in flight");

        promise.SetValue(NProto::TReadBlocksLocalResponse{});
        UNIT_ASSERT(future.HasValue());
    }
}

}   // namespace NCloud::NBlockStore::NCells
