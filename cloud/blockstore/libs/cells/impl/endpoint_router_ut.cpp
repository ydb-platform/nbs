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
    ui32 StopCount = 0;
    TPromise<NProto::TReadBlocksLocalResponse> Pending;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        ++AllocateCount;
        return nullptr;
    }

    TCountingService()
    {
        StopHandler = [this] { ++StopCount; };

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

    // Lifetime is not what the router forwards: stopping it must not stop an
    // endpoint it merely points at, which may well be shared with others.
    Y_UNIT_TEST(ShouldNotStopTheTargetItForwardsTo)
    {
        auto target = std::make_shared<TCountingService>();

        auto router = CreateEndpointRouter(target);
        router->Start();
        router->Stop();

        UNIT_ASSERT_VALUES_EQUAL(0, target->StopCount);
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

    Y_UNIT_TEST(ShouldCountCompletedIoForCurrentTargetAndResetOnSwitch)
    {
        auto first = std::make_shared<TCountingService>();
        first->Pending = NewPromise<NProto::TReadBlocksLocalResponse>();
        auto router = CreateEndpointRouter(first);

        auto read = std::make_shared<NProto::TReadBlocksLocalRequest>();
        read->SetBlockSize(4096);
        read->SetBlocksCount(3);
        auto pending = router->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(), read);
        UNIT_ASSERT_VALUES_EQUAL(router->GetIoStats().ReadRequests, 0);

        auto second = std::make_shared<TCountingService>();
        second->WriteBlocksLocalHandler = [](const auto&)
        {
            return MakeFuture(NProto::TWriteBlocksLocalResponse{});
        };
        second->ZeroBlocksHandler = [](const auto&)
        {
            return MakeFuture(NProto::TZeroBlocksResponse{});
        };
        router->SetTarget(second);
        first->Pending.SetValue(NProto::TReadBlocksLocalResponse{});
        UNIT_ASSERT(pending.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(router->GetIoStats().ReadRequests, 0);

        read = std::make_shared<NProto::TReadBlocksLocalRequest>();
        read->SetBlockSize(4096);
        read->SetBlocksCount(2);
        router->ReadBlocksLocal(MakeIntrusive<TCallContext>(), read);

        auto write = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        write->SetBlockSize(4096);
        write->BlocksCount = 1;
        router->WriteBlocksLocal(MakeIntrusive<TCallContext>(), write);
        router->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TZeroBlocksRequest>());

        second->ReadBlocksLocalHandler = [](const auto&)
        {
            NProto::TReadBlocksLocalResponse response;
            *response.MutableError() = MakeError(E_IO, "read failed");
            return MakeFuture(std::move(response));
        };
        router->ReadBlocksLocal(MakeIntrusive<TCallContext>(), read);

        second->WriteBlocksLocalHandler = [](const auto&)
        {
            NProto::TWriteBlocksLocalResponse response;
            *response.MutableError() = MakeError(E_IO, "write failed");
            return MakeFuture(std::move(response));
        };
        router->WriteBlocksLocal(MakeIntrusive<TCallContext>(), write);

        second->ZeroBlocksHandler = [](const auto&)
        {
            NProto::TZeroBlocksResponse response;
            *response.MutableError() = MakeError(E_IO, "zero failed");
            return MakeFuture(std::move(response));
        };
        router->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TZeroBlocksRequest>());

        const auto stats = router->GetIoStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.ReadRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.ReadErrors, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.ReadBytes, 8192);
        UNIT_ASSERT_VALUES_EQUAL(stats.WriteRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.WriteErrors, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.WriteBytes, 4096);
        UNIT_ASSERT_VALUES_EQUAL(stats.ZeroRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.ZeroErrors, 1);
    }
}

}   // namespace NCloud::NBlockStore::NCells
