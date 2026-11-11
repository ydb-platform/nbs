#include "data_endpoint_factory_test.h"

#include <cloud/blockstore/libs/service/service_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>

#include <array>
#include <atomic>
#include <chrono>
#include <future>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateEndpoint(
    IDataEndpointFactory& factory,
    IClientEndpointHandlerPtr handler)
{
    auto [endpoint, error] =
        factory.CreateDataEndpoint("cell", "host", std::move(handler));
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    UNIT_ASSERT(endpoint);

    return endpoint;
}

////////////////////////////////////////////////////////////////////////////////

class TRecordingHandler final: public IClientEndpointHandler
{
    const std::weak_ptr<TTestDataEndpointFactory> Factory;

public:
    std::weak_ptr<IBlockStore> Endpoint;
    ui32 Notifications = 0;
    ui32 Connections = 0;
    ui32 Unavailable = 0;

    void HandleConnected() noexcept override
    {
        ++Connections;
    }

    void HandleUnavailable() noexcept override
    {
        ++Unavailable;
    }

    THostChannelStatus ObservedStatus;

    explicit TRecordingHandler(std::weak_ptr<TTestDataEndpointFactory> factory)
        : Factory(std::move(factory))
    {}

    void HandleDisconnected() noexcept override
    {
        ++Notifications;
        if (auto factory = Factory.lock()) {
            if (auto endpoint = Endpoint.lock()) {
                ObservedStatus = factory->GetDataEndpointStatus(endpoint);
            }
        }
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TTestDataEndpointFactoryTest)
{
    Y_UNIT_TEST(RecordsRequestsAndConsumesPreparedResults)
    {
        TTestDataEndpointFactory factory;
        factory.EnqueueError(MakeError(E_FAIL, "setup failed"));
        factory.EnqueueEndpoint(std::make_shared<TTestService>());
        auto [failed, error] =
            factory.CreateDataEndpoint("cell-a", "host-a", {});
        UNIT_ASSERT(!failed);
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), E_FAIL);
        UNIT_ASSERT_VALUES_EQUAL(error.GetMessage(), "setup failed");

        auto [endpoint, created] =
            factory.CreateDataEndpoint("cell-b", "host-b", {});
        UNIT_ASSERT_C(!HasError(created), FormatError(created));
        UNIT_ASSERT(endpoint);
        auto [missing, empty] =
            factory.CreateDataEndpoint("cell-c", "host-c", {});
        UNIT_ASSERT(!missing);
        UNIT_ASSERT_VALUES_EQUAL(empty.GetCode(), E_NOT_IMPLEMENTED);

        const auto requests = factory.GetRequests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].CellId, "cell-a");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Host, "host-a");
        UNIT_ASSERT_VALUES_EQUAL(requests[1].CellId, "cell-b");
        UNIT_ASSERT_VALUES_EQUAL(requests[1].Host, "host-b");
        UNIT_ASSERT_VALUES_EQUAL(requests[2].CellId, "cell-c");
        UNIT_ASSERT_VALUES_EQUAL(requests[2].Host, "host-c");
    }

    Y_UNIT_TEST(ForwardsDataRequestsAndTheirResults)
    {
        auto service = std::make_shared<TTestService>();
        auto read = std::make_shared<NProto::TReadBlocksLocalRequest>();
        auto write = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        auto zero = std::make_shared<NProto::TZeroBlocksRequest>();
        auto readDone =
            NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
        ui32 writes = 0;
        ui32 zeros = 0;
        service->ReadBlocksLocalHandler = [&](auto request)
        {
            UNIT_ASSERT(request == read);
            return readDone.GetFuture();
        };
        service->WriteBlocksLocalHandler = [&](auto request)
        {
            UNIT_ASSERT(request == write);
            ++writes;
            return NThreading::MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };
        service->ZeroBlocksHandler = [&](auto request)
        {
            UNIT_ASSERT(request == zero);
            ++zeros;
            return NThreading::MakeFuture<NProto::TZeroBlocksResponse>();
        };
        TTestDataEndpointFactory factory;
        factory.EnqueueEndpoint(service);
        auto endpoint = CreateEndpoint(factory, {});
        auto future =
            endpoint->ReadBlocksLocal(MakeIntrusive<TCallContext>(), read);
        UNIT_ASSERT(!future.HasValue());
        NProto::TReadBlocksLocalResponse response;
        *response.MutableError() = MakeError(E_IO, "read failed");
        readDone.SetValue(std::move(response));
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(future.GetValue().GetError().GetCode(), E_IO);
        UNIT_ASSERT_VALUES_EQUAL(
            future.GetValue().GetError().GetMessage(),
            "read failed");
        UNIT_ASSERT(
            endpoint->WriteBlocksLocal(MakeIntrusive<TCallContext>(), write)
                .Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero)
                        .Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(writes, 1);
        UNIT_ASSERT_VALUES_EQUAL(zeros, 1);
    }

    Y_UNIT_TEST(RejectsUnsupportedOperationsWithoutCallingBackend)
    {
        auto service = std::make_shared<TTestService>();
        ui32 calls = 0;
        service->MountVolumeHandler = [&](auto request)
        {
            Y_UNUSED(request);

            ++calls;
            return NThreading::MakeFuture<NProto::TMountVolumeResponse>();
        };
        service->ReadBlocksHandler = [&](auto request)
        {
            Y_UNUSED(request);

            ++calls;
            return NThreading::MakeFuture<NProto::TReadBlocksResponse>();
        };
        TTestDataEndpointFactory factory;
        factory.EnqueueEndpoint(service);
        auto endpoint = CreateEndpoint(factory, {});
        auto mount = endpoint->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        auto read = endpoint->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksRequest>());
        UNIT_ASSERT(mount.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(read.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            mount.GetValue().GetError().GetCode(),
            E_NOT_IMPLEMENTED);
        UNIT_ASSERT_VALUES_EQUAL(
            read.GetValue().GetError().GetCode(),
            E_NOT_IMPLEMENTED);
        UNIT_ASSERT_VALUES_EQUAL(calls, 0);
        endpoint->Stop();
    }

    Y_UNIT_TEST(StopRejectsRequestsAndDrainsContinuationsWithoutStoppingBackend)
    {
        auto service = std::make_shared<TTestService>();
        auto readDone =
            NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
        auto release = NThreading::NewPromise();
        auto entered = NThreading::NewPromise();
        ui32 writes = 0;
        ui32 zeros = 0;
        std::atomic<ui32> backendStops = 0;
        service->StopHandler = [&]
        {
            ++backendStops;
        };
        service->ReadBlocksLocalHandler = [readDone](auto request)
        {
            Y_UNUSED(request);

            return readDone.GetFuture();
        };
        service->WriteBlocksLocalHandler = [&](auto request)
        {
            Y_UNUSED(request);

            ++writes;
            return NThreading::MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };
        service->ZeroBlocksHandler = [&](auto request)
        {
            Y_UNUSED(request);

            ++zeros;
            return NThreading::MakeFuture<NProto::TZeroBlocksResponse>();
        };
        TTestDataEndpointFactory factory;
        factory.EnqueueEndpoint(service);
        factory.EnqueueEndpoint(service);
        auto first = CreateEndpoint(factory, {});
        auto second = CreateEndpoint(factory, {});
        auto read = first->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksLocalRequest>());
        read.Subscribe(
            [entered, release](const auto& completed) mutable
            {
                Y_UNUSED(completed);

                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        std::future<void> stop;
        std::future<void> complete;
        Y_DEFER
        {
            release.TrySetValue();
            readDone.TrySetValue(NProto::TReadBlocksLocalResponse{});
        };

        stop = std::async(std::launch::async, [first] { first->Stop(); });
        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        while (true) {
            auto zero = first->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TZeroBlocksRequest>());
            UNIT_ASSERT(zero.Wait(TDuration::Seconds(5)));
            if (zero.GetValue().GetError().GetCode() == E_CANCELLED) {
                break;
            }
            UNIT_ASSERT(TInstant::Now() < deadline);
            Sleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT(
            stop.wait_for(std::chrono::seconds(0)) ==
            std::future_status::timeout);
        complete = std::async(
            std::launch::async,
            [readDone]() mutable
            { readDone.SetValue(NProto::TReadBlocksLocalResponse{}); });
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(
            stop.wait_for(std::chrono::seconds(0)) ==
            std::future_status::timeout);
        const auto zerosBefore = zeros;
        auto rejectedRead = first->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksLocalRequest>());
        auto rejectedWrite = first->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TWriteBlocksLocalRequest>());
        UNIT_ASSERT(rejectedRead.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(rejectedWrite.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            rejectedRead.GetValue().GetError().GetCode(),
            E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(
            rejectedWrite.GetValue().GetError().GetCode(),
            E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(writes, 0);
        UNIT_ASSERT_VALUES_EQUAL(zeros, zerosBefore);
        release.SetValue();
        UNIT_ASSERT(
            stop.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready);
        stop.get();
        complete.get();
        first->Stop();
        first->Start();
        auto afterRestart = first->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TZeroBlocksRequest>());
        UNIT_ASSERT(afterRestart.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            afterRestart.GetValue().GetError().GetCode(),
            E_CANCELLED);
        auto independent = second->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TWriteBlocksLocalRequest>());
        UNIT_ASSERT(independent.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!HasError(independent.GetValue().GetError()));
        UNIT_ASSERT_VALUES_EQUAL(writes, 1);
        UNIT_ASSERT(
            factory.GetDataEndpointStatus(first).State ==
            EHostChannelState::Disconnected);
        UNIT_ASSERT_VALUES_EQUAL(backendStops.load(), 0);
        second->Stop();
    }

    Y_UNIT_TEST(KeepsIndependentBindingsForTheSameHost)
    {
        auto service = std::make_shared<TTestService>();
        TTestDataEndpointFactory factory;
        factory.EnqueueEndpoint(service);
        factory.EnqueueEndpoint(service);
        auto first = CreateEndpoint(factory, {});
        auto second = CreateEndpoint(factory, {});
        UNIT_ASSERT(first != second);
        UNIT_ASSERT(
            factory.GetDataEndpointStatus(first).State ==
            EHostChannelState::Connecting);
        factory.SetStatus(
            first,
            {.State = EHostChannelState::Connected,
             .ConnectionAttempts = 3,
             .ConnectionFailures = 2});
        factory.SetStatus(
            second,
            {.State = EHostChannelState::Connecting, .ConnectionAttempts = 1});
        const auto firstStatus = factory.GetDataEndpointStatus(first);
        const auto secondStatus = factory.GetDataEndpointStatus(second);
        UNIT_ASSERT(firstStatus.State == EHostChannelState::Connected);
        UNIT_ASSERT_VALUES_EQUAL(firstStatus.ConnectionAttempts, 3);
        UNIT_ASSERT_VALUES_EQUAL(firstStatus.ConnectionFailures, 2);
        UNIT_ASSERT(secondStatus.State == EHostChannelState::Connecting);
        UNIT_ASSERT_VALUES_EQUAL(secondStatus.ConnectionAttempts, 1);
        UNIT_ASSERT_VALUES_EQUAL(secondStatus.ConnectionFailures, 0);
        UNIT_ASSERT_VALUES_EQUAL(factory.GetRequests().size(), 2);
    }

    Y_UNIT_TEST(PublishesStatusBeforeDisconnectNotificationAndReleasesBinding)
    {
        auto factory = std::make_shared<TTestDataEndpointFactory>();
        auto handler = std::make_shared<TRecordingHandler>(factory);
        factory->EnqueueEndpoint(std::make_shared<TTestService>());
        auto endpoint = CreateEndpoint(*factory, handler);
        handler->Endpoint = endpoint;
        factory->NotifyDisconnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Notifications, 0);

        factory->SetStatus(endpoint, {.State = EHostChannelState::Connected});
        factory->NotifyDisconnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Notifications, 1);
        UNIT_ASSERT(
            handler->ObservedStatus.State == EHostChannelState::Disconnected);
        factory->NotifyDisconnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Notifications, 1);
        factory->SetStatus(endpoint, {.State = EHostChannelState::Connected});
        factory->NotifyDisconnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Notifications, 2);

        const std::weak_ptr<IBlockStore> weakEndpoint = endpoint;
        const std::weak_ptr<IClientEndpointHandler> weakHandler = handler;
        handler.reset();
        endpoint.reset();
        UNIT_ASSERT(weakEndpoint.expired());
        UNIT_ASSERT(weakHandler.expired());
    }

    Y_UNIT_TEST(ReleasesEndpointInResultCallbackWithoutStop)
    {
        auto service = std::make_shared<TTestService>();
        auto completed =
            NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
        service->ReadBlocksLocalHandler = [completed](auto request)
        {
            Y_UNUSED(request);

            return completed.GetFuture();
        };
        TTestDataEndpointFactory factory;
        factory.EnqueueEndpoint(service);
        auto endpoint = CreateEndpoint(factory, {});
        const std::weak_ptr<IBlockStore> weak = endpoint;
        auto response = endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksLocalRequest>());
        response.Subscribe(
            [endpoint = std::move(endpoint)](const auto& future) mutable
            {
                Y_UNUSED(future);

                endpoint.reset();
            });
        completed.SetValue(NProto::TReadBlocksLocalResponse{});
        UNIT_ASSERT(response.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(weak.expired());
    }

    Y_UNIT_TEST(ReportsFailedAttemptsBeforeFirstConnectionAndStopsNotifications)
    {
        auto factory = std::make_shared<TTestDataEndpointFactory>();
        auto handler = std::make_shared<TRecordingHandler>(factory);
        factory->EnqueueEndpoint(std::make_shared<TTestService>());
        auto endpoint = CreateEndpoint(*factory, handler);
        handler->Endpoint = endpoint;
        factory->NotifyUnavailable(endpoint);
        factory->NotifyUnavailable(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Unavailable, 2);
        UNIT_ASSERT_VALUES_EQUAL(handler->Notifications, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            factory->GetDataEndpointStatus(endpoint).ConnectionFailures,
            2);
        factory->NotifyConnected(endpoint);
        factory->NotifyConnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Connections, 1);
        factory->NotifyDisconnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Notifications, 1);
        factory->NotifyConnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Connections, 2);
        endpoint->Stop();
        factory->NotifyUnavailable(endpoint);
        factory->NotifyConnected(endpoint);
        factory->NotifyDisconnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Unavailable, 2);
        UNIT_ASSERT_VALUES_EQUAL(handler->Connections, 2);
        UNIT_ASSERT_VALUES_EQUAL(handler->Notifications, 1);
    }

    Y_UNIT_TEST(CreatesBindingsConcurrently)
    {
        auto service = std::make_shared<TTestService>();
        auto factory = std::make_shared<TTestDataEndpointFactory>();
        std::array<std::future<IBlockStorePtr>, 8> calls;
        for (size_t i = 0; i < calls.size(); ++i) {
            factory->EnqueueEndpoint(service);
        }
        for (auto& call: calls) {
            call = std::async(
                std::launch::async,
                [factory] { return CreateEndpoint(*factory, {}); });
        }
        TVector<IBlockStorePtr> endpoints;
        THashSet<IBlockStore*> addresses;
        for (auto& call: calls) {
            auto endpoint = call.get();
            UNIT_ASSERT(addresses.insert(endpoint.get()).second);
            endpoints.push_back(std::move(endpoint));
        }
        UNIT_ASSERT_VALUES_EQUAL(factory->GetRequests().size(), calls.size());
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
