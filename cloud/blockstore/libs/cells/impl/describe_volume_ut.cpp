#include "describe_volume.h"

#include "remote_storage.h"

#include <cloud/blockstore/config/cells.pb.h>
#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestServiceClient: public IBlockStore
{
    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return {};
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                      \
    std::function<void(const NProto::T##name##Request&)> On##name;  \
    TFuture<NProto::T##name##Response> name(                        \
        TCallContextPtr callContext,                                \
        std::shared_ptr<NProto::T##name##Request> request) override \
    {                                                               \
        Y_UNUSED(callContext);                                      \
        Y_UNUSED(request);                                          \
        ++name##Called;                                             \
        if (On##name) {                                             \
            On##name(*request);                                     \
        }                                                           \
        return name##Promise.GetFuture();                           \
    }                                                               \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

#define BLOCKSTORE_IMPLEMENT_PROMISE(name, ...)         \
    TPromise<NProto::T##name##Response> name##Promise = \
        NewPromise<NProto::T##name##Response>();        \
    ui32 name##Called = 0;                              \
    // BLOCKSTORE_IMPLEMENT_PROMISE

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_PROMISE)

#undef BLOCKSTORE_IMPLEMENT_PROMISE
};

std::shared_ptr<TTestServiceClient> CreateService()
{
    auto service = std::make_shared<TTestServiceClient>();

    auto describeCheck = [](const NProto::TDescribeVolumeRequest& req)
    {
        UNIT_ASSERT_C(
            req.GetHeaders().HasInternal(),
            "Internal should be set");
        UNIT_ASSERT_VALUES_UNEQUAL("", req.GetHeaders().GetCellId());
    };

    service->OnDescribeVolume = describeCheck;
    return service;
}

std::shared_ptr<TTestServiceClient> CreateCellEndpoint(
    const TString& cellId,
    const TString& host,
    TCellHostEndpointsByCellId& endpoints)
{
    auto describeCheck = [](const NProto::TDescribeVolumeRequest& req)
    {
        UNIT_ASSERT_C(
            !req.GetHeaders().HasInternal(),
            "Internal should not be set");
        UNIT_ASSERT_VALUES_UNEQUAL("", req.GetHeaders().GetCellId());
    };

    auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();
    auto service = std::make_shared<TTestServiceClient>();
    service->OnDescribeVolume = describeCheck;
    endpoints[cellId].emplace_back(
        clientAppConfig,
        host,
        service,
        CreateRemoteStorage(service, nullptr));
    return service;
}

NProto::TDescribeVolumeRequest CreateDescribeRequest()
{
    NProto::TDescribeVolumeRequest request;
    request.MutableHeaders()->CopyFrom(NProto::THeaders());
    *request.MutableHeaders()->MutableInternal()->MutableAuthToken() = "auth";
    return request;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDescribeVolumeTest)
{
    Y_UNIT_TEST(ShouldDescribeRemoteVolume)
    {
        TCellHostEndpointsByCellId endpoints;

        NProto::TCellsConfig cellsProto;
        cellsProto.AddCells()->SetCellId("cell1");
        cellsProto.AddCells()->SetCellId("cell2");
        TCellsConfig config(cellsProto);

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("cell1disk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();
        Y_DEFER { bootstrap.Scheduler->Stop(); };

        auto response = DescribeVolume(
            config,
            request,
            localService,
            endpoints,
            false,
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        NProto::TDescribeVolumeResponse msg;
        s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL("cell1", describeResponse.GetCellId());
    }

    Y_UNIT_TEST(ShouldIgnoreVolumeWithSourceDiskIdTag)
    {
        TCellHostEndpointsByCellId endpoints;

        NProto::TCellsConfig cellsProto;
        cellsProto.AddCells()->SetCellId("cell1");
        cellsProto.AddCells()->SetCellId("cell2");
        TCellsConfig config(cellsProto);

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();
        Y_DEFER { bootstrap.Scheduler->Stop(); };

        auto response = DescribeVolume(
            config,
            request,
            localService,
            endpoints,
            false,
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            (*msg.MutableVolume()->MutableTags())[TString(SourceDiskIdTagName)] = "source_disk_id";
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            (*msg.MutableVolume()->MutableTags())[TString(SourceDiskIdTagName)] = "source_disk_id";
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            (*msg.MutableVolume()->MutableTags())[TString(SourceDiskIdTagName)] = "source_disk_id";
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            describeResponse.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldDescribeLocalVolume)
    {
        TCellHostEndpointsByCellId endpoints;

        NProto::TCellsConfig cellsProto;
        cellsProto.AddCells()->SetCellId("cell1");
        cellsProto.AddCells()->SetCellId("cell2");
        TCellsConfig config(cellsProto);

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("localdisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();
        Y_DEFER { bootstrap.Scheduler->Stop(); };

        auto response = DescribeVolume(
            config,
            request,
            localService,
            endpoints,
            false,
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        NProto::TDescribeVolumeResponse msg;
        localService->DescribeVolumePromise.SetValue(std::move(msg));

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL("", describeResponse.GetCellId());
    }

    Y_UNIT_TEST(ShouldReturnFatalErrorIfVolumeIsAbsent)
    {
        TCellHostEndpointsByCellId endpoints;

        NProto::TCellsConfig cellsProto;
        cellsProto.AddCells()->SetCellId("cell1");
        cellsProto.AddCells()->SetCellId("cell2");
        TCellsConfig config(cellsProto);

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();
        Y_DEFER { bootstrap.Scheduler->Stop(); };

        auto response = DescribeVolume(
            config,
            request,
            localService,
            endpoints,
            false,
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            describeResponse.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldReturnRetriableErrorIfAtLeastOneCellIsNotReachable)
    {
        TCellHostEndpointsByCellId endpoints;

        NProto::TCellsConfig cellsProto;
        cellsProto.AddCells()->SetCellId("cell1");
        cellsProto.AddCells()->SetCellId("cell2");
        TCellsConfig config(cellsProto);

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();
        Y_DEFER { bootstrap.Scheduler->Stop(); };

        auto response = DescribeVolume(
            config,
            request,
            localService,
            endpoints,
            false,
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() =
                std::move(MakeError(E_GRPC_UNAVAILABLE, "connection lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            describeResponse.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldReturnRetribleErrorIfAtLeastOneCellUnavailable)
    {
        TCellHostEndpointsByCellId endpoints;

        NProto::TCellsConfig cellsProto;
        cellsProto.AddCells()->SetCellId("cell1");
        TCellsConfig config(cellsProto);

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();
        Y_DEFER { bootstrap.Scheduler->Stop(); };

        auto response = DescribeVolume(
            config,
            request,
            localService,
            endpoints,
            true,
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "Not all cells available",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReplyRetriableErrorOnTimeout)
    {
        TCellHostEndpointsByCellId endpoints;

        NProto::TCellsConfig cellsProto;
        cellsProto.SetDescribeVolumeTimeout(
            TDuration::Seconds(1).MilliSeconds());
        cellsProto.AddCells()->SetCellId("cell1");
        cellsProto.AddCells()->SetCellId("cell2");
        TCellsConfig config(cellsProto);

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();
        Y_DEFER { bootstrap.Scheduler->Stop(); };

        auto response = DescribeVolume(
            config,
            request,
            localService,
            endpoints,
            false,
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        const auto& describeResponse = response.GetValue(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "Describe timeout",
            describeResponse.GetError().GetMessage());

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }
    }

    Y_UNIT_TEST(ShouldReportEveryCellInMonitoringDescribe)
    {
        TCellHostEndpointsByCellId endpoints;
        auto found = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto absent = CreateCellEndpoint("cell2", "s2h1", endpoints);
        // cell3 is configured but has no connected endpoint

        found->DescribeVolumePromise.SetValue(NProto::TDescribeVolumeResponse());
        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = MakeError(E_NOT_FOUND, "lost");
            absent->DescribeVolumePromise.SetValue(std::move(msg));
        }

        auto request = CreateDescribeRequest();
        request.SetDiskId("disk");

        auto results = DescribeVolumeForMonitoring(
            request,
            {"cell1", "cell2", "cell3"},
            endpoints,
            nullptr,
            TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(3, results.size());

        auto byId = [&] (const TString& id) -> const TCellDescribeResult& {
            for (const auto& result: results) {
                if (result.CellId.Defined() && *result.CellId == id) {
                    return result;
                }
            }
            UNIT_FAIL("no result for cell " << id);
            return results[0];
        };

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(ECellDescribeStatus::Found),
            static_cast<int>(byId("cell1").Status));
        UNIT_ASSERT_VALUES_EQUAL("s1h1", byId("cell1").Fqdn);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(ECellDescribeStatus::NotFound),
            static_cast<int>(byId("cell2").Status));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(ECellDescribeStatus::Unavailable),
            static_cast<int>(byId("cell3").Status));
    }

    Y_UNIT_TEST(ShouldReportFailedCellWhenDescribeTimesOut)
    {
        TCellHostEndpointsByCellId endpoints;
        auto pending = CreateCellEndpoint("cell1", "s1h1", endpoints);
        // the promise is never set, so the describe stays in flight

        auto request = CreateDescribeRequest();
        request.SetDiskId("disk");

        auto results = DescribeVolumeForMonitoring(
            request,
            {"cell1"},
            endpoints,
            nullptr,
            TDuration::MilliSeconds(200));

        UNIT_ASSERT_VALUES_EQUAL(1, results.size());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(ECellDescribeStatus::Failed),
            static_cast<int>(results[0].Status));

        pending->DescribeVolumePromise.SetValue(
            NProto::TDescribeVolumeResponse());
    }

    Y_UNIT_TEST(ShouldNotReportMigrationDestinationAsFound)
    {
        TCellHostEndpointsByCellId endpoints;
        auto destination = CreateCellEndpoint("cell1", "s1h1", endpoints);

        NProto::TDescribeVolumeResponse msg;
        (*msg.MutableVolume()
              ->MutableTags())[TString(SourceDiskIdTagName)] = "src";
        destination->DescribeVolumePromise.SetValue(std::move(msg));

        auto request = CreateDescribeRequest();
        request.SetDiskId("disk");

        auto results = DescribeVolumeForMonitoring(
            request,
            {"cell1"},
            endpoints,
            nullptr,
            TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(1, results.size());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(ECellDescribeStatus::NotFound),
            static_cast<int>(results[0].Status));
        UNIT_ASSERT_VALUES_EQUAL("", results[0].Fqdn);
    }

    Y_UNIT_TEST(ShouldQueryLocalServiceInMonitoringDescribe)
    {
        TCellHostEndpointsByCellId endpoints;
        auto cell = CreateCellEndpoint("cell1", "s1h1", endpoints);
        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = MakeError(E_NOT_FOUND, "lost");
            cell->DescribeVolumePromise.SetValue(std::move(msg));
        }

        auto local = std::make_shared<TTestServiceClient>();
        local->DescribeVolumePromise.SetValue(NProto::TDescribeVolumeResponse());

        auto request = CreateDescribeRequest();
        request.SetDiskId("disk");

        auto results = DescribeVolumeForMonitoring(
            request,
            {"cell1"},
            endpoints,
            local,
            TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(2, results.size());
        UNIT_ASSERT_VALUES_EQUAL(1, local->DescribeVolumeCalled);

        const auto& localRow = results.back();
        UNIT_ASSERT(!localRow.CellId.Defined());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(ECellDescribeStatus::Found),
            static_cast<int>(localRow.Status));
    }

    Y_UNIT_TEST(ShouldPreferConcreteAnswerOverFailureWithinCell)
    {
        TCellHostEndpointsByCellId endpoints;
        // two hosts of the same cell: one answers "not found", the other never
        // answers (times out)
        auto answered = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto timedOut = CreateCellEndpoint("cell1", "s1h2", endpoints);
        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = MakeError(E_NOT_FOUND, "lost");
            answered->DescribeVolumePromise.SetValue(std::move(msg));
        }

        auto request = CreateDescribeRequest();
        request.SetDiskId("disk");

        auto results = DescribeVolumeForMonitoring(
            request,
            {"cell1"},
            endpoints,
            nullptr,
            TDuration::MilliSeconds(200));

        UNIT_ASSERT_VALUES_EQUAL(1, results.size());
        // the definitive "not found" wins over the inconclusive timeout
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(ECellDescribeStatus::NotFound),
            static_cast<int>(results[0].Status));

        timedOut->DescribeVolumePromise.SetValue(
            NProto::TDescribeVolumeResponse());
    }

    Y_UNIT_TEST(ShouldBoundFiredRpcsToMonitoringTimeout)
    {
        TCellHostEndpointsByCellId endpoints;
        auto cell = CreateCellEndpoint("cell1", "s1h1", endpoints);

        ui32 seenTimeout = 0;
        cell->OnDescribeVolume =
            [&] (const NProto::TDescribeVolumeRequest& req)
        {
            seenTimeout = req.GetHeaders().GetRequestTimeout();
        };
        cell->DescribeVolumePromise.SetValue(NProto::TDescribeVolumeResponse());

        auto request = CreateDescribeRequest();
        request.SetDiskId("disk");

        DescribeVolumeForMonitoring(
            request,
            {"cell1"},
            endpoints,
            nullptr,
            TDuration::Seconds(7));

        // the fired RPC carries our deadline so it cannot outlive the search
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(7).MilliSeconds(),
            seenTimeout);
    }
}

}   // namespace NCloud::NBlockStore::NCells
