#include "storage_rdma.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define UNIT_ASSERT_SUCCEEDED(e) \
    UNIT_ASSERT_C(SUCCEEDED(e.GetCode()), e.GetMessage())

class TDummyClientEndpoint: public NRdma::IClientEndpoint
{
    TResultOrError<NRdma::TClientRequestPtr> AllocateRequest(
        NRdma::IClientHandlerPtr handler,
        std::unique_ptr<NRdma::TNullContext> context,
        size_t requestBytes,
        size_t responseBytes) override
    {
        Y_UNUSED(handler);
        Y_UNUSED(context);
        Y_UNUSED(requestBytes);
        Y_UNUSED(responseBytes);

        return MakeError(E_NOT_IMPLEMENTED);
    }

    ui64 SendRequest(
        NRdma::TClientRequestPtr req,
        TCallContextPtr callContext) override
    {
        Y_UNUSED(req);
        Y_UNUSED(callContext);
        return 0;
    }

    void CancelRequest(ui64 reqId) override
    {
        Y_UNUSED(reqId);
    }

    NThreading::TFuture<void> Stop() override
    {
        return MakeFuture();
    }

    void TryForceReconnect() override
    {}
};

class TRdmaClientHelper: public NRdma::IClient
{
public:
    TVector<std::tuple<TString, ui32>> StartedEndpoints;

public:
    TFuture<NRdma::IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port) noexcept override
    {
        StartedEndpoints.emplace_back(host, port);

        return MakeFuture<NRdma::IClientEndpointPtr>(
            std::make_shared<TDummyClientEndpoint>());
    }

    void Start() override
    {}

    void Stop() override
    {}

    void DumpHtml(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    }

    bool IsAlignedDataEnabled() const override
    {
        return false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaStorageTest)
{
    Y_UNIT_TEST(ShouldCreateEndpointForEachDeviceOnSameHost)
    {
        auto client = std::make_shared<TRdmaClientHelper>();
        auto provider = CreateRdmaStorageProvider(
            NCloud::NBlockStore::CreateServerStatsStub(),
            client,
            ERdmaTaskQueueOpt::DontUse);

        NProto::TVolume volume;
        volume.SetDiskId("disk0");
        volume.SetBlockSize(4096);
        volume.SetBlocksCount(1000000);
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);

        TString hostName = "host1";
        ui32 hostPort = 1111;
        for (auto i = 0; i < 2; i++) {
            auto* device = volume.AddDevices();
            device->SetDeviceName("dev" + std::to_string(i));
            device->SetDeviceUUID("uuid" + std::to_string(i));
            device->SetAgentId(hostName);
            device->SetBlockCount(4'000);
            device->SetPhysicalOffset(32'000);
            auto* rdma = device->MutableRdmaEndpoint();
            rdma->SetHost(hostName);
            rdma->SetPort(hostPort);
        }

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(2, client->StartedEndpoints.size());

        for (auto i = 0; i < 2; i++) {
            UNIT_ASSERT_VALUES_EQUAL(
                hostName,
                std::get<0>(client->StartedEndpoints[i]));
            UNIT_ASSERT_VALUES_EQUAL(
                hostPort,
                std::get<1>(client->StartedEndpoints[i]));
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
