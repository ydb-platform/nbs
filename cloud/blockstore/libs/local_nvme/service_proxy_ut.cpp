#include "service_proxy.h"

#include "service.h"

#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore: public TBlockStoreImpl<TTestBlockStore, IBlockStore>
{
    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void Start() override
    {}

    void Stop() override
    {}

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeFuture<typename TMethod::TResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED));
    }
};

struct TTestLocalNVMeService: public ILocalNVMeService
{
    TVector<NProto::TNVMeDevice> Devices;

    void Start() override
    {}

    void Stop() override
    {}

    [[nodiscard]] auto ListNVMeDevices()
        -> TFuture<TResultOrError<TVector<NProto::TNVMeDevice>>> override
    {
        return MakeFuture<TResultOrError<TVector<NProto::TNVMeDevice>>>(
            Devices);
    }

    [[nodiscard]] auto AcquireNVMeDevice(const TString& serialNumber)
        -> TFuture<NProto::TError> override
    {
        Y_UNUSED(serialNumber);

        return MakeFuture(MakeError(S_OK));
    }

    [[nodiscard]] auto ReleaseNVMeDevice(const TString& serialNumber)
        -> TFuture<NProto::TError> override
    {
        Y_UNUSED(serialNumber);

        return MakeFuture(MakeError(S_OK));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    std::shared_ptr<TTestBlockStore> TestBlockStore;
    std::shared_ptr<TTestLocalNVMeService> TestLocalNVMeService;
    IBlockStorePtr BlockStore;

    void SetUp(NUnitTest::TTestContext& testContext) override
    {
        Y_UNUSED(testContext);

        TestBlockStore = std::make_shared<TTestBlockStore>();
        TestLocalNVMeService = std::make_shared<TTestLocalNVMeService>();

        NProto::TNVMeDeviceList list;
        ParseProtoTextFromString(
            R"(
            Devices {
                SerialNumber: "NVME_0"
                PCIAddress: "0000:f1:00.0"
                IOMMUGroup: 10
                VfioDevName: "vfio0"
                VendorId: 0x100
                DeviceId: 0x200
                Model: "Test NVMe 1"
            }
            Devices {
                SerialNumber: "NVME_1"
                PCIAddress: "0000:31:00.0"
                VendorId: 0x300
                DeviceId: 0x400
                Model: "Test NVMe 2"
            }
        )",
            list);

        UNIT_ASSERT_VALUES_EQUAL(2, list.DevicesSize());
        TestLocalNVMeService->Devices.assign(
            std::make_move_iterator(list.MutableDevices()->begin()),
            std::make_move_iterator(list.MutableDevices()->end()));

        BlockStore = CreateLocalNVMeServiceProxy(
            std::static_pointer_cast<IBlockStore>(TestBlockStore),
            std::static_pointer_cast<ILocalNVMeService>(TestLocalNVMeService));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalNVMeServiceProxyTest)
{
    Y_UNIT_TEST_F(ShouldListDevices, TFixture)
    {
        auto future = BlockStore->ListNVMeDevices(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TListNVMeDevicesRequest>());
        auto response = future.GetValueSync();
        SortBy(
            *response.MutableDevices(),
            [](const auto& device) { return device.GetSerialNumber(); });

        UNIT_ASSERT_VALUES_EQUAL(
            TestLocalNVMeService->Devices.size(),
            response.DevicesSize());

        for (size_t i = 0; i != TestLocalNVMeService->Devices.size(); ++i) {
            const auto& expected = TestLocalNVMeService->Devices[i];
            const auto& device = response.GetDevices(i);

            UNIT_ASSERT_VALUES_EQUAL(
                expected.GetSerialNumber(),
                device.GetSerialNumber());

            UNIT_ASSERT_VALUES_EQUAL(
                expected.GetPCIAddress(),
                device.GetPCIAddress());

            UNIT_ASSERT_VALUES_EQUAL(
                expected.HasIOMMUGroup(),
                device.HasIOMMUGroup());

            if (expected.HasIOMMUGroup()) {
                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetIOMMUGroup(),
                    device.GetIOMMUGroup());
            }

            UNIT_ASSERT_VALUES_EQUAL(
                expected.HasVfioDevName(),
                device.HasVfioDevName());

            if (expected.HasVfioDevName()) {
                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetVfioDevName(),
                    device.GetVfioDevName());
            }
        }
    }

    Y_UNIT_TEST_F(ShouldAcquireDevice, TFixture)
    {
        auto request = std::make_shared<NProto::TAcquireNVMeDeviceRequest>();
        request->SetSerialNumber("sn");
        auto future = BlockStore->AcquireNVMeDevice(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldReleaseDevice, TFixture)
    {
        auto request = std::make_shared<NProto::TReleaseNVMeDeviceRequest>();
        request->SetSerialNumber("sn");
        auto future = BlockStore->ReleaseNVMeDevice(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore
