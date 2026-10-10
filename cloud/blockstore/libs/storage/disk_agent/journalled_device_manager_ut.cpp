#include "journalled_device_manager.h"

#include <cloud/blockstore/libs/rdma_test/memory_test_storage.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/server/device_manager.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>


#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;
constexpr ui64 DefaultBlockCount = 1_MB / DefaultBlockSize;

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TString DeviceUUID = "uuid-1";
    const TString ClientId = "client-id";
    const TInstant Now = TInstant::Seconds(1);

    TTestBasicRuntime Runtime;
    TActorId DiskAgentActorId;

    ILoggingServicePtr Logging = CreateLoggingService("console");
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

    std::shared_ptr<TMemoryTestStorage> Storage;
    TStorageAdapterPtr StorageAdapter;
    TDeviceClientPtr DeviceClient;

    NJournalled::IDeviceManagerPtr DeviceManager;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        SetupTabletServices(Runtime);

        // The edge actor plays the disk agent, so the tests see the requests
        // the manager sends to it and answer them.
        DiskAgentActorId = Runtime.AllocateEdgeActor();

        Storage = std::make_shared<TMemoryTestStorage>(
            DefaultBlockCount * DefaultBlockSize);

        StorageAdapter = std::make_shared<TStorageAdapter>(
            Storage,
            DefaultBlockSize,
            false,                    // normalize
            TDuration::Seconds(1),    // maxRequestDuration
            TDuration::Seconds(1));   // shutdownTimeout

        DeviceClient = std::make_shared<TDeviceClient>(
            10s,   // releaseInactiveSessionsTimeout
            TVector<std::pair<TString, TStorageAdapterPtr>>{
                {DeviceUUID, StorageAdapter}},
            Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
            false   // kickOutOldClientsEnabled
        );

        Timer->AdvanceTime(Now - TInstant::Zero());

        DeviceManager = CreateDeviceManager(
            Timer,
            DeviceClient,
            Runtime.GetActorSystem(0),
            DiskAgentActorId);
    }

    // Takes the request the manager has sent to the disk agent and answers it
    // with the error.
    template <typename TRequest, typename TResponse>
    auto ReplyToDiskAgentRequest(const NProto::TError& error)
    {
        auto ev = Runtime.GrabEdgeEvent<TRequest>(DiskAgentActorId);
        UNIT_ASSERT(ev);

        auto record = ev->Get()->Record;

        Runtime.Send(new IEventHandle(
            ev->Sender,
            DiskAgentActorId,
            new TResponse(error)));

        return record;
    }

    template <typename T>
    T Wait(TFuture<T> future)
    {
        Runtime.DispatchEvents(
            {.CustomFinalCondition = [&] { return future.HasValue(); }},
            1s);

        UNIT_ASSERT(future.HasValue());
        return future.GetValue();
    }

    void AcquireDevice(
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode)
    {
        auto [_, error] = DeviceClient->AcquireDevices(
            {DeviceUUID},
            clientId,
            Now,
            accessMode,
            0,    // mountSeqNumber
            "",   // diskId
            0);   // volumeGeneration

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalledDeviceManagerTest)
{
    Y_UNIT_TEST_F(ShouldForwardAcquireDevices, TFixture)
    {
        const std::pair<NCloud::NProto::EAccessMode, NProto::EVolumeAccessMode>
            accessModes[]{
                {NCloud::NProto::ACCESS_READ_WRITE,
                 NProto::VOLUME_ACCESS_READ_WRITE},
                {NCloud::NProto::ACCESS_READ_ONLY,
                 NProto::VOLUME_ACCESS_READ_ONLY},
            };

        for (const auto& [accessMode, volumeAccessMode]: accessModes) {
            NCloud::NProto::TAcquireDevicesRequest request;
            request.MutableHeaders()->SetClientId(ClientId);
            request.MutableHeaders()->SetRequestTimeout(1000);
            *request.MutableDeviceUUIDs()->Add() = "uuid-1";
            *request.MutableDeviceUUIDs()->Add() = "uuid-2";
            request.SetAccessMode(accessMode);
            request.SetFastshardId("fastshard-id");
            request.SetGeneration(42);

            auto future = DeviceManager->AcquireDevices(request);

            const auto record = ReplyToDiskAgentRequest<
                TEvDiskAgent::TEvAcquireDevicesRequest,
                TEvDiskAgent::TEvAcquireDevicesResponse>(
                MakeError(E_BS_INVALID_SESSION, "acquire"));

            UNIT_ASSERT_VALUES_EQUAL(
                ClientId,
                record.GetHeaders().GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                1000,
                record.GetHeaders().GetRequestTimeout());
            UNIT_ASSERT_VALUES_EQUAL(2, record.DeviceUUIDsSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", record.GetDeviceUUIDs(0));
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", record.GetDeviceUUIDs(1));
            UNIT_ASSERT_EQUAL(volumeAccessMode, record.GetAccessMode());

            // The disk agent orders the writers of a fastshard by its
            // generation, the mount sequence number is never used
            UNIT_ASSERT_VALUES_EQUAL("fastshard-id", record.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(42, record.GetVolumeGeneration());
            UNIT_ASSERT_VALUES_EQUAL(0, record.GetMountSeqNumber());

            // and the disk agent answer comes back
            const auto response = Wait(std::move(future));
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_INVALID_SESSION,
                response.GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "acquire",
                response.GetError().GetMessage());
        }
    }

    Y_UNIT_TEST_F(ShouldForwardReleaseDevices, TFixture)
    {
        NCloud::NProto::TReleaseDevicesRequest request;
        request.MutableHeaders()->SetClientId(ClientId);
        request.MutableHeaders()->SetRequestTimeout(1000);
        *request.MutableDeviceUUIDs()->Add() = "uuid-1";
        *request.MutableDeviceUUIDs()->Add() = "uuid-2";
        request.SetFastshardId("fastshard-id");
        request.SetGeneration(42);

        auto future = DeviceManager->ReleaseDevices(request);

        const auto record = ReplyToDiskAgentRequest<
            TEvDiskAgent::TEvReleaseDevicesRequest,
            TEvDiskAgent::TEvReleaseDevicesResponse>(
            MakeError(E_REJECTED, "release"));

        UNIT_ASSERT_VALUES_EQUAL(ClientId, record.GetHeaders().GetClientId());
        UNIT_ASSERT_VALUES_EQUAL(1000, record.GetHeaders().GetRequestTimeout());
        UNIT_ASSERT_VALUES_EQUAL(2, record.DeviceUUIDsSize());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", record.GetDeviceUUIDs(0));
        UNIT_ASSERT_VALUES_EQUAL("uuid-2", record.GetDeviceUUIDs(1));
        UNIT_ASSERT_VALUES_EQUAL("fastshard-id", record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(42, record.GetVolumeGeneration());

        const auto response = Wait(std::move(future));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("release", response.GetError().GetMessage());
    }

    Y_UNIT_TEST_F(ShouldRejectUnknownAccessMode, TFixture)
    {
        const auto unknownAccessMode =
            static_cast<NCloud::NProto::EAccessMode>(42);

        {
            NCloud::NProto::TAcquireDevicesRequest request;
            request.MutableHeaders()->SetClientId(ClientId);
            *request.MutableDeviceUUIDs()->Add() = DeviceUUID;
            request.SetAccessMode(unknownAccessMode);

            const auto response =
                Wait(DeviceManager->AcquireDevices(std::move(request)));
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        // the request does not reach the disk agent
        UNIT_ASSERT(!Runtime.GrabEdgeEvent<TEvDiskAgent::TEvAcquireDevicesRequest>(
            DiskAgentActorId,
            10ms));

        AcquireDevice(ClientId, NProto::VOLUME_ACCESS_READ_WRITE);

        const auto error =
            DeviceManager->AccessDevice(DeviceUUID, ClientId, unknownAccessMode);
        UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), FormatError(error));
    }

    Y_UNIT_TEST_F(ShouldCheckDeviceAccess, TFixture)
    {
        const auto accessDevice =
            [&](const TString& clientId, NCloud::NProto::EAccessMode accessMode)
        {
            return DeviceManager->AccessDevice(DeviceUUID, clientId, accessMode)
                .GetCode();
        };

        // Nobody has acquired the device yet

        UNIT_ASSERT_VALUES_EQUAL(
            E_BS_INVALID_SESSION,
            accessDevice(ClientId, NCloud::NProto::ACCESS_READ_ONLY));

        // The writer may both read and write

        AcquireDevice(ClientId, NProto::VOLUME_ACCESS_READ_WRITE);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            accessDevice(ClientId, NCloud::NProto::ACCESS_READ_WRITE));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            accessDevice(ClientId, NCloud::NProto::ACCESS_READ_ONLY));

        // A reader may only read

        const TString readerId = "reader-id";
        AcquireDevice(readerId, NProto::VOLUME_ACCESS_READ_ONLY);

        UNIT_ASSERT_VALUES_EQUAL(
            E_BS_INVALID_SESSION,
            accessDevice(readerId, NCloud::NProto::ACCESS_READ_WRITE));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            accessDevice(readerId, NCloud::NProto::ACCESS_READ_ONLY));

        // and the others may do neither

        for (const auto accessMode:
             {NCloud::NProto::ACCESS_READ_WRITE,
              NCloud::NProto::ACCESS_READ_ONLY})
        {
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_INVALID_SESSION,
                accessDevice("other-client-id", accessMode));
        }
    }

    Y_UNIT_TEST_F(ShouldCreateDeviceForRegion, TFixture)
    {
        const ui64 firstPageNo = 16;

        auto device = DeviceManager->CreateDevice(
            DeviceUUID,
            {.FirstPageNo = firstPageNo, .PageCount = 32},
            DefaultBlockSize);

        {
            TBuffer page;
            page.Fill('A', DefaultBlockSize);

            TVector<NJournalled::TPageRange> ranges;
            ranges.push_back({.FirstPageNo = 0, .Pages = {std::move(page)}});

            const auto error = device->WritePages(std::move(ranges))
                                   .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        }

        // the first page of the region is the page of the device

        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        request->SetStartIndex(firstPageNo);
        request->SetBlocksCount(1);

        const auto response = StorageAdapter->ReadBlocks(
            Now,
            MakeIntrusive<TCallContext>(),
            std::move(request),
            DefaultBlockSize,
            {}   // dataBuffer
        ).GetValueSync();

        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetBlocks().BuffersSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'A'),
            response.GetBlocks().GetBuffers(0));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
