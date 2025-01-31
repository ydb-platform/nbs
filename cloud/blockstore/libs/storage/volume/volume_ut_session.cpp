#include "volume_ut.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace std::chrono_literals;

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

using namespace NTestVolume;

using namespace NTestVolumeHelpers;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    std::unique_ptr<TTestActorRuntime> Runtime;
    TIntrusivePtr<TDiskRegistryState> State;

    void SetupTest(TDuration agentRequestTimeout = 1s)
    {
        NProto::TStorageServiceConfig config;
        config.SetAcquireNonReplicatedDevices(true);
        config.SetNonReplicatedVolumeDirectAcquireEnabled(true);
        config.SetAgentRequestTimeout(agentRequestTimeout.MilliSeconds());
        config.SetClientRemountPeriod(2000);
        State = MakeIntrusive<TDiskRegistryState>();
        Runtime = PrepareTestActorRuntime(config, State);
        auto volume = GetVolumeClient();

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024);

        volume.WaitReady();
    }

    TVolumeClient GetVolumeClient() const
    {
        return {*Runtime};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeSessionTest)
{
    Y_UNIT_TEST_F(ShouldPassAllParamsInAcquireDevicesRequest, TFixture)
    {
        SetupTest();

        auto volume = GetVolumeClient();

        auto response = volume.GetVolumeInfo();
        auto diskInfo = response->Record.GetVolume();
        THashSet<TString> devices;
        for (const auto& d: diskInfo.GetDevices()) {
            devices.emplace(d.GetDeviceUUID());
        }
        for (const auto& m: diskInfo.GetMigrations()) {
            devices.emplace(m.GetTargetDevice().GetDeviceUUID());
        }

        auto statVolumeResponse = volume.StatVolume();

        bool requestSended = false;

        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvAcquireDevicesRequest)
                {
                    requestSended = true;
                    auto* acquireReq =
                        event->Get<TEvDiskAgent::TEvAcquireDevicesRequest>();
                    Y_UNUSED(acquireReq);

                    auto& record = acquireReq->Record;

                    UNIT_ASSERT_EQUAL(
                        record.GetAccessMode(),
                        NProto::VOLUME_ACCESS_READ_WRITE);

                    UNIT_ASSERT_EQUAL(record.GetDiskId(), diskInfo.GetDiskId());

                    const auto& deviceUUIDS = record.GetDeviceUUIDs();
                    UNIT_ASSERT_EQUAL(
                        static_cast<size_t>(deviceUUIDS.size()),
                        devices.size());
                    for (const auto& deviceUUID: deviceUUIDS) {
                        UNIT_ASSERT(devices.contains(deviceUUID));
                    }

                    UNIT_ASSERT_EQUAL(
                        statVolumeResponse->Record.GetVolumeGeneration(),
                        record.GetVolumeGeneration());

                    UNIT_ASSERT_EQUAL(
                        statVolumeResponse->Record.GetMountSeqNumber(),
                        record.GetMountSeqNumber());
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(writer);

        UNIT_ASSERT(requestSended);
    }

    Y_UNIT_TEST_F(ShouldPassAllParamsInReleaseDevicesRequest, TFixture)
    {
        SetupTest();

        auto volume = GetVolumeClient();

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(writer);

        auto response = volume.GetVolumeInfo();
        auto diskInfo = response->Record.GetVolume();
        THashSet<TString> devices;
        for (const auto& d: diskInfo.GetDevices()) {
            devices.emplace(d.GetDeviceUUID());
        }
        for (const auto& m: diskInfo.GetMigrations()) {
            devices.emplace(m.GetTargetDevice().GetDeviceUUID());
        }

        auto statVolumeResponse = volume.StatVolume();

        bool requestSended = false;

        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvReleaseDevicesRequest)
                {
                    requestSended = true;
                    auto* acquireReq =
                        event->Get<TEvDiskAgent::TEvReleaseDevicesRequest>();
                    Y_UNUSED(acquireReq);

                    auto& record = acquireReq->Record;

                    UNIT_ASSERT_EQUAL(record.GetDiskId(), diskInfo.GetDiskId());

                    const auto& deviceUUIDS = record.GetDeviceUUIDs();
                    UNIT_ASSERT_EQUAL(
                        static_cast<size_t>(deviceUUIDS.size()),
                        devices.size());
                    for (const auto& deviceUUID: deviceUUIDS) {
                        UNIT_ASSERT(devices.contains(deviceUUID));
                    }

                    UNIT_ASSERT_EQUAL(
                        statVolumeResponse->Record.GetVolumeGeneration(),
                        record.GetVolumeGeneration());
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        volume.RemoveClient(writer.GetClientId());

        UNIT_ASSERT(requestSended);
    }

    Y_UNIT_TEST_F(ShouldSendAcquireReleaseRequestsDirectlyToDiskAgent, TFixture)
    {
        SetupTest();

        TVolumeClient writerClient = GetVolumeClient();
        auto readerClient1 = GetVolumeClient();
        auto readerClient2 = GetVolumeClient();

        ui32 acquireRequestsToDiskRegistry = 0;
        ui32 releaseRequestsToDiskRegistry = 0;
        ui32 readerAcquireRequests = 0;
        ui32 writerAcquireRequests = 0;
        ui32 releaseRequests = 0;

        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvAcquireDiskRequest:
                        ++acquireRequestsToDiskRegistry;
                        break;
                    case TEvDiskRegistry::EvReleaseDiskRequest:
                        ++releaseRequestsToDiskRegistry;
                        break;
                    case TEvDiskAgent::EvAcquireDevicesRequest: {
                        auto* msg =
                            event
                                ->Get<TEvDiskAgent::TEvAcquireDevicesRequest>();
                        if (msg->Record.GetAccessMode() ==
                            NProto::VOLUME_ACCESS_READ_ONLY)
                        {
                            ++readerAcquireRequests;
                        } else {
                            ++writerAcquireRequests;
                        }
                        break;
                    }
                    case TEvDiskAgent::EvReleaseDevicesRequest:
                        ++releaseRequests;
                        break;
                    default:
                        break;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        Runtime->AdvanceCurrentTime(2s);
        Runtime->DispatchEvents({}, 1ms);

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 0);

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        writerClient.AddClient(writer);

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 0);

        auto reader1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);
        readerClient1.AddClient(reader1);

        auto reader2 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);
        readerClient2.AddClient(reader2);

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 2);

        Runtime->AdvanceCurrentTime(2s);
        Runtime->DispatchEvents({}, 1ms);

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 2);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 4);

        UNIT_ASSERT_VALUES_EQUAL(releaseRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(releaseRequestsToDiskRegistry, 0);

        readerClient1.RemoveClient(reader1.GetClientId());

        Runtime->AdvanceCurrentTime(2s);
        Runtime->DispatchEvents({}, 1ms);

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 3);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 5);

        UNIT_ASSERT_VALUES_EQUAL(releaseRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(releaseRequestsToDiskRegistry, 0);

        writerClient.RemoveClient(writer.GetClientId());
        readerClient2.RemoveClient(reader2.GetClientId());

        Runtime->AdvanceCurrentTime(2s);
        Runtime->DispatchEvents({}, 1ms);

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 3);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 5);

        UNIT_ASSERT_VALUES_EQUAL(releaseRequests, 3);
        UNIT_ASSERT_VALUES_EQUAL(releaseRequestsToDiskRegistry, 0);
    }

    Y_UNIT_TEST_F(ShouldRejectTimedoutAcquireRequests, TFixture)
    {
        SetupTest(100ms);

        auto writerClient = GetVolumeClient();

        std::unique_ptr<IEventHandle> stollenResponse;
        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvAcquireDevicesResponse)
                {
                    stollenResponse.reset(event.Release());
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        writerClient.SendAddClientRequest(writer);
        auto response = writerClient.RecvAddClientResponse();
        UNIT_ASSERT_EQUAL(response->GetError().GetCode(), E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetMessage(), "timeout");
    }

    Y_UNIT_TEST_F(ShouldPassErrorsFromDiskAgent, TFixture)
    {
        SetupTest();

        auto writerClient = GetVolumeClient();

        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvAcquireDevicesResponse)
                {
                    auto response = std::make_unique<
                        TEvDiskAgent::TEvAcquireDevicesResponse>(
                        MakeError(E_TRY_AGAIN));

                    Runtime->Send(new IEventHandle(
                        event->Recipient,
                        event->Sender,
                        response.release(),
                        0,   // flags
                        event->Cookie));

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        writerClient.SendAddClientRequest(writer);
        auto response = writerClient.RecvAddClientResponse();

        UNIT_ASSERT_EQUAL(response->GetError().GetCode(), E_TRY_AGAIN);
    }

    Y_UNIT_TEST_F(ShouldMuteErrorsWithMuteIoErrors, TFixture)
    {
        SetupTest();

        auto writerClient = GetVolumeClient();

        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvAcquireDevicesResponse)
                {
                    auto response = std::make_unique<
                        TEvDiskAgent::TEvAcquireDevicesResponse>(
                        MakeError(E_TRY_AGAIN));

                    Runtime->Send(new IEventHandle(
                        event->Recipient,
                        event->Sender,
                        response.release(),
                        0,   // flags
                        event->Cookie));

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        auto& disk = State->Disks.at("vol0");
        disk.IOMode = NProto::VOLUME_IO_ERROR_READ_ONLY;
        disk.IOModeTs = Runtime->GetCurrentTime();
        disk.MuteIOErrors = true;

        auto volume = GetVolumeClient();
        volume.ReallocateDisk();
        // reallocate disk will trigger pipes reset, so reestablish connection
        volume.ReconnectPipe();

        writerClient.AddClient(writer);
    }

    Y_UNIT_TEST_F(ShouldHandleRequestsUndelivery, TFixture)
    {
        SetupTest();

        auto writerClient = GetVolumeClient();

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        auto agentNodeId = MakeDiskAgentServiceId(
            State->Disks.at("vol0").Devices[0].GetNodeId());

        Runtime->Send(new IEventHandle(
            agentNodeId,
            TActorId(),
            new TEvents::TEvPoisonPill));

        writerClient.SendAddClientRequest(writer);

        auto response = writerClient.RecvAddClientResponse();

        UNIT_ASSERT_EQUAL(response->GetError().GetCode(), E_REJECTED);
        UNIT_ASSERT_EQUAL(response->GetError().GetMessage(), "not delivered");
    }
}
}   // namespace NCloud::NBlockStore::NStorage
