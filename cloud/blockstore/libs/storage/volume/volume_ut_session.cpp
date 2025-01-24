#include "volume_ut.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

using namespace NTestVolume;

using namespace NTestVolumeHelpers;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestContext
{
    TVolumeClient Volume;
    std::unique_ptr<TTestActorRuntime> Runtime;
    TIntrusivePtr<TDiskRegistryState> DRState;
};

TTestContext SetupTest()
{
    NProto::TStorageServiceConfig config;
    config.SetAcquireNonReplicatedDevices(true);
    config.SetNonReplicatedVolumeDirectAcquireEnabled(true);
    config.SetClientRemountPeriod(2000);
    auto state = MakeIntrusive<TDiskRegistryState>();
    auto runtime = PrepareTestActorRuntime(config, state);
    TVolumeClient volume(*runtime);

    volume.UpdateVolumeConfig(
        0,
        0,
        0,
        0,
        false,
        1,
        NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
        1024);

    return {
        .Volume = std::move(volume),
        .Runtime = std::move(runtime),
        .DRState = std::move(state)};
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeTest)
{
    Y_UNIT_TEST(ShouldPassAllParamsInAcquireDevicesRequest)
    {
        auto [volume, runtime, _] = SetupTest();

        TVolumeClient writerClient(*runtime);

        volume.WaitReady();

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

        runtime->SetObserverFunc(
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

                    UNIT_ASSERT_EQUAL(
                        record.GetDiskId(),
                        diskInfo.GetDiskId());

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
        writerClient.AddClient(writer);

        UNIT_ASSERT(requestSended);
    }

    Y_UNIT_TEST(ShouldPassAllParamsInReleaseDevicesRequest)
    {
        auto [volume, runtime, _] = SetupTest();

        TVolumeClient writerClient(*runtime);

        volume.WaitReady();

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        writerClient.AddClient(writer);

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

        runtime->SetObserverFunc(
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

    Y_UNIT_TEST(ShouldSendAcquireReleaseRequestsDirectlyToDiskAgent)
    {
        auto [volume, runtime, _] = SetupTest();

        TVolumeClient writerClient(*runtime);
        TVolumeClient readerClient1(*runtime);
        TVolumeClient readerClient2(*runtime);

        volume.WaitReady();

        ui32 acquireRequestsToDiskRegistry = 0;
        ui32 releaseRequestsToDiskRegistry = 0;
        ui32 readerAcquireRequests = 0;
        ui32 writerAcquireRequests = 0;
        ui32 releaseRequests = 0;

        runtime->SetObserverFunc(
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

        runtime->AdvanceCurrentTime(TDuration::Seconds(2));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

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

        runtime->AdvanceCurrentTime(TDuration::Seconds(2));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 2);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 4);

        UNIT_ASSERT_VALUES_EQUAL(releaseRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(releaseRequestsToDiskRegistry, 0);

        readerClient1.RemoveClient(reader1.GetClientId());

        runtime->AdvanceCurrentTime(TDuration::Seconds(2));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 3);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 5);

        UNIT_ASSERT_VALUES_EQUAL(releaseRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(releaseRequestsToDiskRegistry, 0);

        writerClient.RemoveClient(writer.GetClientId());
        readerClient2.RemoveClient(reader2.GetClientId());

        runtime->AdvanceCurrentTime(TDuration::Seconds(2));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(acquireRequestsToDiskRegistry, 0);
        UNIT_ASSERT_VALUES_EQUAL(writerAcquireRequests, 3);
        UNIT_ASSERT_VALUES_EQUAL(readerAcquireRequests, 5);

        UNIT_ASSERT_VALUES_EQUAL(releaseRequests, 3);
        UNIT_ASSERT_VALUES_EQUAL(releaseRequestsToDiskRegistry, 0);
    }

    Y_UNIT_TEST(ShouldRejectTimedoutAcquireRequests)
    {
        auto [volume, runtime, _] = SetupTest();

        TVolumeClient writerClient(*runtime);

        volume.WaitReady();
        std::unique_ptr<IEventHandle> stollenResponse;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvAcquireDevicesResponse)
                {
                    stollenResponse.reset(event.Release());
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

    Y_UNIT_TEST(ShouldPassErrorsFromDiskAgent)
    {
        auto [volume, runtime, _] = SetupTest();

        TVolumeClient writerClient(*runtime);

        volume.WaitReady();

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvAcquireDevicesResponse)
                {
                    auto response = std::make_unique<
                        TEvDiskAgent::TEvAcquireDevicesResponse>(
                        MakeError(E_TRY_AGAIN));

                    runtime->Send(new IEventHandle(
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

    Y_UNIT_TEST(ShouldMuteErrorsWithMuteIoErrors)
    {
        auto [volume, runtime, state] = SetupTest();

        TVolumeClient writerClient(*runtime);

        volume.WaitReady();


        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvAcquireDevicesResponse)
                {
                    auto response = std::make_unique<
                        TEvDiskAgent::TEvAcquireDevicesResponse>(
                        MakeError(E_TRY_AGAIN));

                    runtime->Send(new IEventHandle(
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

        auto& disk = state->Disks.at("vol0");
        disk.IOMode = NProto::VOLUME_IO_ERROR_READ_ONLY;
        disk.IOModeTs = runtime->GetCurrentTime();
        disk.MuteIOErrors = true;

        volume.ReallocateDisk();
        // reallocate disk will trigger pipes reset, so reestablish connection
        volume.ReconnectPipe();

        writerClient.AddClient(writer);
    }

    Y_UNIT_TEST(ShouldHandleRequestsUndelivery)
    {
        auto [volume, runtime, state] = SetupTest();

        TVolumeClient writerClient(*runtime);

        volume.WaitReady();

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        auto agentNodeId = MakeDiskAgentServiceId(
            state->Disks.at("vol0").Devices[0].GetNodeId());

        runtime->Send(new IEventHandle(
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
