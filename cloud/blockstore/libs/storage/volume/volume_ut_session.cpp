#include "volume_ut.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

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

TVector<NProto::TDeviceConfig> MakeDeviceList(ui32 agentCount, ui32 deviceCount)
{
    TVector<NProto::TDeviceConfig> result;
    for (ui32 i = 1; i <= agentCount; i++) {
        for (ui32 j = 0; j < deviceCount; j++) {
            auto device = MakeDevice(
                Sprintf("uuid-%u.%u", i, j),
                Sprintf("dev%u", j),
                Sprintf("transport%u-%u", i, j));
            device.SetNodeId(i - 1);
            device.SetAgentId(Sprintf("agent-%u", i));
            result.push_back(std::move(device));
        }
    }
    return result;
}

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

        const ui64 blockCount = DefaultDeviceBlockCount *
                                DefaultDeviceBlockSize / DefaultBlockSize * 3;

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            blockCount);

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

    Y_UNIT_TEST_F(ShouldFilterLostDevicesDuringAcquire, TFixture)
    {
        SetupTest();

        auto volume = GetVolumeClient();

        auto response = volume.GetVolumeInfo();
        auto diskInfo = response->Record.GetVolume();

        TVector<TString> devices;
        for (const auto& d: diskInfo.GetDevices()) {
            devices.emplace_back(d.GetDeviceUUID());
        }

        State->LostDevices.emplace_back(devices[0]);

        volume.ReallocateDisk();
        // reallocate disk will trigger pipes reset, so reestablish connection
        volume.ReconnectPipe();

        THashSet<TString> acquiredDevices;
        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvAcquireDevicesRequest)
                {
                    auto* ev =
                        static_cast<TEvDiskAgent::TEvAcquireDevicesRequest*>(
                            event->GetBase());

                    for (const auto& d: ev->Record.GetDeviceUUIDs()) {
                        acquiredDevices.emplace(d);
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto writerClient = GetVolumeClient();

        auto writer = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        writerClient.AddClient(writer);

        // Lost device should not be acquired.
        UNIT_ASSERT_VALUES_EQUAL(devices.size() - 1, acquiredDevices.size());
        for (size_t i = 1; i < devices.size(); ++i) {
            UNIT_ASSERT(acquiredDevices.contains(devices[i]));
        }
        acquiredDevices.clear();

        State->LostDevices.clear();

        volume.ReallocateDisk();
        // reallocate disk will trigger pipes reset, so reestablish connection
        volume.ReconnectPipe();

        Runtime->AdvanceCurrentTime(2s);
        Runtime->DispatchEvents({}, 10ms);

        // All devices should be acquired.
        UNIT_ASSERT_VALUES_EQUAL(devices.size(), acquiredDevices.size());
        for (const auto& device: devices) {
            UNIT_ASSERT(acquiredDevices.contains(device));
        }
    }
}

Y_UNIT_TEST_SUITE(TVolumeAcquireReleaseTest)
{
    Y_UNIT_TEST(ShouldReleaseReplacedDevices)
    {
        constexpr ui32 AgentCount = 3;
        NProto::TStorageServiceConfig config;
        config.SetAcquireNonReplicatedDevices(true);
        config.SetNonReplicatedVolumeDirectAcquireEnabled(true);
        config.SetClientRemountPeriod(TDuration::Seconds(1).MilliSeconds());
        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();

        diskRegistryState->Devices = MakeDeviceList(AgentCount, 3);
        diskRegistryState->AllocateDiskReplicasOnDifferentNodes = true;
        diskRegistryState->ReplicaCount = 2;
        TVector<TDiskAgentStatePtr> agentStates;
        for (ui32 i = 0; i < AgentCount; i++) {
            agentStates.push_back(TDiskAgentStatePtr{});
        }

        auto runtime = PrepareTestActorRuntime(
            config,
            diskRegistryState,
            {},
            {},
            std::move(agentStates));
        auto volume = TVolumeClient(*runtime);

        struct TDeviceRequests
        {
            THashSet<TString> AcquiredDevices;
            TVector<TString> ReleasedDevices;
        };
        THashMap<TString, TDeviceRequests> clientRequests;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskAgent::EvAcquireDevicesRequest: {
                        auto* msg =
                            event
                                ->Get<TEvDiskAgent::TEvAcquireDevicesRequest>();
                        const auto& devices = msg->Record.GetDeviceUUIDs();
                        const auto& clientId =
                            msg->Record.GetHeaders().GetClientId();
                        clientRequests[clientId].AcquiredDevices.insert(
                            devices.begin(),
                            devices.end());
                        break;
                    }
                    case TEvDiskAgent::EvReleaseDevicesRequest: {
                        auto* msg =
                            event
                                ->Get<TEvDiskAgent::TEvReleaseDevicesRequest>();
                        const auto& devices = msg->Record.GetDeviceUUIDs();
                        const auto& clientId =
                            msg->Record.GetHeaders().GetClientId();
                        clientRequests[clientId].ReleasedDevices.insert(
                            clientRequests[clientId].ReleasedDevices.end(),
                            devices.begin(),
                            devices.end());
                        break;
                    }
                }

                return false;
            });

        // Create mirror-3 disk with one device per replica.
        constexpr ui64 BlockCount =
            DefaultDeviceBlockCount * DefaultDeviceBlockSize / DefaultBlockSize;
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            BlockCount);
        volume.WaitReady();

        // Add clients.
        auto clientInfoRW = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        auto clientInfoRO = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);
        volume.AddClient(clientInfoRW);
        volume.AddClient(clientInfoRO);

        // Devices for both clients should be acquired.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return clientRequests[clientInfoRW.GetClientId()]
                               .AcquiredDevices.size() == 3 &&
                       clientRequests[clientInfoRO.GetClientId()]
                               .AcquiredDevices.size() == 3;
            };
            runtime->DispatchEvents(options, TDuration::Seconds(10));
        }

        // No reason to release.
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            clientRequests[clientInfoRW.GetClientId()].ReleasedDevices.size());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            clientRequests[clientInfoRO.GetClientId()].ReleasedDevices.size());

        // Reallocate disk.
        volume.ReallocateDisk();
        volume.ReconnectPipe();
        volume.WaitReady();

        // Release shouldn't be sent.
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            clientRequests[clientInfoRW.GetClientId()].ReleasedDevices.size());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            clientRequests[clientInfoRO.GetClientId()].ReleasedDevices.size());

        // Replace two devices.
        auto stat = volume.StatVolume();
        const auto& volumeConfig = stat->Record.GetVolume();
        TVector<TString> replacedDevices = {
            volumeConfig.GetDevices(0).GetDeviceUUID(),
            volumeConfig.GetReplicas(0).GetDevices(0).GetDeviceUUID()};
        UNIT_ASSERT(
            diskRegistryState->ReplaceDevice("vol0", replacedDevices[0]));
        UNIT_ASSERT(
            diskRegistryState->ReplaceDevice("vol0", replacedDevices[1]));

        // Reallocate disk.
        volume.ReallocateDisk();
        volume.ReconnectPipe();
        volume.WaitReady();

        // Wait for the release.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !clientRequests[clientInfoRW.GetClientId()]
                            .ReleasedDevices.empty() &&
                       !clientRequests[clientInfoRO.GetClientId()]
                            .ReleasedDevices.empty();
            };
            runtime->DispatchEvents(options, TDuration::Seconds(10));
        }
        // Replaced devices should be released.
        ASSERT_VECTOR_CONTENTS_EQUAL(
            replacedDevices,
            clientRequests[clientInfoRW.GetClientId()].ReleasedDevices);
        ASSERT_VECTOR_CONTENTS_EQUAL(
            replacedDevices,
            clientRequests[clientInfoRO.GetClientId()].ReleasedDevices);

        // Start migration.
        stat = volume.StatVolume();
        UNIT_ASSERT(diskRegistryState->StartDeviceMigration(
            stat->Record.GetVolume().GetDevices(0).GetDeviceUUID()));

        clientRequests[clientInfoRW.GetClientId()].ReleasedDevices.clear();
        clientRequests[clientInfoRO.GetClientId()].ReleasedDevices.clear();
        clientRequests[clientInfoRW.GetClientId()].AcquiredDevices.clear();
        clientRequests[clientInfoRO.GetClientId()].AcquiredDevices.clear();

        // Reallocate disk.
        volume.ReallocateDisk();
        volume.ReconnectPipe();
        volume.WaitReady();

        // All replica devices and target migration for both clients should be
        // acquired.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return clientRequests[clientInfoRW.GetClientId()]
                               .AcquiredDevices.size() == 4 &&
                       clientRequests[clientInfoRO.GetClientId()]
                               .AcquiredDevices.size() == 4;
            };
            runtime->DispatchEvents(options, TDuration::Seconds(10));
        }

        // Release shouldn't be sent.
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            clientRequests[clientInfoRW.GetClientId()].ReleasedDevices.size());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            clientRequests[clientInfoRO.GetClientId()].ReleasedDevices.size());

        // Check that the volume is migrating.
        stat = volume.StatVolume();
        const auto& migrations = stat->Record.GetVolume().GetMigrations();
        UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(
            stat->Record.GetVolume().GetDevices(0).GetDeviceUUID(),
            migrations[0].GetSourceDeviceId());

        // Finish migration.
        diskRegistryState->MigrationMode = EMigrationMode::Finish;

        // Reallocate disk.
        volume.ReallocateDisk();
        volume.ReconnectPipe();
        volume.WaitReady();

        // Wait for the release.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !clientRequests[clientInfoRW.GetClientId()]
                            .ReleasedDevices.empty() &&
                       !clientRequests[clientInfoRO.GetClientId()]
                            .ReleasedDevices.empty();
            };
            runtime->DispatchEvents(options, TDuration::Seconds(10));
        }
        // Source device should be released.
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TString>{migrations[0].GetSourceDeviceId()},
            clientRequests[clientInfoRW.GetClientId()].ReleasedDevices);
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TString>{migrations[0].GetSourceDeviceId()},
            clientRequests[clientInfoRO.GetClientId()].ReleasedDevices);
    }
}
}   // namespace NCloud::NBlockStore::NStorage
