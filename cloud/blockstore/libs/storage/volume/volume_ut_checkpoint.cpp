#include "volume_ut.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NTestVolume;

using namespace NTestVolumeHelpers;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeCheckpointTest)
{
    Y_UNIT_TEST(ShouldForwardGetChangedBlocksToMultipartitionVolume)
    {
        NProto::TStorageServiceConfig config;
        config.SetMinChannelCount(4);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            3,  // partition count
            2  // blocksPerStripe
        );

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0
        );
        volume.AddClient(clientInfo);

        auto popCountStr = [](const TString& s) {
            ui64 count = 0;
            for (char c : s) {
                count += PopCount(c);
            }
            return count;
        };

        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(0, 10),
            clientInfo.GetClientId(),
            1);
        volume.CreateCheckpoint("c1");

        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            2);
        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(5, 15),
            clientInfo.GetClientId(),
            2);
        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(63, 64),
            clientInfo.GetClientId(),
            2);
        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(1022, 1024),
            clientInfo.GetClientId(),
            2);
        volume.CreateCheckpoint("c2");

        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(0, 100),
            clientInfo.GetClientId(),
            3);

        auto response = volume.GetChangedBlocks(TBlockRange64::WithLength(0, 1024), "c1", "c2");

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1024 / 8, response->Record.GetMask().size());
        UNIT_ASSERT_VALUES_EQUAL(16, popCountStr(response->Record.GetMask()));  // 0, 5-15, 63-64, 1022-1023
        // range [0-7]
        UNIT_ASSERT_VALUES_EQUAL(0b11100001, ui8(response->Record.GetMask()[0]));
        // range [8-15]
        UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(response->Record.GetMask()[1]));
        // range [56-63]
        UNIT_ASSERT_VALUES_EQUAL(0b10000000, ui8(response->Record.GetMask()[7]));
        // range [64-72]
        UNIT_ASSERT_VALUES_EQUAL(0b00000001, ui8(response->Record.GetMask()[8]));
        // range [1016-1023]
        UNIT_ASSERT_VALUES_EQUAL(0b11000000, ui8(response->Record.GetMask()[127]));

        response = volume.GetChangedBlocks(
            TBlockRange64::MakeClosedInterval(1021, 1035),
            "c1",
            "c2");

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetMask().size());
        UNIT_ASSERT_VALUES_EQUAL(3, popCountStr(response->Record.GetMask()));

        // range [1021-1028]
        UNIT_ASSERT_VALUES_EQUAL(0b00001110, ui8(response->Record.GetMask()[0]));
    }

    Y_UNIT_TEST(ShouldCreateCheckpointsForMultipartitionVolumes)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        {
            auto stat = volume.StatVolume();
            const auto& v = stat->Record.GetVolume();
            UNIT_ASSERT_VALUES_EQUAL(v.GetBlocksCount(), 21 * 1024);
            UNIT_ASSERT_VALUES_EQUAL(v.GetPartitionsCount(), 3);
        }

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        for (ui32 i = 0; i < 21; ++i) {
            volume.WriteBlocks(
                TBlockRange64::WithLength(1024 * i, 1024),
                clientInfo.GetClientId(),
                1
            );
        }

        volume.CreateCheckpoint("c1");

        for (ui32 i = 0; i < 21; ++i) {
            volume.WriteBlocks(
                TBlockRange64::WithLength(1024 * i, 1024),
                clientInfo.GetClientId(),
                2
            );
        }

        for (ui32 i = 0; i < 21; ++i) {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(1024 * i, 1024),
                clientInfo.GetClientId(),
                "c1"
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(1), bufs[j]);
            }
        }

        for (ui32 i = 0; i < 21; ++i) {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(1024 * i, 1024),
                clientInfo.GetClientId()
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[j]);
            }
        }
    }

    Y_UNIT_TEST(ShouldDrainMultipartitionVolumePartitionsBeforeCheckpointCreation)
    {
        NProto::TStorageServiceConfig config;
        config.SetWriteBlobThreshold(1);    // disabling fresh
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);

        TActorId partActorId;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartition::EvWaitReadyResponse
                        // selecting a partition actor - doesn't matter which one
                        && !partActorId)
                {
                    partActorId = event->Sender;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            1
        );

        TDeque<std::unique_ptr<IEventHandle>> addBlobsRequests;
        bool intercept = true;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (intercept) {
                    switch (event->GetTypeRewrite()) {
                        case TEvPartitionPrivate::EvAddBlobsRequest: {
                            addBlobsRequests.emplace_back(event.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        volume.SendWriteBlocksRequest(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2
        );

        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(addBlobsRequests.size());
        TEST_NO_RESPONSE(runtime, WriteBlocks);

        volume.SendCreateCheckpointRequest("c1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        TEST_NO_RESPONSE(runtime, CreateCheckpoint);

        intercept = false;
        for (auto& request: addBlobsRequests) {
            runtime->Send(request.release());
        }

        {
            auto response = volume.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = volume.RecvCreateCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId(),
                "c1"
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 i = 0; i < 1024; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[i]);
            }
        }
    }

    Y_UNIT_TEST(ShouldDrainMultipartitionVolumeRequestsInVolumeBeforeCheckpointCreation)
    {
        NProto::TStorageServiceConfig config;
        config.SetWriteBlobThreshold(1);    // disabling fresh
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);

        TActorId partActorId;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartition::EvWaitReadyResponse
                        // selecting a partition actor - doesn't matter which one
                        && !partActorId)
                {
                    partActorId = event->Sender;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2           // stripe size
        );

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            1
        );

        std::unique_ptr<IEventHandle> partWriteBlocksRequest;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->Recipient == partActorId) {
                    switch (event->GetTypeRewrite()) {
                        case TEvService::EvWriteBlocksRequest: {
                            partWriteBlocksRequest.reset(event.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        volume.SendWriteBlocksRequest(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2
        );

        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(partWriteBlocksRequest);
        TEST_NO_RESPONSE(runtime, WriteBlocks);

        volume.SendCreateCheckpointRequest("c1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        TEST_NO_RESPONSE(runtime, CreateCheckpoint);

        runtime->Send(partWriteBlocksRequest.release());

        {
            auto response = volume.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = volume.RecvCreateCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId(),
                "c1"
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 i = 0; i < 1024; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[i]);
            }
        }
    }

    Y_UNIT_TEST(ShouldFinishMultipartitionVolumeCheckpointCreationAfterReboot)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);

        TActorId partActorId;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartition::EvWaitReadyResponse
                        // selecting a partition actor - doesn't matter which one
                        && !partActorId)
                {
                    partActorId = event->Sender;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            1
        );

        bool intercepted = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvService::EvCreateCheckpointRequest
                        && event->Recipient == partActorId)
                {
                    intercepted = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        volume.SendCreateCheckpointRequest("c1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(intercepted);

        volume.SendWriteBlocksRequest(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            100
        );

        {
            auto response = volume.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        volume.SendZeroBlocksRequest(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId()
        );

        {
            auto response = volume.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        volume.RebootTablet();
        volume.AddClient(clientInfo);
        intercepted = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvService::EvCreateCheckpointResponse) {
                    intercepted = true;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );
        volume.WaitReady();
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(intercepted);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2
        );

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId(),
                "c1"
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 i = 0; i < 1024; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(1), bufs[i]);
            }
        }

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId()
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 i = 0; i < 1024; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[i]);
            }
        }
    }

    Y_UNIT_TEST(ShouldHandleErrorsDuringMultipartitionVolumeCheckpointCreation)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);

        TActorId partActorId;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartition::EvWaitReadyResponse
                        // selecting a partition actor - doesn't matter which one
                        && !partActorId)
                {
                    partActorId = event->Sender;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            1
        );

        bool intercepted = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvService::EvCreateCheckpointRequest
                        && event->Recipient == partActorId)
                {
                    intercepted = true;
                    auto response = std::make_unique<TEvService::TEvCreateCheckpointResponse>(
                        MakeError(E_FAIL, "epic failure")
                    );

                    runtime->Send(new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0, // flags
                        event->Cookie
                    ), 0);

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        volume.SendCreateCheckpointRequest("c1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(intercepted);

        intercepted = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvService::EvCreateCheckpointResponse) {
                    intercepted = true;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );
        volume.ReconnectPipe();
        volume.AddClient(clientInfo);
        volume.WaitReady();
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(intercepted);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2
        );

        {
            auto response = volume.RecvCreateCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_FAIL, response->GetStatus());
        }

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId(),
                "c1"
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 i = 0; i < 1024; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(1), bufs[i]);
            }
        }
    }

    void DoTestShouldCreateCheckpointsForDiskRegistryBased(
        NProto::EStorageMediaKind mediaKind)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        const auto expectedBlockCount =
            DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            mediaKind,
            expectedBlockCount);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Write 21 blocks
        for (ui32 i = 0; i < 21; ++i) {
            volume.WriteBlocks(GetBlockRangeById(i), clientInfo.GetClientId(), i);
        }

        volume.CreateCheckpoint("c1");

        {
            // Write request rejected
            volume.SendWriteBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId(),
                100);

            auto response = volume.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        {
            // Zero request rejected
            volume.SendZeroBlocksRequest(
                GetBlockRangeById(1),
                clientInfo.GetClientId());

            auto response = volume.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        {
            // Read from non exists checkpoint failed
            volume.SendReadBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId(),
                "unknown_checkpoint");

            auto response = volume.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                "Checkpoint id=\"unknown_checkpoint\" not found",
                response->GetError().GetMessage());
            UNIT_ASSERT(HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_SILENT));
        }
        {
            // Read blocks from checkpoint and check values
            for (ui32 i = 0; i < 21; ++i) {
                CheckBlockContent<__LINE__>(
                    volume,
                    clientInfo.GetClientId(),
                    "c1",
                    GetBlockRangeById(i),
                    GetBlockContent(i));
            }
        }

        {
            // Read blocks and check values
            for (ui32 i = 0; i < 21; ++i) {
                CheckBlockContent<__LINE__>(
                    volume,
                    clientInfo.GetClientId(),
                    "",
                    GetBlockRangeById(i),
                    GetBlockContent(i));
            }
        }

        // After checkpoint data deletion
        volume.DeleteCheckpointData("c1");
        {
            // Read from checkpoint without data failed
            volume.SendReadBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId(),
                "c1");

            auto response = volume.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                "Not found data for checkpoint id=\"c1\"",
                response->GetError().GetMessage());
            UNIT_ASSERT(HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_SILENT));
        }

        // Write blocks
        volume.WriteBlocks(
            GetBlockRangeById(25),
            clientInfo.GetClientId(),
            100);
        CheckBlockContent<__LINE__>(
            volume,
            clientInfo.GetClientId(),
            "",
            GetBlockRangeById(25),
            GetBlockContent(100));

        // Zero blocks
        volume.ZeroBlocks(GetBlockRangeById(25), clientInfo.GetClientId());
        CheckBlockContent<__LINE__>(
            volume,
            clientInfo.GetClientId(),
            "",
            GetBlockRangeById(25),
            GetBlockContent(0));

        // Create second checkpoint
        volume.CreateCheckpoint("c2");

        {
            // Write request rejected
            volume.SendWriteBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId(),
                100);

            auto response = volume.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }
        {
            // Read blocks from checkpoint and check values
            for (ui32 i = 0; i < 21; ++i) {
                CheckBlockContent<__LINE__>(
                    volume,
                    clientInfo.GetClientId(),
                    "c2",
                    GetBlockRangeById(i),
                    GetBlockContent(i));
            }
        }

        // After entire checkpoint deletion
        volume.DeleteCheckpoint("c2");

        // Write blocks OK
        volume.WriteBlocks(
            GetBlockRangeById(25),
            clientInfo.GetClientId(),
            100);
        CheckBlockContent<__LINE__>(
            volume,
            clientInfo.GetClientId(),
            "",
            GetBlockRangeById(25),
            GetBlockContent(100));

        // Delete first checkpoint
        volume.DeleteCheckpoint("c");
    }

    Y_UNIT_TEST(ShouldCreateCheckpointsForDiskRegistryBasedVolumes)
    {
        DoTestShouldCreateCheckpointsForDiskRegistryBased(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldCreateCheckpointsForMirror2Volumes)
    {
        DoTestShouldCreateCheckpointsForDiskRegistryBased(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2);
    }

    Y_UNIT_TEST(ShouldCreateCheckpointsForMirror3Volumes)
    {
        DoTestShouldCreateCheckpointsForDiskRegistryBased(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);
    }

    void DoTestShouldCreateAndDeleteCheckpointsForDiskRegistryBased(
        NProto::EStorageMediaKind mediaKind)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        const auto expectedBlockCount =
            DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            mediaKind,
            expectedBlockCount);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Create the first checkpoint and delete it via DeleteCheckpointData
        volume.CreateCheckpoint("c1");
        {
            auto stat = volume.StatVolume();
            UNIT_ASSERT_VALUES_EQUAL(1, stat->Record.GetCheckpoints().size());
            UNIT_ASSERT_VALUES_EQUAL("c1", stat->Record.GetCheckpoints()[0]);
        }
        volume.DeleteCheckpointData("c1");
        {
            auto stat = volume.StatVolume();
            UNIT_ASSERT(stat->Record.GetCheckpoints().empty());
        }

        // Create the second checkpoint and delete it via DeleteCheckpoint
        volume.CreateCheckpoint("c2");
        {
            auto stat = volume.StatVolume();
            UNIT_ASSERT_VALUES_EQUAL(1, stat->Record.GetCheckpoints().size());
            UNIT_ASSERT_VALUES_EQUAL("c2", stat->Record.GetCheckpoints()[0]);
        }
        volume.DeleteCheckpoint("c2");
        {
            auto stat = volume.StatVolume();
            UNIT_ASSERT(stat->Record.GetCheckpoints().empty());
        }
    }

    Y_UNIT_TEST(ShouldCreateAndDeleteCheckpointsForDiskRegistryBasedVolumes)
    {
        DoTestShouldCreateAndDeleteCheckpointsForDiskRegistryBased(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldCreateAndDeleteCheckpointsForMirror2Volumes)
    {
        DoTestShouldCreateAndDeleteCheckpointsForDiskRegistryBased(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2);
    }

    Y_UNIT_TEST(ShouldCreateAndDeleteCheckpointsForMirror3Volumes)
    {
        DoTestShouldCreateAndDeleteCheckpointsForDiskRegistryBased(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);
    }

    void DoShouldDrainBeforeCheckpointCreation(
        NProto::EStorageMediaKind mediaKind,
        bool testWriteRequest)
    {
        NProto::TStorageServiceConfig config;
        // Need timeout longer than timeout in runtime->DispatchEvents().
        config.SetNonReplicatedMinRequestTimeoutSSD(5'000);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            mediaKind,
            DefaultDeviceBlockSize * DefaultDeviceBlockCount /
                DefaultBlockSize);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Fill with 'x'
        volume.WriteBlocks(GetBlockRangeById(0), clientInfo.GetClientId(), 'x');

        // Steal the first write or zero request to the device.
        // We will return it later to complete the execution of the request.
        std::unique_ptr<IEventHandle> stolenDeviceRequest;
        auto stealFirstDeviceRequest = [&](TAutoPtr<IEventHandle>& event) {
            const bool zeroOrWriteRequest =
                event->GetTypeRewrite() ==
                    TEvDiskAgent::EvWriteDeviceBlocksRequest ||
                event->GetTypeRewrite() ==
                    TEvDiskAgent::EvZeroDeviceBlocksRequest;
            if (zeroOrWriteRequest && !stolenDeviceRequest) {
                stolenDeviceRequest.reset(event.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(stealFirstDeviceRequest);

        // Send write or zero request. It is delayed until we return the stolen
        // request
        if (testWriteRequest) {
            volume.SendWriteBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId(),
                's');
        } else {
            volume.SendZeroBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId());
        }
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(stolenDeviceRequest);
        TEST_NO_RESPONSE(runtime, WriteBlocks);

        // Send Create checkpoint request. It delayed untill write request
        // completed.
        volume.SendCreateCheckpointRequest("c1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        TEST_NO_RESPONSE(runtime, CreateCheckpoint);

        // Check whether our read request succeeded and actually read initial
        // content.
        CheckBlockContent<__LINE__>(
            volume,
            clientInfo.GetClientId(),
            "",
            GetBlockRangeById(0),
            GetBlockContent('x'));

        // Return stolen request - write/zero request will be completed after this
        runtime->Send(stolenDeviceRequest.release());

        if (testWriteRequest) {
            auto response = volume.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        } else {
            auto response = volume.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // Checkpoint creation completed.
        {
            auto createCheckpointResponse =
                volume.RecvCreateCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                createCheckpointResponse->GetStatus());
        }

        CheckBlockContent<__LINE__>(
            volume,
            clientInfo.GetClientId(),
            "",
            GetBlockRangeById(0),
            GetBlockContent(testWriteRequest ? 's' : 0));
    }

    Y_UNIT_TEST(ShouldDrainBeforeCheckpointCreationForNonreplicated)
    {
        DoShouldDrainBeforeCheckpointCreation(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED, true);
        DoShouldDrainBeforeCheckpointCreation(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED, false);
    }

    Y_UNIT_TEST(ShouldDrainBeforeCheckpointCreationForMirror2)
    {
        DoShouldDrainBeforeCheckpointCreation(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2, true);
        DoShouldDrainBeforeCheckpointCreation(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2, false);
    }

    Y_UNIT_TEST(ShouldDrainBeforeCheckpointCreationForMirror3)
    {
        DoShouldDrainBeforeCheckpointCreation(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3, true);
        DoShouldDrainBeforeCheckpointCreation(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3, false);
    }

    bool DoShouldCreateCheckpointDuringMigration(int checkpointAt)
    {
        NProto::TStorageServiceConfig config;
        config.SetAcquireNonReplicatedDevices(true);
        config.SetMaxMigrationBandwidth(999'999'999);
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(config, state);

        TVolumeClient volume(*runtime);
        TVolumeClient client1(*runtime);
        TVolumeClient client2(*runtime);

        const auto blocksPerDevice =
            DefaultDeviceBlockCount * DefaultDeviceBlockSize / DefaultBlockSize;

        // creating a nonreplicated disk
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            2.5 * blocksPerDevice
        );

        volume.WaitReady();

        {
            auto stat = volume.StatVolume();
            const auto& devices = stat->Record.GetVolume().GetDevices();
            const auto& migrations = stat->Record.GetVolume().GetMigrations();
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("transport0", devices[0].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("transport1", devices[1].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("transport2", devices[2].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        }

        const auto& disk = state->Disks.at("vol0");
        UNIT_ASSERT_VALUES_EQUAL("", disk.WriterClientId);
        UNIT_ASSERT_VALUES_EQUAL(0, disk.ReaderClientIds.size());
        UNIT_ASSERT_VALUES_EQUAL("", disk.PoolName);

        // registering a writer
        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        {
            auto response = client1.AddClient(clientInfo);
            const auto& volume = response->Record.GetVolume();
            UNIT_ASSERT_VALUES_EQUAL(3, volume.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "transport0",
                volume.GetDevices(0).GetTransportId()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                "transport1",
                volume.GetDevices(1).GetTransportId()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                "transport2",
                volume.GetDevices(2).GetTransportId()
            );
        }
        UNIT_ASSERT_VALUES_EQUAL(clientInfo.GetClientId(), disk.WriterClientId);
        UNIT_ASSERT_VALUES_EQUAL(0, disk.ReaderClientIds.size());

        // writing some data whose migration we will test
        const auto range1 = TBlockRange64::MakeOneBlock(5);
        const auto range2 = TBlockRange64::MakeOneBlock(5 + blocksPerDevice);
        const auto range3 = TBlockRange64::MakeOneBlock(5 + 2 * blocksPerDevice);
        client1.WriteBlocksLocal(
            range1,
            clientInfo.GetClientId(),
            GetBlockContent(1)
        );
        client1.WriteBlocksLocal(
            range2,
            clientInfo.GetClientId(),
            GetBlockContent(2)
        );
        client1.WriteBlocksLocal(
            range3,
            clientInfo.GetClientId(),
            GetBlockContent(3)
        );

        if (checkpointAt == 0) {
            volume.CreateCheckpoint("c1");
        }

        state->MigrationMode = EMigrationMode::InProgress;

        TAutoPtr<IEventHandle> evRangeMigrated;

        auto oldObsereverFunc = runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                const auto migratedEvent =
                    TEvNonreplPartitionPrivate::EvRangeMigrated;
                using TMigratedEvent =
                    TEvNonreplPartitionPrivate::TEvRangeMigrated;

                if (event->GetTypeRewrite() == migratedEvent) {
                    auto range = event->Get<TMigratedEvent>()->Range;
                    if (range.Start > 1024) {
                        evRangeMigrated = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        if (checkpointAt == 1) {
            volume.CreateCheckpoint("c1");
        }

        // reallocating disk
        volume.ReallocateDisk();
        client1.ReconnectPipe();
        client1.AddClient(clientInfo);
        volume.WaitReady();

        if (checkpointAt == 2) {
            volume.CreateCheckpoint("c1");
        }

        // checking that our volume sees the requested migrations
        {
            auto stat = volume.StatVolume();
            const auto& devices = stat->Record.GetVolume().GetDevices();
            const auto& migrations = stat->Record.GetVolume().GetMigrations();
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("transport0", devices[0].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("transport1", devices[1].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("transport2", devices[2].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "transport0",
                migrations[0].GetSourceTransportId()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                "transport0_migration",
                migrations[0].GetTargetDevice().GetTransportId()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                "transport2",
                migrations[1].GetSourceTransportId()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                "transport2_migration",
                migrations[1].GetTargetDevice().GetTransportId()
            );
        }

        // adding a reader
        auto clientInfo2 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);
        client2.AddClient(clientInfo2);

        UNIT_ASSERT_VALUES_EQUAL(clientInfo.GetClientId(), disk.WriterClientId);
        UNIT_ASSERT_VALUES_EQUAL(1, disk.ReaderClientIds.size());
        UNIT_ASSERT_VALUES_EQUAL(clientInfo2.GetClientId(), disk.ReaderClientIds[0]);

        UNIT_ASSERT(evRangeMigrated);

        runtime->SetObserverFunc(oldObsereverFunc);

        runtime->Send(evRangeMigrated.Release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // checking that DR was notified about a finished migration
        UNIT_ASSERT_VALUES_EQUAL(1, state->FinishMigrationRequests);

        state->MigrationMode = EMigrationMode::Finish;

        bool hasMorePlaceToCheck = true;
        if (checkpointAt == 3) {
            hasMorePlaceToCheck = false;
            volume.CreateCheckpoint("c1");
        }

        // reallocating disk
        volume.ReallocateDisk();
        client2.ReconnectPipe();
        client2.AddClient(clientInfo2);
        volume.WaitReady();

        // checking that our volume sees new device list
        {
            auto stat = volume.StatVolume();
            const auto& devices = stat->Record.GetVolume().GetDevices();
            const auto& migrations = stat->Record.GetVolume().GetMigrations();
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("transport0_migration", devices[0].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("transport1", devices[1].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL("transport2_migration", devices[2].GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        }

        // checking that our data has been in fact migrated
        CheckBlockContent<__LINE__>(
            client2,
            clientInfo2.GetClientId(),
            "",
            range1,
            GetBlockContent(1));
        CheckBlockContent<__LINE__>(
            client2,
            clientInfo2.GetClientId(),
            "",
            range2,
            GetBlockContent(2));
        CheckBlockContent<__LINE__>(
            client2,
            clientInfo2.GetClientId(),
            "",
            range3,
            GetBlockContent(3));

        client1.ReconnectPipe(); // reconnect since pipe was closed when client2 started read/write
        client1.RemoveClient(clientInfo.GetClientId());
        UNIT_ASSERT_VALUES_EQUAL("", disk.WriterClientId);
        UNIT_ASSERT_VALUES_EQUAL(1, disk.ReaderClientIds.size());
        UNIT_ASSERT_VALUES_EQUAL(clientInfo2.GetClientId(), disk.ReaderClientIds[0]);

        client2.RemoveClient(clientInfo2.GetClientId());
        UNIT_ASSERT_VALUES_EQUAL("", disk.WriterClientId);
        UNIT_ASSERT_VALUES_EQUAL(0, disk.ReaderClientIds.size());

        return hasMorePlaceToCheck;
    }

    Y_UNIT_TEST(ShouldDoCheckpointDuringMigration)
    {
        int i = 0;
        while (DoShouldCreateCheckpointDuringMigration(i)) {
            ++i;
        }
    }

    Y_UNIT_TEST(ShouldCreateCheckpointDuringResync)
    {
        NProto::TStorageServiceConfig config;
        config.SetAcquireNonReplicatedDevices(true);
        config.SetUseMirrorResync(true);
        config.SetForceMirrorResync(true);
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            config,
            state,
            {}  // featuresConfig
        );

        ui64 writeRequests = 0;
        TAutoPtr<IEventHandle> evResyncFinished;

        auto obs = [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvVolume::EvResyncFinished) {
                // making sure that resync mode doesn't get disabled
                evResyncFinished = event.Release();
                return true;
            }

            if (event->Recipient == MakeStorageStatsServiceId()
                    && event->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters)
            {
                auto* msg = event->Get<TEvStatsService::TEvVolumePartCounters>();

                writeRequests +=
                    msg->DiskCounters->RequestCounters.WriteBlocks.Count;
            }

            return false;
        };

        runtime->SetEventFilter(obs);

        TVolumeClient volume(*runtime);

        state->ReplicaCount = 2;

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            1024,
            "vol0",
            "cloud",
            "folder",
            1,  // partitionCount
            0  // blocksPerStripe
        );

        volume.WaitReady();

        auto stat = volume.StatVolume();
        const auto& devices = stat->Record.GetVolume().GetDevices();
        const auto& replicas = stat->Record.GetVolume().GetReplicas();
        UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        UNIT_ASSERT_VALUES_EQUAL("transport0", devices[0].GetTransportId());

        UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "transport1",
            replicas[0].GetDevices(0).GetTransportId());
        UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "transport2",
            replicas[1].GetDevices(0).GetTransportId());

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        {
            auto response = volume.AddClient(clientInfo);
            const auto& v = response->Record.GetVolume();
            UNIT_ASSERT_VALUES_EQUAL(1, v.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "transport0",
                v.GetDevices(0).GetTransportId()
            );

            UNIT_ASSERT_VALUES_EQUAL(2, v.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, v.GetReplicas(0).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "transport1",
                v.GetReplicas(0).GetDevices(0).GetTransportId());
            UNIT_ASSERT_VALUES_EQUAL(1, v.GetReplicas(1).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "transport2",
                v.GetReplicas(1).GetDevices(0).GetTransportId());
        }

        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            1);

        // Create checkpoint.
        volume.CreateCheckpoint("cp1");

        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(3, writeRequests);

        auto resp = volume.ReadBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId());
        const auto& bufs = resp->Record.GetBlocks().GetBuffers();
        UNIT_ASSERT_VALUES_EQUAL(1, bufs.size());
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(1), bufs[0]);

        while (!evResyncFinished) {
            runtime->DispatchEvents({}, TDuration::Seconds(1));
        }

        runtime->Send(evResyncFinished.Release());

        volume.RemoveClient(clientInfo.GetClientId());
    }

    Y_UNIT_TEST(ShouldCreateDeleteCheckpointForMultipartitionVolumes)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        {
            auto stat = volume.StatVolume();
            const auto& v = stat->Record.GetVolume();
            UNIT_ASSERT_VALUES_EQUAL(v.GetBlocksCount(), 21 * 1024);
            UNIT_ASSERT_VALUES_EQUAL(v.GetPartitionsCount(), 3);
        }

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        for (ui32 i = 0; i < 21; ++i) {
            volume.WriteBlocks(
                TBlockRange64::WithLength(1024 * i, 1024),
                clientInfo.GetClientId(),
                1
            );
        }

        volume.CreateCheckpoint("c1");

        volume.DeleteCheckpoint("c1");

        for (ui32 i = 0; i < 21; ++i) {
            volume.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024 * i, 1024),
                clientInfo.GetClientId(),
                "c1"
            );

            auto resp = volume.RecvReadBlocksResponse();
            UNIT_ASSERT(FAILED(resp->GetStatus()));
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, resp->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldHandleHttpCreateCheckpoint)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        auto httpResponse = volume.RemoteHttpInfo(
            BuildRemoteHttpQuery(
                TestTabletId,
                {{"action","createCheckpoint"}, {"checkpointid","1"}}),
            HTTP_METHOD::HTTP_METHOD_POST);

        UNIT_ASSERT(httpResponse->Html.Contains("Operation successfully completed"));
    }

    Y_UNIT_TEST(ShouldHandleHttpDeleteCheckpoint)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        auto createResponse = volume.RemoteHttpInfo(
            BuildRemoteHttpQuery(
                TestTabletId,
                {{"action","createCheckpoint"}, {"checkpointid","1"}}),
        HTTP_METHOD::HTTP_METHOD_POST);

        UNIT_ASSERT_C(createResponse->Html.Contains("Operation successfully completed"), true);

        auto deleteResponse = volume.RemoteHttpInfo(
            BuildRemoteHttpQuery(
                TestTabletId,
                {{"action","deleteCheckpoint"}, {"checkpointid","1"}}),
            HTTP_METHOD::HTTP_METHOD_POST);

        UNIT_ASSERT_C(deleteResponse->Html.Contains("Operation successfully completed"), true);
    }

    Y_UNIT_TEST(ShouldFailHttpCreateCheckpointOnTabletRestart)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        bool patchRequest = true;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvService::EvCreateCheckpointRequest) {
                    if (patchRequest) {
                        patchRequest = false;
                        auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
                        SendUndeliverableRequest(*runtime, event, std::move(request));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto httpResponse = volume.RemoteHttpInfo(
            BuildRemoteHttpQuery(
                TestTabletId,
                {{"action","createCheckpoint"}, {"checkpointid","1"}}),
            HTTP_METHOD::HTTP_METHOD_POST);

        UNIT_ASSERT_C(httpResponse->Html.Contains("Tablet is dead"), true);
    }

    Y_UNIT_TEST(ShouldFailHttpGetCreateCheckpoint)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();

        auto httpResponse = volume.RemoteHttpInfo(
            BuildRemoteHttpQuery(
                TestTabletId,
                {{"action","createCheckpoint"}, {"checkpointid","1"}}));

        UNIT_ASSERT_C(httpResponse->Html.Contains("Wrong HTTP method"), true);
    }

    Y_UNIT_TEST(ShouldCreateCheckpointsForSinglePartitionVolume)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig();

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            1
        );

        volume.CreateCheckpoint("c1");

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2
        );

        {
            auto stat = volume.StatVolume();
            const auto& cp = stat->Record.GetCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(1, cp.size());
            UNIT_ASSERT_VALUES_EQUAL("c1", stat->Record.GetCheckpoints(0));
        }

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId(),
                "c1"
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(1), bufs[j]);
            }
        }

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId()
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[j]);
            }
        }

        volume.RebootTablet();
        volume.AddClient(clientInfo);
        volume.WaitReady();

        volume.DeleteCheckpoint("c1");

        {
            auto stat = volume.StatVolume();
            const auto& cp = stat->Record.GetCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(0, cp.size());
        }

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId()
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[j]);
            }
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyDeleteCheckpointDataForSinglePartitionVolume)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig();

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            1
        );

        volume.CreateCheckpoint("c1");

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2
        );

        {
            auto stat = volume.StatVolume();
            const auto& cp = stat->Record.GetCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(1, cp.size());
            UNIT_ASSERT_VALUES_EQUAL("c1", stat->Record.GetCheckpoints(0));
        }

        volume.DeleteCheckpointData("c1");

        {
            auto stat = volume.StatVolume();
            const auto& cp = stat->Record.GetCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(0, cp.size());
        }

        volume.RebootTablet();
        volume.AddClient(clientInfo);
        volume.WaitReady();

        {
            auto stat = volume.StatVolume();
            const auto& cp = stat->Record.GetCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(0, cp.size());
        }

        {
            volume.SendReadBlocksRequest(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId(),
                "c1"
            );

            auto resp = volume.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, resp->GetStatus());
        }
        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId()
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[j]);
            }
        }

        auto deleteResponse = volume.DeleteCheckpoint("c1");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, deleteResponse->GetStatus());

        {
            auto stat = volume.StatVolume();
            const auto& cp = stat->Record.GetCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(0, cp.size());
        }

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId()
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[j]);
            }
        }
    }

    Y_UNIT_TEST(ShouldNotRejectRZRequestsDuringSinglePartionCheckpointCreation)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig();

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        auto obs = [&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvService::EvCreateCheckpointResponse) {
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        runtime->SetObserverFunc(obs);

        volume.SendCreateCheckpointRequest("c1");

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2
        );

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId()
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), bufs[j]);
            }
        }

        volume.ZeroBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId());

        {
            auto resp = volume.ReadBlocks(
                TBlockRange64::WithLength(0, 1024),
                clientInfo.GetClientId()
            );
            const auto& bufs = resp->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1024, bufs.size());
            for (ui32 j = 0; j < 1024; ++j) {
                UNIT_ASSERT_VALUES_EQUAL("", bufs[j]);
            }
        }
    }

    Y_UNIT_TEST(ShouldCollectTracesForCheckpointOperationsUponRequest)
    {
        NProto::TStorageServiceConfig config;
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            7 * 1024,   // block count per partition
            "vol0",
            "cloud",
            "folder",
            3,          // partition count
            2
        );

        volume.WaitReady();
        volume.StatVolume();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        {
            auto request = volume.CreateCreateCheckpointRequest("cp1");
            request->Record.MutableHeaders()->MutableInternal()->MutableTrace()->SetIsTraced(true);

            volume.SendToPipe(std::move(request));

            auto response = volume.RecvCreateCheckpointResponse();

            CheckForkJoin(response->Record.GetTrace().GetLWTrace().GetTrace(), true);
        }

        {
            auto request = volume.CreateDeleteCheckpointRequest("cp1");
            request->Record.MutableHeaders()->MutableInternal()->MutableTrace()->SetIsTraced(true);

            volume.SendToPipe(std::move(request));

            auto response = volume.RecvDeleteCheckpointResponse();

            CheckForkJoin(response->Record.GetTrace().GetLWTrace().GetTrace(), true);
        }
    }

    Y_UNIT_TEST(ShouldReportChangedBlocksForLightCheckpoint)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.CreateCheckpoint("c1", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "",
                "c1");
            const auto& mask = response->Record.GetMask();

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1024 / 8, mask.size());
            for (size_t i = 0; i < mask.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[i]));
            }
        }

        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            2);
        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(5, 15),
            clientInfo.GetClientId(),
            2);
        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(63, 64),
            clientInfo.GetClientId(),
            2);
        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(1022, 1023),
            clientInfo.GetClientId(),
            2);
        volume.CreateCheckpoint("c2", true);

        auto popCountStr = [](const TString& s) {
            ui64 count = 0;
            for (char c : s) {
                count += PopCount(c);
            }
            return count;
        };

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c1",
                "c2");
            const auto& mask = response->Record.GetMask();

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1024 / 8, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(16, popCountStr(mask));  // 0, 5-15, 63-64, 1022-1023
            // range [0-7]
            UNIT_ASSERT_VALUES_EQUAL(0b11100001, ui8(mask[0]));
            // range [8-15]
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[1]));
            // range [56-63]
            UNIT_ASSERT_VALUES_EQUAL(0b10000000, ui8(mask[7]));
            // range [64-72]
            UNIT_ASSERT_VALUES_EQUAL(0b00000001, ui8(mask[8]));
            // range [1016-1023]
            UNIT_ASSERT_VALUES_EQUAL(0b11000000, ui8(mask[127]));
        }

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1000),
                "c1",
                "c2");
            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(999 / 8 + 1, mask.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c1")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c2")->GetStatus());
    }

    Y_UNIT_TEST(ShouldReportChangedBlocksForLightCheckpoint2)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            2);
        volume.CreateCheckpoint("c1", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "",
                "c1");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c1",
                "c1");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        // checkpoint creation should be idempotent
        volume.CreateCheckpoint("c1", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "",
                "c1");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(1), clientInfo.GetClientId(), 2);

        volume.CreateCheckpoint("c2", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c1",
                "c2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000010, ui8(mask[0]));
        }

        // checkpoint creation should be idempotent
        volume.CreateCheckpoint("c2", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c1",
                "c2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000010, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c3", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c2",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000000, ui8(mask[0]));
        }

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 2),
            clientInfo.GetClientId(),
            2);

        // should not see new changed block until new checkpoint is created
        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c2",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000000, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c4", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c3",
                "c4");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[0]));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c1")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c2")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c3")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c4")->GetStatus());
    }

    Y_UNIT_TEST(ShouldReportChangedBlocksForLightCheckpointWhenVolumeReboots)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.CreateCheckpoint("c1", true);

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);

        volume.RebootTablet();

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "",
                "c1");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c2", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c1",
                "c2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(1), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c3", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c2",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000010, ui8(mask[0]));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c1")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c2")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c3")->GetStatus());
    }

    Y_UNIT_TEST(ShouldReportChangedBlocksForLightCheckpointWhenVolumeReboots2)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.CreateCheckpoint("c1", true);

        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            2);
        volume.CreateCheckpoint("c2", true);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(1), clientInfo.GetClientId(), 2);

        volume.RebootTablet();

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(2), clientInfo.GetClientId(), 2);

        volume.CreateCheckpoint("c3", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c2",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c4", true);

        // should not see changes earlier than checkpoint c3
        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c3",
                "c4");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000000, ui8(mask[0]));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c1")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c2")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c3")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c4")->GetStatus());
    }

    Y_UNIT_TEST(ShouldReportChangedBlocksForOldLightCheckpoint)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.CreateCheckpoint("c1", true);

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c2", true);

        volume.CreateCheckpoint("c3", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c3");

            // Changes between c1 and c2 are forgotten,
            // So the response of GetChangedBlocks should be pessimized.
            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }
        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }
        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "c1");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }
        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        for (auto checkpointId : TVector<TString>{"c1", "c2", "c3"}) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                volume.DeleteCheckpoint(checkpointId)->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldCreateLightCheckpointIdempotently)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.CreateCheckpoint("c1", true);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c2", true);

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(1), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c1", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000001, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c3", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c2",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000010, ui8(mask[0]));
        }
        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.RebootTablet();

        volume.CreateCheckpoint("c1", true);
        volume.CreateCheckpoint("c4", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c4");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(2), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c3", true);
        volume.CreateCheckpoint("c5", true);
        volume.CreateCheckpoint("c2", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c4",
                "c5");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000100, ui8(mask[0]));
        }

        for (auto checkpointId : TVector<TString>{"c1", "c2", "c3", "c4", "c5"}) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                volume.DeleteCheckpoint(checkpointId)->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldDeleteLightCheckpoint)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.CreateCheckpoint("c1", true);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c2", true);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(1), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c3", true);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c2")->GetStatus());

        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c2");
            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c2",
                "c3");

            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c3")->GetStatus());

        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c3");
            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "c1");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(2), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c4", true);
        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c3",
                "c4");

            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(3), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c5", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c4",
                "c5");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00001000, ui8(mask[0]));
        }

        for (auto checkpointId : TVector<TString>{"c1", "c4"}) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                volume.DeleteCheckpoint(checkpointId)->GetStatus());
        }

        // Checkpoint deletion should be idempotent.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c1")->GetStatus());

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "c5");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        // Checkpoint deletion should be idempotent.
        for (auto checkpointId : TVector<TString>{"c5", "c5"}) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                volume.DeleteCheckpoint(checkpointId)->GetStatus());
        }

        // Should return error because all light checkpoints were deleted.
        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "");
            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(4), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c6", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "c6");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(5), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c7", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c6",
                "c7");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00100000, ui8(mask[0]));
        }

        for (auto checkpointId : TVector<TString>{"c6, c7"}) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                volume.DeleteCheckpoint(checkpointId)->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldReportChangedBlocksForLightCheckpointWhenNormalCheckpointExist)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.CreateCheckpoint("c1", true);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c2", false);
        volume.CreateCheckpoint("c3", true);

        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c2");
            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c2",
                "c3");

            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c2",
                "c2");

            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000001, ui8(mask[0]));
        }

        for (auto checkpointId : TVector<TString>{"c1", "c2", "c3"}) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                volume.DeleteCheckpoint(checkpointId)->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldReportChangedBlocksForLightCheckpointsForEmptyHighCheckpoint)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "");
            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        volume.CreateCheckpoint("c1", true);
        volume.WriteBlocks(TBlockRange64::MakeClosedInterval(0, 1), clientInfo.GetClientId(), 1);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[0]));
        }
        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c2", true);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[0]));
        }

        volume.WriteBlocks(TBlockRange64::MakeClosedInterval(1, 2), clientInfo.GetClientId(), 1);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000111, ui8(mask[0]));
        }

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c2",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000110, ui8(mask[0]));
        }
        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c3", true);
        volume.WriteBlocks(TBlockRange64::MakeClosedInterval(0, 1), clientInfo.GetClientId(), 1);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.DeleteCheckpoint("c2");

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c3",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[0]));
        }

        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c2",
                "");
            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        for (auto checkpointId : TVector<TString>{"c1", "c3"}) {
            volume.DeleteCheckpoint(checkpointId);
        }
    }

    Y_UNIT_TEST(ShouldReadFromLightCheckpoint)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,  // maxBandwidth
            0,  // maxIops
            0,  // burstPercentage
            0,  // maxPostponedWeight
            false,  // throttlingEnabled
            1,  // version
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        const TString checkpointId = "c1";
        volume.CreateCheckpoint(checkpointId, true);

        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            GetBlockContent(2));
        CheckBlockContent<__LINE__>(
            volume,
            clientInfo.GetClientId(),
            checkpointId,
            TBlockRange64::MakeOneBlock(0),
            GetBlockContent(2));

        volume.WriteBlocks(
            TBlockRange64::MakeClosedInterval(10, 112),
            clientInfo.GetClientId(),
            GetBlockContent(42));
        CheckBlockContent<__LINE__>(
            volume,
            clientInfo.GetClientId(),
            checkpointId,
            TBlockRange64::MakeClosedInterval(10, 112),
            GetBlockContent(42));
    }

    Y_UNIT_TEST(ShouldCreateCheckpointWithShadowDisk)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        int allocateRequestCount = 0;
        int deallocateRequestCount = 0;
        auto countAllocateDeallocateDiskRequest =
            [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAllocateCheckpointRequest)
            {
                ++allocateRequestCount;
            }
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvDeallocateCheckpointRequest)
            {
                ++deallocateRequestCount;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(countAllocateDeallocateDiskRequest);

        const auto expectedBlockCount =
            DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            expectedBlockCount);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Write some data.
        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            GetBlockContent(1));

        UNIT_ASSERT_VALUES_EQUAL(0, allocateRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, deallocateRequestCount);

        // Create checkpoint.
        volume.CreateCheckpoint("c1");
        UNIT_ASSERT_VALUES_EQUAL(1, allocateRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, deallocateRequestCount);

        // Writes to the disk are not blocked.
        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            GetBlockContent(2));

        // TODO(drbasic) check checkpoint state (not ready).

        using EReason =
            TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;
        // Set Checkpoint ready.
        volume.UpdateShadowDiskState(
            "c1",
            EReason::FillAdvanced,
            10,
            expectedBlockCount);

        // TODO(drbasic) check checkpoint state (not ready).

        volume.UpdateShadowDiskState(
            "c1",
            EReason::FillCompleted,
            expectedBlockCount,
            expectedBlockCount);

        // TODO(drbasic) check checkpoint state (ready).

        // Writes to the disk are not blocked.
        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            GetBlockContent(2));

        // TODO(drbasic) read from checkpoint (success).

        // Delete checkpoint data.
        volume.DeleteCheckpointData("c1");
        UNIT_ASSERT_VALUES_EQUAL(1, allocateRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, deallocateRequestCount);

        // TODO(drbasic) read from checkpoint (fail).

        // Delete checkpoint.
        volume.DeleteCheckpoint("c1");
        UNIT_ASSERT_VALUES_EQUAL(1, allocateRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(2, deallocateRequestCount);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
