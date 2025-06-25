#include "volume_ut.h"

#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/storage/core/libs/common/media.h>

#include <bit>

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
                count += std::popcount(static_cast<unsigned char>(c));
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

        {   // Validate checkpoint state (ready).
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::READY,
                status->Record.GetCheckpointStatus());
        }

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

        {   // Validate checkpoint state (error).
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::ERROR,
                status->Record.GetCheckpointStatus());
        }

        {
            // Read from checkpoint without data failed
            volume.SendReadBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId(),
                "c1");

            auto response = volume.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                "Data for checkpoint id=\"c1\" deleted",
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
        volume.DeleteCheckpoint("c1");
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
            UNIT_ASSERT_VALUES_EQUAL(1, stat->Record.GetCheckpoints().size());
            UNIT_ASSERT_VALUES_EQUAL("c1", stat->Record.GetCheckpoints()[0]);
        }
        volume.DeleteCheckpoint("c1");
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

        // Send Create checkpoint request. It delayed until write request
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

    void DoMaybeDrainBeforeCheckpointCreation(bool useShadowDisk)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(useShadowDisk);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);

        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            DefaultDeviceBlockSize * DefaultDeviceBlockCount /
                DefaultBlockSize);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Count drain requests.
        ui32 drainRequestCount = 0;
        auto monitorDrainRequest = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() == TEvPartition::EvDrainRequest) {
                ++drainRequestCount;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(monitorDrainRequest);

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Should see drain requests only for checkpoints without shadow disk.
        UNIT_ASSERT_VALUES_EQUAL(useShadowDisk ? 0 : 1, drainRequestCount);
    }

    Y_UNIT_TEST(ShouldDrainBeforeCheckpointCreationWithoutShadowDisk)
    {
        DoMaybeDrainBeforeCheckpointCreation(false);
    }

    Y_UNIT_TEST(ShouldNotDrainBeforeCheckpointCreationWithShadowDisk)
    {
        DoMaybeDrainBeforeCheckpointCreation(true);
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

        auto oldObserverFunc = runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
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
            // Write block during migration.
            // We will steal TEvNonreplPartitionPrivate::EvWriteOrZeroCompleted
            // from TNonreplicatedPartitionMigrationCommonActor. This will delay
            // the drain execution.
            std::unique_ptr<IEventHandle> stolenResponse;
            auto stealWriteResponse = [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvNonreplPartitionPrivate::EvWriteOrZeroCompleted)
                {
                    stolenResponse.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            };
            auto oldObserverFunc = runtime->SetObserverFunc(stealWriteResponse);

            // Execute write request.
            client1.WriteBlocksLocal(
                TBlockRange64::MakeOneBlock(0),
                clientInfo.GetClientId(),
                GetBlockContent(1));
            runtime->DispatchEvents({}, TDuration::Seconds(1));

            // Got stolen TEvNonreplPartitionPrivate::EvWriteOrZeroCompleted.
            UNIT_ASSERT(stolenResponse);
            runtime->SetObserverFunc(oldObserverFunc);

            // Send Create checkpoint request. It delayed until write request
            // completed.
            volume.SendCreateCheckpointRequest("c1");
            runtime->DispatchEvents({}, TDuration::Seconds(1));
            TEST_NO_RESPONSE(runtime, CreateCheckpoint);

            // Return stolen response - write request will be completed after this
            runtime->Send(stolenResponse.release());

            // Checkpoint creation completed.
            auto createCheckpointResponse =
                volume.RecvCreateCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                createCheckpointResponse->GetStatus());
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

        runtime->SetObserverFunc(oldObserverFunc);

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

        {   // Validate checkpoint state (ready).
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::READY,
                status->Record.GetCheckpointStatus());
        }

        volume.DeleteCheckpoint("c1");

        {   // Checkpoint state not found for deleted checkpoint.
            volume.SendGetCheckpointStatusRequest("c1");
            auto response = volume.RecvGetCheckpointStatusResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }

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

        UNIT_ASSERT_C(httpResponse->Html.Contains("tablet is shutting down"), true);
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

        {   // Validate checkpoint state (ready).
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::READY,
                status->Record.GetCheckpointStatus());
        }

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

        {   // Checkpoint state not found for deleted checkpoint.
            volume.SendGetCheckpointStatusRequest("c1");
            auto response = volume.RecvGetCheckpointStatusResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }

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
            UNIT_ASSERT_VALUES_EQUAL(1, cp.size());
            UNIT_ASSERT_VALUES_EQUAL("c1", stat->Record.GetCheckpoints(0));
        }

        volume.RebootTablet();
        volume.AddClient(clientInfo);
        volume.WaitReady();

        {
            auto stat = volume.StatVolume();
            const auto& cp = stat->Record.GetCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(1, cp.size());
            UNIT_ASSERT_VALUES_EQUAL("c1", stat->Record.GetCheckpoints(0));
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

    Y_UNIT_TEST(ShouldNotReadFromCheckpointWithoutData)
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
            1);
        volume.CreateCheckpoint("c1", NProto::ECheckpointType::WITHOUT_DATA);
        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2);

        {
            // Read from checkpoint without data failed
            volume.SendReadBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId(),
                "c1");

            auto response = volume.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                "checkpoint not found: \"c1\"",
                response->GetError().GetMessage());
            UNIT_ASSERT(HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_SILENT));
        }

        volume.CreateCheckpoint("c2");
        volume.DeleteCheckpointData("c2");
        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            2);

        {
            // Read from checkpoint without data failed
            volume.SendReadBlocksRequest(
                GetBlockRangeById(0),
                clientInfo.GetClientId(),
                "c2");

            auto response = volume.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                "checkpoint not found: \"c2\"",
                response->GetError().GetMessage());
            UNIT_ASSERT(HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_SILENT));
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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);

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
        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);

        auto popCountStr = [](const TString& s) {
            ui64 count = 0;
            for (char c : s) {
                count += std::popcount(static_cast<unsigned char>(c));
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
        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);

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
        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);

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
        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c1",
                "c2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000010, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c4", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);

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
        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);

        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(0),
            clientInfo.GetClientId(),
            2);
        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(1), clientInfo.GetClientId(), 2);

        volume.RebootTablet();

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(2), clientInfo.GetClientId(), 2);

        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::WithLength(0, 1024),
                "c2",
                "c3");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c4", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);

        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(1), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c1",
                "c2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00000001, ui8(mask[0]));
        }

        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);
        volume.CreateCheckpoint("c4", NProto::ECheckpointType::LIGHT);

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
        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);
        volume.CreateCheckpoint("c5", NProto::ECheckpointType::LIGHT);
        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(1), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);

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
        volume.CreateCheckpoint("c4", NProto::ECheckpointType::LIGHT);
        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c3",
                "c4");

            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(3), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c5", NProto::ECheckpointType::LIGHT);

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
            S_ALREADY,
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

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            volume.DeleteCheckpoint("c5")->GetStatus());
        // Checkpoint deletion should be idempotent.
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            volume.DeleteCheckpoint("c5")->GetStatus());

        // Should return error because all light checkpoints were deleted.
        {
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "",
                "");
            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        volume.WriteBlocks(TBlockRange64::MakeOneBlock(4), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c6", NProto::ECheckpointType::LIGHT);

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
        volume.CreateCheckpoint("c7", NProto::ECheckpointType::LIGHT);

        {
            auto response = volume.GetChangedBlocks(
                TBlockRange64::MakeClosedInterval(0, 1023),
                "c6",
                "c7");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0b00100000, ui8(mask[0]));
        }

        for (auto checkpointId : TVector<TString>{"c6", "c7"}) {
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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);
        volume.WriteBlocks(TBlockRange64::MakeOneBlock(0), clientInfo.GetClientId(), 2);
        volume.CreateCheckpoint("c2", NProto::ECheckpointType::NORMAL);
        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c1", NProto::ECheckpointType::LIGHT);
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

        volume.CreateCheckpoint("c2", NProto::ECheckpointType::LIGHT);

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

        volume.CreateCheckpoint("c3", NProto::ECheckpointType::LIGHT);
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
        volume.CreateCheckpoint(checkpointId, NProto::ECheckpointType::LIGHT);

        {   // Validate checkpoint state (ready).
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::READY,
                status->Record.GetCheckpointStatus());
        }

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

    void DoShouldCreateCheckpointWithShadowDisk(
        NProto::EStorageMediaKind mediaKind,
        ui32 ioDepth)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetMaxShadowDiskFillIoDepth(ioDepth);
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

        const ui64 expectedBlockCount = 32768;

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

        // Write all '1' to block 1.
        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(1),
            clientInfo.GetClientId(),
            GetBlockContent(1));

        UNIT_ASSERT_VALUES_EQUAL(0, allocateRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, deallocateRequestCount);

        // Create checkpoint.
        volume.CreateCheckpoint("c1");
        UNIT_ASSERT_VALUES_EQUAL(1, allocateRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, deallocateRequestCount);

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        {   // Validate checkpoint state (not ready).
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::NOT_READY,
                status->Record.GetCheckpointStatus());
        }

        auto tryWriteBlock = [&](ui64 blockIndx, ui8 content) -> bool
        {
            auto request = volume.CreateWriteBlocksRequest(
                TBlockRange64::MakeOneBlock(blockIndx),
                clientInfo.GetClientId(),
                GetBlockContent(content));
            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            if (response->GetStatus() != S_OK) {
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
                UNIT_ASSERT_VALUES_EQUAL(
                    "Request WriteBlocks intersects with currently migrated "
                    "range",
                    response->GetError().GetMessage());
                return false;
            }
            return true;
        };

        auto tryReadBlock =
            [&](ui64 blockIndx, ui8 content, const TString& checkpoint) -> bool
        {
            auto request = volume.CreateReadBlocksRequest(
                TBlockRange64::MakeOneBlock(blockIndx),
                clientInfo.GetClientId(),
                checkpoint);
            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvReadBlocksResponse>();
            if (response->GetStatus() != S_OK) {
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
                UNIT_ASSERT_VALUES_EQUAL(
                    "Can't read from checkpoint \"c1\" while the data is being "
                    "filled in.",
                    response->GetError().GetMessage());
                return false;
            }
            const auto& bufs = response->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1, bufs.size());
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(content), bufs[0]);
            return true;
        };

        // The index of the block that is recorded during the filling of the
        // shadow disk. We use it later to check the contents of the disk and
        // the checkpoint.
        ui64 blockIndxToVerify = 0;

        // Writes to the disk are not blocked. But may overlap with migrating
        // blocks.
        for (ui64 i = 0;; ++i) {
            ui64 blockIndx = i % expectedBlockCount;
            ui8 content = i % 256;

            // We are trying to write the block to the disk. It may fail if it
            // intersects with the current migrated range.
            if (tryWriteBlock(blockIndx, content)) {
                // We save the index of the successfully written block with
                // non-zero data to check the contents of the disk and the
                // checkpoint later.
                if (content != 0 && blockIndxToVerify == 0) {
                    blockIndxToVerify = blockIndx;
                }

                // Reading from disk should always be successful.
                UNIT_ASSERT(tryReadBlock(blockIndx, content, ""));

                // Reading from the checkpoint will be successful when the
                // checkpoint preparation is completed.
                if (tryReadBlock(0, 0, "c1")) {
                    // Validate checkpoint state (ready).
                    auto status = volume.GetCheckpointStatus("c1");
                    UNIT_ASSERT_EQUAL(
                        NProto::ECheckpointStatus::READY,
                        status->Record.GetCheckpointStatus());
                    break;
                }

                // Validate checkpoint state (not ready).
                auto status = volume.GetCheckpointStatus("c1");
                UNIT_ASSERT_EQUAL(
                    NProto::ECheckpointStatus::NOT_READY,
                    status->Record.GetCheckpointStatus());
            }

            // Advance migration.
            runtime->DispatchEvents({}, TDuration::MilliSeconds(250));
        }

        // Check that the recording to the disk has happened.
        UNIT_ASSERT_UNEQUAL(0, blockIndxToVerify);

        // Read block blockIndxToVerify from disk. It should contain valid data.
        UNIT_ASSERT(
            tryReadBlock(blockIndxToVerify, blockIndxToVerify % 256, ""));

        // Write all '0' to block blockIndxToVerify.
        UNIT_ASSERT(tryWriteBlock(blockIndxToVerify, 0));

        // Read block blockIndxToVerify from the disk. It should contain all
        // '0'.
        UNIT_ASSERT(tryReadBlock(blockIndxToVerify, 0, ""));

        // Read block blockIndxToVerify from checkpoint. It should contain valid
        // data.
        UNIT_ASSERT(
            tryReadBlock(blockIndxToVerify, blockIndxToVerify % 256, "c1"));

        // Delete checkpoint data.
        volume.DeleteCheckpointData("c1");
        UNIT_ASSERT_VALUES_EQUAL(1, allocateRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, deallocateRequestCount);

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // Read from checkpoint without data should fail.
        {
             auto request = volume.CreateReadBlocksRequest(
                TBlockRange64::MakeOneBlock(0),
                clientInfo.GetClientId(),
                "c1");
            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvReadBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }

        // Delete checkpoint.
        volume.DeleteCheckpoint("c1");
        UNIT_ASSERT_VALUES_EQUAL(1, allocateRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(2, deallocateRequestCount);

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // Write OK.
        UNIT_ASSERT(tryWriteBlock(0, 4));

        // Read block 0 from the disk. It should contain all '4'.
        UNIT_ASSERT(tryReadBlock(0, 4, ""));
    }

    Y_UNIT_TEST(ShouldCreateCheckpointWithShadowDiskNonrepl)
    {
        DoShouldCreateCheckpointWithShadowDisk(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1);
        DoShouldCreateCheckpointWithShadowDisk(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            8);
    }

    Y_UNIT_TEST(ShouldCreateCheckpointWithShadowDiskMirror2)
    {
        DoShouldCreateCheckpointWithShadowDisk(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2,
            1);
        DoShouldCreateCheckpointWithShadowDisk(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2,
            8);
    }

    Y_UNIT_TEST(ShouldCreateCheckpointWithShadowDiskMirror3)
    {
        DoShouldCreateCheckpointWithShadowDisk(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            1);

        DoShouldCreateCheckpointWithShadowDisk(
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            8);
    }

    Y_UNIT_TEST(ShouldCreateCheckpointWithShadowDiskHddNonrepl)
    {
        DoShouldCreateCheckpointWithShadowDisk(
            NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
            1);
        DoShouldCreateCheckpointWithShadowDisk(
            NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
            8);
    }

    void ShouldRetryWhenAcquiringShadowDisk(ui32 responseMessage)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        bool simulateErrorResponse = true;
        auto describeDiskRequestsFilter = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() == responseMessage &&
                simulateErrorResponse)
            {   // Simulate response with error from DiskRegistry.
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistry::EvDescribeDiskResponse)
                {
                    auto* msg =
                        event->Get<TEvDiskRegistry::TEvDescribeDiskResponse>();
                    msg->Record.MutableError()->SetCode(E_REJECTED);
                }
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistry::EvAcquireDiskResponse)
                {
                    auto* msg =
                        event->Get<TEvDiskRegistry::TEvAcquireDiskResponse>();
                    msg->Record.MutableError()->SetCode(E_REJECTED);
                }
                simulateErrorResponse = false;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(describeDiskRequestsFilter);

        // Create volume.
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // Wait for shadow disk become ready.
        for (;;) {
            // Validate checkpoint state (ready).
            auto status =
                volume.GetCheckpointStatus("c1")->Record.GetCheckpointStatus();

            if (status == NProto::ECheckpointStatus::READY) {
                break;
            }

            if (status == NProto::ECheckpointStatus::ERROR) {
                UNIT_ASSERT_C(false, "Got error status for checkpoint");
            }

            // Advance shadow disk fill.
            runtime->DispatchEvents({}, TDuration::MilliSeconds(250));
        }

        // Check that interceptions of messages have occurred.
        UNIT_ASSERT(!simulateErrorResponse);
    }

    Y_UNIT_TEST(ShouldRetryDescribeShadowDisk)
    {
        ShouldRetryWhenAcquiringShadowDisk(
            TEvDiskRegistry::EvDescribeDiskResponse);
    }

    Y_UNIT_TEST(ShouldRetryAcquireShadowDisk)
    {
        ShouldRetryWhenAcquiringShadowDisk(
            TEvDiskRegistry::EvAcquireDiskResponse);
    }

    Y_UNIT_TEST(ShouldStopAcquiringAfterTimeout)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetMaxAcquireShadowDiskTotalTimeoutWhenNonBlocked(2000);

        auto runtime = PrepareTestActorRuntime(config);

        auto describeDiskRequestsFilter = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvDescribeDiskResponse)
            {   // Simulate response with error from DiskRegistry.
                auto* msg =
                    event->Get<TEvDiskRegistry::TEvDescribeDiskResponse>();
                msg->Record.MutableError()->SetCode(E_REJECTED);
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(describeDiskRequestsFilter);

        // Create volume.
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // Wait for shadow disk enter to the error state.
        for (;;) {
            auto status =
                volume.GetCheckpointStatus("c1")->Record.GetCheckpointStatus();

            if (status == NProto::ECheckpointStatus::READY) {
                UNIT_ASSERT_C(false, "Got ready status for checkpoint");
            }

            if (status == NProto::ECheckpointStatus::ERROR) {
                break;
            }

            // Advance time.
            runtime->DispatchEvents({}, TDuration::MilliSeconds(250));
        }
    }

    Y_UNIT_TEST(ShouldStopAcquiringAfterENotFound)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetMaxAcquireShadowDiskTotalTimeoutWhenNonBlocked(2000);

        auto runtime = PrepareTestActorRuntime(config);

        auto describeDiskRequestsFilter = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvDescribeDiskResponse)
            {   // Simulate response with E_NOT_FOUND error from DiskRegistry.
                auto* msg =
                    event->Get<TEvDiskRegistry::TEvDescribeDiskResponse>();
                msg->Record.MutableError()->SetCode(E_NOT_FOUND);
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(describeDiskRequestsFilter);

        // Create volume.
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // Shadow disk entered the error state.
        auto status =
            volume.GetCheckpointStatus("c1")->Record.GetCheckpointStatus();

        UNIT_ASSERT_EQUAL(NProto::ECheckpointStatus::ERROR, status);
    }

    Y_UNIT_TEST(ShouldBlockWritesWhenReAcquire)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetMaxShadowDiskFillBandwidth(1);
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount = 32768;

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

        auto tryWriteBlock = [&](ui64 blockIndx, ui8 content) -> bool
        {
            auto request = volume.CreateWriteBlocksRequest(
                TBlockRange64::MakeOneBlock(blockIndx),
                clientInfo.GetClientId(),
                GetBlockContent(content));
            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            if (response->GetStatus() != S_OK) {
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
                UNIT_ASSERT_VALUES_EQUAL(
                    "Can't write to source disk while shadow disk \"vol0-c1\" "
                    "not ready yet.",
                    response->GetError().GetMessage());
                return false;
            }
            return true;
        };

        auto tryReadBlock = [&](ui64 blockIndx) -> bool
        {
            auto request = volume.CreateReadBlocksRequest(
                TBlockRange64::MakeOneBlock(blockIndx),
                clientInfo.GetClientId());
            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvReadBlocksResponse>();
            if (response->GetStatus() != S_OK) {
                return false;
            }
            return true;
        };

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // We check that attempts to write to the disk are not blocked.
        UNIT_ASSERT(tryWriteBlock(expectedBlockCount - 1, 0));

        {   // Emulate partial disk fill
            using EReason =
                TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

            auto request = std::make_unique<
                TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
                "c1",
                EReason::FillProgressUpdate,
                128);

            volume.SendToPipe(std::move(request));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        }

        // Steal the acquire response.
        // We will return it later to complete the shadow disk acquiring.
        std::unique_ptr<IEventHandle> stolenAcquireResponse;
        auto stealAcquireResponse = [&](TTestActorRuntimeBase& runtime,
                                        TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAcquireDiskResponse)
            {
                stolenAcquireResponse.reset(event.Release());
                return true;
            }
            return false;
        };
        auto oldFilter = runtime->SetEventFilter(stealAcquireResponse);

        // Reboot volume tablet.
        volume.RebootTablet();

        // Check that the acquire response was stolen.
        UNIT_ASSERT(stolenAcquireResponse);
        runtime->SetEventFilter(oldFilter);

        // We check that attempts to write to the disk are blocked. Since the
        // shadow disk is partially filled, and the shadow disk acquiring has
        // not completed yet.
        UNIT_ASSERT(!tryWriteBlock(expectedBlockCount - 1, 0));
        UNIT_ASSERT(tryReadBlock(expectedBlockCount - 1));

        // Return stolen acquire response. The acquiring should be completed.
        runtime->Send(stolenAcquireResponse.release());
        runtime->DispatchEvents({}, TDuration::MilliSeconds(250));

        // We check that attempts to write to the disk are not blocked.
        UNIT_ASSERT(tryWriteBlock(expectedBlockCount - 1, 0));
        UNIT_ASSERT(tryReadBlock(expectedBlockCount - 1));
    }

    Y_UNIT_TEST(ShouldBlockWritesWhenReAcquireForShortTime)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetMaxShadowDiskFillBandwidth(1);
        config.SetMaxAcquireShadowDiskTotalTimeoutWhenBlocked(5000);
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount = 32768;

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

        auto tryWriteBlock = [&](ui64 blockIndx, ui8 content) -> bool
        {
            auto request = volume.CreateWriteBlocksRequest(
                TBlockRange64::MakeOneBlock(blockIndx),
                clientInfo.GetClientId(),
                GetBlockContent(content));
            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            if (response->GetStatus() != S_OK) {
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
                UNIT_ASSERT_VALUES_EQUAL(
                    "Can't write to source disk while shadow disk \"vol0-c1\" "
                    "not ready yet.",
                    response->GetError().GetMessage());
                return false;
            }
            return true;
        };

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // We check that attempts to write to the disk are not blocked.
        UNIT_ASSERT(tryWriteBlock(expectedBlockCount - 1, 0));

        {   // Emulate partial disk fill
            using EReason =
                TEvVolumePrivate::TUpdateShadowDiskStateRequest::EReason;

            auto request = std::make_unique<
                TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
                "c1",
                EReason::FillProgressUpdate,
                128);

            volume.SendToPipe(std::move(request));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(100));
        }

        // Steal the acquire response.
        std::unique_ptr<IEventHandle> stolenAcquireResponse;
        auto stealAcquireResponse = [&](TTestActorRuntimeBase& runtime,
                                        TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAcquireDiskResponse)
            {
                stolenAcquireResponse.reset(event.Release());
                return true;
            }
            return false;
        };
        auto oldFilter = runtime->SetEventFilter(stealAcquireResponse);

        // Reboot volume tablet.
        volume.RebootTablet();

        // Check that the acquire response was stolen.
        UNIT_ASSERT(stolenAcquireResponse);
        runtime->SetEventFilter(oldFilter);

        // We check that attempts to write to the disk are blocked. Since the
        // shadow disk is partially filled, and the shadow disk acquiring has
        // not completed yet.
        UNIT_ASSERT(!tryWriteBlock(expectedBlockCount - 1, 0));

        // Wait MaxAcquireShadowDiskTotalTimeoutWhenBlocked timeout.
        // After that shadow disk enter to the error state and writes to the
        // source disk will be allowed.
        runtime->DispatchEvents(
            {},
            TDuration::MilliSeconds(
                config.GetMaxAcquireShadowDiskTotalTimeoutWhenBlocked()));

        UNIT_ASSERT_EQUAL(
            NProto::ECheckpointStatus::ERROR,
            volume.GetCheckpointStatus("c1")->Record.GetCheckpointStatus());

        // We check that attempts to write to the disk are not blocked.
        UNIT_ASSERT(tryWriteBlock(expectedBlockCount - 1, 0));
    }

    Y_UNIT_TEST(ShouldNotReleaseWhenPeriodicalReAcquiringExceptForSessionError)
    {
        NProto::TStorageServiceConfig config;
        auto remountPeriod = TDuration::Seconds(2);
        config.SetClientRemountPeriod(remountPeriod.MilliSeconds());
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);

        auto runtime = PrepareTestActorRuntime(config);

        // Watch acquiring and releases
        ui32 releaseCount = 0;
        ui32 acquireCount = 0;
        bool responseWithSessionError = false;
        auto watchReleasesAndAcquiring =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAcquireDiskResponse)
            {
                auto* msg =
                    event->Get<TEvDiskRegistry::TEvAcquireDiskResponse>();
                if (responseWithSessionError) {
                    responseWithSessionError = false;
                    msg->Record.MutableError()->SetCode(E_BS_INVALID_SESSION);
                }
                ++acquireCount;
            }
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvReleaseDiskResponse)
            {
                ++releaseCount;
            }
            return false;
        };
        runtime->SetEventFilter(watchReleasesAndAcquiring);

        // Create volume.
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // We do the first acquiring together with the release.
        UNIT_ASSERT_VALUES_EQUAL(1, releaseCount);
        UNIT_ASSERT_VALUES_EQUAL(1, acquireCount);

        auto advanceTime = [&]()
        {
            runtime->AdvanceCurrentTime(remountPeriod);
            TDispatchOptions options;
            options.FinalEvents = {TDispatchOptions::TFinalEventCondition(
                TEvVolumePrivate::EvShadowDiskAcquired)};
            runtime->DispatchEvents(options, TDuration::MilliSeconds(1));
        };

        // When we do a periodic disk re-acquire, we do not need to release the
        // devices.
        advanceTime();
        UNIT_ASSERT_VALUES_EQUAL(1, releaseCount);
        UNIT_ASSERT_VALUES_EQUAL(2, acquireCount);

        // If a session error occurs, we release the session and try to acquire
        // once again.
        responseWithSessionError = true;
        advanceTime();
        UNIT_ASSERT_VALUES_EQUAL(2, releaseCount);
        UNIT_ASSERT_VALUES_EQUAL(4, acquireCount);
    }

    Y_UNIT_TEST(ShouldCreateShadowDiskWhenClientChanged)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount = 32768;

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

        auto tryWriteBlock =
            [&](ui64 blockIndx, const TString& clientId, ui8 content) -> bool
        {
            auto request = volume.CreateWriteBlocksRequest(
                TBlockRange64::MakeOneBlock(blockIndx),
                clientId,
                GetBlockContent(content));
            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            if (response->GetStatus() != S_OK) {
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
                UNIT_ASSERT_VALUES_EQUAL(
                    "Request WriteBlocks intersects with currently migrated "
                    "range",
                    response->GetError().GetMessage());
                return false;
            }
            return true;
        };

        auto tryReadBlock = [&](ui64 blockIndx,
                                ui8 content,
                                const TString& clientId,
                                const TString& checkpoint) -> bool
        {
            auto request = volume.CreateReadBlocksRequest(
                TBlockRange64::MakeOneBlock(blockIndx),
                clientId,
                checkpoint);
            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvReadBlocksResponse>();
            if (response->GetStatus() != S_OK) {
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
                UNIT_ASSERT_VALUES_EQUAL(
                    "Can't read from checkpoint \"c1\" while the data is being "
                    "filled in.",
                    response->GetError().GetMessage());
                return false;
            }
            const auto& bufs = response->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(1, bufs.size());
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(content), bufs[0]);
            return true;
        };

        // Create checkpoint when no client connected.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {   // Validate checkpoint state (not ready).
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::NOT_READY,
                status->Record.GetCheckpointStatus());
        }

        auto clientInfo1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo1);

        // Check that we can write to the disk during the preparation of the
        // shadow disk.
        UNIT_ASSERT(tryWriteBlock(
            expectedBlockCount - 1,
            clientInfo1.GetClientId(),
            '1'));
        // Check that we can read from the disk during the preparation of the
        // shadow disk.
        UNIT_ASSERT(tryReadBlock(
            expectedBlockCount - 1,
            '1',
            clientInfo1.GetClientId(),
            ""));

        // Disconnect client1 and create new one.
        volume.RemoveClient(clientInfo1.GetClientId());
        auto clientInfo2 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo2);

        // Check that we can write to the disk during the preparation of the
        // shadow disk with new client.
        UNIT_ASSERT(tryWriteBlock(
            expectedBlockCount - 1,
            clientInfo2.GetClientId(),
            '2'));
        // Check that we can read from the disk during the preparation of the
        // shadow disk with new client.
        UNIT_ASSERT(tryReadBlock(
            expectedBlockCount - 1,
            '2',
            clientInfo2.GetClientId(),
            ""));

        // Wait for checkpoint get ready.
        for (;;) {
            auto status = volume.GetCheckpointStatus("c1");
            if (status->Record.GetCheckpointStatus() ==
                NProto::ECheckpointStatus::READY)
            {
                break;
            }
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        }

        // Check that we still can write to the disk.
        UNIT_ASSERT(tryWriteBlock(
            0,
            clientInfo2.GetClientId(),
            'x'));
        // Check that we still can read from the disk.
        UNIT_ASSERT(tryReadBlock(
            expectedBlockCount - 1,
            '2',
            clientInfo2.GetClientId(),
            ""));

        // Check that we can now read from checkpoint.
        UNIT_ASSERT(tryReadBlock(0, 0, clientInfo2.GetClientId(), "c1"));
        UNIT_ASSERT(tryReadBlock(
            expectedBlockCount - 1,
            '2',
            clientInfo2.GetClientId(),
            "c1"));

        // Create new read-only client and read from checkpoint and disk.
        auto clientInfo3 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);
        volume.AddClient(clientInfo3);
        UNIT_ASSERT(tryReadBlock(0, 'x', clientInfo3.GetClientId(), ""));
        UNIT_ASSERT(tryReadBlock(
            expectedBlockCount - 1,
            '2',
            clientInfo3.GetClientId(),
            "c1"));
    }

    Y_UNIT_TEST(ShouldReacquireShadowDiskWhenClientChanged)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount = 32768;

        TString acquireClientId;
        TString releaseClientId;
        auto monitorRequests = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAcquireDiskRequest)
            {
                auto* msg = event->Get<TEvDiskRegistry::TEvAcquireDiskRequest>();
                acquireClientId = msg->Record.GetHeaders().GetClientId();
            }
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvReleaseDiskRequest)
            {
                auto* msg = event->Get<TEvDiskRegistry::TEvReleaseDiskRequest>();
                releaseClientId = msg->Record.GetHeaders().GetClientId();
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(monitorRequests);

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

        // Create checkpoint when no client connected.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // Expect that the shadow disk was acquired by "shadow-disk-client".
        UNIT_ASSERT_VALUES_EQUAL(ShadowDiskClientId, acquireClientId);
        UNIT_ASSERT_VALUES_EQUAL(AnyWriterClientId, releaseClientId);
        acquireClientId = "";
        releaseClientId = "";

        // Connect new client. Expect that the shadow disk reacquired by this
        // new client.
        auto clientInfo1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo1);
        UNIT_ASSERT_VALUES_EQUAL(clientInfo1.GetClientId(), acquireClientId);
        UNIT_ASSERT_VALUES_EQUAL(AnyWriterClientId, releaseClientId);
        acquireClientId = "";
        releaseClientId = "";

        // Disconnect client. Expect that the shadow disk reacquired by "shadow-disk-client".
        volume.RemoveClient(clientInfo1.GetClientId());
        UNIT_ASSERT_VALUES_EQUAL(ShadowDiskClientId, acquireClientId);
        UNIT_ASSERT_VALUES_EQUAL(AnyWriterClientId, releaseClientId);
        acquireClientId = "";
        releaseClientId = "";

        // Connect another client. Expect that the shadow disk reacquired by this
        // new client.
        auto clientInfo2 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo2);
        UNIT_ASSERT_VALUES_EQUAL(clientInfo2.GetClientId(), acquireClientId);
        UNIT_ASSERT_VALUES_EQUAL(AnyWriterClientId, releaseClientId);
        acquireClientId = "";
        releaseClientId = "";

        // Wait for checkpoint get ready.
        // Expect that the shadow disk reacquired by "shadow-disk-client".
        for (;;) {
            auto status = volume.GetCheckpointStatus("c1");
            if (status->Record.GetCheckpointStatus() ==
                NProto::ECheckpointStatus::READY)
            {
                break;
            }
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        }

        UNIT_ASSERT_VALUES_EQUAL(ShadowDiskClientId, acquireClientId);
        UNIT_ASSERT_VALUES_EQUAL(AnyWriterClientId, releaseClientId);
        acquireClientId = "";
        releaseClientId = "";

        // Connecting a new client should lead to reacquiring with the same
        // ShadowDiskClientId.
        auto clientInfo3 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.RemoveClient(clientInfo2.GetClientId());
        volume.AddClient(clientInfo3);
        UNIT_ASSERT_VALUES_EQUAL(ShadowDiskClientId, acquireClientId);
        UNIT_ASSERT_VALUES_EQUAL(AnyWriterClientId, releaseClientId);
    }

    Y_UNIT_TEST(ShouldReacquireShadowDiskWhenChangedToReadyWithNonBlockingTimeout)
    {
        auto blockingTimeout = TDuration::Seconds(5);
        auto nonBlockingTimeout = TDuration::Seconds(30);

        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetMaxAcquireShadowDiskTotalTimeoutWhenBlocked(
            blockingTimeout.MilliSeconds());
        config.SetMaxAcquireShadowDiskTotalTimeoutWhenNonBlocked(
            nonBlockingTimeout.MilliSeconds());
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount = 32768;

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

        // Connect new client.
        auto clientInfo1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo1);

        // Create checkpoint when no client connected.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        bool changedToReady = false;
        // Steal the acquire response when state changed to ready.
        std::unique_ptr<IEventHandle> stolenAcquireResponse;
        auto filter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvUpdateShadowDiskStateResponse)
            {
                auto* msg = event->Get<
                    TEvVolumePrivate::TEvUpdateShadowDiskStateResponse>();
                if (msg->NewState == EShadowDiskState::Ready) {
                    changedToReady = true;
                }
            }
            if (changedToReady && event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAcquireDiskResponse)
            {
                stolenAcquireResponse.reset(event.Release());
                return true;
            }
            return false;
        };
        auto oldFilter = runtime->SetEventFilter(filter);

        // Wait for checkpoint get ready and
        // Wait for AcquireResponse stolen.
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return changedToReady && stolenAcquireResponse != nullptr;
            };
            runtime->DispatchEvents(options);
        }

        // Making sure that the shadow disk does not get an error state if the
        // time is less than MaxAcquireShadowDiskTotalTimeoutWhenNonBlocked
        {
            runtime->AdvanceCurrentTime(blockingTimeout);
            runtime->DispatchEvents({}, TDuration::Seconds(1));
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::READY,
                status->Record.GetCheckpointStatus());
        }

        // When acquiring hung for time more than
        // MaxAcquireShadowDiskTotalTimeoutWhenNonBlocked shadow disk will get
        // error state.
        {
            runtime->AdvanceCurrentTime(nonBlockingTimeout);
            runtime->DispatchEvents({}, TDuration::Seconds(1));
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::ERROR,
                status->Record.GetCheckpointStatus());
        }
    }

    Y_UNIT_TEST(ShouldAcquireShadowDiskEvenIfReleaseFailed)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount = 32768;

        TString acquireClientId;
        TString releaseClientId;
        auto monitorRequests = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAcquireDiskRequest)
            {
                auto* msg = event->Get<TEvDiskRegistry::TEvAcquireDiskRequest>();
                acquireClientId = msg->Record.GetHeaders().GetClientId();
            }
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvReleaseDiskRequest)
            {
                auto* msg = event->Get<TEvDiskRegistry::TEvReleaseDiskRequest>();
                releaseClientId = msg->Record.GetHeaders().GetClientId();
                auto response =
                    std::make_unique<TEvDiskRegistry::TEvReleaseDiskResponse>(
                        MakeError(E_INVALID_STATE));
                runtime->Send(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,   // flags
                        event->Cookie),
                    0);
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(monitorRequests);

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

        // Create checkpoint when no client connected.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // Expect that the shadow disk was acquired by "shadow-disk-client".
        UNIT_ASSERT_VALUES_EQUAL(ShadowDiskClientId, acquireClientId);
        acquireClientId = "";

        // Connect new client. Expect that the shadow disk reacquired by this
        // new client.
        // The release will occur twice, by the shadow-disk-client and
        // any-writer client.
        auto clientInfo1 = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo1);
        UNIT_ASSERT_VALUES_EQUAL(clientInfo1.GetClientId(), acquireClientId);
        UNIT_ASSERT_VALUES_EQUAL(AnyWriterClientId, releaseClientId);
    }

    Y_UNIT_TEST(ShouldHandleShadowDiskAllocationError)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        auto makeErrorOnShadowDiskAllocation =
            [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAllocateCheckpointRequest)
            {
                auto response = std::make_unique<
                    TEvDiskRegistry::TEvAllocateCheckpointResponse>(MakeError(
                    E_BS_DISK_ALLOCATION_FAILED,
                    "can't allocate disk"));
                runtime->Send(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,   // flags
                        event->Cookie),
                    0);
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(makeErrorOnShadowDiskAllocation);

        const ui64 expectedBlockCount = 32768;

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

        // Write all '1' to block 1.
        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(1),
            clientInfo.GetClientId(),
            GetBlockContent(1));

        // Try create checkpoint. This will fail because the disk registry will
        // return a disk allocation error.
        volume.SendCreateCheckpointRequest("c1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        auto response = volume.RecvCreateCheckpointResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());

        // Validate checkpoint state (error).
        auto status = volume.GetCheckpointStatus("c1");
        UNIT_ASSERT_EQUAL(
            NProto::ECheckpointStatus::ERROR,
            status->Record.GetCheckpointStatus());

        // Write all '1' to block 2.
        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(2),
            clientInfo.GetClientId(),
            GetBlockContent(1));

        // Delete checkpoint.
        volume.DeleteCheckpoint("c1");

        // Write all '1' to block 3.
        volume.WriteBlocks(
            TBlockRange64::MakeOneBlock(3),
            clientInfo.GetClientId(),
            GetBlockContent(1));
    }

    Y_UNIT_TEST(ShouldUseShadowDiskDevicesWithRightOrder)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        TVector<TString> describedDevices;
        TVector<TString> allocatedDevices;
        auto getDeviceUUIDs = [](const TDevices& devices) -> auto
        {
            TVector<TString> result;
            for (const auto& deviceInfo: devices) {
                result.push_back(deviceInfo.GetDeviceUUID());
            }
            return result;
        };
        auto patchAcquireResponse = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvDescribeDiskResponse)
            {
                auto* msg =
                    event->Get<TEvDiskRegistry::TEvDescribeDiskResponse>();
                describedDevices = getDeviceUUIDs(msg->Record.GetDevices());
            }
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvAcquireDiskResponse)
            {
                auto* msg =
                    event->Get<TEvDiskRegistry::TEvAcquireDiskResponse>();
                auto* devices = msg->Record.MutableDevices();
                for (size_t i = 0; i < describedDevices.size(); ++i) {
                    devices->at(i).SetDeviceUUID(
                        describedDevices[describedDevices.size() - i - 1]);
                }
            }
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvShadowDiskAcquired)
            {
                auto* msg =
                    event->Get<TEvVolumePrivate::TEvShadowDiskAcquired>();
                allocatedDevices = getDeviceUUIDs(msg->Devices);
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(patchAcquireResponse);

        // Create volume.
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768 * 2);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        UNIT_ASSERT_EQUAL(2, describedDevices.size());
        UNIT_ASSERT_EQUAL(2, allocatedDevices.size());
        UNIT_ASSERT_EQUAL(describedDevices, allocatedDevices);
    }

    Y_UNIT_TEST(ShouldReceivePoisonTakenMessage)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        // Create volume.
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768 * 2);

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        auto checkVolumeActorReceivesPoisonTakenMessage = [&]()
        {
            TActorId volumeActorId = ResolveTablet(*runtime, TestTabletId);
            Cout << "volumeActorId = " << volumeActorId << Endl;

            bool poisonTaken = false;
            auto trackPoisonTaken = [&](TTestActorRuntimeBase& runtime,
                                        TAutoPtr<IEventHandle>& event) -> bool
            {
                Y_UNUSED(runtime);

                if (event->GetTypeRewrite() == TEvents::TSystem::PoisonTaken &&
                    event->Recipient == volumeActorId)
                {
                    poisonTaken = true;
                }
                return false;
            };
            auto oldFilter = runtime->SetEventFilter(trackPoisonTaken);

            // Reboot volume tablet.
            volume.RebootTablet();

            // Check volume got PoisonTaken message.
            UNIT_ASSERT(poisonTaken);
            runtime->SetEventFilter(oldFilter);
        };

        // Check that the volume actor receives the TEvPoisonTaken message when
        // the checkpoint does not exist.
        checkVolumeActorReceivesPoisonTakenMessage();

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Check that the volume actor receives the TEvPoisonTaken message when
        // the checkpoint exist.
        checkVolumeActorReceivesPoisonTakenMessage();

        // Check that the volume actor receives the TEvPoisonTaken message when
        // TAcquireShadowDiskActor is waiting for shadow disk acquiring.
        {
            std::unique_ptr<IEventHandle> stolenAcquireResponse;
            auto stealAcquireResponse =
                [&](TTestActorRuntimeBase& runtime,
                    TAutoPtr<IEventHandle>& event) -> bool
            {
                Y_UNUSED(runtime);

                if (event->GetTypeRewrite() ==
                    TEvDiskRegistry::EvAcquireDiskResponse)
                {
                    stolenAcquireResponse.reset(event.Release());
                    return true;
                }
                return false;
            };
            auto oldFilter = runtime->SetEventFilter(stealAcquireResponse);

            // Reboot volume tablet.
            volume.RebootTablet();

            UNIT_ASSERT(stolenAcquireResponse);
            runtime->SetEventFilter(oldFilter);
        }
        checkVolumeActorReceivesPoisonTakenMessage();
    }

    void DoTestShouldNotCreateCheckpointIfItWasDeleted(
        NProto::EStorageMediaKind mediaKind)
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
            mediaKind,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        std::vector<std::pair<NProto::ECheckpointType, TString>> testCases{
            {NProto::ECheckpointType::NORMAL, "checkpoint_normal"},
            {NProto::ECheckpointType::LIGHT, "checkpoint_light"},
            {NProto::ECheckpointType::WITHOUT_DATA, "checkpoint_without_data"}};

        for (auto [checkpointType, checkpointId] : testCases)
        {
            volume.CreateCheckpoint(checkpointId, checkpointType);
            volume.DeleteCheckpoint(checkpointId);

            volume.SendCreateCheckpointRequest(checkpointId, checkpointType);
            UNIT_ASSERT_VALUES_EQUAL(
                E_PRECONDITION_FAILED,
                volume.RecvCreateCheckpointResponse()->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldNotCreateCheckpointIfItWasDeletedForBlobStorageBased)
    {
        DoTestShouldNotCreateCheckpointIfItWasDeleted(
            NProto::STORAGE_MEDIA_SSD);
    }

    Y_UNIT_TEST(ShouldNotCreateCheckpointIfItWasDeletedForDiskRegistryBased)
    {
        DoTestShouldNotCreateCheckpointIfItWasDeleted(
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldNotCreateCheckpointIfCheckpointDataWasDeleted)
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
            NProto::STORAGE_MEDIA_SSD,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        TString checkpointId = "checkpoint";

        volume.CreateCheckpoint(checkpointId);
        volume.DeleteCheckpointData(checkpointId);

        volume.SendCreateCheckpointRequest(checkpointId);
        UNIT_ASSERT_VALUES_EQUAL(
            E_PRECONDITION_FAILED,
            volume.RecvCreateCheckpointResponse()->GetStatus());
    }

    Y_UNIT_TEST(ShouldDeleteCheckpointEvenIfItNeverExisted)
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
            NProto::STORAGE_MEDIA_SSD,
            1024,   // block count per partition
            "vol0",  // diskId
            "cloud",  // cloudId
            "folder",  // folderId
            1,  // partition count
            2  // blocksPerStripe
        );
        volume.WaitReady();

        volume.SendDeleteCheckpointRequest("checkpoint_never_existed");
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            volume.RecvDeleteCheckpointResponse()->GetStatus());
    }

    Y_UNIT_TEST(ShouldHandleGetChangedBlocksForShadowDiskBasedCheckpoint)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetOptimizeVoidBuffersTransferForReadsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount =
            DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;

        const ui64 blocksInProcessingRange =
            MigrationRangeSize / DefaultBlockSize;

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

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Write to blocks on the border of the migration range.
        const auto writtenRange =
            TBlockRange64::WithLength(blocksInProcessingRange - 1, 2);
        volume.WriteBlocks(
            writtenRange,
            clientInfo.GetClientId(),
            GetBlockContent(1));

        // Create checkpoint when no client connected.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            // When checkpoint not ready yet we should get E_REJECTED
            auto status = volume.GetCheckpointStatus("c1");
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointStatus::NOT_READY,
                status->Record.GetCheckpointStatus());
            volume.SendGetChangedBlocksRequest(
                TBlockRange64::WithLength(0, 1024),
                "",
                "c1");
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                volume.RecvGetChangedBlocksResponse()->GetStatus());
        }

        // Wait for checkpoint get ready.
        for (;;) {
            auto status = volume.GetCheckpointStatus("c1");
            if (status->Record.GetCheckpointStatus() ==
                NProto::ECheckpointStatus::READY)
            {
                break;
            }
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        }

        // Check GetChangedBlocks responses
        auto checkBlocks = [&](TBlockRange64 range, bool expectation)
        {
            auto response = volume.GetChangedBlocks(range, "", "c1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(range.Size() / 8, mask.size());
            for (auto m: mask) {
                UNIT_ASSERT_VALUES_EQUAL(
                    expectation ? 255 : 0,
                    static_cast<ui8>(m));
            }
        };

        ui32 expectChangedCount = 0;
        for (size_t i = 0; i < expectedBlockCount; i += blocksInProcessingRange)
        {
            auto processedRange =
                TBlockRange64::WithLength(i, blocksInProcessingRange);
            const bool expectChanged = processedRange.Overlaps(writtenRange);
            if (expectChanged) {
                ++expectChangedCount;
            }
            checkBlocks(processedRange, expectChanged);
        }
        UNIT_ASSERT_VALUES_EQUAL(2, expectChangedCount);
    }

    Y_UNIT_TEST(ShouldHandleGetChangedBlocksForNrdCheckpoint)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(false);
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount =
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

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Create checkpoint c1
        volume.CreateCheckpoint("c1");
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // Check GetChangedBlocks responses
        volume.SendGetChangedBlocksRequest(
            TBlockRange64::WithLength(0, 1024),
            "",
            "c1");
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_IMPLEMENTED,
            volume.RecvGetChangedBlocksResponse()->GetStatus());
    }

    Y_UNIT_TEST(ShouldRegisterBackgroundBandwidth)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetOptimizeVoidBuffersTransferForReadsEnabled(true);
        config.SetMaxShadowDiskFillBandwidth(300);
        auto runtime = PrepareTestActorRuntime(config);

        const ui64 expectedBlockCount =
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

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        ui32 registerSourceCounter = 0;
        auto countRegisterTrafficSourceRequests =
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);

            if (event->GetTypeRewrite() ==
                TEvStatsServicePrivate::EvRegisterTrafficSourceRequest)
            {
                auto* msg = event->Get<
                    TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest>();
                ++registerSourceCounter;
                UNIT_ASSERT_VALUES_EQUAL("vol0-c1", msg->SourceId);
                UNIT_ASSERT_VALUES_EQUAL(300, msg->BandwidthMiBs);
            }
            return false;
        };
        runtime->SetEventFilter(countRegisterTrafficSourceRequests);

        // Create checkpoint when no client connected.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();

        // Expect that the registration of the background bandwidth source has
        // occurred, since the filling of the shadow disk has begun.
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, registerSourceCounter);

        // The background bandwidth source should be re-registered at intervals
        // of once per second.
        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(2, registerSourceCounter);
    }

    Y_UNIT_TEST(ShouldDestroyShadowActorBeforeShadowDiskDeallocation)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        volume.CreateCheckpoint("c1");

        bool seenDrainDoneEvent = false;
        bool seenDeallocateCheckpointEvent = false;
        auto filter = [&](TTestActorRuntimeBase& runtime,
                          TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvExternalDrainDone)
            {
                seenDrainDoneEvent = true;
                UNIT_ASSERT_C(
                    !seenDeallocateCheckpointEvent,
                    "Should catch deallocation event after drain");
            }
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvDeallocateCheckpointRequest)
            {
                seenDeallocateCheckpointEvent = true;
                UNIT_ASSERT_C(
                    seenDrainDoneEvent,
                    "Should catch deallocation event after drain");
            }

            return false;
        };
        runtime->SetEventFilter(filter);

        volume.DeleteCheckpointData("c1");

        UNIT_ASSERT(seenDrainDoneEvent);
        UNIT_ASSERT(seenDeallocateCheckpointEvent);
    }

    Y_UNIT_TEST(ShouldDeallocateShadowDiskEvenShadowActorDrainHang)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        volume.CreateCheckpoint("c1");

        bool seenDrainDoneEvent = false;
        bool seenDeallocateCheckpointEvent = false;
        auto filter = [&](TTestActorRuntimeBase& runtime,
                          TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvExternalDrainDone)
            {
                seenDrainDoneEvent = true;
                UNIT_ASSERT_C(
                    !seenDeallocateCheckpointEvent,
                    "Should catch deallocation event after drain");
                // Drop EvExternalDrainDone event.
                return true;
            }
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvDeallocateCheckpointRequest)
            {
                seenDeallocateCheckpointEvent = true;
                UNIT_ASSERT_C(
                    seenDrainDoneEvent,
                    "Should catch deallocation event after drain");
            }

            return false;
        };
        runtime->SetEventFilter(filter);

        volume.SendDeleteCheckpointDataRequest("c1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(seenDrainDoneEvent);
        UNIT_ASSERT(!seenDeallocateCheckpointEvent);

        runtime->AdvanceCurrentTime(TDuration::Seconds(30));
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        auto response = volume.RecvDeleteCheckpointDataResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT(seenDeallocateCheckpointEvent);

    }

    Y_UNIT_TEST(ShouldShadowDiskReleaseDevicesAfterDeletingCheckpoint)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        volume.CreateCheckpoint("c1");

        constexpr auto ReleaseForAnyReader = 1;
        constexpr auto ReleaseForAnyWriter = 2;

        bool seenReleaseShadowDiskForAnyReader = false;
        bool seenReleaseShadowDiskForAnyWriter = false;

        auto observerReleaseShadowDiskRequest =
            [&](TEvDiskRegistry::TEvReleaseDiskRequest::TPtr& event)
        {
            switch (event->Cookie) {
                case ReleaseForAnyReader:
                    seenReleaseShadowDiskForAnyReader = true;
                    break;
                case ReleaseForAnyWriter:
                    seenReleaseShadowDiskForAnyWriter = true;
                    break;
            }
        };
        auto _ = runtime->AddObserver<TEvDiskRegistry::TEvReleaseDiskRequest>(
            observerReleaseShadowDiskRequest);

        volume.DeleteCheckpoint("c1");

        // check that we saw release requests for any-reader and any-writer
        UNIT_ASSERT(seenReleaseShadowDiskForAnyReader);
        UNIT_ASSERT(seenReleaseShadowDiskForAnyWriter);
    }

    Y_UNIT_TEST(ShouldShadowDiskReleaseDevicesAfterDeletingTwoCheckpoints)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        // create two checkpoints
        volume.CreateCheckpoint("c1");
        volume.CreateCheckpoint("c2");

        constexpr auto ReleaseForAnyReader = 1;
        constexpr auto ReleaseForAnyWriter = 2;

        bool seenReleaseShadowDiskForAnyReader = false;
        bool seenReleaseShadowDiskForAnyWriter = false;

        auto observerReleaseShadowDiskRequest =
            [&](TEvDiskRegistry::TEvReleaseDiskRequest::TPtr& event)
        {
            switch (event->Cookie) {
                case ReleaseForAnyReader:
                    seenReleaseShadowDiskForAnyReader = true;
                    break;
                case ReleaseForAnyWriter:
                    seenReleaseShadowDiskForAnyWriter = true;
                    break;
            }
        };
        auto _ = runtime->AddObserver<TEvDiskRegistry::TEvReleaseDiskRequest>(
            observerReleaseShadowDiskRequest);

        // delete first checkpoint
        volume.DeleteCheckpoint("c1");

        // check that we saw release requests for any-reader and any-writer for
        // first checkpoint
        UNIT_ASSERT(seenReleaseShadowDiskForAnyReader);
        UNIT_ASSERT(seenReleaseShadowDiskForAnyWriter);

        seenReleaseShadowDiskForAnyReader = false;
        seenReleaseShadowDiskForAnyWriter = false;

        // delete second checkpoint
        volume.DeleteCheckpoint("c2");

        // check that we saw release requests for any-reader and any-writer for
        // second checkpoint
        UNIT_ASSERT(seenReleaseShadowDiskForAnyReader);
        UNIT_ASSERT(seenReleaseShadowDiskForAnyWriter);
    }

    Y_UNIT_TEST(ShouldShadowDiskReleaseRequestRetryAfterRetriableError)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        constexpr auto ReleaseForAnyReader = 1;
        constexpr auto ReleaseForAnyWriter = 2;

        bool seenReleaseShadowDiskForAnyReader = false;
        bool seenReleaseShadowDiskForAnyWriter = false;

        bool seenReleaseShadowDiskRetryForAnyReader = false;
        bool seenReleaseShadowDiskRetryForAnyWriter = false;

        bool returnErrorForAnyReader = false;
        bool returnErrorForAnyWriter = false;

        auto observerReleaseShadowDiskRequest =
            [&](TEvDiskRegistry::TEvReleaseDiskRequest::TPtr& event)
        {
            switch (event->Cookie) {
                case ReleaseForAnyReader:
                    if (!returnErrorForAnyReader) {
                        seenReleaseShadowDiskForAnyReader = true;
                    } else {
                        seenReleaseShadowDiskRetryForAnyReader = true;
                    }
                    break;
                case ReleaseForAnyWriter:
                    if (!returnErrorForAnyWriter) {
                        seenReleaseShadowDiskForAnyWriter = true;
                    } else {
                        seenReleaseShadowDiskRetryForAnyWriter = true;
                    }
                    break;
            }
        };
        auto handleObserverReleaseRequest =
            runtime->AddObserver<TEvDiskRegistry::TEvReleaseDiskRequest>(
                observerReleaseShadowDiskRequest);

        auto releaseDiskResponseObserver =
            [&](TEvDiskRegistry::TEvReleaseDiskResponse::TPtr& event)
        {
            // Simulate response with error.
            if (event->Cookie == ReleaseForAnyReader &&
                !returnErrorForAnyReader)
            {
                returnErrorForAnyReader = true;
                event->Get()->Record.MutableError()->SetCode(E_REJECTED);
            }

            if (event->Cookie == ReleaseForAnyWriter &&
                !returnErrorForAnyWriter)
            {
                returnErrorForAnyWriter = true;
                event->Get()->Record.MutableError()->SetCode(E_REJECTED);
            }
        };
        auto handleReleaseDiskResponseObserver =
            runtime->AddObserver<TEvDiskRegistry::TEvReleaseDiskResponse>(
                releaseDiskResponseObserver);

        volume.WaitReady();
        volume.CreateCheckpoint("c1");

        volume.DeleteCheckpoint("c1");

        runtime->AdvanceCurrentTime(TDuration::Seconds(5));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // check that we simulated error
        UNIT_ASSERT(returnErrorForAnyReader);
        UNIT_ASSERT(returnErrorForAnyWriter);

        // check that we saw original request for any-reader and any-writer
        UNIT_ASSERT(seenReleaseShadowDiskForAnyReader);
        UNIT_ASSERT(seenReleaseShadowDiskForAnyWriter);

        // check that we saw retry request for any-reader and any-writer
        UNIT_ASSERT(seenReleaseShadowDiskRetryForAnyReader);
        UNIT_ASSERT(seenReleaseShadowDiskRetryForAnyWriter);
    }

    Y_UNIT_TEST(
        ShouldShadowDiskReleaseDevicesAfterDeletingCheckpointWhenExternalDrainTimeout)
    {
        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            32768);

        volume.WaitReady();

        volume.CreateCheckpoint("c1");

        bool seenDrainDoneEvent = false;
        auto externalDrainDoneFilter =
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvVolumePrivate::EvExternalDrainDone)
            {
                seenDrainDoneEvent = true;
                // Drop EvExternalDrainDone event.
                return true;
            }

            return false;
        };
        runtime->SetEventFilter(externalDrainDoneFilter);

        constexpr auto ReleaseForAnyReader = 1;
        constexpr auto ReleaseForAnyWriter = 2;

        bool seenReleaseShadowDiskForAnyReader = false;
        bool seenReleaseShadowDiskForAnyWriter = false;

        auto observerReleaseShadowDiskRequest =
            [&](TEvDiskRegistry::TEvReleaseDiskRequest::TPtr& event)
        {
            UNIT_ASSERT(seenDrainDoneEvent);
            switch (event->Cookie) {
                case ReleaseForAnyReader:
                    seenReleaseShadowDiskForAnyReader = true;
                    break;
                case ReleaseForAnyWriter:
                    seenReleaseShadowDiskForAnyWriter = true;
                    break;
            }
        };
        auto _ = runtime->AddObserver<TEvDiskRegistry::TEvReleaseDiskRequest>(
            observerReleaseShadowDiskRequest);

        volume.SendDeleteCheckpointDataRequest("c1");
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT(seenDrainDoneEvent);

        runtime->AdvanceCurrentTime(TDuration::Seconds(30));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        auto response = volume.RecvDeleteCheckpointDataResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

        // check that we saw release requests for any-reader and any-writer
        UNIT_ASSERT(seenReleaseShadowDiskForAnyReader);
        UNIT_ASSERT(seenReleaseShadowDiskForAnyWriter);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
