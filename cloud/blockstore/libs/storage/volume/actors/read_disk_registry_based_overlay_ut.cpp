#include "read_disk_registry_based_overlay.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using namespace NBlobMarkers;

Y_UNIT_TEST_SUITE(TNonreplReadTests)
{
    struct TActorSystem
        : NActors::TTestActorRuntimeBase
    {
        void Start()
        {
            SetDispatchTimeout(TDuration::Seconds(5));
            InitNodes();
            AppendToLogSettings(
                TBlockStoreComponents::START,
                TBlockStoreComponents::END,
                GetComponentName);
        }
    };

    struct TSetupEnvironment
        : public TCurrentTestCase
    {
        TActorSystem ActorSystem;
        NActors::TActorId EdgeActor;

        void SetUp(NUnitTest::TTestContext&) override
        {
            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();

            ActorSystem.RegisterService(
                MakeVolumeProxyServiceId(), EdgeActor, 0);
        }
    };

    Y_UNIT_TEST_F(ShouldReadFromOverlayDiskSuccess, TSetupEnvironment)
    {
        TCompressedBitmap usedBlocks(10);
        usedBlocks.Set(0, 10);

        auto request = NProto::TReadBlocksRequest();
        request.SetStartIndex(0);
        request.SetBlocksCount(10);

        const ui32 blockSize = 512;
        auto readActor =
            ActorSystem.Register(new TReadDiskRegistryBasedOverlayActor<
                                 TEvService::TReadBlocksMethod>{
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                request,
                usedBlocks,
                NActors::TActorId(),
                EdgeActor,
                0,
                "BaseDiskId",
                "BaseDiskCheckpointId",
                blockSize,
                EStorageAccessMode::Default,
                TDuration(),
                TLogTitle(
                    GetCycleCount(),
                    TLogTitle::TVolume{.TabletId = 0, .DiskId = "test"})
                    .GetChild(GetCycleCount()),
                /*enableChecksumValidation=*/false});

        auto requestToPartition = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksLocalMethod::TRequest>();
        auto const& recordToPartition = requestToPartition.Get()->Record;
        UNIT_ASSERT_EQUAL(recordToPartition.GetStartIndex(), 0);
        UNIT_ASSERT_EQUAL(recordToPartition.GetBlocksCount(), 10);

        {
            auto guardToPartition = recordToPartition.Sglist.Acquire();
            auto sgListToPartition = guardToPartition.Get();

            for (size_t i = 0; i < 10; ++i) {
                UNIT_ASSERT_EQUAL(sgListToPartition[i].Size(), blockSize);
                memset(const_cast<char*>(
                    sgListToPartition[i].Data()),
                    i,
                    blockSize);
            }
        }

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            new TEvService::TReadBlocksLocalMethod::TResponse()));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksMethod::TResponse>();

        UNIT_ASSERT(!HasError(fullResponse->GetError()));

        const auto& fullBuffers = fullResponse->Record.GetBlocks().GetBuffers();
        for (int i = 0; i < fullBuffers.size(); ++i) {
            UNIT_ASSERT_EQUAL(fullBuffers[i].size(), blockSize);
            UNIT_ASSERT_EQUAL(memcmp(
                fullBuffers[i].data(),
                TString(blockSize, i).data(),
                blockSize), 0);
        }
    }

    Y_UNIT_TEST_F(ShouldReadFromOverlayDiskFail, TSetupEnvironment)
    {
        TCompressedBitmap usedBlocks(10);
        usedBlocks.Set(0, 10);

        auto request = NProto::TReadBlocksRequest();
        request.SetStartIndex(0);
        request.SetBlocksCount(10);

        const ui32 blockSize = 512;
        auto readActor =
            ActorSystem.Register(new TReadDiskRegistryBasedOverlayActor<
                                 TEvService::TReadBlocksMethod>{
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                request,
                usedBlocks,
                NActors::TActorId(),
                EdgeActor,
                0,
                "BaseDiskId",
                "BaseDiskCheckpointId",
                blockSize,
                EStorageAccessMode::Default,
                TDuration(),
                TLogTitle(
                    GetCycleCount(),
                    TLogTitle::TVolume{.TabletId = 0, .DiskId = "test"})
                    .GetChild(GetCycleCount()),
                /*enableChecksumValidation=*/false});

        ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksLocalMethod::TRequest>();

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            new TEvService::TReadBlocksLocalMethod::TResponse(
                MakeError(E_FAIL)
            )));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksMethod::TResponse>();

        UNIT_ASSERT(HasError(fullResponse->GetError()));
    }

    Y_UNIT_TEST_F(ShouldReadFromBaseDiskSuccess, TSetupEnvironment)
    {
        auto RealProxyActorId = NKikimr::MakeBlobStorageProxyID(2181038123);
        ActorSystem.RegisterService(RealProxyActorId, EdgeActor, 0);

        TCompressedBitmap usedBlocks(10);

        const ui32 blockSize = 512;
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            10,
            TString::TUninitialized(blockSize));

        auto request = NProto::TReadBlocksLocalRequest();
        request.SetStartIndex(0);
        request.SetBlocksCount(10);
        request.BlockSize = blockSize;
        request.Sglist = TGuardedSgList(std::move(sglist));

        ActorSystem.Register(new TReadDiskRegistryBasedOverlayActor<
                             TEvService::TReadBlocksLocalMethod>{
            MakeIntrusive<TRequestInfo>(
                EdgeActor,
                0ull,
                MakeIntrusive<TCallContext>()),
            request,
            usedBlocks,
            NActors::TActorId(),
            EdgeActor,
            0,
            "BaseDiskId",
            "BaseDiskCheckpointId",
            blockSize,
            EStorageAccessMode::Default,
            TDuration(),
            TLogTitle(
                GetCycleCount(),
                TLogTitle::TVolume{.TabletId = 0, .DiskId = "test"})
                .GetChild(GetCycleCount()),
            /*enableChecksumValidation=*/false});

        TAutoPtr<NActors::IEventHandle> handle;
        auto describeRequest = ActorSystem.GrabEdgeEventIf<
            TEvVolume::TEvDescribeBlocksRequest>(
                handle,
                [](const auto&) { return true; });

        auto& describeRecord = describeRequest->Record;
        UNIT_ASSERT_EQUAL(describeRecord.GetDiskId(), TString("BaseDiskId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetStartIndex(), 0);
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCount(), 10);
        UNIT_ASSERT_EQUAL(describeRecord.GetCheckpointId(), TString("BaseDiskCheckpointId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCountToRead(), 10);
        UNIT_ASSERT_EQUAL(describeRecord.GetFlags(), 0);

        NProto::TFreshBlockRange freshData1;
        freshData1.SetStartIndex(0);
        freshData1.SetBlocksCount(2);
        freshData1.MutableBlocksContent()->append(
            TString(512, 0).append(TString(512, 1)));

        NProto::TFreshBlockRange freshData2;
        freshData2.SetStartIndex(8);
        freshData2.SetBlocksCount(2);
        freshData2.MutableBlocksContent()->append(
            TString(512, 8).append(TString(512, 9)));

        NKikimrProto::TLogoBlobID protoLogoBlobID1;
        protoLogoBlobID1.SetRawX1(142);
        protoLogoBlobID1.SetRawX2(143);
        protoLogoBlobID1.SetRawX3(0x8000); //blob size 2028

        NKikimr::TLogoBlobID logoBlobID1(
            protoLogoBlobID1.GetRawX1(),
            protoLogoBlobID1.GetRawX2(),
            protoLogoBlobID1.GetRawX3());

        NProto::TRangeInBlob RangeInBlob1;
        RangeInBlob1.SetBlobOffset(1);
        RangeInBlob1.SetBlockIndex(2);
        RangeInBlob1.SetBlocksCount(3);

        NProto::TBlobPiece TBlobPiece1;
        TBlobPiece1.MutableBlobId()->CopyFrom(protoLogoBlobID1);
        TBlobPiece1.SetBSGroupId(2181038123);
        TBlobPiece1.MutableRanges()->Add(std::move(RangeInBlob1));

        NKikimrProto::TLogoBlobID protoLogoBlobID2;
        protoLogoBlobID2.SetRawX1(42);
        protoLogoBlobID2.SetRawX2(43);
        protoLogoBlobID2.SetRawX3(0x8000); //blob size 2028

        NKikimr::TLogoBlobID logoBlobID2(
            protoLogoBlobID2.GetRawX1(),
            protoLogoBlobID2.GetRawX2(),
            protoLogoBlobID2.GetRawX3());

        NProto::TRangeInBlob RangeInBlob2_1;
        RangeInBlob2_1.SetBlobOffset(2);
        RangeInBlob2_1.SetBlockIndex(5);
        RangeInBlob2_1.SetBlocksCount(1);

        NProto::TRangeInBlob RangeInBlob2_2;
        RangeInBlob2_2.SetBlobOffset(0);
        RangeInBlob2_2.SetBlockIndex(6);
        RangeInBlob2_2.SetBlocksCount(2);

        NProto::TBlobPiece TBlobPiece2;
        TBlobPiece2.MutableBlobId()->CopyFrom(protoLogoBlobID2);
        TBlobPiece2.SetBSGroupId(2181038123);
        TBlobPiece2.MutableRanges()->Add(std::move(RangeInBlob2_1));
        TBlobPiece2.MutableRanges()->Add(std::move(RangeInBlob2_2));

        auto describeResponse = new TEvVolume::TEvDescribeBlocksResponse;
        describeResponse->Record.MutableFreshBlockRanges()->Add(
            std::move(freshData1));
        describeResponse->Record.MutableFreshBlockRanges()->Add(
            std::move(freshData2));
        describeResponse->Record.MutableBlobPieces()->Add(
            std::move(TBlobPiece1));
        describeResponse->Record.MutableBlobPieces()->Add(
            std::move(TBlobPiece2));

        ActorSystem.Send(new NActors::IEventHandle(
            handle->Sender,
            EdgeActor,
            describeResponse));

        auto fillResponse = [&] (auto readBlob) {
            UNIT_ASSERT_EQUAL(readBlob->QuerySize, 1);
            UNIT_ASSERT_EQUAL(readBlob->Queries[0].Size, blockSize * 3);

            auto getResult = new NKikimr::TEvBlobStorage::TEvGetResult(
                NKikimrProto::EReplyStatus::OK,
                1,
                2181038123);
            getResult->Responses[0].Status = NKikimrProto::OK;
            getResult->Responses[0].Shift = 0;
            getResult->Responses[0].RequestedSize = blockSize * 3;
            getResult->Responses[0].Id = readBlob->Queries[0].Id;

            if (readBlob->Queries[0].Id == logoBlobID2) {
                UNIT_ASSERT_EQUAL(readBlob->Queries[0].Shift, 0);

                getResult->Responses[0].Buffer.Insert(
                    getResult->Responses[0].Buffer.End(),
                    TRope(TString(blockSize, 6)
                        .append(TString(blockSize, 7))
                        .append(TString(blockSize, 5))));
            } else if (readBlob->Queries[0].Id == logoBlobID1) {
                UNIT_ASSERT_EQUAL(readBlob->Queries[0].Shift, blockSize);

                getResult->Responses[0].Buffer.Insert(
                getResult->Responses[0].Buffer.End(),
                TRope(TString(blockSize, 2)
                    .append(TString(blockSize, 3))
                    .append(TString(blockSize, 4))));
            }

            ActorSystem.Send(new NActors::IEventHandle(
                handle->Sender,
                EdgeActor,
                getResult));
        };

        auto readBlob1 = ActorSystem.GrabEdgeEventIf<
            NKikimr::TEvBlobStorage::TEvGet>(
                handle,
                [](const auto&) { return true; });
        fillResponse(readBlob1);

        auto readBlob2 = ActorSystem.GrabEdgeEventIf<
            NKikimr::TEvBlobStorage::TEvGet>(
                handle,
                [](const auto&) { return true; });
        fillResponse(readBlob2);

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksLocalMethod::TResponse>();
        UNIT_ASSERT(!HasError(fullResponse->GetError()));

        auto guard = request.Sglist.Acquire();
        auto& sgList = guard.Get();
        for (size_t i = 0; i < sgList.size(); ++i) {
            UNIT_ASSERT_EQUAL(sgList[i].Size(), blockSize);
            UNIT_ASSERT_EQUAL(memcmp(
                sgList[i].Data(),
                TString(blockSize, i).data(),
                blockSize), 0);
        }
    }

    Y_UNIT_TEST_F(ShouldDescribeFromBaseDiskFail, TSetupEnvironment)
    {
        TCompressedBitmap usedBlocks(10);

        const ui32 blockSize = 512;
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            10,
            TString::TUninitialized(blockSize));

        auto request = NProto::TReadBlocksLocalRequest();
        request.SetStartIndex(0);
        request.SetBlocksCount(10);
        request.BlockSize = blockSize;
        request.Sglist = TGuardedSgList(std::move(sglist));

        ActorSystem.Register(new TReadDiskRegistryBasedOverlayActor<
                             TEvService::TReadBlocksLocalMethod>{
            MakeIntrusive<TRequestInfo>(
                EdgeActor,
                0ull,
                MakeIntrusive<TCallContext>()),
            request,
            usedBlocks,
            NActors::TActorId(),
            EdgeActor,
            0,
            "BaseDiskId",
            "BaseDiskCheckpointId",
            blockSize,
            EStorageAccessMode::Default,
            TDuration(),
            TLogTitle(
                GetCycleCount(),
                TLogTitle::TVolume{.TabletId = 0, .DiskId = "test"})
                .GetChild(GetCycleCount()),
            /*enableChecksumValidation=*/false});

        TAutoPtr<NActors::IEventHandle> handle;
        ActorSystem.GrabEdgeEventIf<
            TEvVolume::TEvDescribeBlocksRequest>(
                handle,
                [](const auto&) { return true; });

        ActorSystem.Send(new NActors::IEventHandle(
            handle->Sender,
            EdgeActor,
            new TEvVolume::TEvDescribeBlocksResponse(
                MakeError(E_FAIL))));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksLocalMethod::TResponse>();
        UNIT_ASSERT(HasError(fullResponse->GetError()));
    }

    Y_UNIT_TEST_F(ShouldGerRequestFail, TSetupEnvironment)
    {
        auto RealProxyActorId = NKikimr::MakeBlobStorageProxyID(2181038123);
        ActorSystem.RegisterService(RealProxyActorId, EdgeActor, 0);

        TCompressedBitmap usedBlocks(1);

        const ui32 blockSize = 512;
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            1,
            TString::TUninitialized(blockSize));

        auto request = NProto::TReadBlocksLocalRequest();
        request.SetStartIndex(0);
        request.SetBlocksCount(1);
        request.BlockSize = blockSize;
        request.Sglist = TGuardedSgList(std::move(sglist));

        ActorSystem.Register(new TReadDiskRegistryBasedOverlayActor<
                             TEvService::TReadBlocksLocalMethod>{
            MakeIntrusive<TRequestInfo>(
                EdgeActor,
                0ull,
                MakeIntrusive<TCallContext>()),
            request,
            usedBlocks,
            NActors::TActorId(),
            EdgeActor,
            0,
            "BaseDiskId",
            "BaseDiskCheckpointId",
            blockSize,
            EStorageAccessMode::Default,
            TDuration(),
            TLogTitle(
                GetCycleCount(),
                TLogTitle::TVolume{.TabletId = 0, .DiskId = "test"})
                .GetChild(GetCycleCount()),
            /*enableChecksumValidation=*/false});

        TAutoPtr<NActors::IEventHandle> handle;
        auto describeRequest = ActorSystem.GrabEdgeEventIf<
            TEvVolume::TEvDescribeBlocksRequest>(
                handle,
                [](const auto&) { return true; });

        auto& describeRecord = describeRequest->Record;
        UNIT_ASSERT_EQUAL(describeRecord.GetDiskId(), TString("BaseDiskId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetStartIndex(), 0);
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCount(), 1);
        UNIT_ASSERT_EQUAL(describeRecord.GetCheckpointId(), TString("BaseDiskCheckpointId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCountToRead(), 1);
        UNIT_ASSERT_EQUAL(describeRecord.GetFlags(), 0);

        NKikimrProto::TLogoBlobID protoLogoBlobID1;
        protoLogoBlobID1.SetRawX1(142);
        protoLogoBlobID1.SetRawX2(143);
        protoLogoBlobID1.SetRawX3(0x8000); //blob size 2028

        NKikimr::TLogoBlobID logoBlobID1(
            protoLogoBlobID1.GetRawX1(),
            protoLogoBlobID1.GetRawX2(),
            protoLogoBlobID1.GetRawX3());

        NProto::TRangeInBlob RangeInBlob1;
        RangeInBlob1.SetBlobOffset(0);
        RangeInBlob1.SetBlockIndex(0);
        RangeInBlob1.SetBlocksCount(1);

        NProto::TBlobPiece TBlobPiece1;
        TBlobPiece1.MutableBlobId()->CopyFrom(protoLogoBlobID1);
        TBlobPiece1.SetBSGroupId(2181038123);
        TBlobPiece1.MutableRanges()->Add(std::move(RangeInBlob1));

        auto describeResponse = new TEvVolume::TEvDescribeBlocksResponse;
        describeResponse->Record.MutableBlobPieces()->Add(
            std::move(TBlobPiece1));

        ActorSystem.Send(new NActors::IEventHandle(
            handle->Sender,
            EdgeActor,
            describeResponse));

        ActorSystem.GrabEdgeEventIf<
            NKikimr::TEvBlobStorage::TEvGet>(
                handle,
                [](const auto&) { return true; });

        auto getResult1 = new NKikimr::TEvBlobStorage::TEvGetResult(
            NKikimrProto::EReplyStatus::ERROR,
            1,
            2181038123);

        ActorSystem.Send(new NActors::IEventHandle(
            handle->Sender,
            EdgeActor,
            getResult1));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksLocalMethod::TResponse>();
        UNIT_ASSERT(HasError(fullResponse->GetError()));
    }

    Y_UNIT_TEST_F(ShouldReadFromBaseAndOverlayDiskSuccess, TSetupEnvironment)
    {
        TCompressedBitmap usedBlocks(4);
        usedBlocks.Set(0, 1);
        usedBlocks.Set(2, 3);

        const ui32 blockSize = 512;
        auto request = NProto::TReadBlocksRequest();
        request.SetStartIndex(0);
        request.SetBlocksCount(4);

        auto readActor =
            ActorSystem.Register(new TReadDiskRegistryBasedOverlayActor<
                                 TEvService::TReadBlocksMethod>{
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                request,
                usedBlocks,
                NActors::TActorId(),
                EdgeActor,
                0,
                "BaseDiskId",
                "BaseDiskCheckpointId",
                blockSize,
                EStorageAccessMode::Default,
                TDuration(),
                TLogTitle(
                    GetCycleCount(),
                    TLogTitle::TVolume{.TabletId = 0, .DiskId = "test"})
                    .GetChild(GetCycleCount()),
                /*enableChecksumValidation=*/false});

        // read from overlay disk
        auto requestToPartition = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksLocalMethod::TRequest>();
        auto const& recordToPartition = requestToPartition.Get()->Record;
        UNIT_ASSERT_EQUAL(recordToPartition.GetStartIndex(), 0);
        UNIT_ASSERT_EQUAL(recordToPartition.GetBlocksCount(), 3);

        {
            auto guardToPartition = recordToPartition.Sglist.Acquire();
            auto sgListToPartition = guardToPartition.Get();

            memset(const_cast<char*>(sgListToPartition[0].Data()), 0, blockSize);
            UNIT_ASSERT_EQUAL(sgListToPartition[1].Size(), blockSize);
            UNIT_ASSERT_EQUAL(sgListToPartition[1].Data(), 0);
            memset(const_cast<char*>(sgListToPartition[2].Data()), 2, blockSize);
        }

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            new TEvService::TReadBlocksLocalMethod::TResponse()));

        TAutoPtr<NActors::IEventHandle> handle;
        auto describeRequest = ActorSystem.GrabEdgeEventIf<
            TEvVolume::TEvDescribeBlocksRequest>(
                handle,
                [](const auto&) { return true; });

        auto& describeRecord = describeRequest->Record;
        UNIT_ASSERT_EQUAL(describeRecord.GetDiskId(), TString("BaseDiskId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetStartIndex(), 1);
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCount(), 3);
        UNIT_ASSERT_EQUAL(describeRecord.GetCheckpointId(), TString("BaseDiskCheckpointId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCountToRead(), 2);
        UNIT_ASSERT_EQUAL(describeRecord.GetFlags(), 0);

        NProto::TFreshBlockRange freshData1;
        freshData1.SetStartIndex(1);
        freshData1.SetBlocksCount(3);
        freshData1.MutableBlocksContent()->append(TString(512, 1));
        freshData1.MutableBlocksContent()->append(TString(512, 32));
        freshData1.MutableBlocksContent()->append(TString(512, 3));

        auto describeResponse = new TEvVolume::TEvDescribeBlocksResponse;
        describeResponse->Record.MutableFreshBlockRanges()->Add(
            std::move(freshData1));

        ActorSystem.Send(new NActors::IEventHandle(
            handle->Sender,
            EdgeActor,
            describeResponse));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksMethod::TResponse>();

        UNIT_ASSERT(!HasError(fullResponse->GetError()));

        const auto& fullBuffers = fullResponse->Record.GetBlocks().GetBuffers();
        for (int i = 0; i < fullBuffers.size(); ++i) {
            UNIT_ASSERT_EQUAL(fullBuffers[i].size(), blockSize);
            UNIT_ASSERT_EQUAL(memcmp(
                fullBuffers[i].data(),
                TString(blockSize, i).data(),
                blockSize), 0);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
