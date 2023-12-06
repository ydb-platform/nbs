#include "actor_describe_base_disk_blocks.h"

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/testlib/test_runtime.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId EdgeActor;

NActors::TActorId MakeVolumeProxyServiceId()
{
    return EdgeActor;
}

using namespace NBlobMarkers;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadBlocksFromBaseDiskTests)
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

        void SetUp(NUnitTest::TTestContext&) override
        {
            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();
        }
    };

    Y_UNIT_TEST_F(ShouldReadFromOverlayDiskSuccess, TSetupEnvironment)
    {
        const ui32 blockSize = 512;

        TBlockMarks blockMarks{
            TEmptyMark{},
            TFreshMark{},
            TEmptyMark{},
            TEmptyMark{},
            TFreshMark{}};

        auto readActor = ActorSystem.Register(
            new TDescribeBaseDiskBlocksActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                "BaseDiskId",
                "BaseDiskCheckpointId",
                TBlockRange64::WithLength(0, 5),
                TBlockRange64::WithLength(0, 4),
                std::move(blockMarks),
                blockSize));

        auto describeRequest = ActorSystem.GrabEdgeEvent<
            TEvVolume::TEvDescribeBlocksRequest>();

        auto& describeRecord = describeRequest->Record;
        UNIT_ASSERT_EQUAL(describeRecord.GetDiskId(), TString("BaseDiskId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetStartIndex(), 0);
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCount(), 4);
        UNIT_ASSERT_EQUAL(describeRecord.GetCheckpointId(), TString("BaseDiskCheckpointId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCountToRead(), 3);
        UNIT_ASSERT_EQUAL(describeRecord.GetFlags(), 0);

        NProto::TFreshBlockRange freshData;
        freshData.SetStartIndex(0);
        freshData.SetBlocksCount(1);
        freshData.MutableBlocksContent()->append(TString(512, 1));

        NKikimrProto::TLogoBlobID protoLogoBlobID;
        protoLogoBlobID.SetRawX1(142);
        protoLogoBlobID.SetRawX2(143);
        protoLogoBlobID.SetRawX3(0x8000); //blob size 2028

        NKikimr::TLogoBlobID logoBlobID(
            protoLogoBlobID.GetRawX1(),
            protoLogoBlobID.GetRawX2(),
            protoLogoBlobID.GetRawX3());

        NProto::TRangeInBlob RangeInBlob;
        RangeInBlob.SetBlobOffset(0);
        RangeInBlob.SetBlockIndex(2);
        RangeInBlob.SetBlocksCount(2);

        NProto::TBlobPiece TBlobPiece;
        TBlobPiece.MutableBlobId()->CopyFrom(protoLogoBlobID);
        TBlobPiece.SetBSGroupId(42);
        TBlobPiece.MutableRanges()->Add(std::move(RangeInBlob));

        auto describeResponse = new TEvVolume::TEvDescribeBlocksResponse;
        describeResponse->Record.MutableFreshBlockRanges()->Add(
            std::move(freshData));
        describeResponse->Record.MutableBlobPieces()->Add(
            std::move(TBlobPiece));

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            describeResponse));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted>();
        UNIT_ASSERT(!HasError(fullResponse->GetError()));

        auto newBlockMarks = std::move(fullResponse->BlockMarks);
        UNIT_ASSERT_EQUAL(newBlockMarks.size(), 5);
        UNIT_ASSERT(std::holds_alternative<TFreshMarkOnBaseDisk>(
            newBlockMarks[0]));
        {
            auto& value = std::get<TFreshMarkOnBaseDisk>(newBlockMarks[0]);
            UNIT_ASSERT_EQUAL(value.BlockIndex, 0);
            UNIT_ASSERT_EQUAL(value.RefToData.Size(), blockSize);
            UNIT_ASSERT_EQUAL(memcmp(
                value.RefToData.Data(),
                TString(blockSize, 1).data(),
                blockSize), 0);
        }
        UNIT_ASSERT(std::holds_alternative<TFreshMark>(
            newBlockMarks[1]));
        UNIT_ASSERT(std::holds_alternative<TBlobMarkOnBaseDisk>(
            newBlockMarks[2]));
        {
            auto& value = std::get<TBlobMarkOnBaseDisk>(newBlockMarks[2]);
            UNIT_ASSERT_EQUAL(value.BlobId, logoBlobID);
            UNIT_ASSERT_EQUAL(value.BlobOffset, 0);
            UNIT_ASSERT_EQUAL(value.BlockIndex, 2);
            UNIT_ASSERT_EQUAL(value.BSGroupId, 42);
        }
        UNIT_ASSERT(std::holds_alternative<TBlobMarkOnBaseDisk>(
            newBlockMarks[3]));
        {
            auto& value = std::get<TBlobMarkOnBaseDisk>(newBlockMarks[3]);
            UNIT_ASSERT_EQUAL(value.BlobId, logoBlobID);
            UNIT_ASSERT_EQUAL(value.BlobOffset, 1);
            UNIT_ASSERT_EQUAL(value.BlockIndex, 3);
            UNIT_ASSERT_EQUAL(value.BSGroupId, 42);
        }
        UNIT_ASSERT(std::holds_alternative<TFreshMark>(
            newBlockMarks[4]));
    }

    Y_UNIT_TEST_F(ShouldReadFromOverlayDiskFile, TSetupEnvironment)
    {
        const ui32 blockSize = 512;

        TBlockMarks blockMarks{
            TEmptyMark{},
            TFreshMark{},
            TEmptyMark{},
            TEmptyMark{},
            TFreshMark{}};

        auto readActor = ActorSystem.Register(
            new TDescribeBaseDiskBlocksActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                "BaseDiskId",
                "BaseDiskCheckpointId",
                TBlockRange64::WithLength(0, 5),
                TBlockRange64::WithLength(0, 4),
                std::move(blockMarks),
                blockSize));

        auto describeRequest = ActorSystem.GrabEdgeEvent<
            TEvVolume::TEvDescribeBlocksRequest>();

        auto& describeRecord = describeRequest->Record;
        UNIT_ASSERT_EQUAL(describeRecord.GetDiskId(), TString("BaseDiskId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetStartIndex(), 0);
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCount(), 4);
        UNIT_ASSERT_EQUAL(describeRecord.GetCheckpointId(), TString("BaseDiskCheckpointId"));
        UNIT_ASSERT_EQUAL(describeRecord.GetBlocksCountToRead(), 3);
        UNIT_ASSERT_EQUAL(describeRecord.GetFlags(), 0);

        auto describeResponse = new TEvVolume::TEvDescribeBlocksResponse(
            MakeError(E_NOT_FOUND));

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            describeResponse));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted>();
        UNIT_ASSERT(HasError(fullResponse->GetError()));
    }

    Y_UNIT_TEST_F(ShouldNotify, TSetupEnvironment)
    {
        const ui32 blockSize = 512;

        TBlockMarks blockMarks{
            TEmptyMark{},
            TFreshMark{},
            TEmptyMark{},
            TEmptyMark{},
            TFreshMark{}};

        auto readActor = ActorSystem.Register(
            new TDescribeBaseDiskBlocksActor(
                MakeIntrusive<TRequestInfo>(),
                "BaseDiskId",
                "BaseDiskCheckpointId",
                TBlockRange64::WithLength(0, 5),
                TBlockRange64::WithLength(0, 4),
                std::move(blockMarks),
                blockSize,
                EdgeActor));

        auto describeRequest = ActorSystem.GrabEdgeEvent<
            TEvVolume::TEvDescribeBlocksRequest>();

        auto describeResponse = new TEvVolume::TEvDescribeBlocksResponse(
            MakeError(E_NOT_FOUND));

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            describeResponse));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted>();

        UNIT_ASSERT(HasError(fullResponse->GetError()));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
