#include "actor_describe_base_disk_blocks.h"
#include <cloud/blockstore/libs/storage/partition/model/merged_blob_compression.h>
#include <cloud/blockstore/libs/storage/partition/model/merged_blob_compression_policy.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

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

    Y_UNIT_TEST_F(ShouldPreserveCompressionAndNegotiateLegacyProviders, TSetupEnvironment)
    {
        const ui32 blockSize = 4096;
        NProto::TBlobMeta meta;
        meta.MutableMergedBlocks()->SetEnd(16);
        NPartition::TCompressedMergedBlob encoded;
        UNIT_ASSERT(!HasError(NPartition::CompressMergedBlob(
            TString(17 * blockSize, 'x'), blockSize, 10, meta, encoded)));
        UNIT_ASSERT(!encoded.Payload.empty());
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        NPartition::RegisterMergedBlobCompressionCounters(counters);
        auto rejections = counters->GetCounter("CompatibilityRejections", true);
        for (ui32 scenario = 0; scenario < 7; ++scenario) {
            const auto before = rejections->Val();
            auto actor = ActorSystem.Register(new TDescribeBaseDiskBlocksActor(
                MakeIntrusive<TRequestInfo>(EdgeActor, 0ull, MakeIntrusive<TCallContext>()),
                "base", "checkpoint", TBlockRange64::WithLength(0, 4),
                TBlockRange64::WithLength(0, 4), TBlockMarks(4, TEmptyMark{}),
                blockSize));
            auto request = ActorSystem.GrabEdgeEvent<TEvVolume::TEvDescribeBlocksRequest>();
            UNIT_ASSERT_VALUES_EQUAL(request->Record.GetSupportedBlobFormatVersion(), 1);
            const auto makeResponse = [&](bool raw) {
                auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>();
                auto* piece = response->Record.AddBlobPieces();
                const NKikimr::TLogoBlobID blobId(
                    1, 1, 1, 3, raw ? 17 * blockSize : encoded.Payload.size(), 0);
                LogoBlobIDFromLogoBlobID(blobId, piece->MutableBlobId());
                piece->SetBSGroupId(42);
                piece->SetLogicalBlocks(17);
                if (!raw) {
                    *piece->MutableCompression() = encoded.Compression;
                }
                auto* range = piece->AddRanges();
                range->SetBlobOffset(0); range->SetBlockIndex(0); range->SetBlocksCount(2);
                range = piece->AddRanges();
                range->SetBlobOffset(15); range->SetBlockIndex(2); range->SetBlocksCount(2);
                return response;
            };
            auto response = makeResponse(false);
            if (scenario != 1 && scenario != 2 && scenario != 6) {
                response->Record.SetBlobFormatVersion(1);
            }
            if (scenario == 3) {
                response->Record.MutableBlobPieces(0)->MutableCompression()->Clear();
            } else if (scenario == 4) {
                response->Record.MutableBlobPieces(0)->MutableCompression()->SetVersion(99);
            } else if (scenario == 5) {
                response->Record.MutableBlobPieces(0)->ClearCompression();
            }
            ActorSystem.Send(new NActors::IEventHandle(actor, EdgeActor, response.release()));
            if (scenario == 1 || scenario == 2 || scenario == 6) {
                request = ActorSystem.GrabEdgeEvent<TEvVolume::TEvDescribeBlocksRequest>();
                UNIT_ASSERT_VALUES_EQUAL(request->Record.GetSupportedBlobFormatVersion(), 0);
                response = scenario == 1 ? makeResponse(true) :
                    scenario == 6 ? makeResponse(false) :
                    std::make_unique<TEvVolume::TEvDescribeBlocksResponse>(
                        MakeError(E_NOT_IMPLEMENTED, "raw-only consumer"));
                ActorSystem.Send(new NActors::IEventHandle(actor, EdgeActor, response.release()));
            }
            auto result = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted>();
            UNIT_ASSERT_VALUES_EQUAL(
                rejections->Val(), before + (scenario == 2 || scenario == 6 ? 1 : 0));
            if (scenario >= 2) {
                UNIT_ASSERT(HasError(result->GetError()));
                for (const auto& mark: result->BlockMarks) {
                    UNIT_ASSERT(std::holds_alternative<TEmptyMark>(mark));
                }
            } else {
                UNIT_ASSERT_C(!HasError(result->GetError()), FormatError(result->GetError()));
                const auto& first = std::get<TBlobMarkOnBaseDisk>(result->BlockMarks[0]);
                const auto& last = std::get<TBlobMarkOnBaseDisk>(result->BlockMarks[3]);
                UNIT_ASSERT_VALUES_EQUAL(first.BlobOffset, 0);
                UNIT_ASSERT_VALUES_EQUAL(last.BlobOffset, 16);
                UNIT_ASSERT_VALUES_EQUAL(first.Format.LogicalBlocks, 17);
                UNIT_ASSERT_VALUES_EQUAL(bool(first.Format.Compression), scenario == 0);
                if (scenario == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(first.Format.Compression->SerializeAsString(),
                        encoded.Compression.SerializeAsString());
                }
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectMalformedBlobPiecesWithoutChangingMarks, TSetupEnvironment)
    {
        enum class EFailure
        {
            LogicalBlocks,
            LogicalSize,
            MissingMetadata,
            EmptyRange,
            PastLogicalEnd,
            OffsetOverflow,
        };
        for (EFailure failure:
             {EFailure::LogicalBlocks,
              EFailure::LogicalSize,
              EFailure::MissingMetadata,
              EFailure::EmptyRange,
              EFailure::PastLogicalEnd, EFailure::OffsetOverflow})
        {
            // A legacy 32 MiB blob with 512-byte blocks reaches the ui16
            // offset limit without exceeding the physical BlobId size limit.
            const ui32 blockSize =
                failure == EFailure::OffsetOverflow ? 512 : 4096;
            NProto::TBlobMeta meta;
            meta.MutableMergedBlocks()->SetEnd(16);
            NPartition::TCompressedMergedBlob encoded;
            UNIT_ASSERT(!HasError(NPartition::CompressMergedBlob(
                TString(17 * blockSize, 'x'), blockSize, 10, meta, encoded)));
            UNIT_ASSERT(!encoded.Payload.empty());
            auto actor = ActorSystem.Register(new TDescribeBaseDiskBlocksActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor, 0ull, MakeIntrusive<TCallContext>()),
                "base",
                "checkpoint",
                TBlockRange64::WithLength(0, 4),
                TBlockRange64::WithLength(0, 4),
                TBlockMarks{
                    TEmptyMark{},
                    TFreshMark{},
                    TEmptyMark{},
                    TZeroMark{}}, blockSize));
            auto request =
                ActorSystem
                    .GrabEdgeEvent<TEvVolume::TEvDescribeBlocksRequest>();
            UNIT_ASSERT_VALUES_EQUAL(
                request->Record.GetSupportedBlobFormatVersion(), 1);
            auto response =
                std::make_unique<TEvVolume::TEvDescribeBlocksResponse>();
            response->Record.SetBlobFormatVersion(1);
            const NKikimr::TLogoBlobID blobId(
                1, 1, 1, 3, encoded.Payload.size(), 0);
            auto* first = response->Record.AddBlobPieces();
            LogoBlobIDFromLogoBlobID(blobId, first->MutableBlobId());
            first->SetBSGroupId(42);
            first->SetLogicalBlocks(17);
            *first->MutableCompression() = encoded.Compression;
            auto* range = first->AddRanges();
            range->SetBlobOffset(0);
            range->SetBlockIndex(0);
            range->SetBlocksCount(1);

            // Validate the whole response before publishing even the valid
            // first piece; a malformed later piece must preserve all marks.
            auto* piece = response->Record.AddBlobPieces();
            *piece = *first;
            range = piece->MutableRanges(0);
            range->SetBlockIndex(2);
            switch (failure) {
                case EFailure::LogicalBlocks:
                    piece->SetLogicalBlocks(16);
                    break;
                case EFailure::LogicalSize:
                    piece->MutableCompression()->SetLogicalSize(
                        17 * blockSize - 1);
                    break;
                case EFailure::MissingMetadata:
                    piece->ClearCompression();
                    break;
                case EFailure::EmptyRange:
                    range->SetBlocksCount(0);
                    break;
                case EFailure::PastLogicalEnd:
                    range->SetBlobOffset(17);
                    break;
                case EFailure::OffsetOverflow: {
                    piece->ClearCompression();
                    piece->SetLogicalBlocks(65536);
                    const NKikimr::TLogoBlobID legacy(
                        1, 1, 1, 3, 65536 * blockSize, 1);
                    LogoBlobIDFromLogoBlobID(legacy, piece->MutableBlobId());
                    range->SetBlobOffset(65535);
                    break;
                }
            }
            ActorSystem.Send(new NActors::IEventHandle(
                actor, EdgeActor, response.release()));
            auto result = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted>();
            UNIT_ASSERT_VALUES_EQUAL(result->GetError().GetCode(), E_IO);
            UNIT_ASSERT(!result->GetError().GetMessage().empty());
            UNIT_ASSERT_VALUES_EQUAL(result->BlockMarks.size(), 4);
            UNIT_ASSERT(
                std::holds_alternative<TEmptyMark>(result->BlockMarks[0]));
            UNIT_ASSERT(
                std::holds_alternative<TFreshMark>(result->BlockMarks[1]));
            UNIT_ASSERT(
                std::holds_alternative<TEmptyMark>(result->BlockMarks[2]));
            UNIT_ASSERT(
                std::holds_alternative<TZeroMark>(result->BlockMarks[3]));
        }
    }

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
        describeResponse->Record.SetBlobFormatVersion(1);
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
