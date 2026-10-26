#include "actor_read_blob.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/api/public.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/cast.h>
#include <util/system/unaligned_mem.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadBlobTests)
{
    struct TActorSystem: NActors::TTestActorRuntimeBase
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

    struct TSetupEnvironment: public TCurrentTestCase
    {
        TActorSystem ActorSystem;
        NActors::TActorId EdgeActor;

        void SetUp(NUnitTest::TTestContext&) override
        {
            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();
        }
    };

    struct TCompressedReadEnvironment: TSetupEnvironment
    {
        static constexpr ui64 TabletId = 142;
        static constexpr ui32 GroupId = 12;
        static constexpr ui32 BlockSize = 512;
        static constexpr ui32 ChunkSize = 4 * BlockSize;

        TVector<ui16> BlobOffsets = {4, 5, 5, 8, 16};
        TBlobCompressor::TCompressedBlob Blob;
        NKikimr::TLogoBlobID BlobId;
        TVector<TString> Blocks;
        TGuardedSgList GuardedSglist;
        NActors::TActorId ReadActor;
        THolder<NKikimr::TEvBlobStorage::TEvGet> ReadRequest;

        void StartCompressedRead(
            NProto::TBlobCompression::ECodec codec,
            bool shouldCalculateChecksums,
            EStorageAccessMode storageAccessMode = EStorageAccessMode::Default)
        {
            TString payload;
            for (ui32 i = 0; i < 17; ++i) {
                payload.append(TString(BlockSize, 'a' + i));
            }
            const TBlobCompressor writer(TabletId, BlockSize, ChunkSize, codec);
            Blob = writer.CompressBlob(payload);
            BlobId = NKikimr::TLogoBlobID(
                TabletId, 1, 2, 3, SafeIntegerCast<ui32>(Blob.Data.size()), 4);
            GuardedSglist = TGuardedSgList(ResizeBlocks(
                Blocks, BlobOffsets.size(), TString(BlockSize, '?')));

            auto request =
                std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                    BlobId,
                    EdgeActor,
                    BlobOffsets,
                    GuardedSglist,
                    GroupId,
                    false,             // async
                    TInstant::Max(),   // deadline
                    shouldCalculateChecksums,
                    Blob.Compression);

            // Reads must use the blob's metadata, including its chunk size
            // and codec, rather than the reader's write settings.
            auto reader = std::make_shared<TBlobCompressor>(
                TabletId,
                BlockSize, 2 * BlockSize, NProto::TBlobCompression::LZ4);
            ReadActor = ActorSystem.Register(new TReadBlobActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor, 0ull, MakeIntrusive<TCallContext>()),
                EdgeActor,
                NActors::TActorId(),
                TabletId,
                BlockSize,
                shouldCalculateChecksums,
                storageAccessMode,
                std::move(request),
                TDuration(), 0ull, false, std::move(reader)));
            ReadRequest =
                ActorSystem.GrabEdgeEvent<NKikimr::TEvBlobStorage::TEvGet>();
        }

        std::unique_ptr<NKikimr::TEvBlobStorage::TEvGetResult> MakeReadResult()
        {
            auto result =
                std::make_unique<NKikimr::TEvBlobStorage::TEvGetResult>(
                    NKikimrProto::OK, ReadRequest->QuerySize, GroupId);
            for (size_t i = 0; i < ReadRequest->QuerySize; ++i) {
                const auto& query = ReadRequest->Queries[i];
                auto& response = result->Responses[i];
                response.Status = NKikimrProto::OK;
                response.Id = BlobId;
                response.Shift = query.Shift;
                response.RequestedSize = query.Size;
                response.Buffer = TRope(TString(
                    TStringBuf(Blob.Data).SubStr(query.Shift, query.Size)));
            }
            return result;
        }

        THolder<TEvPartitionCommonPrivate::TEvReadBlobResponse> FinishRead(
            std::unique_ptr<NKikimr::TEvBlobStorage::TEvGetResult> result)
        {
            ActorSystem.Send(new NActors::IEventHandle(
                ReadActor, EdgeActor, result.release()));
            auto response = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvReadBlobResponse>();
            auto completed = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
            UNIT_ASSERT_VALUES_EQUAL(
                response->GetError().GetCode(),
                completed->GetError().GetCode());
            UNIT_ASSERT(completed->BlobId == BlobId);
            UNIT_ASSERT_VALUES_EQUAL(
                BlobOffsets.size() * BlockSize, completed->BytesCount);
            return response;
        }
    };

    Y_UNIT_TEST_F(ShouldReadCompressedBlob, TCompressedReadEnvironment)
    {
        for (const auto codec:
             {NProto::TBlobCompression::LZ4, NProto::TBlobCompression::ZSTD})
        {
            for (const bool checksums: {false, true}) {
                StartCompressedRead(codec, checksums);

                // Chunks 1 and 2 are adjacent, while chunk 4 is a partial
                // final chunk. Chunks 0 and 3 must not be read.
                UNIT_ASSERT_VALUES_EQUAL(2, ReadRequest->QuerySize);
                UNIT_ASSERT(ReadRequest->Queries[0].Id == BlobId);
                UNIT_ASSERT(ReadRequest->Queries[1].Id == BlobId);
                UNIT_ASSERT_VALUES_EQUAL(
                    Blob.Compression.GetChunkSizes(0),
                    ReadRequest->Queries[0].Shift);
                UNIT_ASSERT_VALUES_EQUAL(
                    Blob.Compression.GetChunkSizes(1) +
                        Blob.Compression.GetChunkSizes(2),
                    ReadRequest->Queries[0].Size);
                ui32 lastChunkOffset = 0;
                for (size_t i = 0; i < 4; ++i) {
                    lastChunkOffset += Blob.Compression.GetChunkSizes(i);
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    lastChunkOffset, ReadRequest->Queries[1].Shift);
                UNIT_ASSERT_VALUES_EQUAL(
                    Blob.Compression.GetChunkSizes(4),
                    ReadRequest->Queries[1].Size);

                const auto response = FinishRead(MakeReadResult());
                UNIT_ASSERT_C(
                    !HasError(response->GetError()),
                    FormatError(response->GetError()));
                UNIT_ASSERT_VALUES_EQUAL(
                    checksums ? BlobOffsets.size() : 0,
                    response->BlockChecksums.size());
                for (size_t i = 0; i < BlobOffsets.size(); ++i) {
                    const TString expected(BlockSize, 'a' + BlobOffsets[i]);
                    UNIT_ASSERT_VALUES_EQUAL(expected, Blocks[i]);
                    if (checksums) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            ComputeDefaultDigest(
                                {expected.data(), expected.size()}),
                            response->BlockChecksums[i]);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRepairFailedCompressedBlobReads, TCompressedReadEnvironment)
    {
        const TString markerBlock =
            TString(GetBrokenDataMarker()).resize(BlockSize, '\0');

        for (const auto codec:
             {NProto::TBlobCompression::LZ4, NProto::TBlobCompression::ZSTD})
        {
            for (const bool checksums: {false, true}) {
                for (const auto status:
                     {NKikimrProto::NODATA, NKikimrProto::ERROR})
                {
                    // Cover coalesced adjacent chunks as well as three
                    // separate ranges, including duplicate block offsets.
                    for (const auto& offsets: TVector<TVector<ui16>>{
                             {4, 5, 5, 8, 16}, {0, 1, 1, 8, 16}})
                    {
                        BlobOffsets = offsets;
                        const ui32 queryCount = offsets.front() == 4 ? 2 : 3;
                        for (ui32 failed = 1; failed < (1u << queryCount);
                             ++failed)
                        {
                            StartCompressedRead(
                                codec, checksums, EStorageAccessMode::Repair);
                            UNIT_ASSERT_VALUES_EQUAL(
                                queryCount, ReadRequest->QuerySize);
                            auto result = MakeReadResult();
                            for (ui32 i = 0; i < queryCount; ++i) {
                                if (failed & (1u << i)) {
                                    result->Responses[i].Status = status;
                                    result->Responses[i].Buffer = TRope();
                                }
                            }

                            const auto response = FinishRead(std::move(result));
                            UNIT_ASSERT_C(
                                !HasError(response->GetError()),
                                FormatError(response->GetError()));
                            UNIT_ASSERT_VALUES_EQUAL(
                                checksums ? BlobOffsets.size() : 0,
                                response->BlockChecksums.size());
                            for (size_t i = 0; i < BlobOffsets.size(); ++i) {
                                const ui32 queryIndex = queryCount == 2
                                    ? (BlobOffsets[i] == 16 ? 1 : 0)
                                    : BlobOffsets[i] / 8;
                                const TString expected =
                                    failed & (1u << queryIndex)
                                        ? markerBlock
                                        : TString(
                                              BlockSize, 'a' + BlobOffsets[i]);
                                UNIT_ASSERT_VALUES_EQUAL(expected, Blocks[i]);
                                if (checksums) {
                                    UNIT_ASSERT_VALUES_EQUAL(
                                        ComputeDefaultDigest(
                                            {expected.data(), expected.size()}),
                                        response->BlockChecksums[i]);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectMissingCompressedBlobInDefaultMode,
        TCompressedReadEnvironment)
    {
        StartCompressedRead(NProto::TBlobCompression::LZ4, true);
        auto result = MakeReadResult();
        result->Responses[0].Status = NKikimrProto::NODATA;
        result->Responses[0].Buffer = TRope();

        const auto response = FinishRead(std::move(result));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        UNIT_ASSERT(response->BlockChecksums.empty());
        for (const auto& block: Blocks) {
            UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), block);
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectTransientCompressedBlobReadErrorInRepairMode,
        TCompressedReadEnvironment)
    {
        StartCompressedRead(
            NProto::TBlobCompression::LZ4, true, EStorageAccessMode::Repair);
        auto result = MakeReadResult();
        result->Responses[1].Status = NKikimrProto::TIMEOUT;
        result->Responses[1].Buffer = TRope();

        const auto response = FinishRead(std::move(result));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        UNIT_ASSERT(response->BlockChecksums.empty());
        for (const auto& block: Blocks) {
            UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), block);
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectTruncatedCompressedBlob, TCompressedReadEnvironment)
    {
        StartCompressedRead(NProto::TBlobCompression::LZ4, true);
        auto result = MakeReadResult();
        const auto& query = ReadRequest->Queries[0];
        result->Responses[0].Buffer = TRope(
            TString(TStringBuf(Blob.Data).SubStr(query.Shift, query.Size - 1)));

        const auto response = FinishRead(std::move(result));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            response->GetError().GetMessage(), "Incomplete compressed chunk");
        UNIT_ASSERT(response->BlockChecksums.empty());
    }

    Y_UNIT_TEST_F(
        ShouldRejectCorruptCompressedChunk, TCompressedReadEnvironment)
    {
        for (const auto codec:
             {NProto::TBlobCompression::LZ4, NProto::TBlobCompression::ZSTD})
        {
            StartCompressedRead(codec, true);
            auto result = MakeReadResult();
            TString corrupt(ReadRequest->Queries[0].Size, '\xff');
            WriteUnaligned<ui64>(corrupt.begin(), ChunkSize);
            result->Responses[0].Buffer = TRope(corrupt);

            const auto response = FinishRead(std::move(result));
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED, response->GetError().GetCode());
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetError().GetMessage(),
                "Failed to decompress chunk");
            UNIT_ASSERT(response->BlockChecksums.empty());
            for (const auto& block: Blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), block);
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectFailedCompressedBlobRead, TCompressedReadEnvironment)
    {
        StartCompressedRead(NProto::TBlobCompression::LZ4, true);
        auto result = MakeReadResult();
        result->Responses[1].Status = NKikimrProto::ERROR;

        const auto response = FinishRead(std::move(result));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "read error", response->GetError().GetMessage());
        UNIT_ASSERT(response->BlockChecksums.empty());
        for (const auto& block: Blocks) {
            UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), block);
        }
    }

    Y_UNIT_TEST_F(ShouldReadBlobSuccess, TSetupEnvironment)
    {
        const ui32 groupId = 12;
        const ui32 blockSize = 512;
        const NKikimr::TLogoBlobID logoBlobID(142, 143, 0x8000);  //blob size 2028
        const TVector<ui16> blobOffsets{0 , 2, 3};

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            3,
            TString::TUninitialized(blockSize));
        TGuardedSgList guardedSglist(std::move(sglist));

        auto request = std::make_unique<
            TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                logoBlobID,
                EdgeActor,
                blobOffsets,
                guardedSglist,
                groupId,
                false,           // async
                TInstant::Max(), // deadline
                false,           // shouldCalculateChecksums
                std::nullopt     // compressionInfo
            );

        auto readActor = ActorSystem.Register(
            new TReadBlobActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                EdgeActor,
                NActors::TActorId(),
                0,
                blockSize,
                false, // shouldCalculateChecksums
                EStorageAccessMode::Default,
                std::move(request),
                TDuration(),
                0ull,
                false,   // passTraceIdToBlobstorage
                nullptr));   // blobCompressor

        auto readBlob = ActorSystem.GrabEdgeEvent<
            NKikimr::TEvBlobStorage::TEvGet>();

        UNIT_ASSERT_EQUAL(readBlob->QuerySize, 2);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Id, logoBlobID);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Size, blockSize);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Shift, 0);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Id, logoBlobID);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Size, blockSize * 2);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Shift, 1024);

        auto getResult = new NKikimr::TEvBlobStorage::TEvGetResult(
            NKikimrProto::EReplyStatus::OK,
            2,
            142);
        getResult->Responses[0].Status = NKikimrProto::OK;
        getResult->Responses[0].Id = logoBlobID;
        getResult->Responses[0].Shift = 0;
        getResult->Responses[0].RequestedSize = blockSize;
        getResult->Responses[0].Buffer.Insert(
            getResult->Responses[0].Buffer.End(),
            TRope(TString(blockSize, 1)));
        getResult->Responses[1].Status = NKikimrProto::OK;
        getResult->Responses[1].Id = logoBlobID;
        getResult->Responses[1].Shift = 1024;
        getResult->Responses[1].RequestedSize = blockSize * 2;
        getResult->Responses[1].Buffer.Insert(
            getResult->Responses[1].Buffer.End(),
            TRope(TString(blockSize, 2).append(TString(blockSize, 3))));

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            getResult));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvReadBlobResponse>();
        UNIT_ASSERT(!HasError(fullResponse->GetError()));

        auto guard = guardedSglist.Acquire();
        auto& sgList = guard.Get();
        for (size_t i = 0; i < sgList.size(); ++i) {
            UNIT_ASSERT_EQUAL(sgList[i].Size(), blockSize);
            UNIT_ASSERT_EQUAL(memcmp(
                sgList[i].Data(),
                TString(blockSize, i + 1).data(),
                blockSize), 0);
        }
    }

    Y_UNIT_TEST_F(ShouldReadBlobPartFail, TSetupEnvironment)
    {
        const ui32 groupId = 12;
        const ui32 blockSize = 512;
        const NKikimr::TLogoBlobID logoBlobID(142, 143, 0x8000);  //blob size 2028
        const TVector<ui16> blobOffsets{0 , 2, 3};

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            3,
            TString::TUninitialized(blockSize));
        TGuardedSgList guardedSglist(std::move(sglist));

        auto request = std::make_unique<
            TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                logoBlobID,
                EdgeActor,
                blobOffsets,
                guardedSglist,
                groupId,
                false,           // async
                TInstant::Max(), // deadline
                false,           // shouldCalculateChecksums
                std::nullopt     // compressionInfo
            );

        auto readActor = ActorSystem.Register(
            new TReadBlobActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                EdgeActor,
                NActors::TActorId(),
                0,
                blockSize,
                false, // shouldCalculateChecksums
                EStorageAccessMode::Default,
                std::move(request),
                TDuration(),
                0ull,
                false,   // passTraceIdToBlobstorage
                nullptr));   // blobCompressor

        auto readBlob = ActorSystem.GrabEdgeEvent<
            NKikimr::TEvBlobStorage::TEvGet>();

        UNIT_ASSERT_EQUAL(readBlob->QuerySize, 2);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Id, logoBlobID);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Size, blockSize);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Shift, 0);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Id, logoBlobID);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Size, blockSize * 2);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Shift, 1024);

        auto getResult = new NKikimr::TEvBlobStorage::TEvGetResult(
            NKikimrProto::EReplyStatus::OK,
            2,
            142);
        getResult->Responses[0].Status = NKikimrProto::OK;
        getResult->Responses[0].Id = logoBlobID;
        getResult->Responses[0].Shift = 0;
        getResult->Responses[0].RequestedSize = blockSize;
        getResult->Responses[0].Buffer.Insert(
            getResult->Responses[0].Buffer.End(),
            TRope(TString(blockSize, 1)));
        getResult->Responses[1].Status = NKikimrProto::ERROR;

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            getResult));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvReadBlobResponse>();
        UNIT_ASSERT(HasError(fullResponse->GetError()));
    }

    Y_UNIT_TEST_F(ShouldReadBlobFail, TSetupEnvironment)
    {
        const ui32 groupId = 12;
        const ui32 blockSize = 512;
        const NKikimr::TLogoBlobID logoBlobID(142, 143, 0x8000);  //blob size 2028
        const TVector<ui16> blobOffsets{0 , 2, 3};

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            3,
            TString::TUninitialized(blockSize));
        TGuardedSgList guardedSglist(std::move(sglist));

        auto request = std::make_unique<
            TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                logoBlobID,
                EdgeActor,
                blobOffsets,
                guardedSglist,
                groupId,
                false,           // async
                TInstant::Max(), // deadline
                false,           // shouldCalculateChecksums
                std::nullopt     // compressionInfo
            );

        auto readActor = ActorSystem.Register(
            new TReadBlobActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                EdgeActor,
                NActors::TActorId(),
                0,
                blockSize,
                false, // shouldCalculateChecksums
                EStorageAccessMode::Default,
                std::move(request),
                TDuration(),
                0ull,
                false,   // passTraceIdToBlobstorage
                nullptr));   // blobCompressor

        auto readBlob = ActorSystem.GrabEdgeEvent<
            NKikimr::TEvBlobStorage::TEvGet>();

        UNIT_ASSERT_EQUAL(readBlob->QuerySize, 2);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Id, logoBlobID);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Size, blockSize);
        UNIT_ASSERT_EQUAL(readBlob->Queries[0].Shift, 0);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Id, logoBlobID);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Size, blockSize * 2);
        UNIT_ASSERT_EQUAL(readBlob->Queries[1].Shift, 1024);

        auto getResult = new NKikimr::TEvBlobStorage::TEvGetResult(
            NKikimrProto::EReplyStatus::ERROR,
            2,
            142);

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            getResult));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvReadBlobResponse>();
        UNIT_ASSERT(HasError(fullResponse->GetError()));
    }

}

}   // namespace NCloud::NBlockStore::NStorage
