#include "actor_read_blob.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/storage/api/public.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/cast.h>
#include <util/random/fast.h>
#include <util/string/builder.h>
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
        NMonitoring::TDynamicCountersPtr CriticalEvents =
            MakeIntrusive<NMonitoring::TDynamicCounters>();

        void SetUp(NUnitTest::TTestContext&) override
        {
            InitCriticalEventsCounter(CriticalEvents);
            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();
        }

        ui64 GetLocalReadBlobErrors() const
        {
            return CriticalEvents
                ->GetCounter(GetCriticalEventForLocalReadBlobError(), true)
                ->Val();
        }
    };

    struct TCompressedReadEnvironment: TSetupEnvironment
    {
        static constexpr ui64 TabletId = 142;
        static constexpr ui32 GroupId = 12;
        static constexpr ui32 BlockSize = 512;
        static constexpr ui32 ChunkSize = 4 * BlockSize;

        TVector<ui16> BlobOffsets = {4, 5, 6, 8, 16};
        bool IncompressiblePayload = false;
        TCompressedBlob Blob;
        NKikimr::TLogoBlobID BlobId;
        TVector<TString> Blocks;
        TGuardedSgList GuardedSglist;
        NActors::TActorId ReadActor;
        THolder<NKikimr::TEvBlobStorage::TEvGet> ReadRequest;

        void StartCompressedRead(
            NProto::TCompressionMetadata::ECompressionType codec,
            bool shouldCalculateChecksums,
            EStorageAccessMode storageAccessMode = EStorageAccessMode::Default,
            bool waitForReadRequest = true)
        {
            TString payload;
            for (ui32 i = 0; i < 17; ++i) {
                payload.append(TString(BlockSize, 'a' + i));
            }
            if (IncompressiblePayload) {
                TFastRng64 rng(123);
                for (auto& byte: payload) {
                    byte = static_cast<char>(rng.GenRand());
                }
            }
            auto sgListOrError = SgListNormalize(
                TBlockDataRef(payload.data(), payload.size()),
                BlockSize);
            UNIT_ASSERT_C(
                !HasError(sgListOrError.GetError()),
                FormatError(sgListOrError.GetError()));
            auto blobOrError = CompressBlob(
                BlockSize,
                ChunkSize,
                codec,
                sgListOrError.GetResult());
            UNIT_ASSERT_C(
                !HasError(blobOrError.GetError()),
                FormatError(blobOrError.GetError()));
            Blob = blobOrError.ExtractResult();
            BlobId = NKikimr::TLogoBlobID(
                TabletId,
                1,
                2,
                3,
                SafeIntegerCast<ui32>(Blob.Data.size()),
                4);
            GuardedSglist = TGuardedSgList(ResizeBlocks(
                Blocks,
                BlobOffsets.size(),
                TString(BlockSize, '?')));

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

            ReadActor = ActorSystem.Register(new TReadBlobActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                EdgeActor,
                NActors::TActorId(),
                TabletId,
                BlockSize,
                shouldCalculateChecksums,
                storageAccessMode,
                std::move(request),
                TDuration(),
                0ull,
                false));
            if (waitForReadRequest) {
                ReadRequest =
                    ActorSystem
                        .GrabEdgeEvent<NKikimr::TEvBlobStorage::TEvGet>();
            }
        }

        std::unique_ptr<NKikimr::TEvBlobStorage::TEvGetResult> MakeReadResult()
        {
            auto result =
                std::make_unique<NKikimr::TEvBlobStorage::TEvGetResult>(
                    NKikimrProto::OK,
                    ReadRequest->QuerySize,
                    GroupId);
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
            const auto previousLocalReadErrors = GetLocalReadBlobErrors();
            ActorSystem.Send(new NActors::IEventHandle(
                ReadActor,
                EdgeActor,
                result.release()));
            auto response = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvReadBlobResponse>();
            auto completed = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
            UNIT_ASSERT_VALUES_EQUAL(
                response->GetError().GetCode(),
                completed->GetError().GetCode());
            UNIT_ASSERT(completed->BlobId == BlobId);
            ui32 expectedBytesCount = 0;
            for (size_t i = 0; i < ReadRequest->QuerySize; ++i) {
                expectedBytesCount += ReadRequest->Queries[i].Size;
            }
            UNIT_ASSERT_VALUES_EQUAL(expectedBytesCount, completed->BytesCount);
            const bool localError =
                HasError(response->GetError()) &&
                response->ErrorSource == EErrorSource::Local;
            UNIT_ASSERT_VALUES_EQUAL(
                previousLocalReadErrors + localError,
                GetLocalReadBlobErrors());
            return response;
        }
    };

    Y_UNIT_TEST_F(
        ShouldReturnCompressedReadPlanningError,
        TCompressedReadEnvironment)
    {
        BlobOffsets = {40};
        StartCompressedRead(
            NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
            true,
            EStorageAccessMode::Default,
            false);   // waitForReadRequest

        auto response = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvReadBlobResponse>();
        auto completed = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, completed->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, completed->BytesCount);
        UNIT_ASSERT_STRING_CONTAINS(
            response->GetError().GetMessage(),
            "outside the chunk list");
        UNIT_ASSERT(response->BlockChecksums.empty());
        UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), Blocks.front());
        UNIT_ASSERT_VALUES_EQUAL(1, GetLocalReadBlobErrors());
    }

    Y_UNIT_TEST_F(ShouldReportCompressedReadBytes, TCompressedReadEnvironment)
    {
        for (const bool incompressible: {false, true}) {
            IncompressiblePayload = incompressible;
            for (const bool sparse: {false, true}) {
                BlobOffsets.clear();
                for (ui16 i = 0; i < 17; ++i) {
                    if (!sparse || i == 4) {
                        BlobOffsets.push_back(i);
                    }
                }
                StartCompressedRead(
                    NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
                    false);

                UNIT_ASSERT_VALUES_EQUAL(1, ReadRequest->QuerySize);
                UNIT_ASSERT_VALUES_EQUAL(
                    sparse ? Blob.Compression.GetPlainChunks()
                                 .GetCompressedChunkSize(1)
                           : Blob.Data.size(),
                    ReadRequest->Queries[0].Size);
                const auto logicalBytes = BlobOffsets.size() * BlockSize;
                if (incompressible && sparse) {
                    UNIT_ASSERT(ReadRequest->Queries[0].Size > logicalBytes);
                } else if (!incompressible) {
                    UNIT_ASSERT(ReadRequest->Queries[0].Size < logicalBytes);
                }

                const auto response = FinishRead(MakeReadResult());
                UNIT_ASSERT_C(
                    !HasError(response->GetError()),
                    FormatError(response->GetError()));
            }
        }
    }

    Y_UNIT_TEST_F(ShouldReadCompressedBlob, TCompressedReadEnvironment)
    {
        for (const auto codec:
             {NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
              NProto::TCompressionMetadata::COMPRESSION_ZSTD_3})
        {
            for (const bool checksums: {false, true}) {
                StartCompressedRead(codec, checksums);

                // Chunks 1 and 2 are adjacent, while chunk 4 is a partial
                // final chunk. Chunks 0 and 3 must not be read.
                UNIT_ASSERT_VALUES_EQUAL(2, ReadRequest->QuerySize);
                UNIT_ASSERT(ReadRequest->Queries[0].Id == BlobId);
                UNIT_ASSERT(ReadRequest->Queries[1].Id == BlobId);
                UNIT_ASSERT_VALUES_EQUAL(
                    Blob.Compression.GetPlainChunks().GetCompressedChunkSize(0),
                    ReadRequest->Queries[0].Shift);
                UNIT_ASSERT_VALUES_EQUAL(
                    Blob.Compression.GetPlainChunks().GetCompressedChunkSize(
                        1) +
                        Blob.Compression.GetPlainChunks()
                            .GetCompressedChunkSize(2),
                    ReadRequest->Queries[0].Size);
                ui32 lastChunkOffset = 0;
                for (const auto size:
                     Blob.Compression.GetPlainChunks().GetCompressedChunkSize())
                {
                    lastChunkOffset += size;
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    lastChunkOffset,
                    ReadRequest->Queries[1].Shift);
                UNIT_ASSERT_VALUES_EQUAL(
                    BlobId.BlobSize() - lastChunkOffset,
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
                UNIT_ASSERT_VALUES_EQUAL(0, GetLocalReadBlobErrors());
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRepairFailedCompressedBlobReads,
        TCompressedReadEnvironment)
    {
        const TString markerBlock =
            TString(GetBrokenDataMarker()).resize(BlockSize, '\0');

        for (const auto codec:
             {NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
              NProto::TCompressionMetadata::COMPRESSION_ZSTD_3})
        {
            for (const bool checksums: {false, true}) {
                for (const auto status:
                     {NKikimrProto::NODATA, NKikimrProto::ERROR})
                {
                    // Cover coalesced adjacent chunks as well as three
                    // separate ranges.
                    for (const auto& offsets: TVector<TVector<ui16>>{
                             {4, 5, 6, 8, 16},
                             {0, 1, 2, 8, 16}})
                    {
                        BlobOffsets = offsets;
                        const ui32 queryCount = offsets.front() == 4 ? 2 : 3;
                        for (ui32 failed = 1; failed < (1u << queryCount);
                             ++failed)
                        {
                            StartCompressedRead(
                                codec,
                                checksums,
                                EStorageAccessMode::Repair);
                            UNIT_ASSERT_VALUES_EQUAL(
                                queryCount,
                                ReadRequest->QuerySize);
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
                                const ui32 queryIndex =
                                    queryCount == 2
                                        ? (BlobOffsets[i] == 16 ? 1 : 0)
                                        : BlobOffsets[i] / 8;
                                const TString expected =
                                    failed & (1u << queryIndex)
                                        ? markerBlock
                                        : TString(
                                              BlockSize,
                                              'a' + BlobOffsets[i]);
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
        StartCompressedRead(
            NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
            true);
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
            NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
            true,
            EStorageAccessMode::Repair);
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
        ShouldRejectCompressedBlobResponseWithWrongBlobId,
        TCompressedReadEnvironment)
    {
        for (const auto codec:
             {NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
              NProto::TCompressionMetadata::COMPRESSION_ZSTD_3})
        {
            for (const ui32 responseIndex: {0, 1}) {
                StartCompressedRead(codec, true);
                auto result = MakeReadResult();
                UNIT_ASSERT(responseIndex < result->ResponseSz);
                // Keep valid compressed data and change only the blob ID.
                result->Responses[responseIndex].Id = NKikimr::TLogoBlobID(
                    TabletId,
                    1,
                    2,
                    3,
                    BlobId.BlobSize(),
                    5);

                const auto response = FinishRead(std::move(result));
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    response->GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(
                    "TEvBlobStorage::TEvGet failed: invalid response received",
                    response->GetError().GetMessage());
                UNIT_ASSERT(response->BlockChecksums.empty());
                for (const auto& block: Blocks) {
                    UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), block);
                }
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectInvalidCompressedBlobResponseSize,
        TCompressedReadEnvironment)
    {
        ui64 expectedCriticalEvents = 0;
        for (const auto mode:
             {EStorageAccessMode::Default, EStorageAccessMode::Repair})
        {
            for (const ui32 responseIndex: {0, 1}) {
                // Zero, truncated and oversized responses must all fail before
                // any blocks are decoded or replaced with repair markers.
                for (const int delta: {0, -1, 1}) {
                    StartCompressedRead(
                        NProto::TCompressionMetadata::
                            COMPRESSION_LZ4_FAST14_SAFE,
                        true,
                        mode);
                    auto result = MakeReadResult();
                    const auto& query = ReadRequest->Queries[responseIndex];
                    const ui32 size = delta ? query.Size + delta : 0;
                    TString data(
                        TStringBuf(Blob.Data).SubStr(query.Shift, query.Size));
                    data.resize(size, '?');
                    result->Responses[responseIndex].Buffer = TRope(data);
                    if (mode == EStorageAccessMode::Repair) {
                        auto& broken = result->Responses[1 - responseIndex];
                        broken.Status = NKikimrProto::NODATA;
                        broken.Buffer = TRope();
                    } else if (responseIndex == 0 && delta == -1) {
                        // Keep concatenated data intact while shifting one
                        // byte across the response boundary.
                        result->Responses[1].Buffer.Insert(
                            result->Responses[1].Buffer.Begin(),
                            TRope(TString(
                                1,
                                Blob.Data[query.Shift + query.Size - 1])));
                    }

                    const auto response = FinishRead(std::move(result));
                    UNIT_ASSERT_VALUES_EQUAL(
                        E_REJECTED,
                        response->GetError().GetCode());
                    UNIT_ASSERT_VALUES_EQUAL(
                        TStringBuilder()
                            << "TEvBlobStorage::TEvGet failed: Invalid blob "
                               "response size at index "
                            << responseIndex << ": expected " << query.Size
                            << " bytes, got " << size,
                        response->GetError().GetMessage());
                    UNIT_ASSERT(response->ErrorSource == EErrorSource::Local);
                    UNIT_ASSERT(response->BlockChecksums.empty());
                    for (const auto& block: Blocks) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            TString(BlockSize, '?'),
                            block);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(
                        ++expectedCriticalEvents,
                        GetLocalReadBlobErrors());
                }
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectInvalidCompressedBlobResponseOffset,
        TCompressedReadEnvironment)
    {
        for (const auto mode:
             {EStorageAccessMode::Default, EStorageAccessMode::Repair})
        {
            for (const ui32 responseIndex: {0, 1}) {
                StartCompressedRead(
                    NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
                    true,
                    mode);
                auto result = MakeReadResult();
                const auto& query = ReadRequest->Queries[responseIndex];
                result->Responses[responseIndex].Shift = query.Shift + 1;
                if (mode == EStorageAccessMode::Repair) {
                    auto& broken = result->Responses[1 - responseIndex];
                    broken.Status = NKikimrProto::NODATA;
                    broken.Buffer = TRope();
                }

                const auto response = FinishRead(std::move(result));
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    response->GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(
                    TStringBuilder()
                        << "TEvBlobStorage::TEvGet failed: Invalid blob "
                           "response "
                           "offset at index "
                        << responseIndex << ": expected " << query.Shift
                        << ", got " << query.Shift + 1,
                    response->GetError().GetMessage());
                UNIT_ASSERT(response->ErrorSource == EErrorSource::Local);
                UNIT_ASSERT(response->BlockChecksums.empty());
                for (const auto& block: Blocks) {
                    UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), block);
                }
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectCorruptCompressedChunk,
        TCompressedReadEnvironment)
    {
        ui64 expectedCriticalEvents = 0;
        for (const auto codec:
             {NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
              NProto::TCompressionMetadata::COMPRESSION_ZSTD_3})
        {
            StartCompressedRead(codec, true);
            auto result = MakeReadResult();
            TString corrupt(ReadRequest->Queries[0].Size, '\xff');
            WriteUnaligned<ui64>(corrupt.begin(), ChunkSize);
            result->Responses[0].Buffer = TRope(corrupt);

            const auto response = FinishRead(std::move(result));
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                response->GetError().GetCode());
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetError().GetMessage(),
                "Failed to decompress chunk");
            UNIT_ASSERT(response->BlockChecksums.empty());
            for (const auto& block: Blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), block);
            }
            ++expectedCriticalEvents;
            UNIT_ASSERT_VALUES_EQUAL(
                expectedCriticalEvents,
                GetLocalReadBlobErrors());
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectFailedCompressedBlobRead,
        TCompressedReadEnvironment)
    {
        StartCompressedRead(
            NProto::TCompressionMetadata::COMPRESSION_LZ4_FAST14_SAFE,
            true);
        auto result = MakeReadResult();
        result->Responses[1].Status = NKikimrProto::ERROR;

        const auto response = FinishRead(std::move(result));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "TEvBlobStorage::TEvGet failed: read error",
            response->GetError().GetMessage());
        UNIT_ASSERT(response->BlockChecksums.empty());
        UNIT_ASSERT_VALUES_EQUAL(0, GetLocalReadBlobErrors());
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

        auto request =
            std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                logoBlobID,
                EdgeActor,
                blobOffsets,
                guardedSglist,
                groupId,
                false,             // async
                TInstant::Max(),   // deadline
                false,             // shouldCalculateChecksums
                std::nullopt       // compressionMetadata
            );

        auto readActor = ActorSystem.Register(new TReadBlobActor(
            MakeIntrusive<TRequestInfo>(
                EdgeActor,
                0ull,
                MakeIntrusive<TCallContext>()),
            EdgeActor,
            NActors::TActorId(),
            0,
            blockSize,
            false,   // shouldCalculateChecksums
            EStorageAccessMode::Default,
            std::move(request),
            TDuration(),
            0ull,
            false));   // passTraceIdToBlobstorage

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
        UNIT_ASSERT(fullResponse->ErrorSource == std::nullopt);

        auto completed = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
        UNIT_ASSERT(completed->ErrorSource == std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(
            blobOffsets.size() * blockSize,
            completed->BytesCount);

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

    Y_UNIT_TEST_F(ShouldRepairBrokenRanges, TSetupEnvironment)
    {
        const ui32 blockSize = 512;
        const ui32 queryCount = 4;
        const NKikimr::TLogoBlobID blobId(142, 1, 2, 3, 12 * blockSize, 4);
        const TVector<ui16> blobOffsets{1, 2, 4, 7, 8, 10, 11};
        const TVector<size_t> queryIndices{0, 0, 1, 2, 2, 3, 3};
        const auto marker = GetBrokenDataMarkerBlock(blockSize);

        for (const bool checksums: {false, true}) {
            for (ui32 brokenMask = 0; brokenMask < (1u << queryCount);
                 ++brokenMask)
            {
                TVector<TString> blocks;
                auto sglist = ResizeBlocks(
                    blocks,
                    blobOffsets.size(),
                    TString(blockSize, 'x'));
                TGuardedSgList guardedSglist(std::move(sglist));
                auto request = std::make_unique<
                    TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                    blobId,
                    EdgeActor,
                    blobOffsets,
                    guardedSglist,
                    12,
                    false,
                    TInstant::Max(),
                    checksums);
                const auto readActor = ActorSystem.Register(new TReadBlobActor(
                    MakeIntrusive<TRequestInfo>(
                        EdgeActor,
                        0ull,
                        MakeIntrusive<TCallContext>()),
                    EdgeActor,
                    NActors::TActorId(),
                    0,
                    blockSize,
                    checksums,
                    EStorageAccessMode::Repair,
                    std::move(request),
                    TDuration(),
                    0ull,
                    false));

                auto readBlob =
                    ActorSystem
                        .GrabEdgeEvent<NKikimr::TEvBlobStorage::TEvGet>();
                UNIT_ASSERT_VALUES_EQUAL(queryCount, readBlob->QuerySize);

                auto getResult =
                    std::make_unique<NKikimr::TEvBlobStorage::TEvGetResult>(
                        NKikimrProto::OK,
                        queryCount,
                        12);
                for (size_t i = 0; i < queryCount; ++i) {
                    auto& response = getResult->Responses[i];
                    if (brokenMask & (1u << i)) {
                        response.Status =
                            i % 2 ? NKikimrProto::ERROR : NKikimrProto::NODATA;
                        continue;
                    }
                    const auto& query = readBlob->Queries[i];
                    response.Status = NKikimrProto::OK;
                    response.Id = blobId;
                    response.Shift = query.Shift;
                    response.RequestedSize = query.Size;
                    TString data;
                    for (ui32 offset = query.Shift / blockSize;
                         offset < (query.Shift + query.Size) / blockSize;
                         ++offset)
                    {
                        data.append(TString(blockSize, 'a' + offset));
                    }
                    response.Buffer = TRope(std::move(data));
                }
                ActorSystem.Send(new NActors::IEventHandle(
                    readActor,
                    EdgeActor,
                    getResult.release()));

                auto response = ActorSystem.GrabEdgeEvent<
                    TEvPartitionCommonPrivate::TEvReadBlobResponse>();
                UNIT_ASSERT_C(
                    !HasError(response->GetError()),
                    response->GetError());
                UNIT_ASSERT(response->ErrorSource == std::nullopt);
                auto completed = ActorSystem.GrabEdgeEvent<
                    TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
                UNIT_ASSERT(completed->ErrorSource == std::nullopt);
                UNIT_ASSERT_VALUES_EQUAL(
                    checksums ? blobOffsets.size() : 0,
                    response->BlockChecksums.size());
                for (size_t i = 0; i < blocks.size(); ++i) {
                    const auto expected =
                        brokenMask & (1u << queryIndices[i])
                            ? marker
                            : TString(blockSize, 'a' + blobOffsets[i]);
                    UNIT_ASSERT_VALUES_EQUAL(expected, blocks[i]);
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

    Y_UNIT_TEST_F(ShouldRejectDuplicateBlobOffsets, TSetupEnvironment)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        auto invalidOffsets = counters->GetCounter(
            GetCriticalEventForInvalidReadBlobOffsets(),
            true);
        auto localReadErrors =
            counters->GetCounter(GetCriticalEventForLocalReadBlobError(), true);
        ui32 readRequests = 0;
        ActorSystem.SetEventFilter(
            [&](NActors::TTestActorRuntimeBase&,
                TAutoPtr<NActors::IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == NKikimr::TEvBlobStorage::EvGet) {
                    ++readRequests;
                }
                return false;
            });

        for (const auto storageAccessMode:
             {EStorageAccessMode::Default, EStorageAccessMode::Repair})
        {
            const auto previousLocalReadErrors = localReadErrors->Val();
            const auto previousInvalidOffsets = invalidOffsets->Val();
            const ui32 blockSize = 512;
            const NKikimr::TLogoBlobID blobId(142, 143, 0x8000);
            const TVector<ui16> blobOffsets{0, 2, 2, 3};
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                blobOffsets.size(),
                TString(blockSize, 'x'));
            auto request =
                std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                    blobId,
                    EdgeActor,
                    blobOffsets,
                    TGuardedSgList(std::move(sglist)),
                    12,
                    false,
                    TInstant::Max(),
                    false);

            ActorSystem.Register(new TReadBlobActor(
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                EdgeActor,
                NActors::TActorId(),
                0,
                blockSize,
                false,
                storageAccessMode,
                std::move(request),
                TDuration(),
                0ull,
                false));

            auto response = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvReadBlobResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                response->GetError().GetCode());
            UNIT_ASSERT(response->ErrorSource == EErrorSource::Local);
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetError().GetMessage(),
                "Offsets should be sorted in strict ascending order: 0, 2, 2, "
                "3");

            auto completed = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                completed->GetError().GetCode());
            UNIT_ASSERT(completed->ErrorSource == EErrorSource::Local);
            UNIT_ASSERT_VALUES_EQUAL(0, readRequests);
            UNIT_ASSERT_VALUES_EQUAL(
                previousInvalidOffsets + 1,
                invalidOffsets->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                previousLocalReadErrors + 1,
                localReadErrors->Val());
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(blockSize, 'x'), block);
            }
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

        auto request =
            std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                logoBlobID,
                EdgeActor,
                blobOffsets,
                guardedSglist,
                groupId,
                false,             // async
                TInstant::Max(),   // deadline
                false,             // shouldCalculateChecksums
                std::nullopt       // compressionMetadata
            );

        auto readActor = ActorSystem.Register(new TReadBlobActor(
            MakeIntrusive<TRequestInfo>(
                EdgeActor,
                0ull,
                MakeIntrusive<TCallContext>()),
            EdgeActor,
            NActors::TActorId(),
            0,
            blockSize,
            false,   // shouldCalculateChecksums
            EStorageAccessMode::Default,
            std::move(request),
            TDuration(),
            0ull,
            false));   // passTraceIdToBlobstorage

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
        UNIT_ASSERT(fullResponse->ErrorSource == EErrorSource::BlobStorage);

        auto completed = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
        UNIT_ASSERT(completed->ErrorSource == EErrorSource::BlobStorage);
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

        auto request =
            std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                logoBlobID,
                EdgeActor,
                blobOffsets,
                guardedSglist,
                groupId,
                false,             // async
                TInstant::Max(),   // deadline
                false,             // shouldCalculateChecksums
                std::nullopt       // compressionMetadata
            );

        auto readActor = ActorSystem.Register(new TReadBlobActor(
            MakeIntrusive<TRequestInfo>(
                EdgeActor,
                0ull,
                MakeIntrusive<TCallContext>()),
            EdgeActor,
            NActors::TActorId(),
            0,
            blockSize,
            false,   // shouldCalculateChecksums
            EStorageAccessMode::Default,
            std::move(request),
            TDuration(),
            0ull,
            false));   // passTraceIdToBlobstorage

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
        UNIT_ASSERT(fullResponse->ErrorSource == EErrorSource::BlobStorage);

        auto completed = ActorSystem.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
        UNIT_ASSERT(completed->ErrorSource == EErrorSource::BlobStorage);
    }

    Y_UNIT_TEST_F(
        ShouldNotCountInvalidResponsesAsBlobStorageErrors,
        TSetupEnvironment)
    {
        const ui32 blockSize = 512;
        const NKikimr::TLogoBlobID blobId(142, 1, 2, 3, 2 * blockSize, 4);

        ui64 expectedLocalReadErrors = 0;
        for (const auto mode:
             {EStorageAccessMode::Default, EStorageAccessMode::Repair})
        {
            for (const ui32 invalidField: {0, 1, 2}) {
                const bool invalidBlobId = invalidField == 1;
                const bool invalidOffset = invalidField == 2;
                for (const ui32 responseSize:
                     {0u, 1u, blockSize, 2 * blockSize, 3 * blockSize})
                {
                    TVector<TString> blocks;
                    auto sglist =
                        ResizeBlocks(blocks, 2, TString(blockSize, 'x'));
                    auto request = std::make_unique<
                        TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                        blobId,
                        EdgeActor,
                        TVector<ui16>{0, 1},
                        TGuardedSgList(std::move(sglist)),
                        12,
                        false,
                        TInstant::Max(),
                        false);

                    auto readActor = ActorSystem.Register(new TReadBlobActor(
                        MakeIntrusive<TRequestInfo>(
                            EdgeActor,
                            0ull,
                            MakeIntrusive<TCallContext>()),
                        EdgeActor,
                        NActors::TActorId(),
                        0,
                        blockSize,
                        false,
                        mode,
                        std::move(request),
                        TDuration(),
                        0ull,
                        false));

                    auto readBlob =
                        ActorSystem
                            .GrabEdgeEvent<NKikimr::TEvBlobStorage::TEvGet>();
                    UNIT_ASSERT_VALUES_EQUAL(1, readBlob->QuerySize);

                    auto getResult =
                        std::make_unique<NKikimr::TEvBlobStorage::TEvGetResult>(
                            NKikimrProto::OK,
                            1,
                            12);
                    auto& result = getResult->Responses[0];
                    result.Status = NKikimrProto::OK;
                    result.Id = invalidBlobId ? NKikimr::TLogoBlobID() : blobId;
                    result.Shift = invalidOffset ? blockSize : 0;
                    result.RequestedSize = 2 * blockSize;
                    result.Buffer = TRope(TString(responseSize, 'a'));

                    ActorSystem.Send(new NActors::IEventHandle(
                        readActor,
                        EdgeActor,
                        getResult.release()));

                    const bool expectedError = invalidBlobId || invalidOffset ||
                                               responseSize != 2 * blockSize;
                    const auto expectedErrorSource =
                        expectedError ? std::make_optional(EErrorSource::Local)
                                      : std::nullopt;
                    auto response = ActorSystem.GrabEdgeEvent<
                        TEvPartitionCommonPrivate::TEvReadBlobResponse>();
                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedError,
                        HasError(response->GetError()));
                    UNIT_ASSERT(response->ErrorSource == expectedErrorSource);

                    auto completed = ActorSystem.GrabEdgeEvent<
                        TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedError,
                        HasError(completed->GetError()));
                    UNIT_ASSERT(completed->ErrorSource == expectedErrorSource);
                    expectedLocalReadErrors += expectedError;
                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedLocalReadErrors,
                        GetLocalReadBlobErrors());
                    if (expectedError) {
                        for (const auto& block: blocks) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                TString(blockSize, 'x'),
                                block);
                        }
                        if (responseSize != 2 * blockSize) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                TStringBuilder()
                                    << "TEvBlobStorage::TEvGet failed: Invalid "
                                       "blob "
                                       "response size at index 0: expected "
                                    << readBlob->Queries[0].Size
                                    << " bytes, got " << responseSize,
                                response->GetError().GetMessage());
                        } else if (invalidOffset) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                TStringBuilder()
                                    << "TEvBlobStorage::TEvGet failed: Invalid "
                                       "blob response offset at index 0: "
                                       "expected "
                                    << readBlob->Queries[0].Shift << ", got "
                                    << blockSize,
                                response->GetError().GetMessage());
                        }
                    }
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
