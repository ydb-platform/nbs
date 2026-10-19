#include "actor_read_blob.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/partition/model/merged_blob_compression_policy.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadBlobTests)
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
        }
    };

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
                false            // shouldCalculateChecksums
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
                false));

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


    Y_UNIT_TEST_F(ShouldReadCompressedLogicalBlocksAndRejectCorruption, TSetupEnvironment)
    {
        for (ui32 blockSize: {4096u, 65536u, 131072u}) {
            for (ui32 failure: {0u, 1u, 2u, 3u, 4u, 5u, 6u}) {
                TString raw;
                for (ui32 i = 0; i < 17; ++i) {
                    raw.append(TString(blockSize, 'a' + i));
                }
                NProto::TBlobMeta meta;
                meta.MutableMergedBlocks()->SetEnd(16);
                NPartition::TCompressedMergedBlob compressed;
                UNIT_ASSERT(!HasError(NPartition::CompressMergedBlob(
                    raw, blockSize, 10, meta, compressed)));
                UNIT_ASSERT(!compressed.Payload.empty());
                const NKikimr::TLogoBlobID blobId(
                    142, 1, 1, 3, compressed.Payload.size(), 0);
                const TVector<ui16> offsets{16, 0, 2, 0};
                TVector<TString> blocks;
                auto sglist = ResizeBlocks(blocks, offsets.size(), TString(blockSize, '?'));
                TGuardedSgList guarded(std::move(sglist));
                auto request = std::make_unique<
                    TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                        blobId, EdgeActor, offsets, guarded, 12, false,
                        TInstant::Max(), true);
                request->Format.LogicalBlocks = 17;
                if (failure == 2) {
                    compressed.Compression.SetVersion(100);
                }
                request->Format.Compression =
                    std::make_shared<NProto::TBlobCompression>(compressed.Compression);
                if (failure == 3) {
                    guarded.Close();
                }
                TVector<std::shared_ptr<void>> occupied;
                if (failure == 6) {
                    for (ui32 i = 0; i < 4; ++i) {
                        auto token = NPartition::TryAcquireMergedBlobBudget(false, true, 1);
                        UNIT_ASSERT(token);
                        occupied.push_back(std::move(token));
                    }
                }
                auto actor = ActorSystem.Register(new TReadBlobActor(
                    MakeIntrusive<TRequestInfo>(
                        EdgeActor, 0ull, MakeIntrusive<TCallContext>()),
                    EdgeActor, NActors::TActorId(), 142, blockSize, true,
                    EStorageAccessMode::Default, std::move(request),
                    TDuration(), 0ull, false));

                if (failure != 2 && failure != 3 && failure != 6) {
                    auto get = ActorSystem.GrabEdgeEvent<NKikimr::TEvBlobStorage::TEvGet>();
                    const ui32 expectedChunks = blockSize == 4096 ? 2 : 3 * blockSize / 32768;
                    UNIT_ASSERT_VALUES_EQUAL(expectedChunks, get->QuerySize);
                    auto* result = new NKikimr::TEvBlobStorage::TEvGetResult(
                        NKikimrProto::OK, get->QuerySize, 142);
                    for (ui32 i = 0; i < get->QuerySize; ++i) {
                        const auto& query = get->Queries[i];
                        UNIT_ASSERT_VALUES_EQUAL(query.Id, blobId);
                        UNIT_ASSERT(query.Size < 32768);
                        TString encoded = compressed.Payload.substr(query.Shift, query.Size);
                        if (failure == 1 && i + 1 == get->QuerySize) {
                            encoded[0] = static_cast<char>(encoded[0]) ^ 1;
                        }
                        auto& response = result->Responses[i];
                        response.Status = NKikimrProto::OK;
                        response.Id = blobId;
                        response.Shift = query.Shift;
                        response.RequestedSize = query.Size;
                        response.Buffer = TRope(encoded);
                    }
                    if (failure == 4) {
                        guarded.Close();
                    }
                    if (failure == 5) {
                        delete result;
                        ActorSystem.Send(new NActors::IEventHandle(
                            actor, EdgeActor, new NActors::TEvents::TEvPoisonPill()));
                    } else {
                        ActorSystem.Send(new NActors::IEventHandle(actor, EdgeActor, result));
                    }
                }
                auto response = ActorSystem.GrabEdgeEvent<
                    TEvPartitionCommonPrivate::TEvReadBlobResponse>();
                occupied.clear();
                // Completion releases the pool even after cancellation, poison,
                // metadata failure, late corruption or admission rejection.
                TVector<std::shared_ptr<void>> released;
                for (ui32 i = 0; i < 4; ++i) {
                    auto token = NPartition::TryAcquireMergedBlobBudget(false, true, 1);
                    UNIT_ASSERT(token);
                    released.push_back(std::move(token));
                }
                if (failure) {
                    UNIT_ASSERT(HasError(response->GetError()));
                    if (failure == 3 || failure == 4) {
                        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_CANCELLED);
                    }
                    if (failure == 5 || failure == 6) {
                        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_REJECTED);
                    }
                    for (const auto& block: blocks) {
                        UNIT_ASSERT_VALUES_EQUAL(TString(blockSize, '?'), block);
                    }
                } else {
                    UNIT_ASSERT_C(!HasError(response->GetError()), FormatError(response->GetError()));
                    UNIT_ASSERT_VALUES_EQUAL(offsets.size(), response->BlockChecksums.size());
                    for (size_t i = 0; i < offsets.size(); ++i) {
                        TString expected(blockSize, 'a' + offsets[i]);
                        UNIT_ASSERT_VALUES_EQUAL(expected, blocks[i]);
                        UNIT_ASSERT_VALUES_EQUAL(
                            ComputeDefaultDigest({expected.data(), expected.size()}),
                            response->BlockChecksums[i]);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldRejectMalformedCompressedReadsAtomically, TSetupEnvironment)
    {
        enum class EFailure
        {
            InvalidFormat,
            MissingMetadata,
            LogicalBlocks,
            LogicalSize,
            BlobOffset,
            ResponseCount,
            Status,
            BlobId,
            Shift,
            ShortBuffer,
            LongBuffer,
        };
        const ui32 blockSize = 4096;
        const TString raw(17 * blockSize, 'x');
        NProto::TBlobMeta meta;
        meta.MutableMergedBlocks()->SetEnd(16);
        NPartition::TCompressedMergedBlob compressed;
        UNIT_ASSERT(!HasError(NPartition::CompressMergedBlob(
            raw, blockSize, 10, meta, compressed)));
        UNIT_ASSERT(!compressed.Payload.empty());
        const NKikimr::TLogoBlobID blobId(
            142, 1, 1, 3, compressed.Payload.size(), 0);

        for (bool checksums: {false, true}) {
            for (EFailure failure:
                 {EFailure::InvalidFormat,
                  EFailure::MissingMetadata,
                  EFailure::LogicalBlocks,
                  EFailure::LogicalSize,
                  EFailure::BlobOffset,
                  EFailure::ResponseCount,
                  EFailure::Status,
                  EFailure::BlobId,
                  EFailure::Shift, EFailure::ShortBuffer, EFailure::LongBuffer})
            {
                const bool preflight = failure <= EFailure::BlobOffset;
                const TVector<ui16> offsets{
                    0,
                    16,
                    ui16(failure == EFailure::BlobOffset ? 17 : 8)};
                TVector<TString> blocks;
                auto sglist = ResizeBlocks(
                    blocks, offsets.size(), TString(blockSize, '?'));
                TGuardedSgList guarded(std::move(sglist));
                auto request = std::make_unique<
                    TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                    blobId,
                    EdgeActor,
                    offsets, guarded, 12, false, TInstant::Max(), checksums);
                request->Format.LogicalBlocks = 17;
                auto descriptor = std::make_shared<NProto::TBlobCompression>(
                    compressed.Compression);
                request->Format.Compression = descriptor;
                switch (failure) {
                    case EFailure::InvalidFormat:
                        request->Format.Invalid = true;
                        break;
                    case EFailure::MissingMetadata:
                        request->Format.Compression.reset();
                        break;
                    case EFailure::LogicalBlocks:
                        request->Format.LogicalBlocks = 16;
                        break;
                    case EFailure::LogicalSize:
                        descriptor->SetLogicalSize(raw.size() - 1);
                        break;
                    default:
                        break;
                }

                ui32 getRequests = 0;
                // An observer may see the same edge event on several dispatch
                // passes. Count actual sends before they enter the mailbox.
                auto filter = ActorSystem.SetEventFilter(
                    [&](NActors::TTestActorRuntimeBase&,
                        TAutoPtr<NActors::IEventHandle>& event)
                    {
                        if (event->GetTypeRewrite() ==
                            NKikimr::TEvBlobStorage::EvGet)
                        {
                            ++getRequests;
                        }
                        return false;
                    });
                auto actor = ActorSystem.Register(new TReadBlobActor(
                    MakeIntrusive<TRequestInfo>(
                        EdgeActor, 0ull, MakeIntrusive<TCallContext>()),
                    EdgeActor,
                    NActors::TActorId(),
                    142,
                    blockSize,
                    checksums,
                    EStorageAccessMode::Default,
                    std::move(request), TDuration(), 0ull, false));
                if (!preflight) {
                    auto get =
                        ActorSystem
                            .GrabEdgeEvent<NKikimr::TEvBlobStorage::TEvGet>();
                    UNIT_ASSERT_VALUES_EQUAL(get->QuerySize, 3);
                    const ui32 responseCount =
                        get->QuerySize -
                        (failure == EFailure::ResponseCount ? 1 : 0);
                    auto result =
                        std::make_unique<NKikimr::TEvBlobStorage::TEvGetResult>(
                            NKikimrProto::OK, responseCount, 142);
                    for (ui32 i = 0; i < responseCount; ++i) {
                        const auto& query = get->Queries[i];
                        auto& part = result->Responses[i];
                        part.Status = NKikimrProto::OK;
                        part.Id = blobId;
                        part.Shift = query.Shift;
                        part.RequestedSize = query.Size;
                        TString payload =
                            compressed.Payload.substr(query.Shift, query.Size);
                        // Earlier fragments are valid: a late failure must not
                        // publish their already decoded blocks to the client.
                        if (i + 1 == responseCount) {
                            switch (failure) {
                                case EFailure::Status:
                                    part.Status = NKikimrProto::ERROR;
                                    break;
                                case EFailure::BlobId:
                                    part.Id = NKikimr::TLogoBlobID(
                                        142,
                                        1, 1, 3, compressed.Payload.size(), 1);
                                    break;
                                case EFailure::Shift:
                                    ++part.Shift;
                                    break;
                                case EFailure::ShortBuffer:
                                    payload.resize(payload.size() - 1);
                                    break;
                                case EFailure::LongBuffer:
                                    payload.push_back('!');
                                    break;
                                default:
                                    break;
                            }
                        }
                        part.Buffer = TRope(payload);
                    }
                    ActorSystem.Send(new NActors::IEventHandle(
                        actor, EdgeActor, result.release()));
                }

                auto response = ActorSystem.GrabEdgeEvent<
                    TEvPartitionCommonPrivate::TEvReadBlobResponse>();
                ActorSystem.SetEventFilter(std::move(filter));
                UNIT_ASSERT_VALUES_EQUAL(getRequests, preflight ? 0 : 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    response->GetError().GetCode(),
                    failure == EFailure::Status ? E_REJECTED : E_IO);
                UNIT_ASSERT(!response->GetError().GetMessage().empty());
                UNIT_ASSERT(response->BlockChecksums.empty());
                for (const auto& block: blocks) {
                    UNIT_ASSERT_VALUES_EQUAL(block, TString(blockSize, '?'));
                }
                TVector<std::shared_ptr<void>> released;
                for (ui32 i = 0; i < 4; ++i) {
                    auto token =
                        NPartition::TryAcquireMergedBlobBudget(false, true, 1);
                    UNIT_ASSERT(token);
                    released.push_back(std::move(token));
                }
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

        auto request = std::make_unique<
            TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                logoBlobID,
                EdgeActor,
                blobOffsets,
                guardedSglist,
                groupId,
                false,           // async
                TInstant::Max(), // deadline
                false            // shouldCalculateChecksums
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
                false));

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
                false            // shouldCalculateChecksums
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
                false));

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
