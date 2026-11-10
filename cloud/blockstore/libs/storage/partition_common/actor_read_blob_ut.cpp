#include "actor_read_blob.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

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

    Y_UNIT_TEST_F(ShouldRejectDuplicateBlobOffsets, TSetupEnvironment)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        auto invalidOffsets = counters->GetCounter(
            GetCriticalEventForInvalidReadBlobOffsets(),
            true);
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
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetError().GetMessage(),
                "Offsets should be sorted in strict ascending order: 0, 2, 2, "
                "3");

            auto completed = ActorSystem.GrabEdgeEvent<
                TEvPartitionCommonPrivate::TEvReadBlobCompleted>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                completed->GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, readRequests);
            UNIT_ASSERT_VALUES_EQUAL(
                previousInvalidOffsets + 1,
                invalidOffsets->Val());
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
