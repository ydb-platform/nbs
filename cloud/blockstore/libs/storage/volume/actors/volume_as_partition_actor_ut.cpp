#include "volume_as_partition_actor.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ExtractBlockContent(const NProto::TIOVector& blocks)
{
    if (blocks.GetBuffers().empty()) {
        return {};
    }

    TString result;
    result.reserve(blocks.GetBuffers().size() * blocks.GetBuffers(0).size());

    for (const auto& block: blocks.GetBuffers()) {
        result += block;
    }
    return result;
}

std::unique_ptr<TEvSSProxy::TEvDescribeVolumeResponse>
MakeDescribeVolumeResponse(
    const TString& followerDiskId,
    ui32 followerBlockSize,
    ui32 followerBlockCount)
{
    NKikimrSchemeOp::TPathDescription description;
    auto* config =
        description.MutableBlockStoreVolumeDescription()->MutableVolumeConfig();
    config->SetDiskId(followerDiskId);
    config->SetBlockSize(followerBlockSize);
    auto* part = config->MutablePartitions()->Add();
    part->SetBlockCount(followerBlockCount);

    return std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
        "",
        std::move(description));
}

std::unique_ptr<TEvService::TEvWriteBlocksRequest> MakeWriteBlocksRequest(
    const TString& diskId,
    ui64 startIndex,
    ui32 blockSize,
    const TString& blocksContent,
    TString* expandedBlocksContent)
{
    auto result = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    result->Record.SetDiskId(diskId);
    result->Record.SetStartIndex(startIndex);
    for (auto ch: blocksContent) {
        result->Record.MutableBlocks()->AddBuffers(TString(blockSize, ch));
    }

    if (expandedBlocksContent) {
        *expandedBlocksContent =
            ExtractBlockContent(result->Record.GetBlocks());
    }

    return result;
}

std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
MakeWriteBlocksLocalRequest(
    const TString& diskId,
    ui64 startIndex,
    ui32 blockSize,
    const TString& blocksContent,
    TString* expandedBlocksContent)
{
    expandedBlocksContent->reserve(blockSize * blocksContent.size());
    for (auto ch: blocksContent) {
        *expandedBlocksContent += TString(blockSize, ch);
    }

    TSgList sglist;
    sglist.resize(
        blocksContent.size(),
        {expandedBlocksContent->data(), expandedBlocksContent->size()});

    auto result = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
    result->Record.SetDiskId(diskId);
    result->Record.SetStartIndex(startIndex);
    result->Record.BlockSize = blockSize;
    result->Record.BlocksCount = blocksContent.size();
    result->Record.Sglist = TGuardedSgList(std::move(sglist));

    return result;
}

}   // namespace

Y_UNIT_TEST_SUITE(TVolumeAsPartitionActorTests)
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

    template <ui32 TLeaderBlockSize, ui32 TFollowerBlockSize>
    struct TSetupEnvironment: public TCurrentTestCase
    {
        const ui32 LeaderBlockSize = TLeaderBlockSize;
        const ui32 FollowerBlockSize = TFollowerBlockSize;
        const ui64 FollowerBlockCount = 256;
        const TString LeaderDiskId = "leader-disk";
        const TString FollowerDiskId = "follower-disk";
        TLogTitle LogTitle{
            GetCycleCount(),
            TLogTitle::TVolume{.TabletId = 10, .DiskId = LeaderDiskId}};

        TActorSystem ActorSystem;
        NActors::TActorId EdgeActor;
        NActors::TActorId VolumeAsPartitionActor;

        void SetUp(NUnitTest::TTestContext&) override
        {
            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();

            ActorSystem.RegisterService(
                MakeVolumeProxyServiceId(),
                EdgeActor,
                0);

            ActorSystem.RegisterService(MakeSSProxyServiceId(), EdgeActor, 0);

            VolumeAsPartitionActor =
                ActorSystem.Register(new TVolumeAsPartitionActor{
                    LogTitle.GetChild(GetCycleCount()),
                    LeaderBlockSize,
                    FollowerDiskId});
        }

        template <typename TEvent>
        std::unique_ptr<TEvent> GrabEdgeEventAndCookie(ui64* cookie)
        {
            TAutoPtr<NActors::IEventHandle> handle;
            ActorSystem.GrabEdgeEvent<TEvent>(handle);
            if (handle) {
                *cookie = handle->Cookie;
                return std::unique_ptr<TEvent>(
                    handle->Release<TEvent>().Release());
            }
            return nullptr;
        }

        void DescribeFollower()
        {
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                EdgeActor,
                MakeDescribeVolumeResponse(
                    FollowerDiskId,
                    FollowerBlockSize,
                    FollowerBlockCount)
                    .release()));
        }

        void SendWriteRequest(
            ui64 startIndex,
            const TString& blocksContent,
            TString* expandedBlocksContent = nullptr,
            ui64 cookie = 0)
        {
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                EdgeActor,
                MakeWriteBlocksRequest(
                    LeaderDiskId,
                    startIndex,
                    LeaderBlockSize,
                    blocksContent,
                    expandedBlocksContent)
                    .release(),
                0,       // flags
                cookie   // cookie
                ));
        }

        void ReplyFollowerWriteBlocks(NProto::TError error, ui64 followerCookie)
        {
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                EdgeActor,
                new TEvService::TEvWriteBlocksResponse(std::move(error)),
                0,   // flags
                followerCookie));
        }

        void SendWriteBlocksLocalRequest(
            ui64 startIndex,
            const TString& blocksContent,
            TString* expandedBlocksContent,
            ui64 cookie = 0)
        {
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                EdgeActor,
                MakeWriteBlocksLocalRequest(
                    LeaderDiskId,
                    startIndex,
                    LeaderBlockSize,
                    blocksContent,
                    expandedBlocksContent)
                    .release(),
                0,       // flags
                cookie   // cookie
                ));
        }

        void SendZeroRequest(ui64 startIndex, ui32 blockCount, ui64 cookie)
        {
            auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
            request->Record.SetDiskId(LeaderDiskId);
            request->Record.SetStartIndex(startIndex);
            request->Record.SetBlocksCount(blockCount);

            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                EdgeActor,
                request.release(),
                0,       // flags
                cookie   // cookie
                ));
        }

        void ReplyFollowerZeroBlocks(NProto::TError error, ui64 followerCookie)
        {
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                EdgeActor,
                new TEvService::TEvZeroBlocksResponse(std::move(error)),
                0,   // flags
                followerCookie));
        }
    };

    using TSetupEnvironment8_8 = TSetupEnvironment<8, 8>;
    using TSetupEnvironment8_4 = TSetupEnvironment<8, 4>;
    using TSetupEnvironment4_8 = TSetupEnvironment<4, 8>;

    Y_UNIT_TEST_F(ShouldRejectDuringFollowerDescribing, TSetupEnvironment8_8)
    {
        // Intercept describe request
        auto describeRequest =
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
        UNIT_ASSERT_VALUES_EQUAL(FollowerDiskId, describeRequest->DiskId);

        {
            // Send write request when describe not finished
            SendWriteRequest(10, "aaabbbccc", nullptr, 1000);

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
        }

        {   // Send WriteBlocksLocal request
            TString expandedBlocksContent;
            SendWriteBlocksLocalRequest(
                10,
                "aaabbbccc",
                &expandedBlocksContent,
                2000);

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }

        {   // Send zero request
            SendZeroRequest(20, 5, 3000);

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
        }
    }

    Y_UNIT_TEST_F(
        ShouldResponseAbortedWhenFollowerDescribeFailed,
        TSetupEnvironment8_8)
    {
        // Follower disk not exists
        {
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                EdgeActor,
                new TEvSSProxy::TEvDescribeVolumeResponse(
                    MakeError(MAKE_SCHEMESHARD_ERROR(
                        NKikimrScheme::StatusPathDoesNotExist)))));
        }

        {
            // Send write blocks request
            SendWriteRequest(10, "aaabbbccc", nullptr, 1000);

            // Should get E_ABORTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
        }

        {   // Send WriteBlocksLocal request
            TString expandedBlocksContent;
            SendWriteBlocksLocalRequest(
                10,
                "aaabbbccc",
                &expandedBlocksContent,
                2000);

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }

        {   // Send zero blocks request
            SendZeroRequest(20, 5, 3000);

            // Should get E_ABORTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
        }
    }

    Y_UNIT_TEST_F(ShouldFailOnInvalidBlockIndex, TSetupEnvironment8_8)
    {
        // Describe follower OK
        DescribeFollower();

        {
            // Send write blocks request
            SendWriteRequest(250, "aaabbbccc", nullptr, 2000);

            // Should get E_ARGUMENT error
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }

        {   // Send WriteBlocksLocal request
            TString expandedBlocksContent;
            SendWriteBlocksLocalRequest(
                250,
                "aaabbbccc",
                &expandedBlocksContent,
                2000);

            // Should get E_ARGUMENT response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }

        {   // Send zero blocks request
            SendZeroRequest(250, 50, 3000);

            // Should get E_ARGUMENT response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
        }
    }

    Y_UNIT_TEST_F(ShouldFailOnFollowerError, TSetupEnvironment8_8)
    {
        // Describe follower OK
        DescribeFollower();

        {   // Send write blocks request
            SendWriteRequest(10, "aaabbbccc", nullptr, 1000);

            // Intercept forwarded to follower write request.
            ui64 followerCookie = 0;
            auto followerWriteBlocks =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                    &followerCookie);

            // WriteBlocks on follower failed.
            ReplyFollowerWriteBlocks(MakeError(E_REJECTED), followerCookie);

            // Should get E_REJECTED error
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
        }

        {   // Send WriteBlocksLocal request
            TString expandedBlocksContent;
            SendWriteBlocksLocalRequest(
                10,
                "aaabbbccc",
                &expandedBlocksContent,
                2000);

            // Intercept forwarded to follower write request.
            ui64 followerCookie = 0;
            auto followerWriteBlocks =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                    &followerCookie);

            // WriteBlocks on follower failed.
            ReplyFollowerWriteBlocks(MakeError(E_REJECTED), followerCookie);

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }

        {   // Send zero blocks request
            SendZeroRequest(40, 50, 3000);

            // Intercept forwarded to follower zero request.
            ui64 followerCookie = 0;
            auto followerWriteBlocks =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksRequest>(
                    &followerCookie);

            // ZeroBlocks on follower failed.
            ReplyFollowerZeroBlocks(MakeError(E_REJECTED), followerCookie);

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
        }
    }

    Y_UNIT_TEST_F(ShouldHandleUndelivery, TSetupEnvironment8_8)
    {
        // Describe follower OK
        DescribeFollower();

        {   // Send write blocks request
            SendWriteRequest(10, "aaabbbccc", nullptr, 1000);

            // Intercept forwarded to follower write request.
            ui64 followerCookie = 0;
            auto followerWriteBlocks =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                    &followerCookie);

            // Undelivery
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                VolumeAsPartitionActor,
                followerWriteBlocks.release(),
                0,   // flags
                followerCookie));

            // Should get E_REJECTED error
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetError().GetMessage(),
                "Undelivery");
            UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
        }

        {   // Send WriteBlocksLocal request
            TString expandedBlocksContent;
            SendWriteBlocksLocalRequest(
                10,
                "aaabbbccc",
                &expandedBlocksContent,
                2000);

            // Intercept forwarded to follower write request.
            ui64 followerCookie = 0;
            auto followerWriteBlocks =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                    &followerCookie);

            // Undelivery
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                VolumeAsPartitionActor,
                followerWriteBlocks.release(),
                0,   // flags
                followerCookie));

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetError().GetMessage(),
                "Undelivery");
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }

        {   // Send zero blocks request
            SendZeroRequest(40, 50, 3000);

            // Intercept forwarded to follower zero request.
            ui64 followerCookie = 0;
            auto followerWriteBlocks =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksRequest>(
                    &followerCookie);

            // Undelivery
            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                VolumeAsPartitionActor,
                followerWriteBlocks.release(),
                0,   // flags
                followerCookie));

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetError().GetMessage(),
                "Undelivery");
            UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
        }
    }

    Y_UNIT_TEST_F(ShouldFailRequestsWhenZombie, TSetupEnvironment8_8)
    {
        // Describe follower OK
        DescribeFollower();

        ui64 hangRequestCookie = 0;
        {   // Send a write request, which "hangs" and poison pill, which
            // puts the actor to the zombie state.
            SendWriteRequest(1, "x", nullptr, 1000);

            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                &hangRequestCookie);

            ActorSystem.Send(new NActors::IEventHandle(
                VolumeAsPartitionActor,
                EdgeActor,
                new NActors::TEvents::TEvPoisonPill(),
                0,   // flags
                0    // cookie
                ));
        }

        {   // Send write blocks request
            SendWriteRequest(10, "aaabbbccc", nullptr, 2000);

            // Should get E_REJECTED error
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }

        {   // Send WriteBlocksLocal request
            TString expandedBlocksContent;
            SendWriteBlocksLocalRequest(
                10,
                "aaabbbccc",
                &expandedBlocksContent,
                3000);

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
        }

        {   // Send zero blocks request
            SendZeroRequest(40, 50, 4000);

            // Should get E_REJECTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(4000, cookie);
        }

        {
            // Reply to "hang" request
            ReplyFollowerWriteBlocks(MakeError(E_CANCELLED), hangRequestCookie);

            // Should get PoisonTaker response
            ui64 cookie = 0;
            auto poisonTaken =
                GrabEdgeEventAndCookie<NActors::TEvents::TEvPoisonTaken>(
                    &cookie);
            UNIT_ASSERT(poisonTaken);
        }

        {
            // Should get E_CANCELLED error for hang request
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_CANCELLED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
        }
    }

    Y_UNIT_TEST_F(ShouldExecuteWriteBlocksSameBlockSize, TSetupEnvironment8_8)
    {
        // Describe follower OK
        DescribeFollower();

        // User send write request.
        TString expandedSrcBlocksContent;
        SendWriteRequest(20, "aaabbbccc", &expandedSrcBlocksContent, 1000);

        // Intercept forwarded to follower write request.
        ui64 followerCookie = 0;
        auto followerWriteBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerWriteBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerWriteBlocks->Record.GetHeaders().GetClientId());

        TString expandedDstBlocksContent =
            ExtractBlockContent(followerWriteBlocks->Record.GetBlocks());
        UNIT_ASSERT_VALUES_EQUAL(
            expandedSrcBlocksContent,
            expandedDstBlocksContent);

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerWriteBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(20, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(9, followerRange.Size());

        // WriteBlocks on follower success.
        ReplyFollowerWriteBlocks(MakeError(S_OK), followerCookie);

        // Check user write response OK
        ui64 cookie = 0;
        auto writeResponse =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(&cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResponse->GetError().GetCode(),
            FormatError(writeResponse->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
    }

    Y_UNIT_TEST_F(
        ShouldExecuteWriteBlocksLocalSameBlockSize,
        TSetupEnvironment8_8)
    {
        // Describe follower OK
        DescribeFollower();

        // User send write request.
        TString expandedSrcBlocksContent;
        SendWriteBlocksLocalRequest(
            20,
            "aaabbbccc",
            &expandedSrcBlocksContent,
            2000);

        // Intercept forwarded to follower write request.
        ui64 followerCookie = 0;
        auto followerWriteBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerWriteBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerWriteBlocks->Record.GetHeaders().GetClientId());

        TString expandedDstBlocksContent =
            ExtractBlockContent(followerWriteBlocks->Record.GetBlocks());
        UNIT_ASSERT_VALUES_EQUAL(
            expandedSrcBlocksContent,
            expandedDstBlocksContent);

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerWriteBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(20, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(9, followerRange.Size());

        // WriteBlocks on follower success.
        ReplyFollowerWriteBlocks(MakeError(S_OK), followerCookie);

        // Check user write response OK
        ui64 cookie = 0;
        auto response =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                &cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
    }

    Y_UNIT_TEST_F(ShouldExecuteZeroBlocksSameBlockSize, TSetupEnvironment8_8)
    {
        // Describe follower OK
        DescribeFollower();

        // User send zero request.
        SendZeroRequest(20, 50, 3000);

        // Intercept forwarded to follower zero request.
        ui64 followerCookie = 0;
        auto followerZeroBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerZeroBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerZeroBlocks->Record.GetHeaders().GetClientId());

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerZeroBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(20, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(50, followerRange.Size());

        // ZeroBlocks on follower success.
        ReplyFollowerZeroBlocks(MakeError(S_OK), followerCookie);

        // Check user write response OK
        ui64 cookie = 0;
        auto zeroResponse =
            GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(&cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            zeroResponse->GetError().GetCode(),
            FormatError(zeroResponse->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
    }

    Y_UNIT_TEST_F(
        ShouldExecuteWriteBlocksFollowerSmallerBlockSize,
        TSetupEnvironment8_4)
    {
        // Describe follower OK
        DescribeFollower();

        // User send write request
        TString expandedSrcBlocksContent;
        SendWriteRequest(20, "aaabbbccc", &expandedSrcBlocksContent, 1000);

        // Intercept forwarded to follower write request
        ui64 followerCookie = 0;
        auto followerWriteBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerWriteBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerWriteBlocks->Record.GetHeaders().GetClientId());

        TString expandedDstBlocksContent =
            ExtractBlockContent(followerWriteBlocks->Record.GetBlocks());
        UNIT_ASSERT_VALUES_EQUAL(
            expandedSrcBlocksContent,
            expandedDstBlocksContent);

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerWriteBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(40, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(18, followerRange.Size());

        // WriteBlocks on follower success.
        ReplyFollowerWriteBlocks(MakeError(S_OK), followerCookie);

        // Check user write response OK
        ui64 cookie = 0;
        auto response =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(&cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
    }

    Y_UNIT_TEST_F(
        ShouldExecuteWriteBlocksLocalFollowerSmallerBlockSize,
        TSetupEnvironment8_4)
    {
        // Describe follower OK
        DescribeFollower();

        // User send write request
        TString expandedSrcBlocksContent;
        SendWriteBlocksLocalRequest(
            20,
            "aaabbbccc",
            &expandedSrcBlocksContent,
            2000);

        // Intercept forwarded to follower write request
        ui64 followerCookie = 0;
        auto followerWriteBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerWriteBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerWriteBlocks->Record.GetHeaders().GetClientId());

        TString expandedDstBlocksContent =
            ExtractBlockContent(followerWriteBlocks->Record.GetBlocks());
        UNIT_ASSERT_VALUES_EQUAL(
            expandedSrcBlocksContent,
            expandedDstBlocksContent);

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerWriteBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(40, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(18, followerRange.Size());

        // WriteBlocks on follower success.
        ReplyFollowerWriteBlocks(MakeError(S_OK), followerCookie);

        // Check user write response OK
        ui64 cookie = 0;
        auto response =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                &cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
    }

    Y_UNIT_TEST_F(
        ShouldExecuteZeroBlocksFollowerSmallerBlockSize,
        TSetupEnvironment8_4)
    {
        // Describe follower OK
        DescribeFollower();

        // User send zero request
        SendZeroRequest(20, 50, 3000);

        // Intercept forwarded to follower zero request
        ui64 followerCookie = 0;
        auto followerZeroBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerZeroBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerZeroBlocks->Record.GetHeaders().GetClientId());

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerZeroBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(40, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(100, followerRange.Size());

        // ZeroBlocks on follower success.
        ReplyFollowerZeroBlocks(MakeError(S_OK), followerCookie);

        // Check user response OK
        ui64 cookie = 0;
        auto response =
            GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(&cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
    }

    Y_UNIT_TEST_F(
        ShouldExecuteWriteBlocksFollowerLargerBlockSizeAligned,
        TSetupEnvironment4_8)
    {
        // Describe follower OK
        DescribeFollower();

        // User send write request
        TString expandedSrcBlocksContent;
        SendWriteRequest(10, "aaaabbbbcccc", &expandedSrcBlocksContent, 1000);

        // Intercept forwarded to follower write request
        ui64 followerCookie = 0;
        auto followerWriteBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerWriteBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerWriteBlocks->Record.GetHeaders().GetClientId());

        TString expandedDstBlocksContent =
            ExtractBlockContent(followerWriteBlocks->Record.GetBlocks());
        UNIT_ASSERT_VALUES_EQUAL(
            expandedSrcBlocksContent,
            expandedDstBlocksContent);

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerWriteBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(5, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(6, followerRange.Size());

        // WriteBlocks on follower success.
        ReplyFollowerWriteBlocks(MakeError(S_OK), followerCookie);

        // Check user write response OK
        ui64 cookie = 0;
        auto response =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(&cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
    }

    Y_UNIT_TEST_F(
        ShouldExecuteWriteBlocksLocalFollowerLargerBlockSizeAligned,
        TSetupEnvironment4_8)
    {
        // Describe follower OK
        DescribeFollower();

        // User send write request
        TString expandedSrcBlocksContent;
        SendWriteBlocksLocalRequest(
            10,
            "aaaabbbbcccc",
            &expandedSrcBlocksContent,
            2000);

        // Intercept forwarded to follower write request
        ui64 followerCookie = 0;
        auto followerWriteBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerWriteBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerWriteBlocks->Record.GetHeaders().GetClientId());

        TString expandedDstBlocksContent =
            ExtractBlockContent(followerWriteBlocks->Record.GetBlocks());
        UNIT_ASSERT_VALUES_EQUAL(
            expandedSrcBlocksContent,
            expandedDstBlocksContent);

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerWriteBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(5, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(6, followerRange.Size());

        // WriteBlocks on follower success.
        ReplyFollowerWriteBlocks(MakeError(S_OK), followerCookie);

        // Check user write response OK
        ui64 cookie = 0;
        auto response =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                &cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
    }

    Y_UNIT_TEST_F(
        ShouldExecuteZeroBlocksFollowerLargerBlockSizeAligned,
        TSetupEnvironment4_8)
    {
        // Describe follower OK
        DescribeFollower();

        // User send zero request
        SendZeroRequest(10, 50, 3000);

        // Intercept forwarded to follower write request
        ui64 followerCookie = 0;
        auto followerZeroBlocks =
            GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksRequest>(
                &followerCookie);
        UNIT_ASSERT_VALUES_EQUAL(
            FollowerDiskId,
            followerZeroBlocks->Record.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "copy-volume-client",
            followerZeroBlocks->Record.GetHeaders().GetClientId());

        const TBlockRange64 followerRange =
            BuildRequestBlockRange(*followerZeroBlocks, FollowerBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(5, followerRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(25, followerRange.Size());

        // ZeroBlocks on follower success.
        ReplyFollowerZeroBlocks(MakeError(S_OK), followerCookie);

        // Check userresponse OK
        ui64 cookie = 0;
        auto zeroResponse =
            GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(&cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            zeroResponse->GetError().GetCode(),
            FormatError(zeroResponse->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
    }

    Y_UNIT_TEST_F(ShouldFailOnUnaligned, TSetupEnvironment4_8)
    {
        // Describe follower OK
        DescribeFollower();

        {
            // Send write blocks request
            SendWriteRequest(10, "a", nullptr, 1000);

            // Should get E_NOT_IMPLEMENTED error
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
        }

        {   // Send WriteBlocksLocal request
            TString expandedBlocksContent;
            SendWriteBlocksLocalRequest(1, "aa", &expandedBlocksContent, 2000);

            // Should get E_NOT_IMPLEMENTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksLocalResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }

        {   // Send zero blocks request
            SendZeroRequest(1, 3, 3000);

            // Should get E_NOT_IMPLEMENTED response
            ui64 cookie = 0;
            auto response =
                GrabEdgeEventAndCookie<TEvService::TEvZeroBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(3000, cookie);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
