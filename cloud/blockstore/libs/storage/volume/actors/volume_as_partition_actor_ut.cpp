#include "volume_as_partition_actor.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

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

static std::unique_ptr<TEvSSProxy::TEvDescribeVolumeResponse>
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

static std::unique_ptr<TEvService::TEvWriteBlocksRequest>
MakeWriteBlocksRequest(
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

    *expandedBlocksContent = ExtractBlockContent(result->Record.GetBlocks());

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

    struct TSetupEnvironment: public TCurrentTestCase
    {
        TActorSystem ActorSystem;
        NActors::TActorId EdgeActor;

        void SetUp(NUnitTest::TTestContext&) override
        {
            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();

            ActorSystem.RegisterService(
                MakeVolumeProxyServiceId(),
                EdgeActor,
                0);

            ActorSystem.RegisterService(MakeSSProxyServiceId(), EdgeActor, 0);
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
    };

    Y_UNIT_TEST_F(ShouldRejectDuringFollowerDescribing, TSetupEnvironment)
    {
        const ui32 leaderBlockSize = 4096;
        const TString leaderDiskId = "leader-disk";
        const TString followerDiskId = "follower-disk";
        TLogTitle logTitle(10, leaderDiskId, GetCycleCount());

        auto volumeAsPartitionActor =
            ActorSystem.Register(new TVolumeAsPartitionActor{
                logTitle.GetChild(GetCycleCount()),
                leaderBlockSize,
                followerDiskId});

        // Intercept describe request
        auto describeRequest =
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
        UNIT_ASSERT_VALUES_EQUAL(followerDiskId, describeRequest->DiskId);

        // Send write request when describe not finished
        TString expandedSrcBlocksContent;
        ActorSystem.Send(new NActors::IEventHandle(
            volumeAsPartitionActor,
            EdgeActor,
            MakeWriteBlocksRequest(
                leaderDiskId,
                10,
                leaderBlockSize,
                "aaabbbccc",
                &expandedSrcBlocksContent)
                .release(),
            0,     // flags
            1000   // cookie
            ));

        // Should get E_REJECTED response
        ui64 cookie = 0;
        auto writeResponse =
            GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(&cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            writeResponse->GetError().GetCode(),
            FormatError(writeResponse->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
    }

    Y_UNIT_TEST_F(
        ShouldResponseAbortedWhenFollowerDescribeFailed,
        TSetupEnvironment)
    {
        const ui32 leaderBlockSize = 4096;
        const TString leaderDiskId = "leader-disk";
        const TString followerDiskId = "follower-disk";
        TLogTitle logTitle(10, leaderDiskId, GetCycleCount());

        auto volumeAsPartitionActor =
            ActorSystem.Register(new TVolumeAsPartitionActor{
                logTitle.GetChild(GetCycleCount()),
                leaderBlockSize,
                followerDiskId});

        // Describe follower failed
        {
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
            ActorSystem.Send(new NActors::IEventHandle(
                volumeAsPartitionActor,
                EdgeActor,
                new TEvSSProxy::TEvDescribeVolumeResponse(
                    MakeError(MAKE_SCHEMESHARD_ERROR(
                        NKikimrScheme::StatusPathDoesNotExist)))));
        }

        // Send write request
        {
            TString expandedSrcBlocksContent;
            ActorSystem.Send(new NActors::IEventHandle(
                volumeAsPartitionActor,
                EdgeActor,
                MakeWriteBlocksRequest(
                    leaderDiskId,
                    10,
                    leaderBlockSize,
                    "aaabbbccc",
                    &expandedSrcBlocksContent)
                    .release(),
                0,     // flags
                1000   // cookie
                ));

            // Should get E_ABORTED response
            ui64 cookie = 0;
            auto writeResponse =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                writeResponse->GetError().GetCode(),
                FormatError(writeResponse->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1000, cookie);
        }
    }

    Y_UNIT_TEST_F(ShouldForwardWriteBlocks, TSetupEnvironment)
    {
        const ui32 leaderBlockSize = 4096;
        const ui32 followerBlockSize = 4096;
        const ui32 followerBlockCount = 16384;
        const TString leaderDiskId = "leader-disk";
        const TString followerDiskId = "follower-disk";
        TLogTitle logTitle(10, leaderDiskId, GetCycleCount());

        auto volumeAsPartitionActor =
            ActorSystem.Register(new TVolumeAsPartitionActor{
                logTitle.GetChild(GetCycleCount()),
                leaderBlockSize,
                followerDiskId});

        // Describe follower OK
        {
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
            ActorSystem.Send(new NActors::IEventHandle(
                volumeAsPartitionActor,
                EdgeActor,
                MakeDescribeVolumeResponse(
                    followerDiskId,
                    followerBlockSize,
                    followerBlockCount)
                    .release()));
        }

        // User send write request
        TString expandedSrcBlocksContent;
        {
            ActorSystem.Send(new NActors::IEventHandle(
                volumeAsPartitionActor,
                EdgeActor,
                MakeWriteBlocksRequest(
                    leaderDiskId,
                    10,
                    leaderBlockSize,
                    "aaabbbccc",
                    &expandedSrcBlocksContent)
                    .release(),
                0,     // flags
                2000   // cookie
                ));
        }

        // Intercept forwarded to follower write request
        ui64 followerCookie = 0;
        {
            auto followerWriteBlocks =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksRequest>(
                    &followerCookie);
            UNIT_ASSERT_VALUES_EQUAL(
                followerDiskId,
                followerWriteBlocks->Record.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(
                "copy-volume-client",
                followerWriteBlocks->Record.GetHeaders().GetClientId());

            TString expandedDstBlocksContent =
                ExtractBlockContent(followerWriteBlocks->Record.GetBlocks());
            UNIT_ASSERT_VALUES_EQUAL(
                expandedSrcBlocksContent,
                expandedDstBlocksContent);
        }

        {
            // Simulating successful write from the follower
            ActorSystem.Send(new NActors::IEventHandle(
                volumeAsPartitionActor,
                EdgeActor,
                new TEvService::TEvWriteBlocksResponse(MakeError(S_OK)),
                0,   // flags
                followerCookie));
        }

        // Check user write response OK
        {
            ui64 cookie = 0;
            auto leaderWriteResponse =
                GrabEdgeEventAndCookie<TEvService::TEvWriteBlocksResponse>(
                    &cookie);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                leaderWriteResponse->GetError().GetCode(),
                FormatError(leaderWriteResponse->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2000, cookie);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
