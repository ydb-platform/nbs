#include "read_and_clear_empty_blocks_actor.h"

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NBlobMarkers;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TForwardReadTests)
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
        NActors::TActorId EdgeActor;
        TActorSystem ActorSystem;

        void SetUp(NUnitTest::TTestContext&) override
        {
            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();
        }
    };

    Y_UNIT_TEST_F(ShouldReadSuccess, TSetupEnvironment)
    {
        TCompressedBitmap usedBlocks(10);
        usedBlocks.Set(1, 7);
        usedBlocks.Set(8, 9);

        auto request =
            TEvService::TReadBlocksMethod::TRequest::ProtoRecordType();
        request.SetStartIndex(0);
        request.SetBlocksCount(10);

        auto readActor = ActorSystem.Register(
            new TReadAndClearEmptyBlocksActor<TEvService::TReadBlocksMethod>{
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                request,
                usedBlocks,
                EdgeActor,
                0,
                EdgeActor,
                TLogTitle(0, TString("test"), GetCycleCount())
                    .GetChild(GetCycleCount())});

        ActorSystem.GrabEdgeEvent<TEvService::TReadBlocksMethod::TRequest>();

        auto response =
            std::make_unique<TEvService::TReadBlocksMethod::TResponse>();
        auto& buffers = *response->Record.MutableBlocks()->MutableBuffers();
        for (auto i = 0; i < 10; ++i) {
            // Reading returns 10 blocks filled with ones.
            buffers.Add(TString(512, 1));
        }

        ActorSystem.Send(new NActors::IEventHandle(
            readActor,
            EdgeActor,
            response.release()));

        auto fullResponse =
            ActorSystem
                .GrabEdgeEvent<TEvService::TReadBlocksMethod::TResponse>();

        const auto& fullBuffers = fullResponse->Record.GetBlocks().GetBuffers();
        const auto& unencrypted =
            fullResponse->Record.GetUnencryptedBlockMask();
        UNIT_ASSERT_EQUAL(0, unencrypted.size());

        // Check that the blocks marked as unused are filled with zeros.
        for (int i = 0; i < fullBuffers.size(); ++i) {
            auto expectedBlock = TString(512, usedBlocks.Test(i) ? 1 : 0);
            UNIT_ASSERT_EQUAL(expectedBlock, fullBuffers[i]);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
