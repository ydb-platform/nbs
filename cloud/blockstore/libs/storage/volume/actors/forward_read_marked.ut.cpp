#include "forward_read_marked.h"

#include <library/cpp/actors/testlib/test_runtime.h>
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

        auto request = TEvService::TReadBlocksMethod::TRequest::ProtoRecordType();
        request.SetStartIndex(0);
        request.SetBlocksCount(10);

        auto readActor = ActorSystem.Register(
            new TReadMarkedActor<TEvService::TReadBlocksMethod>{
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                request,
                usedBlocks,
                true,
                true,
                EdgeActor,
                0,
                EdgeActor});

        ActorSystem.GrabEdgeEvent<TEvService::TReadBlocksMethod::TRequest>();

        auto response = new TEvService::TReadBlocksMethod::TResponse();
        auto& buffers = *response->Record.MutableBlocks()->MutableBuffers();
        for (auto i = 0; i < 10; ++i) {
            buffers.Add(TString(512, 1));
        }

        ActorSystem.Send(
            new NActors::IEventHandle(readActor, EdgeActor, response));

        auto fullResponse = ActorSystem.GrabEdgeEvent<
            TEvService::TReadBlocksMethod::TResponse>();

        const auto& fullBuffers = fullResponse->Record.GetBlocks().GetBuffers();
        const auto& unencrypted = fullResponse->Record.GetUnencryptedBlockMask();

        for (int i = 0; i < fullBuffers.size(); ++i) {
            for (size_t j = 0; j < fullBuffers[i].size(); ++j) {
                UNIT_ASSERT_EQUAL(fullBuffers[i][j], usedBlocks.Test(i));
            }
        }
        UNIT_ASSERT_EQUAL(unencrypted[0], static_cast<char>(0b10000001));
        UNIT_ASSERT_EQUAL(unencrypted[1], static_cast<char>(0b00000010));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
