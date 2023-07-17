#include "forward_read.h"

#include <library/cpp/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

static NActors::TActorId EdgeActor;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TForwardReadTests)
{
    struct TSetupEnvironment
        : public TCurrentTestCase
    {
        NActors::TTestActorRuntimeBase ActorSystem;

        void SetUp(NUnitTest::TTestContext&) override
        {
            static const TString volumeName = "VOLUME";
            ActorSystem.Initialize();
            ActorSystem.SetDispatchTimeout(TDuration::Seconds(5));
            ActorSystem.GetLogSettings(0)->Append(
                TBlockStoreComponents::VOLUME,
                TBlockStoreComponents::VOLUME + 1,
                [](int){ return volumeName; });


            EdgeActor = ActorSystem.AllocateEdgeActor();
        }
    };

    Y_UNIT_TEST_F(ShouldReadSuccess, TSetupEnvironment)
    {
        THashSet<ui64> unusedBlocks;
        unusedBlocks.insert(7);
        unusedBlocks.insert(10);
        unusedBlocks.insert(11);

        auto request = TEvService::TReadBlocksMethod::TRequest::ProtoRecordType();
        request.SetStartIndex(0);
        request.SetBlocksCount(10);

        auto readActor = ActorSystem.Register(
            new TReadActor<TEvService::TReadBlocksMethod>{
                MakeIntrusive<TRequestInfo>(
                    EdgeActor,
                    0ull,
                    MakeIntrusive<TCallContext>()),
                request,
                unusedBlocks,
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
            const char result  = !unusedBlocks.contains(i);
            for (size_t j = 0; j < fullBuffers[i].size(); ++j) {
                UNIT_ASSERT_EQUAL(fullBuffers[i][j], result);
            }
        }
        UNIT_ASSERT_EQUAL(unencrypted[0], static_cast<char>(0b10000000));
        UNIT_ASSERT_EQUAL(unencrypted[1], static_cast<char>(0b00001100));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
