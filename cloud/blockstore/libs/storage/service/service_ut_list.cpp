#include "service_ut.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceListVolumesTest)
{
    Y_UNIT_TEST(ShouldListVolumes)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TVector<TString> expected = {
            "volume1",
            "volume2",
            "volume3",
            "volume4"};

        TServiceClient service(env.GetRuntime(), nodeIdx);

        for (const auto& diskId: expected) {
            service.CreateVolume(diskId);
        }

        auto response = service.ListVolumes();
        const auto& volumesProto = response->Record.GetVolumes();

        TVector<TString> volumes;
        Copy(
            volumesProto.begin(),
            volumesProto.end(),
            std::back_inserter(volumes));
        Sort(volumes);

        UNIT_ASSERT_VALUES_EQUAL(volumes, expected);
    }

    Y_UNIT_TEST(ShouldFailListVolumesIfDescribeSchemeFails)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("volume1");
        service.CreateVolume("volume2");

        auto error = MakeError(E_ARGUMENT, "Error");

        runtime.SetObserverFunc(
            [nodeIdx, error, &runtime](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeSchemeRequest: {
                        auto response = std::make_unique<
                            TEvSSProxy::TEvDescribeSchemeResponse>(error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0,   // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendListVolumesRequest();
        auto response = service.RecvListVolumesResponse();
        UNIT_ASSERT(response->GetStatus() == error.GetCode());
        UNIT_ASSERT(response->GetErrorReason() == error.GetMessage());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
