#include "tablet_proxy.h"

#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/ss_proxy_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_proxy_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletProxyTest)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.CreateFileStore("test", 1000);

        TIndexTabletProxyClient tabletProxy(env.GetRuntime(), nodeIdx);
        tabletProxy.WaitReady("test");
    }

    Y_UNIT_TEST(ShouldDetectRemoteTabletDeath)
    {
        TTestEnvConfig cfg {.StaticNodes = 1, .DynamicNodes = 2};
        TTestEnv env(cfg);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx1 = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();
        TServiceClient service1(runtime, nodeIdx1);

        ui64 fsTabletId = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        if (!fsTabletId) {
                            auto* msg = event->Get<TEvSSProxy::TEvDescribeFileStoreResponse>();
                            const auto& fsDescription =
                                msg->PathDescription.GetFileStoreDescription();
                            fsTabletId = fsDescription.GetIndexTabletId();
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service1.CreateFileStore("fs", 1000);

//        auto nodeIdx2 = end.CreateNode("nfs");
//        TServiceClient service2(runtime, nodeIdx2);

//        service2.SendRequest(
//            MakeVolumeProxyServiceId(),
//            service2.CreateStatVolumeRequest());

        ui64 disconnections = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvClientDestroyed: {
                        auto* msg = event->Get<TEvTabletPipe::TEvClientDestroyed>();
                        if (msg->TabletId == fsTabletId) {
                            ++disconnections;                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        RebootTablet(runtime, fsTabletId, service1.GetSender(), nodeIdx1);
//        UNIT_ASSERT_VALUES_EQUAL(2, disconnections);

        //auto response = service2.RecvStatVolumeResponse();
        //UNIT_ASSERT(FAILED(response->GetStatus()));
    }
}

}   // namespace NCloud::NFileStore::NStorage
