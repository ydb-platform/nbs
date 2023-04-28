#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceDescribeVolumeTest)
{
    Y_UNIT_TEST(ShouldDescribeVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.DescribeVolume();
    }

    Y_UNIT_TEST(ShouldFailDescribeVolumeWithEmptyDiskId)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        service.SendDescribeVolumeRequest(TString());
        auto response = service.RecvDescribeVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
        UNIT_ASSERT(response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldFailDescribeVolumeIfDescribeSchemeFails)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto error = MakeError(E_ARGUMENT, "Error");

        runtime.SetObserverFunc(
            [=] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        auto response = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

        service.SendDescribeVolumeRequest();
        auto response = service.RecvDescribeVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == error.GetCode());
        UNIT_ASSERT(response->GetErrorReason() == error.GetMessage());
    }

    Y_UNIT_TEST(ShouldFailDescribeVolumeIfDescribeSchemeReturnsWrongPathType)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        runtime.SetObserverFunc(
            [] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        auto& pathDescription = const_cast<NKikimrSchemeOp::TPathDescription&>(msg->PathDescription);
                        pathDescription.MutableSelf()->SetPathType(
                            NKikimrSchemeOp::EPathTypeSolomonVolume);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

        service.SendDescribeVolumeRequest(TString());
        auto response = service.RecvDescribeVolumeResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
        UNIT_ASSERT(response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldReturnTokenVersionInDescribeVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId);

        service.AssignVolume();

        auto response = service.DescribeVolume();
        UNIT_ASSERT(response->Record.GetVolume().GetTokenVersion() == 1);
    }

    Y_UNIT_TEST(ShouldDescribeNonReplicatedDisks)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        {
            auto request = service.CreateCreateVolumeRequest(
                DefaultDiskId,
                200_GB / DefaultBlockSize,
                DefaultBlockSize,
                "", // folderId
                "", // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_UNEQUAL(
                response->Record.GetVolume().GetDevices().size(),
                0);
            for (const auto& device : response->Record.GetVolume().GetDevices()) {
                UNIT_ASSERT_VALUES_UNEQUAL(
                    device.GetBlockCount(),
                    0);
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
