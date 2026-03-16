#include "service_ut.h"


#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/private/api/protos/checkpoints.pb.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceVhostDiscardTest)
{
    void AssertVhostDiscardEnabled(
        TServiceClient& service,
        const TString& diskId,
        bool expectedEnabled)
    {
        auto response = service.DescribeVolume(diskId);
        const auto& volume = response->Record.GetVolume();
        UNIT_ASSERT_VALUES_EQUAL(
            expectedEnabled,
            static_cast<bool>(volume.GetVhostDiscardEnabled()));
    }

    void SetVhostDiscardEnabledFlag(
        TServiceClient& service,
        const TString& diskId,
        bool discardEnabled)
    {
        NPrivateProto::TSetVhostDiscardEnabledFlagRequest request;
        request.SetDiskId(diskId);
        request.SetVhostDiscardEnabled(discardEnabled);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        service.ExecuteAction("setvhostdiscardenabledflag", buf);

        auto volumeConfig = GetVolumeConfig(service, diskId);
        UNIT_ASSERT_VALUES_EQUAL(
            discardEnabled,
            volumeConfig.GetVhostDiscardEnabled());
    }

    Y_UNIT_TEST(ShouldEnableVhostDiscardOnVolumeRestart)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        NProto::TFeaturesConfig featuresConfig;

        {
            auto* feature = featuresConfig.AddFeatures();
            feature->SetName("EnableVhostDiscardOnVolumeRestart");
            auto* whitelist = feature->MutableWhitelist();
            *whitelist->AddEntityIds() = "vol0";
        }

        ui32 nodeIdx = SetupTestEnv(env, config, featuresConfig);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        ui32 eventsSent = 0;
        ui64 volumeTabletId = 0;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvEnableVhostDiscardFlag: {
                        auto* msg =
                            event->Get<TEvService::TEvEnableVhostDiscardFlag>();
                        UNIT_ASSERT_VALUES_EQUAL(msg->DiskId, "vol0");
                        ++eventsSent;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.CreateVolume(
            "vol0",
            1024,
            DefaultBlockSize,
            TString(),  // folderId
            TString(),  // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD
        );

        ui32 expectedEventsSent = 0;

        AssertVhostDiscardEnabled(service, "vol0", false);
        UNIT_ASSERT_VALUES_EQUAL(expectedEventsSent, eventsSent);

        UNIT_ASSERT(volumeTabletId);

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        ++expectedEventsSent;
        AssertVhostDiscardEnabled(service, "vol0", true);
        UNIT_ASSERT_VALUES_EQUAL(expectedEventsSent, eventsSent);

        // Should not send event again.
        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        AssertVhostDiscardEnabled(service, "vol0", true);
        UNIT_ASSERT_VALUES_EQUAL(expectedEventsSent, eventsSent);

        SetVhostDiscardEnabledFlag(service, "vol0", false);
        UNIT_ASSERT_VALUES_EQUAL(expectedEventsSent, eventsSent);

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        ++expectedEventsSent;
        AssertVhostDiscardEnabled(service, "vol0", true);
        UNIT_ASSERT_VALUES_EQUAL(expectedEventsSent, eventsSent);
    }

    Y_UNIT_TEST(ShouldNotEnableVhostDiscardOnVolumeRestartIfFeatureDisabled)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        ui32 eventsSent = 0;
        ui64 volumeTabletId = 0;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvEnableVhostDiscardFlag: {
                        auto* msg =
                            event->Get<TEvService::TEvEnableVhostDiscardFlag>();
                        UNIT_ASSERT_VALUES_EQUAL(msg->DiskId, "vol0");
                        ++eventsSent;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.CreateVolume(
            "vol0",
            1024,
            DefaultBlockSize,
            TString(),  // folderId
            TString(),  // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD
        );

        AssertVhostDiscardEnabled(service, "vol0", false);
        UNIT_ASSERT_VALUES_EQUAL(0, eventsSent);

        UNIT_ASSERT(volumeTabletId);

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);

        AssertVhostDiscardEnabled(service, "vol0", false);
        UNIT_ASSERT_VALUES_EQUAL(0, eventsSent);

        SetVhostDiscardEnabledFlag(service, "vol0", true);
        UNIT_ASSERT_VALUES_EQUAL(0, eventsSent);

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        AssertVhostDiscardEnabled(service, "vol0", true);
        UNIT_ASSERT_VALUES_EQUAL(0, eventsSent);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
