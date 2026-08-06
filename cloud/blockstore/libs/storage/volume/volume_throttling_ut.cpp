#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume_throttling_manager.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/volume/model/volume_throttling_policy.h>
#include <cloud/blockstore/libs/storage/volume/testlib/test_env.h>

#include <contrib/ydb/core/testlib/actors/test_runtime.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NTestVolume;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString diskId = "vol0";
const TString cloudId = "cloud";
const TString folderId = "folder";
const TString otherDiskId = "vol1";

constexpr ui64 MaxBandwidth = 100;
constexpr ui64 MaxIops = 100;

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumeThrottlingRule CreateSpecificDisksRule(
    const TVector<TString>& diskIds,
    double readBwCoef = 1.0,
    double writeBwCoef = 1.0)
{
    NProto::TVolumeThrottlingRule rule;
    for (const auto& id: diskIds) {
        rule.MutableDisks()->AddDiskIds(id);
    }
    rule.MutableCoefficients()->SetMaxReadBandwidth(readBwCoef);
    rule.MutableCoefficients()->SetMaxWriteBandwidth(writeBwCoef);
    return rule;
}

NProto::TVolumeThrottlingRule CreateFilterRule(
    const TVector<TString>& cloudIds,
    const TVector<TString>& folderIds,
    const TVector<NProto::EStorageMediaKind>& mediaKinds,
    double readBwCoef = 1.0,
    double writeBwCoef = 1.0)
{
    NProto::TVolumeThrottlingRule rule;
    auto* filter = rule.MutableFilter();
    for (const auto& id: cloudIds) {
        filter->AddCloudIds(id);
    }
    for (const auto& id: folderIds) {
        filter->AddFolderIds(id);
    }
    for (const auto& kind: mediaKinds) {
        filter->AddMediaKinds(kind);
    }
    rule.MutableCoefficients()->SetMaxReadBandwidth(readBwCoef);
    rule.MutableCoefficients()->SetMaxWriteBandwidth(writeBwCoef);
    return rule;
}

TVolumeClient CreateVolume(
    TTestActorRuntime& runtime,
    const TString& disk = diskId,
    NProto::EStorageMediaKind media = NProto::STORAGE_MEDIA_HDD)
{
    TVolumeClient volume(runtime);
    volume.UpdateVolumeConfig(
        MaxBandwidth,   // maxBandwidth
        MaxIops,        // maxIops
        100,            // burstPercentage
        100,            // maxPostponedWeight
        true,           // throttlingEnabled
        1,              // version
        media,
        2048,       // block count per partition
        disk,       // diskId
        cloudId,    // cloudId
        folderId,   // folderId
        2,          // partitions count
        2           // blocksPerStripe
    );

    volume.WaitReady();

    auto stat = volume.StatVolume();
    const auto& v = stat->Record.GetVolume();
    UNIT_ASSERT_VALUES_EQUAL(disk, v.GetDiskId());
    UNIT_ASSERT_VALUES_EQUAL(folderId, v.GetFolderId());
    UNIT_ASSERT_VALUES_EQUAL(cloudId, v.GetCloudId());

    return volume;
}

////////////////////////////////////////////////////////////////////////////////

void SendThrottlingConfig(
    TVolumeClient& volume,
    ui32 version,
    const TVector<NProto::TVolumeThrottlingRule>& rules)
{
    auto notification = std::make_unique<
        TEvVolumeThrottlingManager::TEvVolumeThrottlingConfigNotification>();
    notification->Config.SetVersion(version);
    for (const auto& rule: rules) {
        *notification->Config.AddRules() = rule;
    }
    volume.SendToPipe(std::move(notification));
}

NProto::TVolumeVolatileThrottlingInfo GetVolatileThrottlingInfo(
    TVolumeClient& volume)
{
    return volume.StatVolume()->Record.GetVolatileThrottlingInfo();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeActorUpdateThrottlingConfigTest)
{
    Y_UNIT_TEST(ShouldApplyNewConfig)
    {
        auto runtime = PrepareTestActorRuntime();
        TVolumeClient volume = CreateVolume(*runtime);

        double readBwCoef = 0.5;
        ui32 version = 1;
        // Send notification with version 1
        SendThrottlingConfig(
            volume,
            version,
            {CreateSpecificDisksRule({diskId}, readBwCoef)});
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // Verify policy updated
        const auto& throttlingInfo = GetVolatileThrottlingInfo(volume);
        UNIT_ASSERT_VALUES_EQUAL(version, throttlingInfo.GetVersion());

        const auto& policy = throttlingInfo.GetActualPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(
            MaxBandwidth * readBwCoef,
            policy.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(MaxBandwidth, policy.GetMaxWriteBandwidth());
    }

    Y_UNIT_TEST(ShouldIgnoreOldConfig)
    {
        auto runtime = PrepareTestActorRuntime();
        TVolumeClient volume = CreateVolume(*runtime);

        // Set initial version 5
        double readBwCoef = 0.5;
        ui32 version = 5;
        SendThrottlingConfig(
            volume,
            version,
            {CreateSpecificDisksRule({diskId}, readBwCoef)});
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // Send older version 3
        SendThrottlingConfig(
            volume,
            3,
            {CreateSpecificDisksRule({diskId}, 0.3)});
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // Verify policy unchanged (still 0.5 coefficient)
        const auto& throttlingInfo = GetVolatileThrottlingInfo(volume);
        UNIT_ASSERT_VALUES_EQUAL(version, throttlingInfo.GetVersion());

        const auto& policy = throttlingInfo.GetActualPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(
            MaxBandwidth * readBwCoef,
            policy.GetMaxReadBandwidth());
    }

    Y_UNIT_TEST(ShouldResetOnEmptyConfig)
    {
        auto runtime = PrepareTestActorRuntime();
        TVolumeClient volume = CreateVolume(*runtime);

        {
            // Set initial coefficient
            ui32 version = 1;
            double readBwCoef = 0.5;
            SendThrottlingConfig(
                volume,
                version,
                {CreateSpecificDisksRule({diskId}, readBwCoef)});
            runtime->DispatchEvents({}, TDuration::Seconds(1));

            // Verify policy updated
            const auto& throttlingInfo = GetVolatileThrottlingInfo(volume);
            UNIT_ASSERT_VALUES_EQUAL(version, throttlingInfo.GetVersion());

            const auto& policy = throttlingInfo.GetActualPerformanceProfile();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBandwidth * readBwCoef,
                policy.GetMaxReadBandwidth());
        }

        {
            // Reset with empty config
            ui32 version = 2;
            SendThrottlingConfig(volume, version, {});
            runtime->DispatchEvents({}, TDuration::Seconds(1));

            // Verify policy reset to defaults
            const auto& throttlingInfo = GetVolatileThrottlingInfo(volume);
            UNIT_ASSERT_VALUES_EQUAL(version, throttlingInfo.GetVersion());

            const auto& policy = throttlingInfo.GetActualPerformanceProfile();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBandwidth,
                policy.GetMaxReadBandwidth());
        }
    }

    Y_UNIT_TEST(ShouldPrioritizeSpecificDiskOverFilter)
    {
        auto runtime = PrepareTestActorRuntime();
        TVolumeClient volume = CreateVolume(*runtime);

        // Send conflicting rules - specific disk should take precedence
        ui32 version = 1;
        double readBwCoef = 0.5;
        SendThrottlingConfig(
            volume,
            version,
            {CreateFilterRule(
                 {cloudId},
                 {folderId},
                 {NProto::STORAGE_MEDIA_HDD},
                 0.3),
             CreateSpecificDisksRule({diskId}, readBwCoef)});
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // Verify specific disk rule applied
        const auto& throttlingInfo = GetVolatileThrottlingInfo(volume);
        UNIT_ASSERT_VALUES_EQUAL(version, throttlingInfo.GetVersion());

        const auto& policy = throttlingInfo.GetActualPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(
            MaxBandwidth * readBwCoef,
            policy.GetMaxReadBandwidth());
    }

    Y_UNIT_TEST(ShouldApplyLastMatchingFilterRule)
    {
        auto runtime = PrepareTestActorRuntime();
        TVolumeClient volume = CreateVolume(*runtime);

        // Send multiple matching filter rules - last (ruleB) should win
        ui32 version = 1;
        double readBwCoef = 0.5;
        auto ruleA = CreateFilterRule(
            {cloudId},
            {folderId},
            {NProto::STORAGE_MEDIA_HDD},
            0.3);
        auto ruleB = CreateFilterRule(
            {cloudId},
            {folderId},
            {NProto::STORAGE_MEDIA_HDD},
            readBwCoef);
        SendThrottlingConfig(volume, version, {ruleA, ruleB});
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // Verify last matching rule applied
        const auto& throttlingInfo = GetVolatileThrottlingInfo(volume);
        UNIT_ASSERT_VALUES_EQUAL(version, throttlingInfo.GetVersion());

        const auto& policy = throttlingInfo.GetActualPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(
            MaxBandwidth * readBwCoef,
            policy.GetMaxReadBandwidth());
    }

    Y_UNIT_TEST(ShouldApplyCorrectRuleBasedOnMediaKind)
    {
        auto runtime = PrepareTestActorRuntime();
        TVolumeClient volume =
            CreateVolume(*runtime, diskId, NProto::STORAGE_MEDIA_SSD);

        // Rules with different media kinds
        ui32 version = 1;
        double readBwCoef = 0.5;
        SendThrottlingConfig(
            volume,
            version,
            {CreateFilterRule({}, {}, {NProto::STORAGE_MEDIA_HDD}, 0.3),
             CreateFilterRule(
                 {},
                 {},
                 {NProto::STORAGE_MEDIA_SSD},
                 readBwCoef)});
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // Verify SSD rule applied
        const auto& throttlingInfo = GetVolatileThrottlingInfo(volume);
        UNIT_ASSERT_VALUES_EQUAL(version, throttlingInfo.GetVersion());

        const auto& policy = throttlingInfo.GetActualPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(
            MaxBandwidth * readBwCoef,
            policy.GetMaxReadBandwidth());
    }

    Y_UNIT_TEST(ShouldNotApplyToUnmatchedVolume)
    {
        auto runtime = PrepareTestActorRuntime();
        TVolumeClient volume = CreateVolume(*runtime);

        // Rule for different cloud
        ui32 version = 1;
        SendThrottlingConfig(
            volume,
            1,
            {CreateFilterRule({"other-cloud"}, {}, {}, 0.5)});
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // Verify policy unchanged
        const auto& throttlingInfo = GetVolatileThrottlingInfo(volume);
        UNIT_ASSERT_VALUES_EQUAL(version, throttlingInfo.GetVersion());

        const auto& policy = throttlingInfo.GetActualPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(MaxBandwidth, policy.GetMaxReadBandwidth());
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeActorShapingThrottlingTest)
{
    Y_UNIT_TEST(ShouldAddShapingDelayAndScheduleResponseOnSuccess)
    {
        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetThrottlingEnabled(true);
        {
            auto* quota = storageConfig.MutableShapingThrottlerConfig()
                              ->MutableHddQuota();
            quota->MutableWrite()->SetIops(1);
            quota->MutableWrite()->SetBandwidth(1_MB);
            quota->SetExpectedIoParallelism(1);
            quota->SetMaxBudget(1);
            quota->SetBudgetRefillTime(1000);
            quota->SetBudgetSpendRate(1.0);
        }

        auto runtime = PrepareTestActorRuntime(storageConfig);
        TVolumeClient volume = CreateVolume(*runtime);
        volume.UpdateVolumeConfig(
            1_GB,      // maxBandwidth
            100'000,   // maxIops
            100,       // burstPercentage
            1000_MB,   // maxPostponedWeight
            true,      // throttlingEnabled
            2,         // version
            NProto::STORAGE_MEDIA_HDD,
            2048,       // block count per partition
            diskId,     // diskId
            cloudId,    // cloudId
            folderId,   // folderId
            2,          // partitions count
            2           // blocksPerStripe
        );

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        bool firstWriteRequest = true;
        TDuration scheduledDelay;
        TAutoPtr<IEventHandle> scheduledResponse;

        runtime->SetScheduledEventFilter(
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event,
                TDuration delay,
                TInstant& deadline)
            {
                Y_UNUSED(runtime);

                if (event->GetTypeRewrite() ==
                        TEvService::EvWriteBlocksResponse &&
                    delay != TDuration::Zero())
                {
                    scheduledDelay = delay;
                    scheduledResponse = event.Release();
                    deadline = runtime.GetCurrentTime();
                    return true;
                }

                return false;
            });

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvService::EvWriteBlocksRequest) {
                    if (firstWriteRequest) {
                        firstWriteRequest = false;
                        return false;
                    }

                    runtime.SendAsync(new IEventHandle(
                        /*recipient=*/event->Sender,   // volume actor
                        /*sender=*/event->Recipient,   // partition actor
                        new TEvService::TEvWriteBlocksResponse(MakeError(S_OK)),
                        0,   // flags
                        event->Cookie,
                        nullptr));
                    return true;
                }

                return false;
            });

        volume.SendWriteBlocksRequest(
            TBlockRange64::WithLength(0, 1000),
            clientInfo.GetClientId(),
            1);

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT(scheduledResponse);
        UNIT_ASSERT_UNEQUAL(TDuration::Zero(), scheduledDelay);

        runtime->Send(scheduledResponse.Release());
        auto response = volume.RecvWriteBlocksResponse();
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

        const auto& throttler = response->Record.GetHeaders().GetThrottler();
        UNIT_ASSERT_VALUES_EQUAL(
            scheduledDelay.MicroSeconds(),
            throttler.GetShapingDelay());
    }

    Y_UNIT_TEST(ShouldNotApplyShapingDelayOnFailure)
    {
        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetThrottlingEnabled(true);
        {
            auto* quota = storageConfig.MutableShapingThrottlerConfig()
                              ->MutableHddQuota();
            quota->MutableWrite()->SetIops(10);
            quota->MutableWrite()->SetBandwidth(10_MB);
            quota->SetExpectedIoParallelism(1);
            quota->SetMaxBudget(1);
            quota->SetBudgetRefillTime(1000);
            quota->SetBudgetSpendRate(1.0);
        }

        auto runtime = PrepareTestActorRuntime(storageConfig);
        TVolumeClient volume = CreateVolume(*runtime);
        volume.UpdateVolumeConfig(
            1_GB,      // maxBandwidth
            100'000,   // maxIops
            100,       // burstPercentage
            1000_MB,   // maxPostponedWeight
            true,      // throttlingEnabled
            2,         // version
            NProto::STORAGE_MEDIA_HDD,
            2048,       // block count per partition
            diskId,     // diskId
            cloudId,    // cloudId
            folderId,   // folderId
            2,          // partitions count
            2           // blocksPerStripe
        );

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        bool firstWriteResponse = true;
        bool scheduled = false;

        runtime->SetScheduledEventFilter(
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event,
                TDuration delay,
                TInstant& deadline)
            {
                Y_UNUSED(runtime);
                Y_UNUSED(delay);
                Y_UNUSED(deadline);

                if (event->GetTypeRewrite() ==
                    TEvService::TEvWriteBlocksResponse::EventType)
                {
                    scheduled = true;
                }
                return false;
            });

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) -> bool
            {
                if (event->GetTypeRewrite() ==
                    TEvService::EvWriteBlocksResponse) {
                    if (firstWriteResponse) {
                        auto* response =
                            event->Get<TEvService::TEvWriteBlocksResponse>();
                        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
                        *response->Record.MutableError() =
                            MakeError(E_REJECTED);
                        firstWriteResponse = false;
                    }
                }

                return false;
            });

        volume.SendWriteBlocksRequest(
            TBlockRange64::WithLength(0, 1000),
            clientInfo.GetClientId(),
            1);

        auto response = volume.RecvWriteBlocksResponse();
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        UNIT_ASSERT_C(!scheduled, "Failure response should not be scheduled");

        const auto& throttler = response->Record.GetHeaders().GetThrottler();
        UNIT_ASSERT_VALUES_EQUAL(0, throttler.GetShapingDelay());
    }

    Y_UNIT_TEST(ShouldNotApplyShapingDelayWhenThrottlingIsDisabled)
    {
        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetThrottlingEnabled(false);
        {
            auto* quota = storageConfig.MutableShapingThrottlerConfig()
                              ->MutableHddQuota();
            quota->MutableWrite()->SetIops(10);
            quota->MutableWrite()->SetBandwidth(10_MB);
            quota->SetExpectedIoParallelism(1);
            quota->SetMaxBudget(1);
            quota->SetBudgetRefillTime(1000);
            quota->SetBudgetSpendRate(1.0);
        }

        auto runtime = PrepareTestActorRuntime(storageConfig);
        TVolumeClient volume = CreateVolume(*runtime);
        volume.UpdateVolumeConfig(
            1_GB,      // maxBandwidth
            100'000,   // maxIops
            100,       // burstPercentage
            1000_MB,   // maxPostponedWeight
            true,      // throttlingEnabled
            2,         // version
            NProto::STORAGE_MEDIA_HDD,
            2048,       // block count per partition
            diskId,     // diskId
            cloudId,    // cloudId
            folderId,   // folderId
            2,          // partitions count
            2           // blocksPerStripe
        );

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        bool scheduled = false;
        runtime->SetScheduledEventFilter(
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event,
                TDuration delay,
                TInstant& deadline)
            {
                Y_UNUSED(runtime);
                Y_UNUSED(delay);
                Y_UNUSED(deadline);

                if (event->GetTypeRewrite() ==
                    TEvService::TEvWriteBlocksResponse::EventType)
                {
                    scheduled = true;
                }
                return false;
            });

        auto response = volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1000),
            clientInfo.GetClientId(),
            1);

        UNIT_ASSERT_C(!scheduled, "Failure response should not be scheduled");
        const auto& throttler = response->Record.GetHeaders().GetThrottler();
        UNIT_ASSERT_VALUES_EQUAL(0, throttler.GetShapingDelay());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
