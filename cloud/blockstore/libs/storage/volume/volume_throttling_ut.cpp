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

const ui64 maxBandwidth = 100;
const ui64 maxIops = 100;

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
        maxBandwidth,   // maxBandwidth
        maxIops,        // maxIops
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
    auto notification = std::make_unique<TEvVolumeThrottlingManager::TEvVolumeThrottlingConfigNotification>();
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
            maxBandwidth * readBwCoef,
            policy.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(maxBandwidth, policy.GetMaxWriteBandwidth());
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
            maxBandwidth * readBwCoef,
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
                maxBandwidth * readBwCoef,
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
                maxBandwidth,
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
            maxBandwidth * readBwCoef,
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
            maxBandwidth * readBwCoef,
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
            maxBandwidth * readBwCoef,
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
        UNIT_ASSERT_VALUES_EQUAL(maxBandwidth, policy.GetMaxReadBandwidth());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
