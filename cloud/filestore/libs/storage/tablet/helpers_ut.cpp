#include "helpers.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <google/protobuf/util/message_differencer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/filestore_config.pb.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TConfigs
    : public NUnitTest::TBaseFixture
{
    NKikimrFileStore::TConfig SrcConfig;
    NProto::TFileSystem SrcFileSystem;
    NProto::TFileStorePerformanceProfile SrcPerformanceProfile;

    NProto::TFileSystem TargetFileSystem;
    NProtoPrivate::TFileSystemConfig TargetFileSystemConfig;
    TThrottlerConfig TargetThrottlerConfig;

    TString ReportDiff;
    google::protobuf::io::StringOutputStream Stream;
    google::protobuf::util::MessageDifferencer Comparator;
    google::protobuf::util::MessageDifferencer::StreamReporter Reporter;

    TConfigs()
        : Stream(&ReportDiff)
        , Reporter(&Stream)
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Comparator.set_report_matches(false);
        Comparator.set_report_moves(false);

        Comparator.ReportDifferencesTo(&Reporter);

        // Setup SrcConfig
        {
            auto profile = SrcConfig.MutableExplicitChannelProfiles()->Add();
            profile->SetDataKind(1);
            profile->SetPoolKind("test_pool_1");
        }
        {
            auto profile = SrcConfig.MutableExplicitChannelProfiles()->Add();
            profile->SetDataKind(2);
            profile->SetPoolKind("test_pool_2");
        }

        SrcConfig.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_HYBRID);
        SrcConfig.SetVersion(3);
        SrcConfig.SetFileSystemId("test_file_system_id_1");
        SrcConfig.SetProjectId("test_project_id_1");
        SrcConfig.SetFolderId("test_folder_id_1");
        SrcConfig.SetCloudId("test_cloud_id_1");
        SrcConfig.SetBlockSize(4);
        SrcConfig.SetBlocksCount(5);
        SrcConfig.SetNodesCount(6);
        SrcConfig.SetRangeIdHasherType(7);
        SrcConfig.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL);

        SrcConfig.SetPerformanceProfileThrottlingEnabled(true);
        SrcConfig.SetPerformanceProfileMaxReadIops(8);
        SrcConfig.SetPerformanceProfileMaxReadBandwidth(9);
        SrcConfig.SetPerformanceProfileMaxWriteIops(10);
        SrcConfig.SetPerformanceProfileMaxWriteBandwidth(11);
        SrcConfig.SetPerformanceProfileBoostTime(12);
        SrcConfig.SetPerformanceProfileBoostRefillTime(13);
        SrcConfig.SetPerformanceProfileBoostPercentage(14);
        SrcConfig.SetPerformanceProfileBurstPercentage(15);
        SrcConfig.SetPerformanceProfileMaxPostponedWeight(16);
        SrcConfig.SetPerformanceProfileMaxWriteCostMultiplier(17);
        SrcConfig.SetPerformanceProfileDefaultPostponedRequestWeight(18);
        SrcConfig.SetPerformanceProfileMaxPostponedTime(19);
        SrcConfig.SetPerformanceProfileMaxPostponedCount(20);

        for (size_t i = 0; i < SrcConfig.ExplicitChannelProfilesSize(); ++i) {
            const auto& tmpSrc = SrcConfig.GetExplicitChannelProfiles(i);
            auto& tmpDst =
                *TargetFileSystem.MutableExplicitChannelProfiles()->Add();

            tmpDst.SetDataKind(tmpSrc.GetDataKind());
            tmpDst.SetPoolKind(tmpSrc.GetPoolKind());
        }

        TargetFileSystem.SetVersion(SrcConfig.GetVersion());
        TargetFileSystem.SetFileSystemId(SrcConfig.GetFileSystemId());
        TargetFileSystem.SetProjectId(SrcConfig.GetProjectId());
        TargetFileSystem.SetFolderId(SrcConfig.GetFolderId());
        TargetFileSystem.SetCloudId(SrcConfig.GetCloudId());
        TargetFileSystem.SetBlockSize(SrcConfig.GetBlockSize());
        TargetFileSystem.SetBlocksCount(SrcConfig.GetBlocksCount());
        TargetFileSystem.SetNodesCount(SrcConfig.GetNodesCount());
        TargetFileSystem.SetRangeIdHasherType(SrcConfig.GetRangeIdHasherType());
        TargetFileSystem.SetStorageMediaKind(
            static_cast<NCloud::NProto::EStorageMediaKind>(
                SrcConfig.GetStorageMediaKind()));

        {
            // Performance profile.
            auto& performanceProfile = *TargetFileSystem.MutablePerformanceProfile();
            performanceProfile.SetThrottlingEnabled(
                SrcConfig.GetPerformanceProfileThrottlingEnabled());
            performanceProfile.SetMaxReadIops(
                SrcConfig.GetPerformanceProfileMaxReadIops());
            performanceProfile.SetMaxReadBandwidth(
                SrcConfig.GetPerformanceProfileMaxReadBandwidth());
            performanceProfile.SetMaxWriteIops(
                SrcConfig.GetPerformanceProfileMaxWriteIops());
            performanceProfile.SetMaxWriteBandwidth(
                SrcConfig.GetPerformanceProfileMaxWriteBandwidth());
            performanceProfile.SetBoostTime(
                SrcConfig.GetPerformanceProfileBoostTime());
            performanceProfile.SetBoostRefillTime(
                SrcConfig.GetPerformanceProfileBoostRefillTime());
            performanceProfile.SetBoostPercentage(
                SrcConfig.GetPerformanceProfileBoostPercentage());
            performanceProfile.SetBurstPercentage(
                SrcConfig.GetPerformanceProfileBurstPercentage());
            performanceProfile.SetMaxPostponedWeight(
                SrcConfig.GetPerformanceProfileMaxPostponedWeight());
            performanceProfile.SetMaxWriteCostMultiplier(
                SrcConfig.GetPerformanceProfileMaxWriteCostMultiplier());
            performanceProfile.SetDefaultPostponedRequestWeight(
                SrcConfig.GetPerformanceProfileDefaultPostponedRequestWeight());
            performanceProfile.SetMaxPostponedTime(
                SrcConfig.GetPerformanceProfileMaxPostponedTime());
            performanceProfile.SetMaxPostponedCount(
                SrcConfig.GetPerformanceProfileMaxPostponedCount());
        }

        // Setup SrcFileSystem
        {
            auto profile = SrcFileSystem.MutableExplicitChannelProfiles()->Add();
            profile->SetDataKind(3);
            profile->SetPoolKind("test_pool_3");
        }
        {
            auto profile = SrcFileSystem.MutableExplicitChannelProfiles()->Add();
            profile->SetDataKind(4);
            profile->SetPoolKind("test_pool_4");
        }

        SrcFileSystem.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_HDD);
        SrcFileSystem.SetVersion(21);
        SrcFileSystem.SetFileSystemId("test_file_system_id_2");
        SrcFileSystem.SetProjectId("test_project_id_2");
        SrcFileSystem.SetFolderId("test_folder_id_2");
        SrcFileSystem.SetCloudId("test_cloud_id_2");
        SrcFileSystem.SetBlockSize(22);
        SrcFileSystem.SetBlocksCount(23);
        SrcFileSystem.SetNodesCount(24);
        SrcFileSystem.SetRangeIdHasherType(25);
        SrcFileSystem.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL);

        SrcFileSystem.MutablePerformanceProfile()->SetThrottlingEnabled(true);
        SrcFileSystem.MutablePerformanceProfile()->SetMaxReadIops(26);
        SrcFileSystem.MutablePerformanceProfile()->SetMaxReadBandwidth(27);
        SrcFileSystem.MutablePerformanceProfile()->SetMaxWriteIops(28);
        SrcFileSystem.MutablePerformanceProfile()->SetMaxWriteBandwidth(29);
        SrcFileSystem.MutablePerformanceProfile()->SetBoostTime(30);
        SrcFileSystem.MutablePerformanceProfile()->SetBoostRefillTime(31);
        SrcFileSystem.MutablePerformanceProfile()->SetBoostPercentage(32);
        SrcFileSystem.MutablePerformanceProfile()->SetBurstPercentage(33);
        SrcFileSystem.MutablePerformanceProfile()->SetMaxPostponedWeight(34);
        SrcFileSystem.MutablePerformanceProfile()->SetMaxWriteCostMultiplier(35);
        SrcFileSystem.MutablePerformanceProfile()->SetDefaultPostponedRequestWeight(36);
        SrcFileSystem.MutablePerformanceProfile()->SetMaxPostponedTime(37);
        SrcFileSystem.MutablePerformanceProfile()->SetMaxPostponedCount(38);

        for (size_t i = 0; i < SrcFileSystem.ExplicitChannelProfilesSize(); ++i) {
            const auto& tmpSrc = SrcFileSystem.GetExplicitChannelProfiles(i);
            auto& tmpDst = *TargetFileSystemConfig.MutableExplicitChannelProfiles()->Add();

            tmpDst.SetDataKind(tmpSrc.GetDataKind());
            tmpDst.SetPoolKind(tmpSrc.GetPoolKind());
        }

        TargetFileSystemConfig.SetVersion(SrcFileSystem.GetVersion());
        TargetFileSystemConfig.SetFileSystemId(SrcFileSystem.GetFileSystemId());
        TargetFileSystemConfig.SetProjectId(SrcFileSystem.GetProjectId());
        TargetFileSystemConfig.SetFolderId(SrcFileSystem.GetFolderId());
        TargetFileSystemConfig.SetCloudId(SrcFileSystem.GetCloudId());
        TargetFileSystemConfig.SetBlockSize(SrcFileSystem.GetBlockSize());
        TargetFileSystemConfig.SetBlocksCount(SrcFileSystem.GetBlocksCount());
        TargetFileSystemConfig.SetNodesCount(SrcFileSystem.GetNodesCount());
        TargetFileSystemConfig.SetRangeIdHasherType(SrcFileSystem.GetRangeIdHasherType());
        TargetFileSystemConfig.SetStorageMediaKind(
            static_cast<NCloud::NProto::EStorageMediaKind>(
                SrcFileSystem.GetStorageMediaKind()));

        TargetFileSystemConfig.MutablePerformanceProfile()->CopyFrom(SrcFileSystem.GetPerformanceProfile());

        // Setup SrcThrottlerConfig
        SrcPerformanceProfile.SetThrottlingEnabled(true);
        SrcPerformanceProfile.SetMaxReadIops(39);
        SrcPerformanceProfile.SetMaxReadBandwidth(40);
        SrcPerformanceProfile.SetMaxWriteIops(41);
        SrcPerformanceProfile.SetMaxWriteBandwidth(42);
        SrcPerformanceProfile.SetBoostTime(43);
        SrcPerformanceProfile.SetBoostRefillTime(44);
        SrcPerformanceProfile.SetBoostRefillTime(45);
        SrcPerformanceProfile.SetBoostPercentage(46);
        SrcPerformanceProfile.SetBurstPercentage(47);
        SrcPerformanceProfile.SetMaxPostponedWeight(48);
        SrcPerformanceProfile.SetMaxWriteCostMultiplier(49);
        SrcPerformanceProfile.SetDefaultPostponedRequestWeight(50);
        SrcPerformanceProfile.SetMaxPostponedTime(51);
        SrcPerformanceProfile.SetMaxPostponedCount(52);

        TargetThrottlerConfig.ThrottlingEnabled =
            SrcPerformanceProfile.GetThrottlingEnabled();
        TargetThrottlerConfig.BurstPercentage =
            SrcPerformanceProfile.GetBurstPercentage();
        TargetThrottlerConfig.DefaultPostponedRequestWeight =
            SrcPerformanceProfile.GetDefaultPostponedRequestWeight();

        {
            // Default parameters.
            auto& p = TargetThrottlerConfig.DefaultParameters;
            p.MaxReadIops = SrcPerformanceProfile.GetMaxReadIops();
            p.MaxWriteIops = SrcPerformanceProfile.GetMaxWriteIops();
            p.MaxReadBandwidth = SrcPerformanceProfile.GetMaxReadBandwidth();
            p.MaxWriteBandwidth = SrcPerformanceProfile.GetMaxWriteBandwidth();
        }

        {
            // Boost parameters.
            auto& p = TargetThrottlerConfig.BoostParameters;
            p.BoostTime =
                TDuration::MilliSeconds(SrcPerformanceProfile.GetBoostTime());
            p.BoostRefillTime =
                TDuration::MilliSeconds(SrcPerformanceProfile.GetBoostRefillTime());
            p.BoostPercentage = SrcPerformanceProfile.GetBoostPercentage();
        }

        {
            // Default limits.
            auto& l = TargetThrottlerConfig.DefaultThresholds;
            l.MaxPostponedWeight = SrcPerformanceProfile.GetMaxPostponedWeight();
            l.MaxPostponedCount = SrcPerformanceProfile.GetMaxPostponedCount();
            l.MaxPostponedTime =
                TDuration::MilliSeconds(SrcPerformanceProfile.GetMaxPostponedTime());
            l.MaxWriteCostMultiplier = SrcPerformanceProfile.GetMaxWriteCostMultiplier();
        }
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THelpers)
{
    Y_UNIT_TEST_F(ShouldCorrectlyConvertConfigToFileSystem, TConfigs)
    {
        NProto::TFileSystem fileSystem;
        Convert(SrcConfig, fileSystem);

        Comparator.Compare(TargetFileSystem, fileSystem);
        UNIT_ASSERT_VALUES_EQUAL("", ReportDiff);
    }

    Y_UNIT_TEST_F(ShouldCorrectlyConvertFileSystemToFileSystemConfig, TConfigs)
    {
        NProtoPrivate::TFileSystemConfig fileSystemConfig;
        Convert(SrcFileSystem, fileSystemConfig);

        Comparator.Compare(TargetFileSystemConfig, fileSystemConfig);
        UNIT_ASSERT_VALUES_EQUAL("", ReportDiff);
    }

    Y_UNIT_TEST_F(ShouldCorrectlyConvertPerformanceProfileToThrottlerConfig, TConfigs)
    {
        TThrottlerConfig config;
        Convert(SrcPerformanceProfile, config);

        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.ThrottlingEnabled,
            config.ThrottlingEnabled);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.BurstPercentage,
            config.BurstPercentage);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultPostponedRequestWeight,
            config.DefaultPostponedRequestWeight);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultParameters.MaxReadIops,
            config.DefaultParameters.MaxReadIops);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultParameters.MaxReadBandwidth,
            config.DefaultParameters.MaxReadBandwidth);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultParameters.MaxWriteIops,
            config.DefaultParameters.MaxWriteIops);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultParameters.MaxWriteBandwidth,
            config.DefaultParameters.MaxWriteBandwidth);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.BoostParameters.BoostTime,
            config.BoostParameters.BoostTime);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.BoostParameters.BoostRefillTime,
            config.BoostParameters.BoostRefillTime);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.BoostParameters.BoostPercentage,
            config.BoostParameters.BoostPercentage);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultThresholds.MaxPostponedWeight,
            config.DefaultThresholds.MaxPostponedWeight);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultThresholds.MaxPostponedCount,
            config.DefaultThresholds.MaxPostponedCount);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultThresholds.MaxPostponedTime,
            config.DefaultThresholds.MaxPostponedTime);
        UNIT_ASSERT_VALUES_EQUAL(
            TargetThrottlerConfig.DefaultThresholds.MaxWriteCostMultiplier,
            config.DefaultThresholds.MaxWriteCostMultiplier);
    }
}

}   // namespace NCloud::NFileStore::NStorage
