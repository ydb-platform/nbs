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
    NProto::TFileStorePerformanceProfile TargetProfile;
    NKikimrFileStore::TConfig TargetConfig;

    NProto::TFileStorePerformanceProfile SrcProfile;
    NKikimrFileStore::TConfig SrcConfig;

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

        // Setup PerformanceProfile
        SrcProfile.SetThrottlingEnabled(true);
        SrcProfile.SetMaxReadIops(1);
        SrcProfile.SetMaxReadBandwidth(2);
        SrcProfile.SetMaxWriteIops(3);
        SrcProfile.SetMaxWriteBandwidth(4);
        SrcProfile.SetBoostTime(5);
        SrcProfile.SetBoostRefillTime(6);
        SrcProfile.SetBoostPercentage(7);
        SrcProfile.SetBurstPercentage(8);
        SrcProfile.SetDefaultPostponedRequestWeight(9);
        SrcProfile.SetMaxPostponedWeight(10);
        SrcProfile.SetMaxWriteCostMultiplier(11);
        SrcProfile.SetMaxPostponedTime(12);
        SrcProfile.SetMaxPostponedCount(13);

        TargetConfig.SetPerformanceProfileThrottlingEnabled(
            SrcProfile.GetThrottlingEnabled());
        TargetConfig.SetPerformanceProfileMaxReadIops(
            SrcProfile.GetMaxReadIops());
        TargetConfig.SetPerformanceProfileMaxReadBandwidth(
            SrcProfile.GetMaxReadBandwidth());
        TargetConfig.SetPerformanceProfileMaxWriteIops(
            SrcProfile.GetMaxWriteIops());
        TargetConfig.SetPerformanceProfileMaxWriteBandwidth(
            SrcProfile.GetMaxWriteBandwidth());
        TargetConfig.SetPerformanceProfileBoostTime(
            SrcProfile.GetBoostTime());
        TargetConfig.SetPerformanceProfileBoostRefillTime(
            SrcProfile.GetBoostRefillTime());
        TargetConfig.SetPerformanceProfileBoostPercentage(
            SrcProfile.GetBoostPercentage());
        TargetConfig.SetPerformanceProfileBurstPercentage(
            SrcProfile.GetBurstPercentage());
        TargetConfig.SetPerformanceProfileDefaultPostponedRequestWeight(
            SrcProfile.GetDefaultPostponedRequestWeight());
        TargetConfig.SetPerformanceProfileMaxPostponedWeight(
            SrcProfile.GetMaxPostponedWeight());
        TargetConfig.SetPerformanceProfileMaxWriteCostMultiplier(
            SrcProfile.GetMaxWriteCostMultiplier());
        TargetConfig.SetPerformanceProfileMaxPostponedTime(
            SrcProfile.GetMaxPostponedTime());
        TargetConfig.SetPerformanceProfileMaxPostponedCount(
            SrcProfile.GetMaxPostponedCount());

        // Setup TConfig
        SrcConfig.SetPerformanceProfileThrottlingEnabled(true);
        SrcConfig.SetPerformanceProfileMaxReadIops(14);
        SrcConfig.SetPerformanceProfileMaxReadBandwidth(15);
        SrcConfig.SetPerformanceProfileMaxWriteIops(16);
        SrcConfig.SetPerformanceProfileMaxWriteBandwidth(17);
        SrcConfig.SetPerformanceProfileBoostTime(18);
        SrcConfig.SetPerformanceProfileBoostRefillTime(19);
        SrcConfig.SetPerformanceProfileBoostPercentage(20);
        SrcConfig.SetPerformanceProfileBurstPercentage(21);
        SrcConfig.SetPerformanceProfileDefaultPostponedRequestWeight(22);
        SrcConfig.SetPerformanceProfileMaxPostponedWeight(23);
        SrcConfig.SetPerformanceProfileMaxWriteCostMultiplier(24);
        SrcConfig.SetPerformanceProfileMaxPostponedTime(25);
        SrcConfig.SetPerformanceProfileMaxPostponedCount(26);

        TargetProfile.SetThrottlingEnabled(
            SrcConfig.GetPerformanceProfileThrottlingEnabled());
        TargetProfile.SetMaxReadIops(
            SrcConfig.GetPerformanceProfileMaxReadIops());
        TargetProfile.SetMaxReadBandwidth(
            SrcConfig.GetPerformanceProfileMaxReadBandwidth());
        TargetProfile.SetMaxWriteIops(
            SrcConfig.GetPerformanceProfileMaxWriteIops());
        TargetProfile.SetMaxWriteBandwidth(
            SrcConfig.GetPerformanceProfileMaxWriteBandwidth());
        TargetProfile.SetBoostTime(
            SrcConfig.GetPerformanceProfileBoostTime());
        TargetProfile.SetBoostRefillTime(
            SrcConfig.GetPerformanceProfileBoostRefillTime());
        TargetProfile.SetBoostPercentage(
            SrcConfig.GetPerformanceProfileBoostPercentage());
        TargetProfile.SetBurstPercentage(
            SrcConfig.GetPerformanceProfileBurstPercentage());
        TargetProfile.SetDefaultPostponedRequestWeight(
            SrcConfig.GetPerformanceProfileDefaultPostponedRequestWeight());
        TargetProfile.SetMaxPostponedWeight(
            SrcConfig.GetPerformanceProfileMaxPostponedWeight());
        TargetProfile.SetMaxWriteCostMultiplier(
            SrcConfig.GetPerformanceProfileMaxWriteCostMultiplier());
        TargetProfile.SetMaxPostponedTime(
            SrcConfig.GetPerformanceProfileMaxPostponedTime());
        TargetProfile.SetMaxPostponedCount(
            SrcConfig.GetPerformanceProfileMaxPostponedCount());
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THelpers)
{
    Y_UNIT_TEST_F(ShouldCorrectlyConvertConfigToProfile, TConfigs)
    {
        NProto::TFileStorePerformanceProfile profile;
        Convert(SrcConfig, profile);

        Comparator.Compare(TargetProfile, profile);
        UNIT_ASSERT_VALUES_EQUAL("", ReportDiff);
    }

    Y_UNIT_TEST_F(ShouldCorrectlyConvertProfileToConfig, TConfigs)
    {
        NKikimrFileStore::TConfig config;
        Convert(SrcProfile, config);

        Comparator.Compare(TargetConfig, config);
        UNIT_ASSERT_VALUES_EQUAL("", ReportDiff);
    }
}

}   // namespace NCloud::NFileStore::NStorage
