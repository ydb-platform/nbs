#include "helpers.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <google/protobuf/util/message_differencer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

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

    auto MakeListNodesInternalResponse()
    {
        NProtoPrivate::TListNodesInternalResponse internalResponse;
        Store("name1", "shard1", "nodename1", 0, internalResponse);
        Store("name2", "", "", 1, internalResponse);
        internalResponse.AddNodes()->SetId(111);
        Store("name3", "", "", 2, internalResponse);
        internalResponse.AddNodes()->SetId(222);
        Store("name4", "shard2", "nodename2", 3, internalResponse);
        Store("name5", "", "", 4, internalResponse);
        internalResponse.AddNodes()->SetId(333);
        internalResponse.SetCookie("cookie");
        return internalResponse;
    }

    Y_UNIT_TEST(ShouldConvertListNodesInternalResponse)
    {
        auto internalResponse = MakeListNodesInternalResponse();

        NProto::TListNodesResponse response;
        Convert(internalResponse, response);

        UNIT_ASSERT_C(
            !HasError(response.GetError()),
            FormatError(response.GetError()));

        UNIT_ASSERT_VALUES_EQUAL(5, response.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("name1", response.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL("name2", response.GetNames(1));
        UNIT_ASSERT_VALUES_EQUAL("name3", response.GetNames(2));
        UNIT_ASSERT_VALUES_EQUAL("name4", response.GetNames(3));
        UNIT_ASSERT_VALUES_EQUAL("name5", response.GetNames(4));

        UNIT_ASSERT_VALUES_EQUAL(5, response.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "shard1",
            response.GetNodes(0).GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(
            "nodename1",
            response.GetNodes(0).GetShardNodeName());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            response.GetNodes(1).GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL("", response.GetNodes(1).GetShardNodeName());
        UNIT_ASSERT_VALUES_EQUAL(111, response.GetNodes(1).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            response.GetNodes(2).GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL("", response.GetNodes(2).GetShardNodeName());
        UNIT_ASSERT_VALUES_EQUAL(222, response.GetNodes(2).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            "shard2",
            response.GetNodes(3).GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(
            "nodename2",
            response.GetNodes(3).GetShardNodeName());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            response.GetNodes(4).GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL("", response.GetNodes(4).GetShardNodeName());
        UNIT_ASSERT_VALUES_EQUAL(333, response.GetNodes(4).GetId());

        UNIT_ASSERT_VALUES_EQUAL("cookie", response.GetCookie());
    }

    Y_UNIT_TEST(ShouldConvertListNodesInternalResponseError)
    {
        NProtoPrivate::TListNodesInternalResponse internalResponse;
        internalResponse.MutableError()->SetCode(E_REJECTED);
        NProto::TListNodesResponse response;
        Convert(internalResponse, response);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    Y_UNIT_TEST(ShouldConvertListNodesInternalResponseBroken)
    {
        auto internalResponse = MakeListNodesInternalResponse();
        internalResponse.MutableNameBuffer()->resize(
            internalResponse.GetNameBuffer().size() / 2);

        NProto::TListNodesResponse response;
        Convert(internalResponse, response);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        internalResponse = MakeListNodesInternalResponse();
        internalResponse.MutableExternalRefBuffer()->resize(
            internalResponse.GetExternalRefBuffer().size() / 2);

        response.Clear();
        Convert(internalResponse, response);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        internalResponse = MakeListNodesInternalResponse();
        internalResponse.ClearExternalRefIndices();

        response.Clear();
        Convert(internalResponse, response);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        internalResponse = MakeListNodesInternalResponse();
        internalResponse.ClearShardIdSizes();

        response.Clear();
        Convert(internalResponse, response);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        internalResponse = MakeListNodesInternalResponse();
        internalResponse.ClearShardNodeNameSizes();

        response.Clear();
        Convert(internalResponse, response);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        internalResponse = MakeListNodesInternalResponse();
        internalResponse.ClearNodes();

        response.Clear();
        Convert(internalResponse, response);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }
}

}   // namespace NCloud::NFileStore::NStorage
