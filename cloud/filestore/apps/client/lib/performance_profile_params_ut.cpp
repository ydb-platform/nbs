#include "performance_profile_params.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <google/protobuf/util/message_differencer.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <array>
#include <vector>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TAppOpts
    : public NUnitTest::TBaseFixture
{
    const ui64 MaxReadBandwidth = 1;
    const ui32 MaxReadIops = 2;
    const ui64 MaxWriteBandwidth = 3;
    const ui32 MaxWriteIops = 4;
    const ui32 BoostTime = 5;
    const ui32 BoostRefillTime = 6;
    const ui32 BoostPercentage = 7;
    const ui32 BurstPercentage = 8;
    const ui64 DefaultPostponedRequestWeight = 9;
    const ui64 MaxPostponedWeight = 10;
    const ui32 MaxWriteCostMultiplier = 11;
    const ui32 MaxPostponedTime = 12;
    const ui32 MaxPostponedCount = 13;

    NLastGetopt::TOpts Opts;
    const TPerformanceProfileParams PerformanceProfileParams;
    TMaybe<NLastGetopt::TOptsParseResultException> OptsParseResult;

    TString ReportDiff;
    google::protobuf::io::StringOutputStream Stream;
    google::protobuf::util::MessageDifferencer Comparator;
    google::protobuf::util::MessageDifferencer::StreamReporter Reporter;

    NProto::TFileStorePerformanceProfile TargetProfile;

    TAppOpts()
        : PerformanceProfileParams(Opts)
        , Stream(&ReportDiff)
        , Reporter(&Stream)
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Comparator.set_report_matches(false);
        Comparator.set_report_moves(false);

        Comparator.ReportDifferencesTo(&Reporter);

        std::array<std::string, 28> arr = {
            "./test-filestore-client",
            "--performance-profile-throttling-enabled",
            "--performance-profile-max-read-bandwidth",
            std::to_string(MaxReadBandwidth),
            "--performance-profile-max-read-iops",
            std::to_string(MaxReadIops),
            "--performance-profile-max-write-bandwidth",
            std::to_string(MaxWriteBandwidth),
            "--performance-profile-max-write-iops",
            std::to_string(MaxWriteIops),
            "--performance-profile-boost-time",
            std::to_string(BoostTime),
            "--performance-profile-boost-refill-time",
            std::to_string(BoostRefillTime),
            "--performance-profile-boost-percentage",
            std::to_string(BoostPercentage),
            "--performance-profile-burst-percentage",
            std::to_string(BurstPercentage),
            "--performance-profile-default-postponed-request-weight",
            std::to_string(DefaultPostponedRequestWeight),
            "--performance-profile-max-postponed-weight",
            std::to_string(MaxPostponedWeight),
            "--performance-profile-max-write-cost-multiplier",
            std::to_string(MaxWriteCostMultiplier),
            "--performance-profile-max-postponed-time",
            std::to_string(MaxPostponedTime),
            "--performance-profile-max-postponed-count",
            std::to_string(MaxPostponedCount)
        };

        std::vector<const char*> vec;
        vec.reserve(arr.size());
        std::transform(
            std::begin(arr),
            std::end(arr),
            std::back_inserter(vec),
            [](const std::string& str) { return str.c_str(); });

        OptsParseResult.ConstructInPlace(&Opts, vec.size(), vec.data());

        TargetProfile.SetThrottlingEnabled(true);
        TargetProfile.SetMaxReadIops(MaxReadIops);
        TargetProfile.SetMaxReadBandwidth(MaxReadBandwidth);
        TargetProfile.SetMaxWriteIops(MaxWriteIops);
        TargetProfile.SetMaxWriteBandwidth(MaxWriteBandwidth);
        TargetProfile.SetBoostTime(BoostTime);
        TargetProfile.SetBoostRefillTime(BoostRefillTime);
        TargetProfile.SetBoostPercentage(BoostPercentage);
        TargetProfile.SetBurstPercentage(BurstPercentage);
        TargetProfile.SetDefaultPostponedRequestWeight(DefaultPostponedRequestWeight);
        TargetProfile.SetMaxPostponedWeight(MaxPostponedWeight);
        TargetProfile.SetMaxWriteCostMultiplier(MaxWriteCostMultiplier);
        TargetProfile.SetMaxPostponedTime(MaxPostponedTime);
        TargetProfile.SetMaxPostponedCount(MaxPostponedCount);
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPerformanceProfileParams)
{
    Y_UNIT_TEST_F(ShouldCorectlyFillCreateFileStoreRequest, TAppOpts)
    {
        auto request = std::make_shared<NProto::TCreateFileStoreRequest>();
        PerformanceProfileParams.FillRequest(*request);

        Comparator.Compare(TargetProfile, request->GetPerformanceProfile());
        UNIT_ASSERT_VALUES_EQUAL("", ReportDiff);
    }

    Y_UNIT_TEST_F(ShouldCorrectlyFillResizeFileStoreRequest, TAppOpts)
    {
        auto request = std::make_shared<NProto::TResizeFileStoreRequest>();
        PerformanceProfileParams.FillRequest(*request);

        Comparator.Compare(TargetProfile, request->GetPerformanceProfile());
        UNIT_ASSERT_VALUES_EQUAL("", ReportDiff);
    }
}

}   // namespace NCloud::NFileStore::NClient
