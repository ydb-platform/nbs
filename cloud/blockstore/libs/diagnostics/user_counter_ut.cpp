#include "user_counter.h"

#include <cloud/storage/core/libs/diagnostics/histogram_types.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/text/text.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NUserCounter {

using namespace NCloud::NStorage::NUserStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

NJson::TJsonValue GetValue(const auto& object, const auto& name)
{
    for (const auto& data: object["sensors"].GetArray()) {
        if (data["labels"]["name"] == name) {
            if (!data.Has("hist")) {
                return data["value"];
            }
        }
    }
    UNIT_ASSERT_C(false, "Value not found " + name);
    return NJson::TJsonValue{};
};

NJson::TJsonValue GetHist(
    const auto& object, const auto& name, const auto& valueName)
{
    for (const auto& data: object["sensors"].GetArray()) {
        if (data["labels"]["name"] == name) {
            if (data.Has("hist")) {
                return data["hist"][valueName];
            }
        }
    }
    UNIT_ASSERT_C(false, "Value not found " + name + "/" + valueName);
    return NJson::TJsonValue{};
};

void ValidateJsons(
    const NJson::TJsonValue& expectedJson,
    const NJson::TJsonValue& actualJson)
{
    for(const auto& jsonValue: expectedJson["sensors"].GetArray()) {
        const TString name = jsonValue["labels"]["name"].GetString();

        if (jsonValue.Has("hist")) {
            for (const auto* valueName: {"bounds", "buckets", "inf"}) {
                UNIT_ASSERT_STRINGS_EQUAL_C(
                    NJson::WriteJson(GetHist(expectedJson, name, valueName)),
                    NJson::WriteJson(GetHist(actualJson, name, valueName)),
                    name
                );
            }
        } else {
            UNIT_ASSERT_STRINGS_EQUAL_C(
                NJson::WriteJson(GetValue(expectedJson, name)),
                NJson::WriteJson(GetValue(actualJson, name)),
                name
            );
        }
    }
}

void SetTimeHistogramCountersUs(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
    const TString& histName,
    bool setSingleCounter)
{
    auto subgroup = counters->GetSubgroup("histogram", histName)
                        ->GetSubgroup("units", "usec");

    if (setSingleCounter) {
        const auto& buckets = TRequestUsTimeBuckets::Buckets;
        const auto& bounds = ConvertToHistBounds(buckets);
        auto histogram = subgroup->GetHistogram(
            histName,
            NMonitoring::ExplicitHistogram(bounds));
        histogram->Collect(1., 1);
        histogram->Collect(100., 2);
        histogram->Collect(200., 3);
        histogram->Collect(300., 4);
        histogram->Collect(400., 5);
        histogram->Collect(500., 6);
        histogram->Collect(600., 7);
        histogram->Collect(700., 8);
        histogram->Collect(800., 9);
        histogram->Collect(900., 0);
        histogram->Collect(1000., 1);
        histogram->Collect(2000., 2);
        histogram->Collect(5000., 3);
        histogram->Collect(10000., 4);
        histogram->Collect(20000., 5);
        histogram->Collect(50000., 6);
        histogram->Collect(100000., 7);
        histogram->Collect(200000., 8);
        histogram->Collect(500000., 9);
        histogram->Collect(1000000., 10);
        histogram->Collect(2000000., 11);
        histogram->Collect(5000000., 12);
        histogram->Collect(10000000., 13);
        histogram->Collect(35000000., 14);
        histogram->Collect(100000000., 15); // Inf
    } else {
        subgroup->GetCounter("1")->Set(1);
        subgroup->GetCounter("100")->Set(2);
        subgroup->GetCounter("200")->Set(3);
        subgroup->GetCounter("300")->Set(4);
        subgroup->GetCounter("400")->Set(5);
        subgroup->GetCounter("500")->Set(6);
        subgroup->GetCounter("600")->Set(7);
        subgroup->GetCounter("700")->Set(8);
        subgroup->GetCounter("800")->Set(9);
        subgroup->GetCounter("900")->Set(0);
        subgroup->GetCounter("1000")->Set(1);
        subgroup->GetCounter("2000")->Set(2);
        subgroup->GetCounter("5000")->Set(3);
        subgroup->GetCounter("10000")->Set(4);
        subgroup->GetCounter("20000")->Set(5);
        subgroup->GetCounter("50000")->Set(6);
        subgroup->GetCounter("100000")->Set(7);
        subgroup->GetCounter("200000")->Set(8);
        subgroup->GetCounter("500000")->Set(9);
        subgroup->GetCounter("1000000")->Set(10);
        subgroup->GetCounter("2000000")->Set(11);
        subgroup->GetCounter("5000000")->Set(12);
        subgroup->GetCounter("10000000")->Set(13);
        subgroup->GetCounter("35000000")->Set(14);
        subgroup->GetCounter("Inf")->Set(15);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TUserWrapperTest)
{
    Y_UNIT_TEST(UserServerVolumeInstanceTests)
    {
        auto setCounters = [](const NMonitoring::TDynamicCounterPtr& stats,
                              const TString& name,
                              bool setSingleCounter)
        {
            auto request = stats->GetSubgroup("request", name);
            request->GetCounter("Count")->Set(1);
            request->GetCounter("MaxCount")->Set(10);
            request->GetCounter("Errors/Fatal")->Set(2);
            request->GetCounter("RequestBytes")->Set(3);
            request->GetCounter("MaxRequestBytes")->Set(30);
            request->GetCounter("InProgress")->Set(4);
            request->GetCounter("MaxInProgress")->Set(40);
            request->GetCounter("InProgressBytes")->Set(5);
            request->GetCounter("MaxInProgressBytes")->Set(50);

            SetTimeHistogramCountersUs(request, "Time", setSingleCounter);
        };

        struct TTestConfiguration
        {
            bool ReportZeroBlocksMetrics;
            bool ReportHistogramAsSingleCounter;
            TString Resource;
        };

        std::vector<TTestConfiguration> testConfigurations = {
            {true, false, "user_server_volume_instance_test"},
            {false, false, "user_server_volume_instance_skip_zero_blocks_test"},
            {true, true, "user_server_volume_instance_test"},
            {false, true, "user_server_volume_instance_skip_zero_blocks_test"}};

        for (const auto& config: testConfigurations) {
            auto stats = MakeIntrusive<TDynamicCounters>();
            const auto setSingleCounter = config.ReportHistogramAsSingleCounter;
            setCounters(stats, "ReadBlocks", setSingleCounter);
            setCounters(stats, "WriteBlocks", setSingleCounter);
            setCounters(stats, "ZeroBlocks", setSingleCounter);

            auto supplier = CreateUserCounterSupplier();
            RegisterServerVolumeInstance(
                *supplier,
                "cloudId",
                "folderId",
                "diskId",
                "instanceId",
                config.ReportZeroBlocksMetrics,
                stats);

            const TString testResult = NResource::Find(config.Resource);

            NJson::TJsonValue testJson =
                NJson::ReadJsonFastTree(testResult, true);

            TStringStream jsonOut;
            auto encoder = EncoderJson(&jsonOut);
            supplier->Accept(TInstant::Seconds(12), encoder.Get());

            NJson::TJsonValue resultJson =
                NJson::ReadJsonFastTree(jsonOut.Str(), true);

            ValidateJsons(testJson, resultJson);
        }
    }

    Y_UNIT_TEST(UserServiceVolumeInstanceTests)
    {
        auto makeCounters = [](const NMonitoring::TDynamicCounterPtr& stats,
                               const TString& name,
                               bool setSingleCounter)
        {
            stats->GetCounter("UsedQuota")->Set(1);
            stats->GetCounter("MaxUsedQuota")->Set(10);

            auto request = stats->GetSubgroup("request", name);
            SetTimeHistogramCountersUs(
                request,
                "ThrottlerDelay",
                setSingleCounter);
        };

        for (bool reportHistogramAsSingleCounter: {false, true}) {
            auto stats = MakeIntrusive<TDynamicCounters>();

            makeCounters(stats, "ReadBlocks", reportHistogramAsSingleCounter);
            makeCounters(stats, "WriteBlocks", reportHistogramAsSingleCounter);
            makeCounters(stats, "ZeroBlocks", reportHistogramAsSingleCounter);

            auto supplier = CreateUserCounterSupplier();
            RegisterServiceVolume(
                *supplier,
                "cloudId",
                "folderId",
                "diskId",
                stats);

            const TString testResult =
                NResource::Find("user_service_volume_instance_test");

            NJson::TJsonValue testJson =
                NJson::ReadJsonFastTree(testResult, true);

            TStringStream jsonOut;
            auto encoder = EncoderJson(&jsonOut);
            supplier->Accept(TInstant::Seconds(12), encoder.Get());

            NJson::TJsonValue resultJson =
                NJson::ReadJsonFastTree(jsonOut.Str(), true);

            ValidateJsons(testJson, resultJson);
        }
    }
}

}   // namespace NCloud::NBlockStore::NUserCounter
