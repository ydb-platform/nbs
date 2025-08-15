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
    UNIT_ASSERT(false);
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
    UNIT_ASSERT(false);
    return NJson::TJsonValue{};
};

void ValidateJsons(
    const NJson::TJsonValue& testJson,
    const NJson::TJsonValue& resultJson)
{
    for(const auto& jsonValue: testJson["sensors"].GetArray()) {
        const TString name = jsonValue["labels"]["name"].GetString();

        if (jsonValue.Has("hist")) {
            for (auto valueName: {"bounds", "buckets", "inf"}) {
                UNIT_ASSERT_STRINGS_EQUAL_C(
                    NJson::WriteJson(GetHist(resultJson, name, valueName)),
                    NJson::WriteJson(GetHist(testJson, name, valueName)),
                    name
                );
            }
        } else {
            UNIT_ASSERT_STRINGS_EQUAL_C(
                NJson::WriteJson(GetValue(resultJson, name)),
                NJson::WriteJson(GetValue(testJson, name)),
                name
            );
        }
    }
}

void ValidateTestResult(
    const std::shared_ptr<IUserCounterSupplier>& supplier,
    const TString& canonicResultResourceName)
{
    const TString testResult = NResource::Find(canonicResultResourceName);

    NJson::TJsonValue testJson =
        NJson::ReadJsonFastTree(testResult, true);

    TStringStream jsonOut;
    auto encoder = NMonitoring::EncoderJson(&jsonOut);
    supplier->Accept(TInstant::Seconds(12), encoder.Get());

    NJson::TJsonValue resultJson =
        NJson::ReadJsonFastTree(jsonOut.Str(), true);

    ValidateJsons(testJson, resultJson);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TUserWrapperTest)
{
    Y_UNIT_TEST(UserServerVolumeInstanceTests)
    {
        NMonitoring::TDynamicCounterPtr stats;

        auto makeCounters = [&stats] (const TString& name, bool reportHistogramAsSingleCounter) {
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

            auto requestTimeGroup =
                request->GetSubgroup("histogram", "Time");
            if (reportHistogramAsSingleCounter) {
                const auto& buckets = TRequestMsTimeBuckets::Buckets;
                const auto& bounds = ConvertToHistBounds(buckets);
                auto requestTimeHistogram = requestTimeGroup->GetHistogram(
                            "Time",
                            NMonitoring::ExplicitHistogram(bounds));
                requestTimeHistogram->Collect(0.001, 1);
                requestTimeHistogram->Collect(0.1, 2);
                requestTimeHistogram->Collect(0.2, 3);
                requestTimeHistogram->Collect(0.3, 4);
                requestTimeHistogram->Collect(0.4, 5);
                requestTimeHistogram->Collect(0.5, 6);
                requestTimeHistogram->Collect(0.6, 7);
                requestTimeHistogram->Collect(0.7, 8);
                requestTimeHistogram->Collect(0.8, 9);
                requestTimeHistogram->Collect(0.9, 0);
                requestTimeHistogram->Collect(1., 1);
                requestTimeHistogram->Collect(2., 2);
                requestTimeHistogram->Collect(5., 3);
                requestTimeHistogram->Collect(10., 4);
                requestTimeHistogram->Collect(20., 5);
                requestTimeHistogram->Collect(50., 6);
                requestTimeHistogram->Collect(100., 7);
                requestTimeHistogram->Collect(200., 8);
                requestTimeHistogram->Collect(500., 9);
                requestTimeHistogram->Collect(1000., 10);
                requestTimeHistogram->Collect(2000., 11);
                requestTimeHistogram->Collect(5000., 12);
                requestTimeHistogram->Collect(10000., 13);
                requestTimeHistogram->Collect(35000., 14);
                requestTimeHistogram->Collect(36000., 15);
            } else {
                requestTimeGroup->GetCounter("0.001ms")->Set(1);
                requestTimeGroup->GetCounter("0.1ms")->Set(2);
                requestTimeGroup->GetCounter("0.2ms")->Set(3);
                requestTimeGroup->GetCounter("0.3ms")->Set(4);
                requestTimeGroup->GetCounter("0.4ms")->Set(5);
                requestTimeGroup->GetCounter("0.5ms")->Set(6);
                requestTimeGroup->GetCounter("0.6ms")->Set(7);
                requestTimeGroup->GetCounter("0.7ms")->Set(8);
                requestTimeGroup->GetCounter("0.8ms")->Set(9);
                requestTimeGroup->GetCounter("0.9ms")->Set(0);
                requestTimeGroup->GetCounter("1ms")->Set(1);
                requestTimeGroup->GetCounter("2ms")->Set(2);
                requestTimeGroup->GetCounter("5ms")->Set(3);
                requestTimeGroup->GetCounter("10ms")->Set(4);
                requestTimeGroup->GetCounter("20ms")->Set(5);
                requestTimeGroup->GetCounter("50ms")->Set(6);
                requestTimeGroup->GetCounter("100ms")->Set(7);
                requestTimeGroup->GetCounter("200ms")->Set(8);
                requestTimeGroup->GetCounter("500ms")->Set(9);
                requestTimeGroup->GetCounter("1000ms")->Set(10);
                requestTimeGroup->GetCounter("2000ms")->Set(11);
                requestTimeGroup->GetCounter("5000ms")->Set(12);
                requestTimeGroup->GetCounter("10000ms")->Set(13);
                requestTimeGroup->GetCounter("35000ms")->Set(14);
                requestTimeGroup->GetCounter("Inf")->Set(15);
            }
        };

        struct TestConfiguration {
            bool ReportZeroBlocksMetrics;
            bool ReportHistogramAsSingleCounter;
            TString Resource;
        };

        std::vector<TestConfiguration> testConfigurations = {
            {true, false, "user_server_volume_instance_test"},
            {false, false, "user_server_volume_instance_skip_zero_blocks_test"},
            {true, true, "user_server_volume_instance_test"},
            {false, true, "user_server_volume_instance_skip_zero_blocks_test"}};

        for (const auto& config: testConfigurations) {
            stats = MakeIntrusive<TDynamicCounters>();
            makeCounters("ReadBlocks", config.ReportHistogramAsSingleCounter);
            makeCounters("WriteBlocks", config.ReportHistogramAsSingleCounter);
            makeCounters("ZeroBlocks", config.ReportHistogramAsSingleCounter);

            auto supplier = CreateUserCounterSupplier();
            RegisterServerVolumeInstance(
                *supplier,
                "cloudId",
                "folderId",
                "diskId",
                "instanceId",
                config.ReportZeroBlocksMetrics,
                stats);

            ValidateTestResult(supplier, config.Resource);
        }
    }

    Y_UNIT_TEST(UserServiceVolumeInstanceTests)
    {
       NMonitoring::TDynamicCounterPtr stats;

        auto makeCounters = [&stats] (const TString& name, bool reportHistogramAsSingleCounter) {
            stats->GetCounter("UsedQuota")->Set(1);
            stats->GetCounter("MaxUsedQuota")->Set(10);

            auto request = stats->GetSubgroup("request", name);
            auto requestTimeGroup =
                request->GetSubgroup("histogram", "ThrottlerDelay");

            if (reportHistogramAsSingleCounter) {
                const auto& buckets = TRequestUsTimeBuckets::Buckets;
                const auto& bounds = ConvertToHistBounds(buckets);
                auto requestTimeHistogram = requestTimeGroup->GetHistogram(
                            "ThrottlerDelay",
                            NMonitoring::ExplicitHistogram(bounds));
                requestTimeHistogram->Collect(1., 1);
                requestTimeHistogram->Collect(100., 2);
                requestTimeHistogram->Collect(200., 3);
                requestTimeHistogram->Collect(300., 4);
                requestTimeHistogram->Collect(400., 5);
                requestTimeHistogram->Collect(500., 6);
                requestTimeHistogram->Collect(600., 7);
                requestTimeHistogram->Collect(700., 8);
                requestTimeHistogram->Collect(800., 9);
                requestTimeHistogram->Collect(900., 0);
                requestTimeHistogram->Collect(1000., 1);
                requestTimeHistogram->Collect(2000., 2);
                requestTimeHistogram->Collect(5000., 3);
                requestTimeHistogram->Collect(10000., 4);
                requestTimeHistogram->Collect(20000., 5);
                requestTimeHistogram->Collect(50000., 6);
                requestTimeHistogram->Collect(100000., 7);
                requestTimeHistogram->Collect(200000., 8);
                requestTimeHistogram->Collect(500000., 9);
                requestTimeHistogram->Collect(1000000., 10);
                requestTimeHistogram->Collect(2000000., 11);
                requestTimeHistogram->Collect(5000000., 12);
                requestTimeHistogram->Collect(10000000., 13);
                requestTimeHistogram->Collect(35000000., 14);
                requestTimeHistogram->Collect(36000000., 15);
            } else {
                requestTimeGroup->GetCounter("1")->Set(1);
                requestTimeGroup->GetCounter("100")->Set(2);
                requestTimeGroup->GetCounter("200")->Set(3);
                requestTimeGroup->GetCounter("300")->Set(4);
                requestTimeGroup->GetCounter("400")->Set(5);
                requestTimeGroup->GetCounter("500")->Set(6);
                requestTimeGroup->GetCounter("600")->Set(7);
                requestTimeGroup->GetCounter("700")->Set(8);
                requestTimeGroup->GetCounter("800")->Set(9);
                requestTimeGroup->GetCounter("900")->Set(0);
                requestTimeGroup->GetCounter("1000")->Set(1);
                requestTimeGroup->GetCounter("2000")->Set(2);
                requestTimeGroup->GetCounter("5000")->Set(3);
                requestTimeGroup->GetCounter("10000")->Set(4);
                requestTimeGroup->GetCounter("20000")->Set(5);
                requestTimeGroup->GetCounter("50000")->Set(6);
                requestTimeGroup->GetCounter("100000")->Set(7);
                requestTimeGroup->GetCounter("200000")->Set(8);
                requestTimeGroup->GetCounter("500000")->Set(9);
                requestTimeGroup->GetCounter("1000000")->Set(10);
                requestTimeGroup->GetCounter("2000000")->Set(11);
                requestTimeGroup->GetCounter("5000000")->Set(12);
                requestTimeGroup->GetCounter("10000000")->Set(13);
                requestTimeGroup->GetCounter("35000000")->Set(14);
                requestTimeGroup->GetCounter("Inf")->Set(15);
            }
        };

        for (bool reportHistogramAsSingleCounter : {false, true}) {
            stats = MakeIntrusive<TDynamicCounters>();

            makeCounters("ReadBlocks", reportHistogramAsSingleCounter);
            makeCounters("WriteBlocks", reportHistogramAsSingleCounter);
            makeCounters("ZeroBlocks", reportHistogramAsSingleCounter);

            auto supplier = CreateUserCounterSupplier();
            RegisterServiceVolume(
                *supplier,
                "cloudId",
                "folderId",
                "diskId",
                stats);

            ValidateTestResult(supplier, "user_service_volume_instance_test");
        }
    }
}

}   // NCloud::NBlockStore::NUserCounter
