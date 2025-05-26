#include "user_counter.h"

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TUserWrapperTest)
{
    Y_UNIT_TEST(UserServerVolumeInstanceTests)
    {
        NMonitoring::TDynamicCounterPtr stats =
            MakeIntrusive<TDynamicCounters>();

        auto makeCounters = [&stats] (const TString& name) {
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

            auto requestTimeHist =
                request->GetSubgroup("histogram", "Time");
            requestTimeHist->GetCounter("0.001ms")->Set(1);
            requestTimeHist->GetCounter("0.1ms")->Set(2);
            requestTimeHist->GetCounter("0.2ms")->Set(3);
            requestTimeHist->GetCounter("0.3ms")->Set(4);
            requestTimeHist->GetCounter("0.4ms")->Set(5);
            requestTimeHist->GetCounter("0.5ms")->Set(6);
            requestTimeHist->GetCounter("0.6ms")->Set(7);
            requestTimeHist->GetCounter("0.7ms")->Set(8);
            requestTimeHist->GetCounter("0.8ms")->Set(9);
            requestTimeHist->GetCounter("0.9ms")->Set(0);
            requestTimeHist->GetCounter("1ms")->Set(1);
            requestTimeHist->GetCounter("2ms")->Set(2);
            requestTimeHist->GetCounter("5ms")->Set(3);
            requestTimeHist->GetCounter("10ms")->Set(4);
            requestTimeHist->GetCounter("20ms")->Set(5);
            requestTimeHist->GetCounter("50ms")->Set(6);
            requestTimeHist->GetCounter("100ms")->Set(7);
            requestTimeHist->GetCounter("200ms")->Set(8);
            requestTimeHist->GetCounter("500ms")->Set(9);
            requestTimeHist->GetCounter("1000ms")->Set(10);
            requestTimeHist->GetCounter("2000ms")->Set(11);
            requestTimeHist->GetCounter("5000ms")->Set(12);
            requestTimeHist->GetCounter("10000ms")->Set(13);
            requestTimeHist->GetCounter("35000ms")->Set(14);
            requestTimeHist->GetCounter("Inf")->Set(15);
        };

        makeCounters("ReadBlocks");
        makeCounters("WriteBlocks");
        makeCounters("ZeroBlocks");

        struct TestConfiguration {
            bool ReportZeroBlocksMetrics;
            TString Resource;
        };

        std::vector<TestConfiguration> testConfigurations = {
            {true, "user_server_volume_instance_test"},
            {false, "user_server_volume_instance_skip_zero_blocks_test"}};

        for (const auto& config: testConfigurations) {
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
       NMonitoring::TDynamicCounterPtr stats =
            MakeIntrusive<TDynamicCounters>();

        auto makeCounters = [&stats] (const TString& name) {
            stats->GetCounter("UsedQuota")->Set(1);
            stats->GetCounter("MaxUsedQuota")->Set(10);

            auto request = stats->GetSubgroup("request", name);
            auto requestTimeHist =
                request->GetSubgroup("histogram", "ThrottlerDelay");
            requestTimeHist->GetCounter("1")->Set(1);
            requestTimeHist->GetCounter("100")->Set(2);
            requestTimeHist->GetCounter("200")->Set(3);
            requestTimeHist->GetCounter("300")->Set(4);
            requestTimeHist->GetCounter("400")->Set(5);
            requestTimeHist->GetCounter("500")->Set(6);
            requestTimeHist->GetCounter("600")->Set(7);
            requestTimeHist->GetCounter("700")->Set(8);
            requestTimeHist->GetCounter("800")->Set(9);
            requestTimeHist->GetCounter("900")->Set(0);
            requestTimeHist->GetCounter("1000")->Set(1);
            requestTimeHist->GetCounter("2000")->Set(2);
            requestTimeHist->GetCounter("5000")->Set(3);
            requestTimeHist->GetCounter("10000")->Set(4);
            requestTimeHist->GetCounter("20000")->Set(5);
            requestTimeHist->GetCounter("50000")->Set(6);
            requestTimeHist->GetCounter("100000")->Set(7);
            requestTimeHist->GetCounter("200000")->Set(8);
            requestTimeHist->GetCounter("500000")->Set(9);
            requestTimeHist->GetCounter("1000000")->Set(10);
            requestTimeHist->GetCounter("2000000")->Set(11);
            requestTimeHist->GetCounter("5000000")->Set(12);
            requestTimeHist->GetCounter("10000000")->Set(13);
            requestTimeHist->GetCounter("35000000")->Set(14);
            requestTimeHist->GetCounter("Inf")->Set(15);
        };

        makeCounters("ReadBlocks");
        makeCounters("WriteBlocks");
        makeCounters("ZeroBlocks");

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

}   // NCloud::NBlockStore::NUserCounter
