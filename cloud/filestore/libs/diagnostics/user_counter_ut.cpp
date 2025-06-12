#include "user_counter.h"

#include "config.h"
#include "request_stats.h"

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/text/text.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NUserCounter {

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
    const auto& object,
    const auto& name,
    const auto& valueName)
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

////////////////////////////////////////////////////////////////////////////////

const TString METRIC_COMPONENT = "test";
const TString METRIC_FS_COMPONENT = METRIC_COMPONENT + "_fs";

struct TEnv
    : public NUnitTest::TBaseFixture
{
    NMonitoring::TDynamicCountersPtr Counters;
    ITimerPtr Timer;
    std::shared_ptr<IUserCounterSupplier> Supplier;
    IRequestStatsRegistryPtr Registry;

    TEnv()
        : Counters(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , Timer(CreateWallClockTimer())
        , Supplier(CreateUserCounterSupplier())
        , Registry(CreateRequestStatsRegistry(
            METRIC_COMPONENT,
            std::make_shared<TDiagnosticsConfig>(),
            Counters,
            Timer,
            Supplier))
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TUserWrapperTest)
{
    Y_UNIT_TEST_F(ShouldMultipleRegister, TEnv)
    {
        const TString fsId = "test_fs";
        const TString clientId = "test_client";
        const TString cloudId = "test_cloud";
        const TString folderId = "test_folder";

        const TString testResult = NResource::Find("counters.json");
        auto testJson = NJson::ReadJsonFastTree(testResult, true);
        auto emptyJson = NJson::ReadJsonFastTree("{}", true);

        auto stats =
            Registry->GetFileSystemStats(cloudId, folderId, fsId, clientId);

        // First registration
        Registry->RegisterUserStats(cloudId, folderId, fsId, clientId);

        TStringStream firstOut;
        auto firstEncoder = EncoderJson(&firstOut);
        Supplier->Accept(TInstant::Seconds(12), firstEncoder.Get());

        auto firstResult = NJson::ReadJsonFastTree(firstOut.Str(), true);
        ValidateJsons(testJson, firstResult);

        // Second registration
        Registry->RegisterUserStats(cloudId, folderId, fsId, clientId);

        TStringStream secondOut;
        auto secondEncoder = EncoderJson(&secondOut);
        Supplier->Accept(TInstant::Seconds(12), secondEncoder.Get());

        auto secondResult = NJson::ReadJsonFastTree(secondOut.Str(), true);

        ValidateJsons(testJson, secondResult);

        // Unregister
        Registry->Unregister(fsId, clientId);

        TStringStream thirdOut;
        auto thirdEncoder = EncoderJson(&thirdOut);
        Supplier->Accept(TInstant::Seconds(12), thirdEncoder.Get());

        auto thirdResult = NJson::ReadJsonFastTree(thirdOut.Str(), true);
        ValidateJsons(emptyJson, thirdResult);
    }
}

}   // namespace NCloud::NFileStore::NUserCounter
