#include "user_counter.h"

#include "config.h"
#include "request_stats.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/text/text.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/hook/hook.h>
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
    UNIT_ASSERT_C(false, "Value not found " << name);
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
    UNIT_ASSERT_C(false, "Value not found " << name << "/" << valueName);
    return NJson::TJsonValue{};
};

NJson::TJsonValue GetHistBucket(
    const auto& object,
    const auto& name,
    const auto& bucketName)
{
    for (const auto& data: object["sensors"].GetArray()) {
        if (data["labels"]["name"] == name) {
            if (data.Has("hist")) {
                if (bucketName == "Inf") {
                    return data["hist"]["inf"];
                }

                auto bounds = data["hist"]["bounds"].GetArray();
                auto buckets = data["hist"]["buckets"].GetArray();

                for (size_t i = 0; i < bounds.size(); ++i) {
                    if (ToString(bounds[i].GetInteger()) == bucketName) {
                        return buckets[i];
                    }
                }
            }
        }
    }
    UNIT_ASSERT_C(
        false,
        "Value not found sensor=" << name << ", bucket=" << bucketName);
    return NJson::TJsonValue{};
};

void ValidateJsons(
    const NJson::TJsonValue& expectedJson,
    const NJson::TJsonValue& actualJson)
{
    for (const auto& jsonValue: expectedJson["sensors"].GetArray()) {
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

void ValidateTestResult(
    const std::shared_ptr<IUserCounterSupplier>& supplier,
    const NJson::TJsonValue& expectedJson)
{
    TStringStream jsonOut;
    auto encoder = NMonitoring::EncoderJson(&jsonOut);
    supplier->Accept(TInstant::Seconds(12), encoder.Get());

    auto actualJson = NJson::ReadJsonFastTree(jsonOut.Str(), true);
    ValidateJsons(expectedJson, actualJson);
}

void SetTimeHistogramCountersMs(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
    const TString& histName)
{
    auto subgroup = counters->GetSubgroup("histogram", histName)
                        ->GetSubgroup("units", "msec");
    subgroup->GetCounter("0.001ms")->Set(1);
    subgroup->GetCounter("0.1ms")->Set(2);
    subgroup->GetCounter("0.2ms")->Set(3);
    subgroup->GetCounter("0.3ms")->Set(4);
    subgroup->GetCounter("0.4ms")->Set(5);
    subgroup->GetCounter("0.5ms")->Set(6);
    subgroup->GetCounter("0.6ms")->Set(7);
    subgroup->GetCounter("0.7ms")->Set(8);
    subgroup->GetCounter("0.8ms")->Set(9);
    subgroup->GetCounter("0.9ms")->Set(0);
    subgroup->GetCounter("1ms")->Set(1);
    subgroup->GetCounter("2ms")->Set(2);
    subgroup->GetCounter("5ms")->Set(3);
    subgroup->GetCounter("10ms")->Set(4);
    subgroup->GetCounter("20ms")->Set(5);
    subgroup->GetCounter("50ms")->Set(6);
    subgroup->GetCounter("100ms")->Set(7);
    subgroup->GetCounter("200ms")->Set(8);
    subgroup->GetCounter("500ms")->Set(9);
    subgroup->GetCounter("1000ms")->Set(10);
    subgroup->GetCounter("2000ms")->Set(11);
    subgroup->GetCounter("5000ms")->Set(12);
    subgroup->GetCounter("10000ms")->Set(13);
    subgroup->GetCounter("35000ms")->Set(14);
    subgroup->GetCounter("Inf")->Set(15);
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
    Y_TEST_HOOK_BEFORE_RUN(InitTest)
    {
        // NHPTimer warmup, see issue #2830 for more information
        Y_UNUSED(GetCyclesPerMillisecond());
    }

    Y_UNIT_TEST_F(ShouldMultipleRegister, TEnv)
    {
        const TString fsId = "test_fs";
        const TString clientId = "test_client";
        const TString cloudId = "test_cloud";
        const TString folderId = "test_folder";

        const TString testResult = NResource::Find("user_counters_empty.json");
        auto expectedJson = NJson::ReadJsonFastTree(testResult, true);
        auto emptyJson = NJson::ReadJsonFastTree("{}", true);

        auto stats =
            Registry->GetFileSystemStats(fsId, clientId, cloudId, folderId);

        // First registration
        Registry->RegisterUserStats(fsId, clientId, cloudId, folderId);
        ValidateTestResult(Supplier, expectedJson);

        // Second registration
        Registry->RegisterUserStats(fsId, clientId, cloudId, folderId);
        ValidateTestResult(Supplier, expectedJson);

        // Unregister
        Registry->Unregister(fsId, clientId);
        ValidateTestResult(Supplier, emptyJson);
    }

    Y_UNIT_TEST_F(ShouldReportUserStatsIsSourceMetricsChanged, TEnv)
    {
        const TString fsId = "test_fs";
        const TString clientId = "test_client";
        const TString cloudId = "test_cloud";
        const TString folderId = "test_folder";

        Registry->GetFileSystemStats(fsId, clientId, cloudId, folderId);
        Registry->RegisterUserStats(fsId, clientId, cloudId, folderId);

        auto counters = Counters->GetSubgroup("component", METRIC_FS_COMPONENT)
                            ->GetSubgroup("host", "cluster")
                            ->GetSubgroup("filesystem", fsId)
                            ->GetSubgroup("client", clientId)
                            ->GetSubgroup("cloud", cloudId)
                            ->GetSubgroup("folder", folderId);

        auto setSourceRequestsCounters = [&counters](const TString& request)
        {
            auto requestCounters = counters->GetSubgroup("request", request);
            requestCounters->GetCounter("Count")->Set(42);
            requestCounters->GetCounter("MaxCount")->Set(142);
            requestCounters->GetCounter("Errors/Fatal")->Set(7);
            requestCounters->GetCounter("Time")->Set(100500);

            SetTimeHistogramCountersMs(requestCounters, "Time");
        };

        auto requests = {
            "AllocateData",  "CreateHandle", "CreateNode",    "DestroyHandle",
            "GetNodeAttr",   "GetNodeXAttr", "ListNodeXAttr", "ListNodes",
            "RenameNode",    "SetNodeAttr",  "SetNodeXAttr",  "UnlinkNode",
            "StatFileStore", "ReadLink",     "AccessNode",    "RemoveNodeXAttr",
            "ReleaseLock",   "AcquireLock",  "WriteData",     "ReadData",
        };

        for (const auto& request: requests) {
            setSourceRequestsCounters(request);
        }

        Registry->UpdateStats(true);

        const TString testResult = NResource::Find("user_counters.json");
        auto expectedJson = NJson::ReadJsonFastTree(testResult, true);
        ValidateTestResult(Supplier, expectedJson);
    }

    Y_UNIT_TEST_F(ShouldReportUserStats, TEnv)
    {
        const TString fsId = "test_fs";
        const TString clientId = "test_client";
        const TString cloudId = "test_cloud";
        const TString folderId = "test_folder";

        auto stats =
            Registry->GetFileSystemStats(fsId, clientId, cloudId, folderId);
        Registry->RegisterUserStats(fsId, clientId, cloudId, folderId);

        auto emulateRequest = [&](EFileStoreRequest request,
                                  ui64 size,
                                  TDuration duration,
                                  bool isError = false)
        {
            auto context =
                MakeIntrusive<TCallContext>(fsId, static_cast<ui64>(1));
            context->RequestType = request;
            context->RequestSize = size;

            stats->RequestStarted(*context);
            context->SetRequestStartedCycles(
                context->GetRequestStartedCycles() -
                DurationToCyclesSafe(duration));

            NProto::TError error =
                isError ? MakeError(E_NOT_IMPLEMENTED, "Test error") : MakeError(S_OK);
            stats->RequestCompleted(*context, error);
        };

        // fill only 3 largest buckets due to flaps on small durations
        const auto writeData = EFileStoreRequest::WriteData;
        const auto readData = EFileStoreRequest::ReadData;
        emulateRequest(writeData, 1_MB, TDuration::Seconds(8));
        emulateRequest(readData, 1_MB, TDuration::Seconds(15));
        emulateRequest(readData, 1_MB, TDuration::Seconds(25));
        emulateRequest(readData, 1_MB, TDuration::Seconds(55));
        emulateRequest(readData, 1_MB, TDuration::Seconds(60));
        emulateRequest(readData, 1_MB, TDuration::Seconds(100), true);

        TStringStream jsonOut;
        auto encoder = NMonitoring::EncoderJson(&jsonOut);
        Supplier->Accept(TInstant::Seconds(12), encoder.Get());

        auto resultJson = NJson::ReadJsonFastTree(jsonOut.Str(), true);

        NJson::TJsonValue value;
        value = GetValue(resultJson, "filestore.write_ops");
        UNIT_ASSERT_EQUAL(1, value.GetInteger());

        value = GetHistBucket(resultJson, "filestore.write_latency", "10000");
        UNIT_ASSERT_EQUAL(1, value.GetInteger());

        value = GetValue(resultJson, "filestore.read_ops");
        UNIT_ASSERT_EQUAL(4, value.GetInteger());

        value = GetValue(resultJson, "filestore.read_errors");
        UNIT_ASSERT_EQUAL(1, value.GetInteger());

        value = GetHistBucket(resultJson, "filestore.read_latency", "35000");
        UNIT_ASSERT_EQUAL(2, value.GetInteger());

        value = GetHistBucket(resultJson, "filestore.read_latency", "Inf");
        UNIT_ASSERT_EQUAL(3, value.GetInteger());
    }
}

}   // namespace NCloud::NFileStore::NUserCounter
