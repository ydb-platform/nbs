#include "request_counters.h"

#include "monitoring.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/histogram_types.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/stream/str.h>
#include <util/string/cast.h>

#include <array>
#include <chrono>
#include <future>
#include <thread>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    size_t RequestBytes = 0;
    TDuration RequestTime;
    TDuration PostponedTime;
    TDuration BackoffTime;
    TDuration ShapingTime;
    bool Aligned = false;
    ui64 RequestCompletionTime = 0;
    std::optional<ui64> LogicalRequestBytes;
    EDiagnosticsErrorKind ErrorKind = EDiagnosticsErrorKind::Success;
};

////////////////////////////////////////////////////////////////////////////////

void AddRequestStats(
    TRequestCounters& requestCounters,
    TRequestCounters::TRequestType requestType,
    std::initializer_list<TRequest> requests)
{
    for (const auto& request: requests) {
        auto requestStarted = requestCounters.RequestStarted(
            requestType,
            request.RequestBytes);

        auto realRequestStarted = requestStarted -
            DurationToCyclesSafe(request.RequestTime) -
            request.RequestCompletionTime;

        ui64 responseSent = request.RequestCompletionTime ?
            realRequestStarted + DurationToCyclesSafe(request.RequestTime) : 0;

        requestCounters.RequestCompleted(
            requestType,
            realRequestStarted,
            request.PostponedTime,
            request.BackoffTime,
            request.ShapingTime,
            request.RequestBytes,
            request.ErrorKind,
            NCloud::NProto::EF_NONE,
            request.Aligned,
            ECalcMaxTime::ENABLE,
            responseSent,
            request.LogicalRequestBytes);
    }
}

void AddIncompleteStats(
    TRequestCounters& requestCounters,
    TRequestCounters::TRequestType requestType,
    std::initializer_list<TDuration> requests)
{
    for (auto executionTime: requests) {
        auto totalTime = executionTime;
        requestCounters.AddIncompleteStats(
            requestType,
            executionTime,
            totalTime,
            ECalcMaxTime::ENABLE);
    }
}

////////////////////////////////////////////////////////////////////////////////

const ui32 WriteRequestType = 0;
const ui32 ReadRequestType = 1;

const TString RequestNames[] {
    "WriteBlocks",
    "ReadBlocks",
};

auto RequestType2Name(TRequestCounters::TRequestType t) {
    UNIT_ASSERT(t == 0 || t == 1);
    return RequestNames[t];
}

auto IsReadWriteRequest(TRequestCounters::TRequestType t)
{
    UNIT_ASSERT(t == 0 || t == 1);
    return true;
}

auto IsStartEndpointRequest(TRequestCounters::TRequestType t)
{
    Y_UNUSED(t);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestCountersOptions
{
    TRequestCounters::EOptions Options = {};
    EHistogramCounterOptions HistogramCounterOptions =
        EHistogramCounterOption::ReportMultipleCounters;
    TVector<TSizeInterval> ExecutionTimeSizeClasses;
};

auto MakeRequestCounters(TRequestCountersOptions options = {})
{
    return TRequestCounters(
        CreateWallClockTimer(),
        2,
        RequestType2Name,
        IsReadWriteRequest,
        IsStartEndpointRequest,
        options.Options,
        options.HistogramCounterOptions,
        options.ExecutionTimeSizeClasses);
}

auto MakeRequestCountersPtr(TRequestCountersOptions options = {})
{
    return std::make_shared<TRequestCounters>(
        CreateWallClockTimer(),
        2,
        RequestType2Name,
        IsReadWriteRequest,
        IsStartEndpointRequest,
        options.Options,
        options.HistogramCounterOptions,
        options.ExecutionTimeSizeClasses);
}

////////////////////////////////////////////////////////////////////////////////

class TIoSizeSnapshotConsumer final: public NMonitoring::ICountableConsumer
{
public:
    ui64 Count = 0;
    ui64 Bytes = 0;
    ui32 CountSensors = 0;
    ui32 ByteSensors = 0;

    void OnCounter(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TCounterForPtr* counter) override
    {
        if (labelValue == "IoSizeCount" || labelValue == "IoSizeBytes") {
            UNIT_ASSERT_VALUES_EQUAL("sensor", labelName);
            UNIT_ASSERT(counter->ForDerivative());
            if (labelValue == "IoSizeCount") {
                Count = counter->Val();
                ++CountSensors;
            } else {
                Bytes = counter->Val();
                ++ByteSensors;
            }
        }
    }

    void OnHistogram(
        const TString&,
        const TString&, NMonitoring::IHistogramSnapshotPtr, bool) override
    {}

    void OnGroupBegin(
        const TString&,
        const TString&, const NMonitoring::TDynamicCounters*) override
    {}

    void OnGroupEnd(
        const TString&,
        const TString&, const NMonitoring::TDynamicCounters*) override
    {}
};

// Pause the real encoder after it has read the count, so that a completion can
// update the live counters before the encoder receives the byte snapshot.
class TInterleavingIoSizeConsumer final: public NMonitoring::ICountableConsumer
{
private:
    NMonitoring::ICountableConsumer& Consumer;
    const std::function<void()> Interleave;
    TVector<bool> WriteGroups;

public:
    bool Interleaved = false;

    TInterleavingIoSizeConsumer(
        NMonitoring::ICountableConsumer& consumer,
        std::function<void()> interleave)
        : Consumer(consumer)
        , Interleave(std::move(interleave))
    {}

    void OnCounter(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TCounterForPtr* counter) override
    {
        Consumer.OnCounter(labelName, labelValue, counter);
        // Complete a request after the first pair member has been encoded,
        // regardless of the counter tree's traversal order.
        if (!Interleaved && WriteGroups.back() &&
            (labelValue == "IoSizeCount" || labelValue == "IoSizeBytes"))
        {
            Interleaved = true;
            Interleave();
        }
    }

    void OnHistogram(
        const TString& labelName,
        const TString& labelValue,
        NMonitoring::IHistogramSnapshotPtr snapshot, bool derivative) override
    {
        Consumer.OnHistogram(
            labelName, labelValue, std::move(snapshot), derivative);
    }

    void OnGroupBegin(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TDynamicCounters* group) override
    {
        WriteGroups.push_back(
            (!WriteGroups.empty() && WriteGroups.back()) ||
            (labelName == "request" && labelValue == "WriteBlocks"));
        Consumer.OnGroupBegin(labelName, labelValue, group);
    }

    void OnGroupEnd(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TDynamicCounters* group) override
    {
        Consumer.OnGroupEnd(labelName, labelValue, group);
        WriteGroups.pop_back();
    }

    NMonitoring::TCountableBase::EVisibility Visibility() const override
    {
        return Consumer.Visibility();
    }
};

TIoSizeSnapshotConsumer SnapshotIoSize(
    const NMonitoring::TDynamicCounters& group)
{
    TIoSizeSnapshotConsumer snapshot;
    group.Accept({}, {}, snapshot);
    UNIT_ASSERT_VALUES_EQUAL(1, snapshot.CountSensors);
    UNIT_ASSERT_VALUES_EQUAL(1, snapshot.ByteSensors);
    return snapshot;
}

void AssertEncodedIoSize(const TString& encoded, ui64 count, ui64 bytes)
{
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(encoded, &json, true));
    ui32 countSensors = 0;
    ui32 byteSensors = 0;
    for (const auto& metric: json["sensors"].GetArraySafe()) {
        const auto& labels = metric["labels"];
        if (labels["request"].GetString() != "WriteBlocks") {
            continue;
        }
        const auto& sensor = labels["sensor"].GetString();
        if (sensor == "IoSizeCount") {
            ++countSensors;
            UNIT_ASSERT_VALUES_EQUAL(count, metric["value"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL("RATE", metric["kind"].GetString());
        } else if (sensor == "IoSizeBytes") {
            ++byteSensors;
            UNIT_ASSERT_VALUES_EQUAL(bytes, metric["value"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL("RATE", metric["kind"].GetString());
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(1, countSensors);
    UNIT_ASSERT_VALUES_EQUAL(1, byteSensors);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestCountersTest)
{
    Y_TEST_HOOK_BEFORE_RUN(InitTest)
    {
        // NHPTimer warmup, see issue #2830 for more information
        Y_UNUSED(GetCyclesPerMillisecond());
    }

    Y_UNIT_TEST(ShouldCountLogicalIoSize)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        counters.Register(*monitoring->GetCounters());
        AddRequestStats(
            counters,
            WriteRequestType,
            {{.RequestBytes = 4096, .LogicalRequestBytes = 512},
             {.RequestBytes = 8192, .LogicalRequestBytes = 5632}});
        AddRequestStats(
            counters,
            ReadRequestType,
            {{.RequestBytes = 8192, .LogicalRequestBytes = 5632}});
        for (auto type: {WriteRequestType, ReadRequestType}) {
            auto group = monitoring->GetCounters()->GetSubgroup(
                "request", RequestNames[type]);
            const bool write = type == WriteRequestType;
            UNIT_ASSERT_VALUES_EQUAL(
                write ? 2 : 1, group->GetCounter("IoSizeCount", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                write ? 6144 : 5632,
                group->GetCounter("IoSizeBytes", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                write ? 12288 : 8192,
                group->GetCounter("RequestBytes", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                group->GetCounter("Count", true)->Val(),
                group->GetCounter("IoSizeCount", true)->Val());
        }
    }

    Y_UNIT_TEST(ShouldUseIoSizeFallbackAndPreserveExplicitZero)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        counters.Register(*monitoring->GetCounters());
        AddRequestStats(
            counters,
            WriteRequestType,
            {{.RequestBytes = 4096},
             {.RequestBytes = 4096, .LogicalRequestBytes = 0}});
        auto group =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            2, group->GetCounter("IoSizeCount", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            4096, group->GetCounter("IoSizeBytes", true)->Val());
    }

    Y_UNIT_TEST(ShouldApplyCountErrorPolicyToIoSize)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        counters.Register(*monitoring->GetCounters());
        for (auto type: {WriteRequestType, ReadRequestType}) {
            for (auto error:
                 {EDiagnosticsErrorKind::Success,
                  EDiagnosticsErrorKind::ErrorAborted,
                  EDiagnosticsErrorKind::ErrorFatal,
                  EDiagnosticsErrorKind::ErrorRetriable,
                  EDiagnosticsErrorKind::ErrorThrottling,
                  EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint,
                  EDiagnosticsErrorKind::ErrorSession,
                  EDiagnosticsErrorKind::ErrorSilent})
            {
                AddRequestStats(
                    counters,
                    type,
                    {{.RequestBytes = 4096,
                      .LogicalRequestBytes = 512,
                      .ErrorKind = error}});
            }
            auto group = monitoring->GetCounters()->GetSubgroup(
                "request", RequestNames[type]);
            UNIT_ASSERT_VALUES_EQUAL(
                2, group->GetCounter("Count", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                2, group->GetCounter("IoSizeCount", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                1024, group->GetCounter("IoSizeBytes", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                6, group->GetCounter("Errors", true)->Val());
        }
    }

    Y_UNIT_TEST(ShouldRegisterIoSizeOnlyWhenEnabledForReadWrite)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto disabled = MakeRequestCounters();
        disabled.Register(*monitoring->GetCounters());
        AddRequestStats(disabled, WriteRequestType, {{.RequestBytes = 4096}});
        auto group =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");
        UNIT_ASSERT(!group->FindCounter("IoSizeCount"));
        UNIT_ASSERT(!group->FindCounter("IoSizeBytes"));

        auto controlGroup =
            monitoring->GetCounters()->GetSubgroup("control", "enabled");
        TRequestCounters control(
            CreateWallClockTimer(),
            1,
            [](auto) { return TString("MountVolume"); },
            [](auto) { return false; },
            [](auto) { return false; },
            TRequestCounters::EOption::ReportIoSize,
            EHistogramCounterOption::ReportMultipleCounters,
            {});
        control.Register(*controlGroup);
        AddRequestStats(control, 0, {{.RequestBytes = 4096}});
        group = controlGroup->GetSubgroup("request", "MountVolume");
        UNIT_ASSERT(!group->FindCounter("IoSizeCount"));
        UNIT_ASSERT(!group->FindCounter("IoSizeBytes"));
    }

    Y_UNIT_TEST(ShouldForwardIoSizeToSubscribers)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto root = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        auto outer = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        auto inner = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        const auto rootGroup = monitoring->GetCounters();
        const auto outerGroup = rootGroup->GetSubgroup("subscriber", "outer");
        const auto innerGroup = rootGroup->GetSubgroup("subscriber", "inner");
        root->Register(*rootGroup);
        outer->Register(*outerGroup);
        inner->Register(*innerGroup);
        root->Subscribe(outer);
        outer->Subscribe(inner);
        AddRequestStats(
            *root,
            ReadRequestType,
            {{.RequestBytes = 8192, .LogicalRequestBytes = 5632}});
        for (const auto& parent: {rootGroup, outerGroup, innerGroup}) {
            auto group = parent->GetSubgroup("request", "ReadBlocks");
            UNIT_ASSERT_VALUES_EQUAL(
                1, group->GetCounter("IoSizeCount", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                5632, group->GetCounter("IoSizeBytes", true)->Val());
            const auto snapshot = SnapshotIoSize(*group);
            UNIT_ASSERT_VALUES_EQUAL(1, snapshot.Count);
            UNIT_ASSERT_VALUES_EQUAL(5632, snapshot.Bytes);
        }
    }

    Y_UNIT_TEST(ShouldCountIoSizeOnceAfterRetries)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        counters.Register(*monitoring->GetCounters());
        const auto started = counters.RequestStarted(WriteRequestType, 4096);
        for (int i = 0; i != 2; ++i) {
            counters.AddRetryStats(
                WriteRequestType,
                EDiagnosticsErrorKind::ErrorRetriable, NCloud::NProto::EF_NONE);
        }
        auto group =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            0, group->GetCounter("IoSizeCount", true)->Val());
        counters.RequestCompleted(
            WriteRequestType,
            started,
            TDuration::Zero(),
            TDuration::Zero(),
            TDuration::Zero(),
            4096,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE, true, ECalcMaxTime::ENABLE, 0, 512);
        UNIT_ASSERT_VALUES_EQUAL(
            1, group->GetCounter("IoSizeCount", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            512, group->GetCounter("IoSizeBytes", true)->Val());
    }

    Y_UNIT_TEST(ShouldKeep64BitIoSize)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        counters.Register(*monitoring->GetCounters());
        AddRequestStats(
            counters,
            ReadRequestType,
            {{.RequestBytes = 8_GB, .LogicalRequestBytes = 8_GB}});
        auto group =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            8_GB, group->GetCounter("IoSizeBytes", true)->Val());
    }

    Y_UNIT_TEST(ShouldEncodeCoherentIoSizeDuringCompletion)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        counters.Register(*monitoring->GetCounters());
        AddRequestStats(
            counters,
            WriteRequestType,
            {{.RequestBytes = 4096, .LogicalRequestBytes = 4096}});

        std::promise<void> firstCounterEncoded;
        auto startCompletion = firstCounterEncoded.get_future();
        std::promise<void> completed;
        auto completionDone = completed.get_future();
        std::thread writer(
            [&]
            {
                try {
                    UNIT_ASSERT(
                        startCompletion.wait_for(std::chrono::seconds(5)) ==
                        std::future_status::ready);
                    AddRequestStats(
                        counters,
                        WriteRequestType,
                        {{.RequestBytes = 4096, .LogicalRequestBytes = 4096}});
                    completed.set_value();
                } catch (...) {
                    completed.set_exception(std::current_exception());
                }
            });
        Y_DEFER
        {
            writer.join();
        };

        TString encoded;
        TStringOutput out(encoded);
        auto encoder =
            NMonitoring::CreateEncoder(&out, NMonitoring::EFormat::JSON);
        TInterleavingIoSizeConsumer consumer(
            *encoder,
            [&]
            {
                firstCounterEncoded.set_value();
                UNIT_ASSERT(
                    completionDone.wait_for(std::chrono::seconds(5)) ==
                    std::future_status::ready);
                completionDone.get();
            });
        monitoring->GetCounters()->Accept({}, {}, consumer);
        UNIT_ASSERT(consumer.Interleaved);
        AssertEncodedIoSize(encoded, 1, 4096);
        AssertEncodedIoSize(
            NMonitoring::ToJson(*monitoring->GetCounters()), 2, 8192);
    }

    Y_UNIT_TEST(ShouldExportCoherentIoSizeWithConcurrentWriters)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        counters.Register(*monitoring->GetCounters());
        auto group =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");
        constexpr ui64 RequestsPerWriter = 1000;
        constexpr ui64 RequestBytes = 4096;
        std::array<std::thread, 4> writers;
        std::promise<void> start;
        auto started = start.get_future().share();
        for (auto& writer: writers) {
            writer = std::thread(
                [&]
                {
                    started.wait();
                    for (ui64 i = 0; i < RequestsPerWriter; ++i) {
                        AddRequestStats(
                            counters,
                            WriteRequestType,
                            {{.RequestBytes = RequestBytes,
                              .LogicalRequestBytes = RequestBytes}});
                    }
                });
        }
        Y_DEFER
        {
            for (auto& writer: writers) {
                if (writer.joinable()) {
                    writer.join();
                }
            }
        };
        start.set_value();

        for (ui64 i = 0; i < 1000; ++i) {
            const auto snapshot = SnapshotIoSize(*group);
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot.Count * RequestBytes, snapshot.Bytes);
        }
        for (auto& writer: writers) {
            writer.join();
        }
        const auto snapshot = SnapshotIoSize(*group);
        UNIT_ASSERT_VALUES_EQUAL(
            RequestsPerWriter * writers.size(), snapshot.Count);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Count * RequestBytes, snapshot.Bytes);
        AssertEncodedIoSize(
            NMonitoring::ToJson(*monitoring->GetCounters()),
            snapshot.Count, snapshot.Bytes);
    }

    Y_UNIT_TEST(ShouldReuseIoSizePairAndPreserveGroupAliases)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto group =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");
        auto count = group->GetCounter("Count", true);
        auto counters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize |
                        TRequestCounters::EOption::LazyRequestInitialization});
        counters.Register(*monitoring->GetCounters());
        auto ioSizeCount = group->GetCounter("IoSizeCount", true);
        auto ioSizeBytes = group->GetCounter("IoSizeBytes", true);
        const auto initial = SnapshotIoSize(*group);
        UNIT_ASSERT_VALUES_EQUAL(0, initial.Count);
        UNIT_ASSERT_VALUES_EQUAL(0, initial.Bytes);

        auto other = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::ReportIoSize});
        other.Register(*monitoring->GetCounters());
        counters.Register(*monitoring->GetCounters());
        AddRequestStats(
            counters,
            WriteRequestType,
            {{.RequestBytes = 4096, .LogicalRequestBytes = 512}});
        AddRequestStats(
            other,
            WriteRequestType,
            {{.RequestBytes = 4096, .LogicalRequestBytes = 1536}});

        UNIT_ASSERT(
            group ==
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks"));
        UNIT_ASSERT(count == group->GetCounter("Count", true));
        UNIT_ASSERT(ioSizeCount == group->GetCounter("IoSizeCount", true));
        UNIT_ASSERT(ioSizeBytes == group->GetCounter("IoSizeBytes", true));
        const auto snapshot = SnapshotIoSize(*group);
        UNIT_ASSERT_VALUES_EQUAL(2, snapshot.Count);
        UNIT_ASSERT_VALUES_EQUAL(2048, snapshot.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Count, ioSizeCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Bytes, ioSizeBytes->Val());
        AssertEncodedIoSize(
            NMonitoring::ToJson(*monitoring->GetCounters()), 2, 2048);
    }

    Y_UNIT_TEST(ShouldShareIoSizePairAfterConcurrentRegistration)
    {
        auto monitoring = CreateMonitoringServiceStub();
        std::array<TRequestCountersPtr, 4> owners;
        std::array<std::thread, 4> registrars;
        for (size_t i = 0; i < owners.size(); ++i) {
            owners[i] = MakeRequestCountersPtr(
                {.Options = TRequestCounters::EOption::ReportIoSize});
            registrars[i] = std::thread(
                [&, i] { owners[i]->Register(*monitoring->GetCounters()); });
        }
        for (auto& registrar: registrars) {
            registrar.join();
        }

        for (const auto& owner: owners) {
            AddRequestStats(
                *owner,
                WriteRequestType,
                {{.RequestBytes = 4096, .LogicalRequestBytes = 4096}});
        }
        auto group =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");
        const auto snapshot = SnapshotIoSize(*group);
        UNIT_ASSERT_VALUES_EQUAL(owners.size(), snapshot.Count);
        UNIT_ASSERT_VALUES_EQUAL(owners.size() * 4096, snapshot.Bytes);
    }

    Y_UNIT_TEST(ShouldTrackRequestsInProgress)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto inProgress = counters->GetCounter("InProgress");
        auto inProgressBytes = counters->GetCounter("InProgressBytes");

        UNIT_ASSERT_EQUAL(inProgress->Val(), 0);
        UNIT_ASSERT_EQUAL(inProgressBytes->Val(), 0);

        auto started = requestCounters.RequestStarted(
            WriteRequestType,
            1_MB);

        UNIT_ASSERT_EQUAL(inProgress->Val(), 1);
        UNIT_ASSERT_EQUAL(inProgressBytes->Val(), 1_MB);

        requestCounters.RequestCompleted(
            WriteRequestType,
            started,
            TDuration::Zero(),   // postponedTime
            TDuration::Zero(),   // backoffTime
            TDuration::Zero(),   // shapingTime
            1_MB,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE,
            false,
            ECalcMaxTime::ENABLE,
            0);

        UNIT_ASSERT_EQUAL(inProgress->Val(), 0);
        UNIT_ASSERT_EQUAL(inProgressBytes->Val(), 0);
    }

    Y_UNIT_TEST(ShouldTrackIncompleteRequests)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counter = monitoring->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("MaxTime");
        UNIT_ASSERT_EQUAL(counter->Val(), 0);

        AddIncompleteStats(requestCounters, WriteRequestType, {
            TDuration::MilliSeconds(100),
            TDuration::MilliSeconds(150),
            TDuration::MilliSeconds(50),
            TDuration::MilliSeconds(200),
        });

        requestCounters.UpdateStats();
        UNIT_ASSERT_EQUAL(counter->Val(), 200'000);

        AddIncompleteStats(requestCounters, WriteRequestType, {
            TDuration::MilliSeconds(30),
            TDuration::MilliSeconds(170),
            TDuration::MilliSeconds(150),
            TDuration::MilliSeconds(90),
        });

        requestCounters.UpdateStats();
        UNIT_ASSERT_EQUAL(counter->Val(), 200'000);

        AddIncompleteStats(requestCounters, WriteRequestType, {
            TDuration::MilliSeconds(130),
            TDuration::MilliSeconds(70),
            TDuration::MilliSeconds(250),
            TDuration::MilliSeconds(190),
        });

        requestCounters.UpdateStats();
        UNIT_ASSERT_EQUAL(counter->Val(), 250'000);
    }

    Y_UNIT_TEST(ShouldTrackPostponedRequests)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto postponedQueueSize = counters->GetCounter("PostponedQueueSize");
        auto maxPostponedQueueSize = counters->GetCounter("MaxPostponedQueueSize");

        UNIT_ASSERT_EQUAL(postponedQueueSize->Val(), 0);
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 0);

        requestCounters.RequestPostponed(WriteRequestType);
        UNIT_ASSERT_EQUAL(postponedQueueSize->Val(), 1);
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 0);

        requestCounters.RequestPostponed(WriteRequestType);
        UNIT_ASSERT_EQUAL(postponedQueueSize->Val(), 2);
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 0);

        requestCounters.RequestAdvanced(WriteRequestType);
        UNIT_ASSERT_EQUAL(postponedQueueSize->Val(), 1);
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 0);

        requestCounters.UpdateStats();
        UNIT_ASSERT_EQUAL(maxPostponedQueueSize->Val(), 2);
    }

    Y_UNIT_TEST(ShouldTrackFastPathHits)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto fastPathHits = counters->GetCounter("FastPathHits");

        UNIT_ASSERT_EQUAL(fastPathHits->Val(), 0);

        requestCounters.RequestFastPathHit(WriteRequestType);
        requestCounters.RequestFastPathHit(WriteRequestType);
        requestCounters.RequestFastPathHit(WriteRequestType);

        UNIT_ASSERT_EQUAL(fastPathHits->Val(), 3);
    }

    Y_UNIT_TEST(ShouldFillTimePercentiles)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto readBlocks = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "ReadBlocks");

        AddRequestStats(
            requestCounters,
            ReadRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(401),
                 .PostponedTime = TDuration::MilliSeconds(50),
                 .BackoffTime = TDuration::MilliSeconds(200),
                 .ShapingTime = TDuration::MilliSeconds(100)},
            });

        requestCounters.UpdateStats(true);

        {
            auto percentiles = writeBlocks->GetSubgroup("percentiles", "Time")
                                   ->GetSubgroup("units", "usec");

            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }

        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "ExecutionTime")
                    ->GetSubgroup("units", "usec");

            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }

        {
            auto percentiles = readBlocks->GetSubgroup("percentiles", "Time")
                                   ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(500000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(350000, p50->Val());
        }

        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "ExecutionTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(100000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(75000, p50->Val());
        }
    }


    Y_UNIT_TEST(ShouldFillTimePercentilesWithRequestCompletionTime)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto readBlocks = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "ReadBlocks");

        AddRequestStats(
            requestCounters,
            ReadRequestType,
            {{.RequestBytes = 1_MB,
              .RequestTime = TDuration::MilliSeconds(106),
              .PostponedTime = TDuration::MilliSeconds(50),
              .Aligned = false,
              .RequestCompletionTime =
                  DurationToCyclesSafe(TDuration::MilliSeconds(45))}});

        requestCounters.UpdateStats(true);

        {
            auto percentiles = writeBlocks->GetSubgroup("percentiles", "Time")
                                   ->GetSubgroup("units", "usec");

            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }

        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "ExecutionTime")
                    ->GetSubgroup("units", "usec");

            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }

        {
            auto percentiles = readBlocks->GetSubgroup("percentiles", "Time")
                                   ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(200000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(150000, p50->Val());
        }

        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "ExecutionTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(100000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(75000, p50->Val());
        }

        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "RequestCompletionTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(50000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(35000, p50->Val());
        }
    }

    Y_UNIT_TEST(ShouldFillBackoffTimeHistorgram)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        auto readBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");

        AddRequestStats(
            requestCounters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(200),
                 .PostponedTime = TDuration::MilliSeconds(100),
                 .BackoffTime = TDuration::MilliSeconds(50)},
            });

        requestCounters.UpdateStats(true);

        // Check the percentiles for BackoffTime
        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "BackoffTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(50000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(35000, p50->Val());
        }

        // Check the percentiles for ThrottlerDelay
        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "ThrottlerDelay")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(100000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(75000, p50->Val());
        }

        // Check the histogram for BackoffTime
        {
            auto histGroup =
                writeBlocks->GetSubgroup("histogram", "BackoffTime")
                    ->GetSubgroup("units", "usec");

            TMap<TString, uint64_t> expectedValues;
            for (const auto& name: TRequestUsTimeBuckets::MakeNames()) {
                expectedValues[name] = 0;
            }
            expectedValues["50000"] = 1;

            for (const auto& [name, value]: expectedValues) {
                auto counter = histGroup->FindCounter(name);
                UNIT_ASSERT_C(
                    counter,
                    "Counter " + name.Quote() + " not found");
                UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
            }
        }

        // Check the histogram for ThrottlerDelay
        {
            auto histGroup =
                writeBlocks->GetSubgroup("histogram", "ThrottlerDelay")
                    ->GetSubgroup("units", "usec");

            TMap<TString, uint64_t> expectedValues;
            for (const auto& name: TRequestUsTimeBuckets::MakeNames()) {
                expectedValues[name] = 0;
            }
            expectedValues["100000"] = 1;

            for (const auto& [name, value]: expectedValues) {
                auto counter = histGroup->FindCounter(name);
                UNIT_ASSERT_C(
                    counter,
                    "Counter " + name.Quote() + " not found");
                UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
            }
        }

        // Percentiles for BackoffTime for read blocks should be empty
        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "BackoffTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }
    }

    Y_UNIT_TEST(ShouldFillShapingTimeHistogramAndPercentiles)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        auto readBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "ReadBlocks");

        AddRequestStats(
            requestCounters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(200),
                 .PostponedTime = TDuration::MilliSeconds(50),
                 .BackoffTime = TDuration::MilliSeconds(30),
                 .ShapingTime = TDuration::MilliSeconds(100)},
            });

        requestCounters.UpdateStats(true);

        // Check the percentiles for ShapingTime
        {
            auto percentiles =
                writeBlocks->GetSubgroup("percentiles", "ShapingTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(100000, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(75000, p50->Val());
        }

        // Check the histogram for ShapingTime
        {
            auto histGroup =
                writeBlocks->GetSubgroup("histogram", "ShapingTime")
                    ->GetSubgroup("units", "usec");

            TMap<TString, uint64_t> expectedValues;
            for (const auto& name: TRequestUsTimeBuckets::MakeNames()) {
                expectedValues[name] = 0;
            }
            expectedValues["100000"] = 1;

            for (const auto& [name, value]: expectedValues) {
                auto counter = histGroup->FindCounter(name);
                UNIT_ASSERT_C(
                    counter,
                    "Counter " + name.Quote() + " not found");
                UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
            }
        }

        // Percentiles for ShapingTime for read blocks should be empty
        {
            auto percentiles =
                readBlocks->GetSubgroup("percentiles", "ShapingTime")
                    ->GetSubgroup("units", "usec");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(0, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, p50->Val());
        }
    }

    Y_UNIT_TEST(ShouldFillSizePercentiles)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        AddRequestStats(
            requestCounters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100),
                 .BackoffTime = TDuration::Zero(),
                 .ShapingTime = TDuration::Zero()},
                {.RequestBytes = 2_MB,
                 .RequestTime = TDuration::MilliSeconds(100),
                 .BackoffTime = TDuration::Zero(),
                 .ShapingTime = TDuration::Zero()},
                {.RequestBytes = 3_MB,
                 .RequestTime = TDuration::MilliSeconds(100),
                 .BackoffTime = TDuration::Zero(),
                 .ShapingTime = TDuration::Zero()},
            });

        requestCounters.UpdateStats(true);

        auto percentiles = counters->GetSubgroup("percentiles", "Size")
                               ->GetSubgroup("units", "KB");
        auto p100 = percentiles->GetCounter("100");
        auto p50 = percentiles->GetCounter("50");

        UNIT_ASSERT_VALUES_EQUAL(4*1024, p100->Val());
        UNIT_ASSERT_VALUES_EQUAL(1.5*1024, p50->Val());
    }

    Y_UNIT_TEST(ShouldCountSilentErrors)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters();
        requestCounters.Register(*monitoring->GetCounters());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto shoot = [&] (auto errorKind) {
            auto requestStarted = requestCounters.RequestStarted(
                WriteRequestType,
                1_MB);

            requestCounters.RequestCompleted(
                WriteRequestType,
                requestStarted -
                    DurationToCyclesSafe(TDuration::MilliSeconds(201)),
                TDuration::MilliSeconds(100),   // postponedTime
                TDuration::Zero(),              // backoffTime
                TDuration::Zero(),              // shapingTime
                1_MB,
                errorKind,
                NCloud::NProto::EF_NONE,
                false,
                ECalcMaxTime::ENABLE,
                0);
        };

        shoot(EDiagnosticsErrorKind::ErrorAborted);
        shoot(EDiagnosticsErrorKind::ErrorFatal);
        shoot(EDiagnosticsErrorKind::ErrorRetriable);
        shoot(EDiagnosticsErrorKind::ErrorThrottling);
        shoot(EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint);
        shoot(EDiagnosticsErrorKind::ErrorSession);
        shoot(EDiagnosticsErrorKind::ErrorSilent);

        requestCounters.UpdateStats(true);

        auto errors = counters->GetCounter("Errors");
        UNIT_ASSERT_VALUES_EQUAL(6, errors->Val());

        auto abort = counters->GetCounter("Errors/Aborted");
        UNIT_ASSERT_VALUES_EQUAL(1, abort->Val());

        auto fatal = counters->GetCounter("Errors/Fatal");
        UNIT_ASSERT_VALUES_EQUAL(1, fatal->Val());

        auto retriable = counters->GetCounter("Errors/Retriable");
        UNIT_ASSERT_VALUES_EQUAL(1, retriable->Val());

        auto throttling = counters->GetCounter("Errors/Throttling");
        UNIT_ASSERT_VALUES_EQUAL(1, throttling->Val());

        auto rejectedByCheckpoint = counters->GetCounter("Errors/CheckpointReject");
        UNIT_ASSERT_VALUES_EQUAL(1, rejectedByCheckpoint->Val());

        auto session = counters->GetCounter("Errors/Session");
        UNIT_ASSERT_VALUES_EQUAL(1, session->Val());

        auto silent = counters->GetCounter("Errors/Silent");
        UNIT_ASSERT_VALUES_EQUAL(1, silent->Val());
    }

    Y_UNIT_TEST(ShouldCountHwProblems)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::AddSpecialCounters,
             .ExecutionTimeSizeClasses = {}});
        requestCounters.Register(*monitoring->GetCounters());

        const auto requestType = ReadRequestType;

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", RequestType2Name(requestType));

        auto shoot = [&] (auto errorKind, ui32 errorFlags) {
            auto requestStarted = requestCounters.RequestStarted(
                requestType,
                1_MB);

            requestCounters.RequestCompleted(
                requestType,
                requestStarted -
                    DurationToCyclesSafe(TDuration::MilliSeconds(201)),
                TDuration::MilliSeconds(100),   // postponedTime
                TDuration::Zero(),              // backoffTime
                TDuration::Zero(),              // shapingTime
                1_MB,
                errorKind,
                errorFlags,
                false,
                ECalcMaxTime::ENABLE,
                0);
        };

        shoot(EDiagnosticsErrorKind::ErrorFatal,
            NCloud::NProto::EF_NONE);
        shoot(EDiagnosticsErrorKind::ErrorRetriable,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);
        shoot(EDiagnosticsErrorKind::ErrorThrottling,
            NCloud::NProto::EF_NONE);
        shoot(EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint,
            NCloud::NProto::EF_NONE);
        shoot(EDiagnosticsErrorKind::ErrorSession,
            NCloud::NProto::EF_NONE);
        shoot(EDiagnosticsErrorKind::ErrorSilent,
            NCloud::NProto::EF_HW_PROBLEMS_DETECTED);

        requestCounters.UpdateStats(true);

        auto errors = counters->GetCounter("Errors");
        UNIT_ASSERT_VALUES_EQUAL(5, errors->Val());

        auto fatal = counters->GetCounter("Errors/Fatal");
        UNIT_ASSERT_VALUES_EQUAL(1, fatal->Val());

        auto retriable = counters->GetCounter("Errors/Retriable");
        UNIT_ASSERT_VALUES_EQUAL(1, retriable->Val());

        auto throttling = counters->GetCounter("Errors/Throttling");
        UNIT_ASSERT_VALUES_EQUAL(1, throttling->Val());

        auto checkpointReject = counters->GetCounter("Errors/CheckpointReject");
        UNIT_ASSERT_VALUES_EQUAL(1, checkpointReject->Val());

        auto session = counters->GetCounter("Errors/Session");
        UNIT_ASSERT_VALUES_EQUAL(1, session->Val());

        auto silent = counters->GetCounter("Errors/Silent");
        UNIT_ASSERT_VALUES_EQUAL(1, silent->Val());

        auto hwProblems =
            monitoring->GetCounters()->GetCounter("HwProblems");
        UNIT_ASSERT_VALUES_EQUAL(2, hwProblems->Val());
    }

    Y_UNIT_TEST(ShouldNotUpdateSubscribers)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr();
        counters->Register(*monitoring->GetCounters());

        auto subscriber = MakeRequestCountersPtr();
        subscriber->Register(*monitoring->GetCounters()->GetSubgroup("subscribers", "s"));
        counters->Subscribe(subscriber);

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 2_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 3_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            });

        counters->UpdateStats();

        {
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("MaxTime");

            UNIT_ASSERT(maxTime->Val() > 0);
        }

        {
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("subscribers", "s")
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("maxTime");

            UNIT_ASSERT_EQUAL(0, maxTime->Val());
        }
    }

    Y_UNIT_TEST(ShouldNotifySubscribers)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr();
        counters->Register(*monitoring->GetCounters());

        auto outerSubscriber = MakeRequestCountersPtr();
        outerSubscriber->Register(*monitoring->GetCounters()->GetSubgroup("subscribers", "outer"));
        counters->Subscribe(outerSubscriber);

        auto innerSubscriber = MakeRequestCountersPtr();
        innerSubscriber->Register(*monitoring->GetCounters()->GetSubgroup("subscribers", "inner"));
        outerSubscriber->Subscribe(innerSubscriber);

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 2_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 3_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            });

        {
            counters->UpdateStats();
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("MaxTime");

            UNIT_ASSERT(maxTime->Val() > 0);
        }

        {
            outerSubscriber->UpdateStats();
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("subscribers", "outer")
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("MaxTime");

            UNIT_ASSERT(maxTime->Val() > 0);
        }

        {
            innerSubscriber->UpdateStats();
            auto maxTime = monitoring
                ->GetCounters()
                ->GetSubgroup("subscribers", "inner")
                ->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("MaxTime");

            UNIT_ASSERT(maxTime->Val() > 0);
        }
    }

    Y_UNIT_TEST(ShouldTrackSizeClasses)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .ExecutionTimeSizeClasses = {}});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Minutes(1)},
                {.RequestBytes = 1_MB, .RequestTime = TDuration::Minutes(1)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::Minutes(1),
                 .Aligned = true},
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::Minutes(1),
                 .Aligned = true},
            });

        counters->UpdateStats();
        {
            auto time = monitoring
                ->GetCounters()
                ->GetSubgroup("request", "WriteBlocks")
                ->GetSubgroup("sizeclass", "Unaligned")
                ->GetSubgroup("histogram", "Time")
                ->GetSubgroup("units", "usec")
                ->GetCounter("Inf");

            UNIT_ASSERT_VALUES_EQUAL(time->Val(), 2);
        }
    }

    void ShouldReportCompoundTimeHistogramWithMultipleCounters(
        EHistogramCounterOptions options)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .HistogramCounterOptions =
                 options | EHistogramCounterOption::ReportMultipleCounters});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(8)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(20)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(30)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(37)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(50)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(100)},
            });

        counters->UpdateStats();
        const auto timeHist = monitoring->GetCounters()
                                  ->GetSubgroup("request", "WriteBlocks")
                                  ->GetSubgroup("histogram", "Time");

        const auto usGroup = timeHist->GetSubgroup("units", "usec");
        const auto msGroup = timeHist;

        const auto validateCounters =
            [](TIntrusivePtr<NMonitoring::TDynamicCounters> group,
               TVector<TString> bucketNames,
               TStringBuf suffix,
               bool shouldExist)
        {
            TMap<TString, uint64_t> expectedHistogramValues;
            for (const auto& bucketName: bucketNames) {
                expectedHistogramValues[bucketName] = 0;
            }
            expectedHistogramValues[TString("10000") + suffix] = 1;
            expectedHistogramValues[TString("35000") + suffix] = 2;
            expectedHistogramValues["Inf"] = 3;

            for (const auto& [name, value]: expectedHistogramValues) {
                const auto counter = group->FindCounter(name);
                if (shouldExist) {
                    UNIT_ASSERT_C(
                        counter,
                        "Counter " + name.Quote() + " not found");
                    UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
                } else {
                    UNIT_ASSERT_C(
                        !counter,
                        "Counter " + name.Quote() + " should not exist");
                }
            }
        };

        validateCounters(
            usGroup,
            TRequestUsTimeBuckets::MakeNames(),
            "000",
            !(options & EHistogramCounterOption::UseMsUnitsForTimeHistogram));
        validateCounters(
            msGroup,
            TRequestMsTimeBuckets::MakeNames(),
            "ms",
            options & EHistogramCounterOption::UseMsUnitsForTimeHistogram);
    }

    Y_UNIT_TEST(ShouldReportCompoundTimeHistogram_UseMsUnitsForTimeHistogram)
    {
        ShouldReportCompoundTimeHistogramWithMultipleCounters(
            EHistogramCounterOption::UseMsUnitsForTimeHistogram);
    }

    Y_UNIT_TEST(ShouldReportCompoundTimeHistogram_UseUsUnitsForTimeHistogram)
    {
        ShouldReportCompoundTimeHistogramWithMultipleCounters(
            EHistogramCounterOptions());
    }

    void ShouldReportCompoundTimeHistogramWithSingleCounter(
        EHistogramCounterOptions options)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .HistogramCounterOptions =
                 options | EHistogramCounterOption::ReportSingleCounter});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(8)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(20)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(30)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(37)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(50)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(100)},
            });

        const TMap<size_t, uint64_t> expectedHistogramValues = {
            {22, 1},   // 10000ms
            {23, 2},   // 35000ms
            {24, 3},   // Inf
        };

        counters->UpdateStats();

        const auto timeHist = monitoring->GetCounters()
                                  ->GetSubgroup("request", "WriteBlocks")
                                  ->GetSubgroup("histogram", "Time");

        const auto usGroup = timeHist->GetSubgroup("units", "usec");
        const auto msGroup = timeHist;

        const auto validateCounters =
            [expectedHistogramValues](
                TIntrusivePtr<NMonitoring::TDynamicCounters> group,
                bool shouldExist)
        {
            const auto histogram = group->FindHistogram("Time");
            if (!shouldExist) {
                UNIT_ASSERT(!histogram);
                return;
            }
            UNIT_ASSERT(histogram);
            const auto snapshot = histogram->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot->Count(),
                TRequestMsTimeBuckets::Buckets.size());
            for (size_t bucketId = 0; bucketId < snapshot->Count(); bucketId++)
            {
                auto expectedValue = expectedHistogramValues.contains(bucketId)
                                         ? expectedHistogramValues.at(bucketId)
                                         : 0;
                UNIT_ASSERT_VALUES_EQUAL(
                    snapshot->Value(bucketId),
                    expectedValue);
            }
        };

        validateCounters(
            usGroup,
            !(options & EHistogramCounterOption::UseMsUnitsForTimeHistogram));
        validateCounters(
            msGroup,
            options & EHistogramCounterOption::UseMsUnitsForTimeHistogram);
    }

    Y_UNIT_TEST(
        ShouldReportCompoundTimeHistogramWithSingleCounter_UseMsUnitsForTimeHistogram)
    {
        ShouldReportCompoundTimeHistogramWithSingleCounter(
            EHistogramCounterOption::UseMsUnitsForTimeHistogram);
    }

    Y_UNIT_TEST(
        ShouldReportCompoundTimeHistogramWithSingleCounter_UseUsUnitsForTimeHistogram)
    {
        ShouldReportCompoundTimeHistogramWithSingleCounter(
            EHistogramCounterOptions());
    }

    Y_UNIT_TEST(ShouldReportHistogramAsMultipleSensors)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .HistogramCounterOptions =
                 EHistogramCounterOption::ReportMultipleCounters,
             .ExecutionTimeSizeClasses = {}});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(8)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(20)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(30)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(37)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(50)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(100)},
            });

        TMap<TString, uint64_t> expectedHistogramValues;
        for (const auto& bucketName : TRequestUsTimeBuckets::MakeNames()) {
            expectedHistogramValues[bucketName] = 0;
        }
        expectedHistogramValues["10000000"] = 1;
        expectedHistogramValues["35000000"] = 2;
        expectedHistogramValues["Inf"] = 3;

        counters->UpdateStats();
        const auto group = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetSubgroup("histogram", "Time")
            ->GetSubgroup("units", "usec");

        for (const auto& [name, value]: expectedHistogramValues) {
            const auto counter = group->FindCounter(name);
            UNIT_ASSERT_C(counter, "Counter " + name.Quote() + " not found");
            UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
        }
    }

    Y_UNIT_TEST(ShouldReportHistogramAsSingleSensor)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .HistogramCounterOptions =
                 EHistogramCounterOption::ReportSingleCounter,
             .ExecutionTimeSizeClasses = {}});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(8)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(20)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(30)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(37)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(50)},
                {.RequestBytes = 1_KB, .RequestTime = TDuration::Seconds(100)},
            });

        const TMap<size_t, uint64_t> expectedHistogramValues = {
            { 22, 1 }, // 10000ms
            { 23, 2 }, // 35000ms
            { 24, 3 }, // Inf
        };

        counters->UpdateStats();

        const auto histogram = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetSubgroup("histogram", "Time")
            ->GetSubgroup("units", "usec")
            ->FindHistogram("Time");
        UNIT_ASSERT(histogram);

        const auto snapshot = histogram->Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot->Count(), TRequestMsTimeBuckets::Buckets.size());
        for (size_t bucketId = 0; bucketId < snapshot->Count(); bucketId++) {
            auto expectedValue = expectedHistogramValues.contains(bucketId) ?
                expectedHistogramValues.at(bucketId) : 0;
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Value(bucketId), expectedValue);
        }
    }

    Y_UNIT_TEST(ShouldNotReportHistogramIfOptionIsNotSet)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .ExecutionTimeSizeClasses = {}});
        counters->Register(*monitoring->GetCounters());

        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(800)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(1500)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(2000)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(8000)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(36000)},
                {.RequestBytes = 1_KB,
                 .RequestTime = TDuration::MilliSeconds(100000)},
            });

        auto counter = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetSubgroup("histogram", "Time")
            ->GetSubgroup("units", "usec")
            ->FindCounter("1ms");

        UNIT_ASSERT(!counter);

        auto histogram = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks")
            ->GetSubgroup("histogram", "Time")
            ->GetSubgroup("units", "usec")
            ->FindHistogram("Time");

        UNIT_ASSERT(!histogram);
    }

    Y_UNIT_TEST(ShouldReportStatsForLargeRequests)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = MakeRequestCountersPtr();
        counters->Register(*monitoring->GetCounters());
        AddRequestStats(
            *counters,
            WriteRequestType,
            {
                {.RequestBytes = 8_GB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            });

        counters->UpdateStats();
        auto requestBytes = monitoring->GetCounters()
                                ->GetSubgroup("request", "WriteBlocks")
                                ->GetCounter("RequestBytes");

        UNIT_ASSERT_EQUAL_C(8_GB, requestBytes->Val(), requestBytes->Val());
    }

    Y_UNIT_TEST(ShouldNotRegisterOrAccountThrottlingMetrics)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCounters(
            {.Options =
                 TRequestCounters::EOption::ThrottlingHistogramsDisabled});
        requestCounters.Register(*monitoring->GetCounters());

        auto writeBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        AddRequestStats(
            requestCounters,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(200),
                 .PostponedTime = TDuration::MilliSeconds(50),
                 .BackoffTime = TDuration::MilliSeconds(30),
                 .ShapingTime = TDuration::MilliSeconds(40)},
            });

        requestCounters.UpdateStats(true);

        // Time percentile must still be reported
        {
            auto p100 = writeBlocks->GetSubgroup("percentiles", "Time")
                            ->GetSubgroup("units", "usec")
                            ->GetCounter("100");
            UNIT_ASSERT_VALUES_UNEQUAL(0, p100->Val());
        }

        for (const TString histogram:
             {"ExecutionTime", "ThrottlerDelay", "BackoffTime", "ShapingTime"})
        {
            auto percentilesGroup =
                writeBlocks->FindSubgroup("percentiles", histogram);
            UNIT_ASSERT_C(
                !percentilesGroup,
                "Percentiles for " << histogram << " should not be registered");
        }
    }

    Y_UNIT_TEST(ShouldNotRegisterDisaggregatedCounters)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto fsCounters = monitoring->GetCounters()->GetSubgroup("type", "fs");
        auto totalCounters = monitoring->GetCounters()->GetSubgroup("type", "total");

        auto aggregated = MakeRequestCountersPtr({});
        aggregated->Register(*totalCounters);

        auto perFs = MakeRequestCounters(
            {.Options = TRequestCounters::EOption::DisaggregatedCountersDisabled});
        perFs.Register(*fsCounters);
        perFs.Subscribe(aggregated);

        AddRequestStats(
            perFs,
            WriteRequestType,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            });

        perFs.UpdateStats(true);
        aggregated->UpdateStats(true);

        // Per-fs monitoring group must have no request subgroups at all
        UNIT_ASSERT_C(
            !fsCounters->FindSubgroup("request", "WriteBlocks"),
            "Per-fs request subgroup should not be registered");

        // Aggregated must still receive the stats via subscription
        auto count = totalCounters
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("Count", true);
        UNIT_ASSERT_VALUES_EQUAL(1, count->Val());
    }

    Y_UNIT_TEST(ShouldFillTimePercentilesForDifferentSizeClassesSeparately)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto requestCounters = MakeRequestCountersPtr(
            {.Options = TRequestCounters::EOption::ReportDataPlaneHistogram,
             .ExecutionTimeSizeClasses = {{4_KB, 512_KB}, {1_MB, 4_MB}}});
        requestCounters->Register(*monitoring->GetCounters());

        auto writeBlocks =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        auto addRequestStats = [&](ui64 size)
        {
            AddRequestStats(
                *requestCounters,
                WriteRequestType,
                {
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(11),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(23),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(33),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(40),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(50),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                    {.RequestBytes = size,
                     .RequestTime = TDuration::Seconds(100),
                     .PostponedTime = TDuration::Seconds(1),
                     .BackoffTime = TDuration::Seconds(1),
                     .ShapingTime = TDuration::Seconds(1)},
                });
        };

        // 1 size class
        addRequestStats(4_KB);
        // 2 size class
        addRequestStats(1_MB);

        // no size class
        addRequestStats(512_KB);
        addRequestStats(4_MB);

        TMap<TString, uint64_t> expectedHistogramValues;
        for (const auto& bucketName: TRequestUsTimeBuckets::MakeNames()) {
            expectedHistogramValues[bucketName] = 0;
        }
        expectedHistogramValues["10000000"] = 1;
        expectedHistogramValues["35000000"] = 2;
        expectedHistogramValues["Inf"] = 3;

        requestCounters->UpdateStats();

        auto checkSizeClass = [&](ui64 start, ui64 end)
        {
            const auto group = monitoring->GetCounters()
                                   ->GetSubgroup("request", "WriteBlocks")
                                   ->GetSubgroup(
                                       "sizeclass",
                                       ToString(TSizeInterval{start, end}))
                                   ->GetSubgroup("histogram", "ExecutionTime")
                                   ->GetSubgroup("units", "usec");

            for (const auto& [name, value]: expectedHistogramValues) {
                const auto counter = group->FindCounter(name);
                UNIT_ASSERT_C(
                    counter,
                    "Counter " + name.Quote() + " not found");
                UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
            }
        };

        checkSizeClass(4_KB, 512_KB);

        checkSizeClass(1_MB, 4_MB);
    }
}

}   // namespace NCloud
