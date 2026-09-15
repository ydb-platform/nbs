#include "manually_preempted_volumes.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/logger/stream.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Initialize an isolated counter for synchronous file-error reports.
NMonitoring::TDynamicCounters::TCounterPtr InitFileErrorCounter()
{
    ResetCriticalEventsCounter();
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    NBlockStore::InitCriticalEventsCounter(counters);
    auto counter = counters->FindCounter(
        GetCriticalEventForManuallyPreemptedVolumesFileError());
    UNIT_ASSERT(counter);
    return counter;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TManuallyPreemptedVolumesTest)
{
    // Check immediate error logging and one publication after monitoring
    // starts.
    Y_UNIT_TEST(ShouldReportFileErrorBeforeCountersInitialized)
    {
        // Enable startup reporting before a monitoring root is available.
        ResetCriticalEventsCounter();
        TStringStream logStream;
        TLog log(MakeHolder<TStreamLogBackend>(&logStream));
        SetCriticalEventsLog(log);
        Y_DEFER
        {
            SetCriticalEventsLog(TLog());
            ResetCriticalEventsCounter();
        };
        InitProcessCriticalEventsReporting();

        // Load malformed input and retain its diagnostic in the immediate
        // report.
        TTempDir dir;
        const auto filePath = dir.Path() / "preempted-volumes.json";
        TOFStream(filePath).Write("{");
        auto volumes = CreateManuallyPreemptedVolumes(filePath, log);
        UNIT_ASSERT_VALUES_EQUAL(0, volumes->GetSize());
        const TString logged = logStream.Str();
        UNIT_ASSERT_STRING_CONTAINS(
            logged,
            "CRITICAL_EVENT:AppCriticalEvents/"
            "ManuallyPreemptedVolumesFileError");
        UNIT_ASSERT_STRING_CONTAINS(
            logged,
            "Failed to load preempted volumes list with error:");

        // Preserve the pending event across a tick without a monitoring root.
        auto handler = CreateCriticalEventsStatsHandler();
        handler->UpdateStats(true);
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        NBlockStore::InitCriticalEventsCounter(counters);
        auto counter = counters->FindCounter(
            GetCriticalEventForManuallyPreemptedVolumesFileError());
        UNIT_ASSERT(counter);
        UNIT_ASSERT(!counter->ForDerivative());
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());

        // Publish the accumulated event count, then zero for an empty interval.
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());
        UNIT_ASSERT_VALUES_EQUAL(logged, logStream.Str());
    }

    Y_UNIT_TEST(ShouldWriteAndReadPreemptedVolumes)
    {
        auto volumes = CreateManuallyPreemptedVolumes();
        volumes->AddVolume("Volume1", {TInstant::Now()});
        volumes->AddVolume("Volume2", {TInstant::Now()});

        auto output = volumes->Serialize();

        auto loaded = CreateManuallyPreemptedVolumes();

        auto readResult = loaded->Deserialize(std::move(output));

        UNIT_ASSERT_C(
            SUCCEEDED(readResult.GetCode()),
            "Unable to read what was written");

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), volumes->GetSize());
        UNIT_ASSERT_VALUES_EQUAL(loaded->Serialize(), output);
    }

    Y_UNIT_TEST(ShouldWriteAndReadEmptyPreemptedVolumes)
    {
        auto volumes = CreateManuallyPreemptedVolumes();

        auto output = volumes->Serialize();

        auto loaded = CreateManuallyPreemptedVolumes();

        auto readResult = loaded->Deserialize(std::move(output));

        UNIT_ASSERT_C(
            SUCCEEDED(readResult.GetCode()),
            "Unable to read what was written");

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
    }

    Y_UNIT_TEST(ShouldReadFromEmptyFile)
    {
        auto loaded = CreateManuallyPreemptedVolumes();

        auto readResult = loaded->Deserialize("");

        UNIT_ASSERT_C(
            SUCCEEDED(readResult.GetCode()),
            "Unable to read what was written");

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
    }

    Y_UNIT_TEST(ShouldLoadFromFileIfFeatureNotDisabled)
    {
        auto preemptedVolumesStr = R"(
            {"Volumes": [
                    {"DiskId": "volume1", "Timestamp": 123},
                    {"DiskId": "volume2", "Timestamp": 345}
                ]
            }
        )";

        TTempDir dir;
        auto preemptedVolumesPath = dir.Path() / "nbs-preempted-volumes.txt";
        TOFStream(preemptedVolumesPath.GetPath()).Write(preemptedVolumesStr);

        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile(preemptedVolumesPath.GetPath());
        config.SetDisableManuallyPreemptedVolumesTracking(false);

        auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(config),
            std::make_shared<NFeatures::TFeaturesConfig>());

        TLog log;
        auto errorCounter = InitFileErrorCounter();
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(errorCounter->Val(), 0);

        auto volume1res = loaded->GetVolume("volume1");
        UNIT_ASSERT_VALUES_EQUAL(volume1res.has_value(), true);
        UNIT_ASSERT_VALUES_EQUAL(
            volume1res->LastUpdate,
            TInstant::MicroSeconds(123));

        auto volume2res = loaded->GetVolume("volume2");
        UNIT_ASSERT_VALUES_EQUAL(volume2res.has_value(), true);
        UNIT_ASSERT_VALUES_EQUAL(
            volume2res->LastUpdate,
            TInstant::MicroSeconds(345));
    }

    Y_UNIT_TEST(ShouldNotLoadFromFileIfFeatureIsDisabled)
    {
        auto preemptedVolumesStr = R"(
            {"Volumes": [
                    {"DiskId": "volume1", "Timestamp": 123},
                    {"DiskId": "volume2", "Timestamp": 345}
                ]
            }
        )";

        TTempDir dir;
        auto preemptedVolumesPath = dir.Path() / "nbs-preempted-volumes.txt";
        TOFStream(preemptedVolumesPath.GetPath()).Write(preemptedVolumesStr);

        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile(preemptedVolumesPath.GetPath());
        config.SetDisableManuallyPreemptedVolumesTracking(true);

        auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(config),
            std::make_shared<NFeatures::TFeaturesConfig>());

        TLog log;
        auto errorCounter = InitFileErrorCounter();
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(errorCounter->Val(), 0);
    }

    Y_UNIT_TEST(ShouldRaiseCriticalEventIfFileIsBroken)
    {
        auto preemptedVolumesStr = R"(
            {"Volumes": [
                    {DiskId: "volume1", "Timestamp": 123},
                    {"DiskId": "volume2", "Timestamp": 345}
                ]
            }
        )";

        TTempDir dir;
        auto preemptedVolumesPath = dir.Path() / "nbs-preempted-volumes.txt";
        TOFStream(preemptedVolumesPath.GetPath()).Write(preemptedVolumesStr);

        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile(preemptedVolumesPath.GetPath());
        config.SetDisableManuallyPreemptedVolumesTracking(false);

        auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(config),
            std::make_shared<NFeatures::TFeaturesConfig>());

        TLog log;
        auto errorCounter = InitFileErrorCounter();
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(errorCounter->Val(), 1);
    }

    Y_UNIT_TEST(ShouldCreateManuallyPreemptedVolumesFileIfItDoesNotExist)
    {
        TTempDir dir;

        NProto::TStorageServiceConfig config;
        auto fpath = dir.Path() / "abc.json";
        config.SetManuallyPreemptedVolumesFile(fpath.GetPath());
        config.SetDisableManuallyPreemptedVolumesTracking(false);

        auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(config),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT(!fpath.IsFile());

        TLog log;
        auto errorCounter = InitFileErrorCounter();
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(errorCounter->Val(), 0);
        UNIT_ASSERT(fpath.IsFile());
    }

    Y_UNIT_TEST(ShouldRaiseCriticalEventIfFileDoesNotExistAndCannotBeCreated)
    {
        TTempDir dir;

        NProto::TStorageServiceConfig config;
        auto fpath = dir.Path() / "nosuchdir" / "abc.json";
        config.SetManuallyPreemptedVolumesFile(fpath.GetPath());
        config.SetDisableManuallyPreemptedVolumesTracking(false);

        auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(config),
            std::make_shared<NFeatures::TFeaturesConfig>());

        TLog log;
        auto errorCounter = InitFileErrorCounter();
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(errorCounter->Val(), 1);
        UNIT_ASSERT(!fpath.IsFile());
    }

    Y_UNIT_TEST(ShouldLoadEmptyPreemptedVolumesFile)
    {
        auto preemptedVolumesStr = "";

        TTempDir dir;
        auto preemptedVolumesPath = dir.Path() / "nbs-preempted-volumes.txt";
        TOFStream(preemptedVolumesPath.GetPath()).Write(preemptedVolumesStr);

        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile(preemptedVolumesPath.GetPath());
        config.SetDisableManuallyPreemptedVolumesTracking(false);

        auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(config),
            std::make_shared<NFeatures::TFeaturesConfig>());

        TLog log;
        auto errorCounter = InitFileErrorCounter();
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(errorCounter->Val(), 0);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
