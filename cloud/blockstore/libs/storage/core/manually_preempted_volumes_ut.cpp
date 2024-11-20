#include "manually_preempted_volumes.h"

#include "config.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TManuallyPreemptedVolumesTest)
{
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
        TVector<TString> criticalEvents;
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log,
            criticalEvents);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(criticalEvents.size(), 0);

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
        TVector<TString> criticalEvents;
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log,
            criticalEvents);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(criticalEvents.size(), 0);
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
        TVector<TString> criticalEvents;
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log,
            criticalEvents);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(criticalEvents.size(), 1);
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
        TVector<TString> criticalEvents;
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log,
            criticalEvents);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(criticalEvents.size(), 0);
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
        TVector<TString> criticalEvents;
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log,
            criticalEvents);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(criticalEvents.size(), 1);
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
        TVector<TString> criticalEvents;
        auto loaded = CreateManuallyPreemptedVolumes(
            storageConfig,
            log,
            criticalEvents);

        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(criticalEvents.size(), 0);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
