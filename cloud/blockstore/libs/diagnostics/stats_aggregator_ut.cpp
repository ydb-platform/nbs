#include "stats_aggregator.h"

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>

namespace NCloud::NBlockStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 WriteBlocksCount = 64;
constexpr ui32 WriteBlocksInProgress = 32;

constexpr ui32 WriteBlocksPerVolumeCount = 42;
constexpr ui32 WriteBlocksPerVolumeInProgress = 21;

constexpr size_t BUCKETS_COUNT = 15;
constexpr std::array<ui32, BUCKETS_COUNT> TimeBuckets =
    {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 35000, Max<ui32>()};

////////////////////////////////////////////////////////////////////////////////

enum class EClientVersion
{
    None,
    OldClient,
    NewClient
};

////////////////////////////////////////////////////////////////////////////////

void BuildClientVersion(
    TDynamicCounterPtr clientGroup,
    EClientVersion clientVersionOption)
{
    switch (clientVersionOption) {
        case EClientVersion::OldClient: {
            auto versionCounter = clientGroup->GetNamedCounter(
                "version",
                "xxx",
                false);
            *versionCounter += 1;
            break;
        }
        case EClientVersion::NewClient: {
            auto revisionGroup = clientGroup->GetSubgroup("revision", "xxx");
            auto versionCounter = revisionGroup->GetCounter(
                "version",
                false);
            *versionCounter += 1;
            break;
        }
        default: break;
    }
}

////////////////////////////////////////////////////////////////////////////////

TDynamicCounterPtr SetupTestClientCounters(
    TVector<TString> diskIds,
    TVector<ui32> multipliers,
    bool insertHostLabel = true,
    EClientVersion clientVersionOption = EClientVersion::None)
{
    ui32 sum = 0;
    for (const auto m: multipliers) {
        sum += m;
    }

    TDynamicCounterPtr clientCounters = new TDynamicCounters;
    {
        auto rootGroup = clientCounters->GetSubgroup("counters", "blockstore");

        auto clientGroup = rootGroup->GetSubgroup("component", "client");
        {
            BuildClientVersion(clientGroup, clientVersionOption);
            auto writeBlocksGroup = clientGroup->GetSubgroup("request", "WriteBlocks");

            auto countCounter = writeBlocksGroup->GetCounter("Count", true);
            *countCounter = sum * WriteBlocksCount;

            auto inProgressCounter = writeBlocksGroup->GetCounter("InProgress");
            *inProgressCounter = sum * WriteBlocksInProgress;

            auto timeGroup = writeBlocksGroup->GetSubgroup("histogram", "Time");
            timeGroup = timeGroup->GetSubgroup("units", "usec");

            for (auto bucket : TimeBuckets) {
                auto bucketCounter = timeGroup->GetCounter(ToString(bucket)+"ms", true);
                *bucketCounter = sum;
            }
        }

        for (ui32 i = 0; i < diskIds.size(); ++i) {
            auto clientVolumeGroup = rootGroup->GetSubgroup("component", "client_volume");
            {
                if (insertHostLabel) {
                    clientVolumeGroup = clientVolumeGroup->GetSubgroup("host", "cluster");
                }
                auto volumeGroup = clientVolumeGroup->GetSubgroup("volume", diskIds[i]);
                auto writeBlocksGroup = volumeGroup->GetSubgroup("request", "WriteBlocks");

                auto countCounter = writeBlocksGroup->GetCounter("Count", true);
                *countCounter = multipliers[i] * WriteBlocksPerVolumeCount;

                auto inProgressCounter = writeBlocksGroup->GetCounter("InProgress");
                *inProgressCounter = multipliers[i] * WriteBlocksPerVolumeInProgress;
            }
        }
    }

    return clientCounters;
}

bool CheckClientPercentileCounters(
    TDynamicCounterPtr counters,
    const TString& diskType,
    const TString& requestName,
    const TString& sizeName,
    const TString& percentilesName)
{
    auto rootGroup = counters->FindSubgroup("counters", "blockstore");
    if (!rootGroup) {
        return false;
    }

    auto clientGroup = rootGroup->FindSubgroup("component", "client");
    if (!clientGroup) {
        return false;
    }

    TDynamicCountersPtr countersToCheck = clientGroup;

    if (diskType) {
        countersToCheck = clientGroup->FindSubgroup("type", diskType);
        if (!countersToCheck) {
            return false;
        }
    }

    countersToCheck = countersToCheck->FindSubgroup("request", requestName);
    if (!countersToCheck) {
        return false;
    }

    if (sizeName) {
        countersToCheck = countersToCheck->FindSubgroup("size", sizeName);
        if (!countersToCheck) {
            return false;
        }
    }

    countersToCheck = countersToCheck->FindSubgroup("percentiles", percentilesName);
    return (bool)countersToCheck;
}

TString EncodeCounters(TDynamicCounterPtr counters)
{
    TString encodedCounters;
    TStringOutput out(encodedCounters);
    auto encoder = CreateEncoder(&out, EFormat::SPACK);
    counters->Accept("", "", *encoder);

    return encodedCounters;
}

bool CompareCounters(TDynamicCounterPtr lhs, TDynamicCounterPtr rhs)
{
    return EncodeCounters(lhs) == EncodeCounters(rhs);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStatsAggregatorTest)
{
    Y_UNIT_TEST(ShouldAggregateStatsFromOneClient)
    {
        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto statsAggregator = CreateStatsAggregator(
            timer,
            scheduler,
            logging,
            monitoring,
            UpdateClientStats);

        auto diskId = CreateGuidAsString();
        auto clientCounters = SetupTestClientCounters({diskId}, {1});
        auto encodedClientCounters = EncodeCounters(clientCounters);

        statsAggregator->Start();

        statsAggregator->AddStats(diskId, encodedClientCounters);
        scheduler->RunAllScheduledTasks();

        statsAggregator->Stop();

        auto counters = monitoring->GetCounters();
        UNIT_ASSERT(CompareCounters(counters, clientCounters));
    }

    Y_UNIT_TEST(ShouldAggregateStatsFromTwoClients)
    {
        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto statsAggregator = CreateStatsAggregator(
            timer,
            scheduler,
            logging,
            monitoring,
            UpdateClientStats);

        statsAggregator->Start();

        auto diskId1 = CreateGuidAsString();
        auto firstClientCounters = SetupTestClientCounters({diskId1}, {1});
        auto encodedFirstClientCounters = EncodeCounters(firstClientCounters);
        statsAggregator->AddStats(diskId1, encodedFirstClientCounters);

        auto diskId2 = CreateGuidAsString();
        auto secondClientCounters = SetupTestClientCounters({diskId2}, {2});
        auto encodedSecondClientCounters = EncodeCounters(secondClientCounters);
        statsAggregator->AddStats(diskId2, encodedSecondClientCounters);

        scheduler->RunAllScheduledTasks();

        statsAggregator->Stop();

        // Counters with all values equal to the sum of first + second clients'
        // counters
        auto referenceCounters = SetupTestClientCounters({diskId1, diskId2}, {1, 2});

        auto counters = monitoring->GetCounters();
        UNIT_ASSERT(CompareCounters(counters, referenceCounters));
    }

    Y_UNIT_TEST(ShouldNotAggregateStatsFromExpiredClients)
    {
        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto statsAggregator = CreateStatsAggregator(
            timer,
            scheduler,
            logging,
            monitoring,
            UpdateClientStats);

        statsAggregator->Start();

        auto diskId1 = CreateGuidAsString();
        auto firstClientCounters = SetupTestClientCounters({diskId1}, {1});
        auto encodedFirstClientCounters = EncodeCounters(firstClientCounters);
        statsAggregator->AddStats(
            diskId1,
            encodedFirstClientCounters,
            TInstant::Now() - 3*UpdateCountersInterval);

        auto diskId2 = CreateGuidAsString();
        auto secondClientCounters = SetupTestClientCounters({diskId2}, {2});
        auto encodedSecondClientCounters = EncodeCounters(secondClientCounters);
        statsAggregator->AddStats(diskId2, encodedSecondClientCounters);

        auto diskId3 = CreateGuidAsString();
        auto thirdClientCounters = SetupTestClientCounters({diskId3}, {3});
        auto encodedThirdClientCounters = EncodeCounters(thirdClientCounters);
        statsAggregator->AddStats(diskId3, encodedThirdClientCounters);

        scheduler->RunAllScheduledTasks();

        // Counters with all values equal to the sum of second + third clients'
        // counters (first client should be considered expired)
        auto referenceCounters = SetupTestClientCounters({diskId2, diskId3}, {2, 3});

        auto counters = monitoring->GetCounters();
        UNIT_ASSERT(CompareCounters(counters, referenceCounters));
    }

    Y_UNIT_TEST(ShouldCreateAggregatedPercentilesFromClients)
    {
        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        auto percentiles = CreateClientPercentileCalculator(logging);

        auto statsAggregator = CreateStatsAggregator(
            timer,
            scheduler,
            logging,
            monitoring,
            [=] (
                NMonitoring::TDynamicCountersPtr updatedCounters,
                NMonitoring::TDynamicCountersPtr baseCounters)
            {
                percentiles->CalculatePercentiles(updatedCounters);
                UpdateClientStats(updatedCounters, baseCounters);
            });

        statsAggregator->Start();

        auto diskId1 = CreateGuidAsString();
        auto firstClientCounters = SetupTestClientCounters({diskId1}, {1});
        auto encodedFirstClientCounters = EncodeCounters(firstClientCounters);
        statsAggregator->AddStats(diskId1, encodedFirstClientCounters);

        auto diskId2 = CreateGuidAsString();
        auto secondClientCounters = SetupTestClientCounters({diskId2}, {2});
        auto encodedSecondClientCounters = EncodeCounters(secondClientCounters);
        statsAggregator->AddStats(diskId2, encodedSecondClientCounters);

        scheduler->RunAllScheduledTasks();

        auto counters = monitoring->GetCounters();
        UNIT_ASSERT(CheckClientPercentileCounters(counters, {}, "WriteBlocks", {}, "Time"));
    }

    Y_UNIT_TEST(ShouldAddHostLabelIfNecessary)
    {
        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto statsAggregator = CreateStatsAggregator(
            timer,
            scheduler,
            logging,
            monitoring,
            UpdateClientStats);

        statsAggregator->Start();

        auto diskId1 = CreateGuidAsString();
        auto firstClientCounters = SetupTestClientCounters({diskId1}, {1}, false);
        auto encodedFirstClientCounters = EncodeCounters(firstClientCounters);
        statsAggregator->AddStats(diskId1, encodedFirstClientCounters);

        auto diskId2 = CreateGuidAsString();
        auto secondClientCounters = SetupTestClientCounters({diskId2}, {2}, false);
        auto encodedSecondClientCounters = EncodeCounters(secondClientCounters);
        statsAggregator->AddStats(diskId2, encodedSecondClientCounters);

        scheduler->RunAllScheduledTasks();

        statsAggregator->Stop();

        // Counters with all values equal to the sum of first + second clients'
        // counters
        auto referenceCounters = SetupTestClientCounters({diskId1, diskId2}, {1, 2});

        auto counters = monitoring->GetCounters();
        UNIT_ASSERT(CompareCounters(counters, referenceCounters));
    }

    Y_UNIT_TEST(ShouldAddHostLabelIfNessesaryAndAggregate)
    {
        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto statsAggregator = CreateStatsAggregator(
            timer,
            scheduler,
            logging,
            monitoring,
            UpdateClientStats);

        statsAggregator->Start();

        auto diskId1 = CreateGuidAsString();
        auto firstClientCounters = SetupTestClientCounters({diskId1}, {1});
        auto encodedFirstClientCounters = EncodeCounters(firstClientCounters);
        statsAggregator->AddStats(diskId1, encodedFirstClientCounters);

        auto diskId2 = CreateGuidAsString();
        auto secondClientCounters = SetupTestClientCounters({diskId2}, {2}, false);
        auto encodedSecondClientCounters = EncodeCounters(secondClientCounters);
        statsAggregator->AddStats(diskId2, encodedSecondClientCounters);

        scheduler->RunAllScheduledTasks();

        statsAggregator->Stop();

        // Counters with all values equal to the sum of first + second clients'
        // counters
        auto referenceCounters = SetupTestClientCounters({diskId1, diskId2}, {1, 2});

        auto counters = monitoring->GetCounters();
        UNIT_ASSERT(CompareCounters(counters, referenceCounters));
    }

    Y_UNIT_TEST(ShouldFixOldClientVersion)
    {
        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto statsAggregator = CreateStatsAggregator(
            timer,
            scheduler,
            logging,
            monitoring,
            UpdateClientStats);

        statsAggregator->Start();

        auto diskId1 = CreateGuidAsString();
        auto firstClientCounters = SetupTestClientCounters({diskId1}, {1}, true, EClientVersion::None);
        auto encodedFirstClientCounters = EncodeCounters(firstClientCounters);
        statsAggregator->AddStats(diskId1, encodedFirstClientCounters);

        auto diskId2 = CreateGuidAsString();
        auto secondClientCounters = SetupTestClientCounters({diskId2}, {2}, false, EClientVersion::OldClient);
        auto encodedSecondClientCounters = EncodeCounters(secondClientCounters);
        statsAggregator->AddStats(diskId2, encodedSecondClientCounters);

        scheduler->RunAllScheduledTasks();

        statsAggregator->Stop();

        // Counters with all values equal to the sum of first + second clients'
        // counters
        auto referenceCounters = SetupTestClientCounters({diskId1, diskId2}, {1, 2}, true, EClientVersion::NewClient);

        auto counters = monitoring->GetCounters();
        UNIT_ASSERT(CompareCounters(counters, referenceCounters));
    }

    Y_UNIT_TEST(ShouldHandleNewClientVersion)
    {
        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto statsAggregator = CreateStatsAggregator(
            timer,
            scheduler,
            logging,
            monitoring,
            UpdateClientStats);

        statsAggregator->Start();

        auto diskId1 = CreateGuidAsString();
        auto firstClientCounters = SetupTestClientCounters({diskId1}, {1}, true, EClientVersion::None);
        auto encodedFirstClientCounters = EncodeCounters(firstClientCounters);
        statsAggregator->AddStats(diskId1, encodedFirstClientCounters);

        auto diskId2 = CreateGuidAsString();
        auto secondClientCounters = SetupTestClientCounters({diskId2}, {2}, false, EClientVersion::OldClient);
        auto encodedSecondClientCounters = EncodeCounters(secondClientCounters);
        statsAggregator->AddStats(diskId2, encodedSecondClientCounters);

        scheduler->RunAllScheduledTasks();

        statsAggregator->Stop();

        // Counters with all values equal to the sum of first + second clients'
        // counters
        auto referenceCounters = SetupTestClientCounters({diskId1, diskId2}, {1, 2}, true, EClientVersion::NewClient);

        auto counters = monitoring->GetCounters();
        UNIT_ASSERT(CompareCounters(counters, referenceCounters));
    }

}

}   // namespace NCloud::NBlockStore
