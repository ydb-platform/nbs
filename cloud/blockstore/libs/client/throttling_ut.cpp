#include "throttling.h"

#include "client.h"
#include "config.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats_test.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_logger.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/throttling/throttler_policy.h>
#include <cloud/blockstore/libs/throttling/throttler_tracker.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/throttling/helpers.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestThrottlerPolicy: IThrottlerPolicy
{
    TDuration PostponeTimeout;

    TDuration SuggestDelay(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        size_t byteCount) override
    {
        Y_UNUSED(now);
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);

        return byteCount ? PostponeTimeout : TDuration::Zero();
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const override
    {
        Y_UNUSED(ts);

        return PostponeTimeout.GetValue() / 1e6;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestCountingPolicy
{
    int PostponedCount[static_cast<int>(EBlockStoreRequest::MAX)] = {};
    int PostponedServerCount[static_cast<int>(EBlockStoreRequest::MAX)] = {};
    int AdvancedCount[static_cast<int>(EBlockStoreRequest::MAX)] = {};
    int AdvancedServerCount[static_cast<int>(EBlockStoreRequest::MAX)] = {};

    void RequestPostponed(EBlockStoreRequest requestType)
    {
        UNIT_ASSERT(IsReadWriteRequest(requestType));
        ++PostponedCount[static_cast<int>(requestType)];
    }

    void RequestPostponedServer(EBlockStoreRequest requestType)
    {
        UNIT_ASSERT(IsReadWriteRequest(requestType));
        ++PostponedServerCount[static_cast<int>(requestType)];
    }

    void RequestAdvanced(EBlockStoreRequest requestType)
    {
        UNIT_ASSERT(IsReadWriteRequest(requestType));
        ++AdvancedCount[static_cast<int>(requestType)];
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType)
    {
        UNIT_ASSERT(IsReadWriteRequest(requestType));
        ++AdvancedServerCount[static_cast<int>(requestType)];
    }
};

struct TSingleVolumeProcessingPolicy
{
    std::shared_ptr<TTestVolumeInfo<TRequestCountingPolicy>> VolumeInfo
        = std::make_shared<TTestVolumeInfo<TRequestCountingPolicy>>();

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId)
    {
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);

        VolumeInfo->Volume = volume;
        return true;
    }

    void UnmountVolume(
        const TString& diskId,
        const TString& clientId)
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId)
    {
        Y_UNUSED(diskId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const
    {
        Y_UNUSED(clientId);

        return diskId == VolumeInfo->Volume.GetDiskId() ? VolumeInfo : nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestStats final
    : public IRequestStats
{
    ui32 PostponedCount[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};
    ui32 PostponedServerCount[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};
    ui32 AdvancedCount[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};
    ui32 AdvancedServerCount[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};

    ui64 RequestStarted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui32 requestBytes) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(requestBytes);
        return 0;
    }

    TDuration RequestCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        ui32 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ECalcMaxTime calcMaxTime,
        ui64 responseSent) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(requestStarted);
        Y_UNUSED(postponedTime);
        Y_UNUSED(requestBytes);
        Y_UNUSED(errorKind);
        Y_UNUSED(errorFlags);
        Y_UNUSED(unaligned);
        Y_UNUSED(calcMaxTime);
        Y_UNUSED(responseSent);
        return TDuration::Zero();
    }

    void AddIncompleteStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime requestTime,
        ECalcMaxTime calcMaxTime) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(requestTime);
        Y_UNUSED(calcMaxTime);
    }

    void AddRetryStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(errorKind);
        Y_UNUSED(errorFlags);
    }

    void RequestPostponed(EBlockStoreRequest requestType) override
    {
        ++PostponedCount[static_cast<size_t>(requestType)];
    }

    void RequestPostponedServer(EBlockStoreRequest requestType) override
    {
        ++PostponedServerCount[static_cast<size_t>(requestType)];
    }

    void RequestAdvanced(EBlockStoreRequest requestType) override
    {
        ++AdvancedCount[static_cast<size_t>(requestType)];
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType) override
    {
        ++AdvancedServerCount[static_cast<size_t>(requestType)];
    }

    void RequestFastPathHit(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void BatchCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(count);
        Y_UNUSED(bytes);
        Y_UNUSED(errors);
        Y_UNUSED(timeHist);
        Y_UNUSED(sizeHist);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NProto::TMountVolumeRequest> CreateMountVolumeRequest()
{
    NProto::TMountVolumeRequest r;
    r.SetDiskId("xxx");
    return std::make_shared<NProto::TMountVolumeRequest>(std::move(r));
}

std::shared_ptr<NProto::TUnmountVolumeRequest> CreateUnmountVolumeRequest()
{
    NProto::TUnmountVolumeRequest r;
    r.SetDiskId("xxx");
    return std::make_shared<NProto::TUnmountVolumeRequest>(std::move(r));
}

std::shared_ptr<NProto::TZeroBlocksRequest> CreateZeroBlocksRequest(
    size_t blockCount)
{
    NProto::TZeroBlocksRequest r;
    r.SetDiskId("xxx");
    r.SetBlocksCount(blockCount);
    return std::make_shared<NProto::TZeroBlocksRequest>(std::move(r));
}

std::shared_ptr<NProto::TWriteBlocksRequest> CreateWriteBlocksRequest(
    size_t blockCount)
{
    NProto::TWriteBlocksRequest r;
    r.SetDiskId("xxx");
    auto buffers = r.MutableBlocks()->MutableBuffers();
    for (size_t i = 0; i < blockCount; ++i) {
        buffers->Add()->assign(5, 'a');
    }
    return std::make_shared<NProto::TWriteBlocksRequest>(std::move(r));
}

std::shared_ptr<NProto::TReadBlocksRequest> CreateReadBlocksRequest(
    size_t blockCount)
{
    NProto::TReadBlocksRequest r;
    r.SetDiskId("xxx");
    r.SetBlocksCount(blockCount);
    return std::make_shared<NProto::TReadBlocksRequest>(std::move(r));
}

std::shared_ptr<NProto::TWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
    size_t blockCount)
{
    NProto::TWriteBlocksLocalRequest r;
    r.SetDiskId("xxx");
    r.BlocksCount = blockCount;
    return std::make_shared<NProto::TWriteBlocksLocalRequest>(std::move(r));
}

std::shared_ptr<NProto::TReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
    size_t blockCount)
{
    NProto::TReadBlocksLocalRequest r;
    r.SetDiskId("xxx");
    r.SetBlocksCount(blockCount);
    return std::make_shared<NProto::TReadBlocksLocalRequest>(std::move(r));
}

////////////////////////////////////////////////////////////////////////////////

ui32 GetApproximateValue(
    ui32 byteCount,
    ui32 iops,
    ui32 bandwidth)
{
    const auto maxIops = CalculateThrottlerC1(iops, bandwidth);
    const auto maxBandwidth = CalculateThrottlerC2(iops, bandwidth);
    return byteCount - static_cast<double>(maxBandwidth) / maxIops;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TThrottlingClientTest)
{
    Y_UNIT_TEST(TestPreparePerformanceProfileForHostType)
    {
        NProto::TClientConfig clientConfig;
        NProto::TClientProfile clientProfile;
        clientProfile.SetCpuUnitCount(100);
        clientProfile.SetHostType(NProto::HOST_TYPE_DEDICATED);
        auto& tc = *clientConfig.MutableThrottlingConfig();
        tc.SetIopsPerCpuUnit(1);
        tc.SetBandwidthPerCpuUnit(10);

        NProto::TClientPerformanceProfile performanceProfile;
        UNIT_ASSERT(!PreparePerformanceProfile(
            THostPerformanceProfile{},
            clientConfig,
            clientProfile,
            performanceProfile
        ));

        clientProfile.SetHostType(NProto::HOST_TYPE_DEFAULT);
        UNIT_ASSERT(PreparePerformanceProfile(
            THostPerformanceProfile{},
            clientConfig,
            clientProfile,
            performanceProfile
        ));
    }

    Y_UNIT_TEST(TestPostponePolicy)
    {
        NProto::TClientConfig clientConfig;
        NProto::TClientProfile clientProfile;
        clientProfile.SetCpuUnitCount(512);
        auto& tc = *clientConfig.MutableThrottlingConfig();
        tc.SetIopsPerCpuUnit(1);
        tc.SetBandwidthPerCpuUnit(2);
        tc.SetBurstDivisor(10);
        tc.MutableNonreplThrottlingConfig()->SetReadIopsPerCpuUnit(8);
        tc.MutableNonreplThrottlingConfig()->SetWriteIopsPerCpuUnit(8);
        tc.MutableNonreplThrottlingConfig()->SetReadBandwidthPerCpuUnit(4);
        tc.MutableNonreplThrottlingConfig()->SetWriteBandwidthPerCpuUnit(4);

        NProto::TClientPerformanceProfile performanceProfile;
        UNIT_ASSERT(PreparePerformanceProfile(
            THostPerformanceProfile{},
            clientConfig,
            clientProfile,
            performanceProfile
        ));
        auto policy = CreateClientThrottlerPolicy(performanceProfile);

#define DO_TEST(expectedDelayMcs, timeMcs, mediaKind, requestType, bs)         \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            TDuration::MicroSeconds(expectedDelayMcs),                         \
            policy->SuggestDelay(                                              \
                TInstant::MicroSeconds(timeMcs),                               \
                mediaKind,                                                     \
                requestType,                                                   \
                bs                                                             \
            )                                                                  \
        );                                                                     \
// DO_TEST

        DO_TEST(
            0,
            1'000'000,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::ReadBlocks,
            GetApproximateValue(
                207460_KB,
                performanceProfile.GetSSDProfile().GetMaxReadIops(),
                performanceProfile.GetSSDProfile().GetMaxReadBandwidth())
        );
        DO_TEST(
            1'000'000,
            1'000'000,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::WriteBlocks,
            GetApproximateValue(
                2_GB,
                performanceProfile.GetSSDProfile().GetMaxWriteIops(),
                performanceProfile.GetSSDProfile().GetMaxWriteBandwidth())
        );
        DO_TEST(
            0,
            2'000'000,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::ReadBlocksLocal,
            GetApproximateValue(
                2_GB,
                performanceProfile.GetSSDProfile().GetMaxWriteIops(),
                performanceProfile.GetSSDProfile().GetMaxWriteBandwidth())
        );
        DO_TEST(
            500'000,
            2'000'000,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::ReadBlocksLocal,
            GetApproximateValue(
                1023_MB,
                performanceProfile.GetSSDProfile().GetMaxReadIops(),
                performanceProfile.GetSSDProfile().GetMaxReadBandwidth())
        );
        for (int i = 0; i < 20; ++i) {
            DO_TEST(
                0,
                2'000'000,
                NCloud::NProto::STORAGE_MEDIA_SSD,
                EBlockStoreRequest::ZeroBlocks,
                GetApproximateValue(
                    1023_MB,
                    performanceProfile.GetSSDProfile().GetMaxReadIops(),
                    performanceProfile.GetSSDProfile().GetMaxReadBandwidth()));
        }
        for (int i = 0; i < 20; ++i) {
            DO_TEST(
                0, 2'500'000,
                NCloud::NProto::STORAGE_MEDIA_SSD,
                EBlockStoreRequest::WriteBlocksLocal,
                GetApproximateValue(
                    10_MB,
                    performanceProfile.GetSSDProfile().GetMaxWriteIops(),
                    performanceProfile.GetSSDProfile().GetMaxWriteBandwidth())
            );
        }
        DO_TEST(
            2'649,
            2'500'000,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::ReadBlocks,
            GetApproximateValue(
                10_MB,
                performanceProfile.GetSSDProfile().GetMaxReadIops(),
                performanceProfile.GetSSDProfile().GetMaxReadBandwidth())
        );
        DO_TEST(
            2'038,
            2'500'000,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            EBlockStoreRequest::ReadBlocksLocal,
            GetApproximateValue(
                10_MB,
                performanceProfile.GetNonreplProfile().GetMaxReadIops(),
                performanceProfile.GetNonreplProfile().GetMaxReadBandwidth())
        );
        // TODO: STORAGE_MEDIA_HDD_NONREPLICATED

#undef DO_TEST
    }

    Y_UNIT_TEST(TestPreparePerformanceProfileOverflow)
    {
        NProto::TClientConfig clientConfig;
        NProto::TClientProfile clientProfile;
        ui64 iops = 400;
        ui64 bw = 100;

        auto& tc = *clientConfig.MutableThrottlingConfig();
        tc.SetIopsPerCpuUnit(iops);
        tc.SetBandwidthPerCpuUnit(bw);
        tc.SetBurstDivisor(10);
        auto& nonreplConfig = *tc.MutableNonreplThrottlingConfig();
        nonreplConfig.SetReadIopsPerCpuUnit(2 * iops);
        nonreplConfig.SetWriteIopsPerCpuUnit(2 * iops);
        nonreplConfig.SetReadBandwidthPerCpuUnit(2 * bw);
        nonreplConfig.SetWriteBandwidthPerCpuUnit(2 * bw);
        nonreplConfig.SetMaxReadIops(100'000);
        nonreplConfig.SetMaxWriteIops(200'000);
        nonreplConfig.SetMaxReadBandwidth(1'000);
        nonreplConfig.SetMaxWriteBandwidth(2'000);

        for (ui32 cpuUnitCount = 1; cpuUnitCount < 100000; ++cpuUnitCount) {
            clientProfile.SetCpuUnitCount(cpuUnitCount);

            const auto correctedCpuUnitCount = Max(100u, cpuUnitCount);
            const auto expectedIops = iops * correctedCpuUnitCount;
            const auto expectedBandwidth = bw * correctedCpuUnitCount * 1_MB;
            const auto expectedReadIopsNonreplicated = Min(100'000UL, 2 * expectedIops);
            const auto expectedWriteIopsNonreplicated = Min(200'000UL, 2 * expectedIops);
            const auto expectedReadBandwidthNonreplicated =
                Min(1'000_MB, 2 * expectedBandwidth);
            const auto expectedWriteBandwidthNonreplicated =
                Min(2'000_MB, 2 * expectedBandwidth);

            NProto::TClientPerformanceProfile performanceProfile;
            UNIT_ASSERT(PreparePerformanceProfile(
                THostPerformanceProfile{},
                clientConfig,
                clientProfile,
                performanceProfile
            ));

            auto profiles = {
                &performanceProfile.GetHDDProfile(),
                &performanceProfile.GetSSDProfile(),
            };

            for (const auto* profile: profiles) {
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedIops,
                    profile->GetMaxReadIops()
                );

                UNIT_ASSERT_VALUES_EQUAL(
                    expectedIops,
                    profile->GetMaxWriteIops()
                );

                UNIT_ASSERT_VALUES_EQUAL(
                    expectedBandwidth,
                    profile->GetMaxReadBandwidth()
                );

                UNIT_ASSERT_VALUES_EQUAL(
                    expectedBandwidth,
                    profile->GetMaxWriteBandwidth()
                );
            }

            UNIT_ASSERT_VALUES_EQUAL(
                expectedReadIopsNonreplicated,
                performanceProfile.GetNonreplProfile().GetMaxReadIops()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                expectedWriteIopsNonreplicated,
                performanceProfile.GetNonreplProfile().GetMaxWriteIops()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                expectedReadBandwidthNonreplicated,
                performanceProfile.GetNonreplProfile().GetMaxReadBandwidth()
            );

            UNIT_ASSERT_VALUES_EQUAL(
                expectedWriteBandwidthNonreplicated,
                performanceProfile.GetNonreplProfile().GetMaxWriteBandwidth()
            );
        }
    }

    Y_UNIT_TEST(TestPreparePerformanceProfileDefaultNonreplicatedLimits)
    {
        NProto::TClientConfig clientConfig;
        NProto::TClientProfile clientProfile;
        ui64 iops = 400;
        ui64 bw = 1;
        clientConfig.MutableThrottlingConfig()->SetIopsPerCpuUnit(iops);
        clientConfig.MutableThrottlingConfig()->SetBandwidthPerCpuUnit(bw);
        clientConfig.MutableThrottlingConfig()->SetBurstDivisor(10);
        clientProfile.SetCpuUnitCount(100);

        NProto::TClientPerformanceProfile performanceProfile;
        UNIT_ASSERT(PreparePerformanceProfile(
            THostPerformanceProfile{},
            clientConfig,
            clientProfile,
            performanceProfile
        ));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            performanceProfile.GetNonreplProfile().GetMaxReadIops()
        );

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            performanceProfile.GetNonreplProfile().GetMaxWriteIops()
        );

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            performanceProfile.GetNonreplProfile().GetMaxReadBandwidth()
        );

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            performanceProfile.GetNonreplProfile().GetMaxWriteBandwidth()
        );
    }

    Y_UNIT_TEST(ShouldReturnCorrectStatusCodeFromClient)
    {
        auto client = std::make_shared<TTestService>();

        client->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TPingResponse>(TErrorResponse(
                    E_NOT_IMPLEMENTED,
                    TStringBuilder() << "Unsupported request"));
            };

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            CreateThrottlerMetricsStub(),
            std::make_shared<TTestThrottlerPolicy>(),
            CreateThrottlerTrackerStub(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateVolumeStatsStub());

        auto throttling = CreateThrottlingClient(
            std::move(client),
            std::move(throttler));

        auto futurePing = throttling->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());
        UNIT_ASSERT(futurePing.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_IMPLEMENTED,
            futurePing.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldPostponeRequests)
    {
        auto client = std::make_shared<TTestService>();

#define SET_HANDLER(name)                                                      \
        client->name##Handler =                                                \
            [&] (std::shared_ptr<NProto::T##name##Request> request) {          \
                Y_UNUSED(request);                                             \
                return MakeFuture(NProto::T##name##Response());                \
            };                                                                 \
// SET_HANDLER

        SET_HANDLER(ZeroBlocks);
        SET_HANDLER(WriteBlocks);
        SET_HANDLER(ReadBlocks);
        SET_HANDLER(WriteBlocksLocal);
        SET_HANDLER(ReadBlocksLocal);
        SET_HANDLER(UnmountVolume);

#undef SET_HANDLER

        auto volumeStats = std::make_shared<
            TTestVolumeStats<TSingleVolumeProcessingPolicy>>();

        const auto clientId = "test_client";

        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TVolume volume;
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(100);
                volumeStats->MountVolume(std::move(volume), clientId, "");

                NProto::TMountVolumeResponse r;
                r.MutableVolume()->SetDiskId(request->GetDiskId());

                return MakeFuture(std::move(r));
            };

        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TTestThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto logging = CreateLoggingService("console");

        auto monitoring = CreateMonitoringServiceStub();
        auto logger = CreateClientThrottlerLogger(requestStats, logging);

        auto throttler = CreateThrottler(
            logger,
            CreateThrottlerMetricsStub(),
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto throttling = CreateThrottlingClient(
            client,
            std::move(throttler));

        auto fm = throttling->MountVolume(
            MakeIntrusive<TCallContext>(),
            CreateMountVolumeRequest());
        UNIT_ASSERT(fm.HasValue());

        auto fz = throttling->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            CreateZeroBlocksRequest(1));
        auto fw = throttling->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            CreateWriteBlocksRequest(1));
        auto fr = throttling->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            CreateReadBlocksRequest(1));
        auto fwl = throttling->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            CreateWriteBlocksLocalRequest(1));
        auto frl = throttling->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            CreateReadBlocksLocalRequest(1));
        auto fum = throttling->UnmountVolume(
            MakeIntrusive<TCallContext>(),
            CreateUnmountVolumeRequest());
        UNIT_ASSERT(fz.HasValue());
        UNIT_ASSERT(fw.HasValue());
        UNIT_ASSERT(fr.HasValue());
        UNIT_ASSERT(fwl.HasValue());
        UNIT_ASSERT(frl.HasValue());
        UNIT_ASSERT(fum.HasValue());

        const auto& pc = volumeStats->VolumeInfo->PostponedCount;
        const auto& ac = volumeStats->VolumeInfo->AdvancedCount;
        UNIT_ASSERT_VALUES_EQUAL(0, pc[static_cast<int>(EBlockStoreRequest::ZeroBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, pc[static_cast<int>(EBlockStoreRequest::WriteBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, pc[static_cast<int>(EBlockStoreRequest::ReadBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, pc[static_cast<int>(EBlockStoreRequest::WriteBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(0, pc[static_cast<int>(EBlockStoreRequest::ReadBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::ZeroBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::WriteBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::ReadBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::WriteBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::ReadBlocksLocal)]);

        policy->PostponeTimeout = TDuration::Seconds(1);

        fz = throttling->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            CreateZeroBlocksRequest(1));
        fw = throttling->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            CreateWriteBlocksRequest(1));
        fr = throttling->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            CreateReadBlocksRequest(1));
        fwl = throttling->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            CreateWriteBlocksLocalRequest(1));
        frl = throttling->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            CreateReadBlocksLocalRequest(1));
        fum = throttling->UnmountVolume(
            MakeIntrusive<TCallContext>(),
            CreateUnmountVolumeRequest());

        UNIT_ASSERT(!fz.HasValue());
        UNIT_ASSERT(!fw.HasValue());
        UNIT_ASSERT(!fr.HasValue());
        UNIT_ASSERT(!fwl.HasValue());
        UNIT_ASSERT(!frl.HasValue());
        UNIT_ASSERT(fum.HasValue());

        auto anotherDiskRequest = CreateReadBlocksRequest(1);
        anotherDiskRequest->SetDiskId("yyy");
        auto anotherDiskResponse = throttling->ReadBlocks(
            MakeIntrusive<TCallContext>(), anotherDiskRequest);
        UNIT_ASSERT(!anotherDiskResponse.HasValue());

        policy->PostponeTimeout = TDuration::Zero();
        auto fz2 = throttling->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            CreateZeroBlocksRequest(1));
        auto fw2 = throttling->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            CreateWriteBlocksRequest(1));
        auto fr2 = throttling->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            CreateReadBlocksRequest(1));
        auto fwl2 = throttling->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            CreateWriteBlocksLocalRequest(1));
        auto frl2 = throttling->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            CreateReadBlocksLocalRequest(1));
        UNIT_ASSERT(!fz2.HasValue());
        UNIT_ASSERT(!fw2.HasValue());
        UNIT_ASSERT(!fr2.HasValue());
        UNIT_ASSERT(!fwl2.HasValue());
        UNIT_ASSERT(!frl2.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::ZeroBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::WriteBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::ReadBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::WriteBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::ReadBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::ZeroBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::WriteBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::ReadBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::WriteBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(0, ac[static_cast<int>(EBlockStoreRequest::ReadBlocksLocal)]);

        scheduler->RunAllScheduledTasks();
        UNIT_ASSERT(fz.HasValue());
        UNIT_ASSERT(fw.HasValue());
        UNIT_ASSERT(fr.HasValue());
        UNIT_ASSERT(fwl.HasValue());
        UNIT_ASSERT(frl.HasValue());
        UNIT_ASSERT(anotherDiskResponse.HasValue());
        UNIT_ASSERT(fz2.HasValue());
        UNIT_ASSERT(fw2.HasValue());
        UNIT_ASSERT(fr2.HasValue());
        UNIT_ASSERT(fwl2.HasValue());
        UNIT_ASSERT(frl2.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::ZeroBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::WriteBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::ReadBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::WriteBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(2, pc[static_cast<int>(EBlockStoreRequest::ReadBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(2, ac[static_cast<int>(EBlockStoreRequest::ZeroBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, ac[static_cast<int>(EBlockStoreRequest::WriteBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, ac[static_cast<int>(EBlockStoreRequest::ReadBlocks)]);
        UNIT_ASSERT_VALUES_EQUAL(2, ac[static_cast<int>(EBlockStoreRequest::WriteBlocksLocal)]);
        UNIT_ASSERT_VALUES_EQUAL(2, ac[static_cast<int>(EBlockStoreRequest::ReadBlocksLocal)]);

        policy->PostponeTimeout = TDuration::Seconds(1);
        anotherDiskRequest = CreateReadBlocksRequest(1);
        anotherDiskRequest->SetDiskId("yyy");
        anotherDiskResponse = throttling->ReadBlocks(
            MakeIntrusive<TCallContext>(), anotherDiskRequest);
        UNIT_ASSERT(anotherDiskResponse.HasValue());
        fz = throttling->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            CreateZeroBlocksRequest(1));
        UNIT_ASSERT(!fz.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(3, pc[static_cast<int>(EBlockStoreRequest::ZeroBlocks)]);

        policy->PostponeTimeout = TDuration::Zero();
        scheduler->RunAllScheduledTasks();
        UNIT_ASSERT(fz.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(3, ac[static_cast<int>(EBlockStoreRequest::ZeroBlocks)]);
    }

    Y_UNIT_TEST(ShouldProvideSameThrottlerForSameClientId)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto throttlerProvider = CreateThrottlerProvider(
            THostPerformanceProfile{},
            CreateLoggingService("console"),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            monitoring->GetCounters(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub());

        NProto::TClientConfig clientConfig;
        clientConfig.MutableThrottlingConfig()->SetIopsPerCpuUnit(1);
        clientConfig.MutableThrottlingConfig()->SetBandwidthPerCpuUnit(10);

        NProto::TClientProfile clientProfile;
        clientProfile.SetCpuUnitCount(100);

        auto clientId = "testClientId1";
        auto otherClientId = "testClientId2";

        clientConfig.SetClientId(clientId);
        auto throttler1 = throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            NProto::TClientPerformanceProfile());
        UNIT_ASSERT(throttler1 != nullptr);
        auto profile1 = throttlerProvider->GetPerformanceProfile(clientId);

        clientConfig.SetClientId(otherClientId);
        auto throttler2 = throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            NProto::TClientPerformanceProfile());
        UNIT_ASSERT(throttler2 != nullptr && throttler2 != throttler1);
        auto profile2 = throttlerProvider->GetPerformanceProfile(otherClientId);

        clientConfig.SetClientId(clientId);
        auto throttler3 = throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            NProto::TClientPerformanceProfile());
        UNIT_ASSERT(throttler3 == throttler1);

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(comparator.Equals(
            profile1,
            throttlerProvider->GetPerformanceProfile(clientId)));

        // Should remove unused throttlers

        throttler1.reset();
        throttler3.reset();
        throttlerProvider->Clean();

        UNIT_ASSERT(!comparator.Equals(
            profile1,
            throttlerProvider->GetPerformanceProfile(clientId)));

        throttler2.reset();
        clientConfig.SetClientId(clientId);
        throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            NProto::TClientPerformanceProfile());

        UNIT_ASSERT(!comparator.Equals(
            profile2,
            throttlerProvider->GetPerformanceProfile(otherClientId)));
    }

    Y_UNIT_TEST(ShouldChangePerformanceProfileForSameClientId)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto throttlerProvider = CreateThrottlerProvider(
            THostPerformanceProfile{},
            CreateLoggingService("console"),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            monitoring->GetCounters(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub());

        auto clientId = "testClientId1";

        NProto::TClientConfig clientConfig;
        clientConfig.SetClientId(clientId);
        clientConfig.MutableThrottlingConfig()->SetIopsPerCpuUnit(1);
        clientConfig.MutableThrottlingConfig()->SetBandwidthPerCpuUnit(10);

        NProto::TClientProfile clientProfile;
        clientProfile.SetCpuUnitCount(100);

        NProto::TClientPerformanceProfile performanceProfile;

        performanceProfile.SetBurstTime(100);
        auto throttler1 = throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            performanceProfile);
        UNIT_ASSERT(throttler1 != nullptr);
        auto firstProfile = throttlerProvider->GetPerformanceProfile(clientId);

        performanceProfile.SetBurstTime(200);
        auto throttler2 = throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            performanceProfile);
        UNIT_ASSERT(throttler2 == throttler1);

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(!comparator.Equals(
            firstProfile,
            throttlerProvider->GetPerformanceProfile(clientId)));
    }

    Y_UNIT_TEST(ShouldNotProvideThrottlerIfClientProfileIsNotInitialized)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto throttlerProvider = CreateThrottlerProvider(
            THostPerformanceProfile{},
            CreateLoggingService("console"),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            monitoring->GetCounters(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub());

        NProto::TClientConfig clientConfig;
        clientConfig.MutableThrottlingConfig()->SetIopsPerCpuUnit(1);
        clientConfig.MutableThrottlingConfig()->SetBandwidthPerCpuUnit(10);

        auto clientId = "testClientId";
        NProto::TClientProfile clientProfile;
        clientConfig.SetClientId(clientId);

        auto throttler = throttlerProvider->GetThrottler(
            clientConfig,
            NProto::TClientProfile(),
            NProto::TClientPerformanceProfile());
        UNIT_ASSERT(throttler == nullptr);

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(comparator.Equals(
            NProto::TClientPerformanceProfile(),
            throttlerProvider->GetPerformanceProfile(clientId)));
    }

    Y_UNIT_TEST(ShouldProvideDifferentThrottlersForEmptyClientId)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto throttlerProvider = CreateThrottlerProvider(
            THostPerformanceProfile{},
            CreateLoggingService("console"),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            monitoring->GetCounters(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub());

        NProto::TClientConfig clientConfig;
        clientConfig.MutableThrottlingConfig()->SetIopsPerCpuUnit(1);
        clientConfig.MutableThrottlingConfig()->SetBandwidthPerCpuUnit(10);

        NProto::TClientProfile clientProfile;
        clientProfile.SetCpuUnitCount(100);

        NProto::TClientPerformanceProfile performanceProfile;
        auto throttler1 = throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            performanceProfile);
        UNIT_ASSERT(throttler1 != nullptr);

        clientConfig.SetClientId("testClientId");
        auto throttler2 = throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            performanceProfile);
        UNIT_ASSERT(throttler2 != nullptr && throttler2 != throttler1);

        clientConfig.ClearClientId();
        auto throttler3 = throttlerProvider->GetThrottler(
            clientConfig,
            clientProfile,
            performanceProfile);
        UNIT_ASSERT(throttler3 != throttler1);
    }

    Y_UNIT_TEST(ShouldRegisterCountersOnlyAfterFirstNonZeroQuotaValue)
    {
        const TString instanceId = "test_instance";
        auto volumeStats = std::make_shared<
            TTestVolumeStats<TSingleVolumeProcessingPolicy>>();

        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TTestThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        auto metrics = CreateThrottlerMetrics(
            timer,
            totalCounters,
            "server");

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            metrics,
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = std::make_shared<TTestService>();

#define SET_HANDLER(name)                                                      \
        client->name##Handler =                                                \
            [&] (std::shared_ptr<NProto::T##name##Request> request) {          \
                Y_UNUSED(request);                                             \
                return MakeFuture(NProto::T##name##Response());                \
            };                                                                 \
// SET_HANDLER

        SET_HANDLER(UnmountVolume);

#undef SET_HANDLER

        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TVolume volume;
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(100);

                NProto::TMountVolumeResponse r;
                r.MutableVolume()->SetDiskId(request->GetDiskId());

                return MakeFuture(std::move(r));
            };

        const TString usedQuota = "UsedQuota";
        const TString maxUsedQuota = "MaxUsedQuota";
        auto getDiskGroupFunction = [&] (const TString& diskId)
        {
            return totalCounters
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->FindSubgroup("volume", diskId);
        };

        auto getInstanceGroupFunction = [&] (
            const TString& diskId,
            const TString& instanceId)
        {
            auto diskGroup = getDiskGroupFunction(diskId);
            UNIT_ASSERT_C(
                diskGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                    << " should be initialized");
            return diskGroup->FindSubgroup("instance", instanceId);
        };

        auto getCounterFunction = [&] (
            const TString& diskId,
            const TString& instanceId,
            const TString& sensor)
        {
            auto instanceGroup = getInstanceGroupFunction(diskId, instanceId);
            UNIT_ASSERT_C(
                instanceGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                    << ", instance:" << instanceId
                    << " should be initialized");
            return instanceGroup->FindCounter(sensor);
        };

        const TString volumeId = "test_volume";
        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetInstanceId(instanceId);
        mountRequest->SetDiskId(volumeId);

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        UNIT_ASSERT_C(
            totalCounters->FindSubgroup("component", "server"),
            "Subgroup should be initialized");
        UNIT_ASSERT_C(
            getDiskGroupFunction(volumeId),
            "Subgroup should be initialized");

        metrics->UpdateUsedQuota(12);
        metrics->UpdateMaxUsedQuota();

        {
            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        auto unmountRequest = std::make_shared<NProto::TUnmountVolumeRequest>();
        unmountRequest->SetInstanceId(instanceId);
        unmountRequest->SetDiskId(volumeId);

        throttler->UnmountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            unmountRequest);

        {
            // Delete performs on next read
            totalCounters->GetSubgroup("component", "server")->ReadSnapshot();
            getInstanceGroupFunction(volumeId, instanceId)->ReadSnapshot();

            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_C(
                !usedQuotaCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaCounter,
                "MaxUsedQuota counters should not be initialized");
            UNIT_ASSERT_C(
                !usedQuotaVolumeCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should not be initialized");
        }
    }

    Y_UNIT_TEST(ShouldTrimCountersAfterTimeout)
    {
        const TString instanceId = "test_instance";
        auto volumeStats = std::make_shared<
            TTestVolumeStats<TSingleVolumeProcessingPolicy>>();

        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TTestThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        auto metrics = CreateThrottlerMetrics(
            timer,
            totalCounters,
            "server");

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            metrics,
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = std::make_shared<TTestService>();
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TVolume volume;
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(100);

                NProto::TMountVolumeResponse r;
                r.MutableVolume()->SetDiskId(request->GetDiskId());

                return MakeFuture(std::move(r));
            };

        const TString usedQuota = "UsedQuota";
        const TString maxUsedQuota = "MaxUsedQuota";
        auto getDiskGroupFunction = [&] (const TString& diskId)
        {
            return totalCounters
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->FindSubgroup("volume", diskId);
        };

        auto getInstanceGroupFunction = [&] (
            const TString& diskId,
            const TString& instanceId)
        {
            auto diskGroup = getDiskGroupFunction(diskId);
            UNIT_ASSERT_C(
                diskGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                    << " should be initialized");
            return diskGroup->FindSubgroup("instance", instanceId);
        };

        auto getCounterFunction = [&] (
            const TString& diskId,
            const TString& instanceId,
            const TString& sensor)
        {
            auto instanceGroup = getInstanceGroupFunction(diskId, instanceId);
            UNIT_ASSERT_C(
                instanceGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                    << ", instance:" << instanceId
                    << " should be initialized");
            return instanceGroup->FindCounter(sensor);
        };

        const TString volumeId = "test_volume";
        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetInstanceId(instanceId);
        mountRequest->SetDiskId(volumeId);

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

        metrics->UpdateUsedQuota(12);
        metrics->UpdateMaxUsedQuota();
        metrics->Trim(timer->Now());

        {
            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);
        metrics->Trim(timer->Now());

        {
            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);
        metrics->Trim(timer->Now());

        {
            // Delete performs on next read
            totalCounters->GetSubgroup("component", "server")->ReadSnapshot();
            getInstanceGroupFunction(volumeId, instanceId)->ReadSnapshot();

            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_C(
                !usedQuotaCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaCounter,
                "MaxUsedQuota counters should not be initialized");
            UNIT_ASSERT_C(
                !usedQuotaVolumeCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should not be initialized");
        }
    }

    Y_UNIT_TEST(ShouldTrimCountersAfterTimeoutWithZeroQuota)
    {
        const TString instanceId = "test_instance";
        auto volumeStats = std::make_shared<
            TTestVolumeStats<TSingleVolumeProcessingPolicy>>();

        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TTestThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        auto metrics = CreateThrottlerMetrics(
            timer,
            totalCounters,
            "server");

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            metrics,
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = std::make_shared<TTestService>();
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TVolume volume;
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(100);

                NProto::TMountVolumeResponse r;
                r.MutableVolume()->SetDiskId(request->GetDiskId());

                return MakeFuture(std::move(r));
            };

        const TString usedQuota = "UsedQuota";
        const TString maxUsedQuota = "MaxUsedQuota";
        auto getDiskGroupFunction = [&] (const TString& diskId)
        {
            return totalCounters
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->FindSubgroup("volume", diskId);
        };

        auto getInstanceGroupFunction = [&] (
            const TString& diskId,
            const TString& instanceId)
        {
            auto diskGroup = getDiskGroupFunction(diskId);
            UNIT_ASSERT_C(
                diskGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                    << " should be initialized");
            return diskGroup->FindSubgroup("instance", instanceId);
        };

        auto getCounterFunction = [&] (
            const TString& diskId,
            const TString& instanceId,
            const TString& sensor)
        {
            auto instanceGroup = getInstanceGroupFunction(diskId, instanceId);
            UNIT_ASSERT_C(
                instanceGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                    << ", instance:" << instanceId
                    << " should be initialized");
            return instanceGroup->FindCounter(sensor);
        };

        const TString volumeId = "test_volume";
        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetInstanceId(instanceId);
        mountRequest->SetDiskId(volumeId);

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

        metrics->UpdateUsedQuota(12);
        metrics->UpdateMaxUsedQuota();

        {
            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        throttler->MountVolume(
            client,
            MakeIntrusive<TCallContext>(),
            mountRequest);

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

        metrics->UpdateUsedQuota(0);
        metrics->UpdateMaxUsedQuota();
        metrics->Trim(timer->Now());

        {
            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "MaxUsedQuota counters should be initialized");
            UNIT_ASSERT_C(
                usedQuotaVolumeCounter,
                "UsedQuota counter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(0, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, usedQuotaVolumeCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(12, maxUsedQuotaVolumeCounter->Val());
        }

        timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);
        metrics->Trim(timer->Now());

        {
            // Delete performs on next read
            totalCounters->GetSubgroup("component", "server")->ReadSnapshot();
            getInstanceGroupFunction(volumeId, instanceId)->ReadSnapshot();

            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter = getCounterFunction(
                volumeId,
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_C(
                !usedQuotaCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaCounter,
                "MaxUsedQuota counters should not be initialized");
            UNIT_ASSERT_C(
                !usedQuotaVolumeCounter,
                "UsedQuota counter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaVolumeCounter,
                "MaxUsedQuota counters should not be initialized");
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyMultipleMountMultipleUnmount)
    {
        const TString instanceId = "test_instance";
        auto volumeStats = std::make_shared<
            TTestVolumeStats<TSingleVolumeProcessingPolicy>>();

        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = std::make_shared<TTestScheduler>();
        scheduler->Start();

        auto policy = std::make_shared<TTestThrottlerPolicy>();

        auto requestStats = CreateRequestStatsStub();
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        auto metrics = CreateThrottlerMetrics(
            timer,
            totalCounters,
            "server");

        auto throttler = CreateThrottler(
            CreateThrottlerLoggerStub(),
            metrics,
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = std::make_shared<TTestService>();

#define SET_HANDLER(name)                                                      \
        client->name##Handler =                                                \
            [&] (std::shared_ptr<NProto::T##name##Request> request) {          \
                Y_UNUSED(request);                                             \
                return MakeFuture(NProto::T##name##Response());                \
            };                                                                 \
// SET_HANDLER

        SET_HANDLER(UnmountVolume);

#undef SET_HANDLER

        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TVolume volume;
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(100);

                NProto::TMountVolumeResponse r;
                r.MutableVolume()->SetDiskId(request->GetDiskId());

                return MakeFuture(std::move(r));
            };

        const TVector<TString> diskIds = {
            "first_test_disk_id",
            "second_test_disk_id"
        };

        const TString usedQuota = "UsedQuota";
        const TString maxUsedQuota = "MaxUsedQuota";
        auto mountVolumeFunction = [&] (
            const TString& diskId,
            const TString& instanceId)
        {
            auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
            mountRequest->SetInstanceId(instanceId);
            mountRequest->SetDiskId(diskId);

            throttler->MountVolume(
                client,
                MakeIntrusive<TCallContext>(),
                mountRequest);

            timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

            metrics->UpdateUsedQuota(static_cast<ui64>(
                policy->CalculateCurrentSpentBudgetShare(timer->Now()) * 100.));
            metrics->UpdateMaxUsedQuota();
        };

        auto getDiskGroupFunction = [&] (const TString& diskId)
        {
            return totalCounters
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->FindSubgroup("volume", diskId);
        };

        auto getInstanceGroupFunction = [&] (
            const TString& diskId,
            const TString& instanceId)
        {
            auto diskGroup = getDiskGroupFunction(diskId);
            UNIT_ASSERT_C(
                diskGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                    << " should be initialized");
            return diskGroup->FindSubgroup("instance", instanceId);
        };

        auto getCounterFunction = [&] (
            const TString& diskId,
            const TString& instanceId,
            const TString& sensor)
        {
            auto instanceGroup = getInstanceGroupFunction(diskId, instanceId);
            UNIT_ASSERT_C(
                instanceGroup,
                TStringBuilder() << "Subgroup volume:" << diskId
                    << ", instance:" << instanceId
                    << " should be initialized");
            return instanceGroup->FindCounter(sensor);
        };

        auto unmountVolumeFunction = [&] (
            const TString& diskId,
            const TString& instanceId)
        {
            auto unmountRequest =
                std::make_shared<NProto::TUnmountVolumeRequest>();
            unmountRequest->SetInstanceId(instanceId);
            unmountRequest->SetDiskId(diskId);

            throttler->UnmountVolume(
                client,
                MakeIntrusive<TCallContext>(),
                unmountRequest);

            timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL / 2);

            metrics->UpdateUsedQuota(static_cast<ui64>(
                policy->CalculateCurrentSpentBudgetShare(timer->Now()) * 100.));
            metrics->UpdateMaxUsedQuota();

            // Delete performs on next read
            totalCounters->GetSubgroup("component", "server")->ReadSnapshot();
            getInstanceGroupFunction(diskId, instanceId)->ReadSnapshot();
        };

        policy->PostponeTimeout = TDuration::MilliSeconds(500);
        mountVolumeFunction(diskIds[0], instanceId);

        UNIT_ASSERT_C(
            !getDiskGroupFunction(diskIds[1]),
            TStringBuilder() << "Subgroup volume:" << diskIds[1]
                << " should not be initialized");

        {
            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0],
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0],
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(50, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(400);
        mountVolumeFunction(diskIds[1], instanceId);

        {
            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0],
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0],
                instanceId,
                maxUsedQuota);
            auto usedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1],
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter1 = getCounterFunction(
                diskIds[1],
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(40, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(40, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(40, usedQuotaVolumeCounter1->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter1->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(300);
        unmountVolumeFunction(diskIds[1], instanceId);

        {
            auto usedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(usedQuota);
            auto maxUsedQuotaCounter = totalCounters
                ->GetSubgroup("component", "server")
                ->FindCounter(maxUsedQuota);
            auto usedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0],
                instanceId,
                usedQuota);
            auto maxUsedQuotaVolumeCounter0 = getCounterFunction(
                diskIds[0],
                instanceId,
                maxUsedQuota);

            UNIT_ASSERT_VALUES_EQUAL(30, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(30, usedQuotaVolumeCounter0->Val());
            UNIT_ASSERT_VALUES_EQUAL(50, maxUsedQuotaVolumeCounter0->Val());
        }

        policy->PostponeTimeout = TDuration::MilliSeconds(80);
        unmountVolumeFunction(diskIds[0], instanceId);

        {
            UNIT_ASSERT_C(
                !totalCounters
                    ->GetSubgroup("component", "server")
                    ->FindCounter(usedQuota),
                "UsedQuota should not be initialized");
            UNIT_ASSERT_C(
                !totalCounters
                    ->GetSubgroup("component", "server")
                    ->FindCounter(maxUsedQuota),
                "MaxUsedQuota should not be initialized");
        }
    }

    Y_UNIT_TEST(TestPreparePerformanceProfileWithHostProfile)
    {
        THostPerformanceProfile hostProfile = {
            .CpuCount = 60,
            .NetworkMbitThroughput = 25'000,
        };

        NProto::TClientConfig clientConfig;
        auto* tc = clientConfig.MutableThrottlingConfig();
        tc->SetIopsPerCpuUnit(43'000'000);
        tc->SetBandwidthPerCpuUnit(43);

        tc->SetMaxIopsPerHost(126'000);
        tc->SetMaxBandwidthPerHost(2700_MB);
        tc->SetNetworkThroughputPercentage(50);

        tc->MutableHDDThrottlingConfig()->SetHostOvercommitPercentage(200);
        tc->MutableSSDThrottlingConfig()->SetHostOvercommitPercentage(150);

        NProto::TClientProfile clientProfile;
        clientProfile.SetCpuUnitCount(100);

        NProto::TClientPerformanceProfile performanceProfile;
        UNIT_ASSERT(PreparePerformanceProfile(
            hostProfile,
            clientConfig,
            clientProfile,
            performanceProfile
        ));

        auto& hddProfile = performanceProfile.GetHDDProfile();
        UNIT_ASSERT_VALUES_EQUAL(4200, hddProfile.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(4200, hddProfile.GetMaxWriteIops());
        UNIT_ASSERT_VALUES_EQUAL(54613332, hddProfile.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(54613332, hddProfile.GetMaxWriteBandwidth());

        auto& ssdProfile = performanceProfile.GetSSDProfile();
        UNIT_ASSERT_VALUES_EQUAL(3150, ssdProfile.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(3150, ssdProfile.GetMaxWriteIops());
        UNIT_ASSERT_VALUES_EQUAL(40959999, ssdProfile.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(40959999, ssdProfile.GetMaxWriteBandwidth());
    }

    Y_UNIT_TEST(ShouldReceiveAndProcessRequests)
    {
        auto volumeStats =
            std::make_shared<TTestVolumeStats<TSingleVolumeProcessingPolicy>>();

        NProto::TVolume vol;
        vol.SetDiskId("xxx");
        vol.SetBlockSize(4096);
        volumeStats->MountVolume(vol, "test_client", "test_instance");

        auto requestStats = std::make_shared<TRequestStats>();

        auto logger = CreateClientThrottlerLogger(
            requestStats,
            CreateLoggingService("console"));

        auto policy = std::make_shared<TTestThrottlerPolicy>();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto timer = std::make_shared<TTestTimer>();
        auto throttler = CreateThrottler(
            logger,
            CreateThrottlerMetricsStub(),
            policy,
            CreateThrottlerTrackerStub(),
            timer,
            scheduler,
            volumeStats);

        auto client = CreateBlockStoreStub();

        policy->PostponeTimeout = TDuration::Max();
        constexpr auto requestCount = 7;

#define DO_REQUEST(name)                                                       \
    throttler->name(                                                           \
        client,                                                                \
        MakeIntrusive<TCallContext>(),                                         \
        Create##name##Request(1));                                             \
// DO_REQUEST

        for (ui32 i = 0; i < requestCount; ++i) {
            DO_REQUEST(WriteBlocks);
            DO_REQUEST(WriteBlocksLocal);
            DO_REQUEST(ReadBlocks);
            DO_REQUEST(ReadBlocksLocal);
            DO_REQUEST(ZeroBlocks);
        }

#undef DO_REQUEST

#define DO_TEST(name, ...)                                                     \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        volumeStats->VolumeInfo->PostponedCount[                               \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        volumeStats->VolumeInfo->PostponedServerCount[                         \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        volumeStats->VolumeInfo->AdvancedCount[                                \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        volumeStats->VolumeInfo->AdvancedServerCount[                          \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        requestStats->PostponedCount[                                          \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        requestStats->PostponedServerCount[                                    \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        requestStats->AdvancedCount[                                           \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        requestStats->AdvancedServerCount[                                     \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
// DO_TEST

        BLOCKSTORE_SERVICE(DO_TEST, requestCount);

        scheduler->RunAllScheduledTasks();

        BLOCKSTORE_SERVICE(DO_TEST, requestCount);

#undef DO_TEST

        policy->PostponeTimeout = TDuration::Zero();
        scheduler->RunAllScheduledTasks();

#define DO_TEST(name, ...)                                                     \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        volumeStats->VolumeInfo->PostponedCount[                               \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        volumeStats->VolumeInfo->PostponedServerCount[                         \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        volumeStats->VolumeInfo->AdvancedCount[                                \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        volumeStats->VolumeInfo->AdvancedServerCount[                          \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        requestStats->PostponedCount[                                          \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        requestStats->PostponedServerCount[                                    \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        requestStats->AdvancedCount[                                           \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        requestStats->AdvancedServerCount[                                     \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
// DO_TEST

        BLOCKSTORE_SERVICE(DO_TEST, requestCount);

#undef DO_TEST
    }

    // TODO: tests with multiple threads using our client
}

}   // namespace NCloud::NBlockStore::NClient
