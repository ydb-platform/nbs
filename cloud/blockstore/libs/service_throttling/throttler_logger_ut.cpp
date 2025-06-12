#include "throttler_logger.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats_test.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_logger.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/throttling/throttler_policy.h>
#include <cloud/blockstore/libs/throttling/throttler_tracker.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestService final
    : public IBlockStore
{
    void Start() override {}
    void Stop() override {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    using T##name##Handler = std::function<                                    \
        NThreading::TFuture<NProto::T##name##Response>(                        \
            TCallContextPtr callContext,                                       \
            std::shared_ptr<NProto::T##name##Request> request)                 \
        >;                                                                     \
                                                                               \
    T##name##Handler name##Handler;                                            \
                                                                               \
    NThreading::TFuture<NProto::T##name##Response> name(                       \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return name##Handler(std::move(callContext),std::move(request));       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TThrottlerPolicy final
    : public IThrottlerPolicy
{
public:
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
        Y_UNUSED(byteCount);

        return PostponeTimeout;
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

struct TVolumeProcessingPolicy
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

        return diskId == VolumeInfo->Volume.GetDiskId()
            ? VolumeInfo
            : nullptr;
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
        ui64 requestBytes) override
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
        ui64 requestBytes,
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

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    std::shared_ptr<NProto::T##name##Request> Create##name##Request()          \
    {                                                                          \
        return std::make_shared<NProto::T##name##Request>();                   \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceThrotterLoggerTest)
{
    Y_UNIT_TEST(ShouldReceiveAndProcessRequests)
    {
        auto volumeStats =
            std::make_shared<TTestVolumeStats<TVolumeProcessingPolicy>>();
        auto requestStats = std::make_shared<TRequestStats>();

        auto logger = CreateServiceThrottlerLogger(
            requestStats,
            CreateLoggingService("console"));

        auto policy = std::make_shared<TThrottlerPolicy>();
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

#define DO_REQUEST(name, ...)                                                  \
    throttler->name(                                                           \
        client,                                                                \
        MakeIntrusive<TCallContext>(),                                         \
        Create##name##Request());                                              \
// DO_REQUEST

        for (ui32 i = 0; i < requestCount; ++i) {
            BLOCKSTORE_SERVICE(DO_REQUEST)
        }

#undef DO_REQUEST

#define DO_TEST(name, ...)                                                     \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        volumeStats->VolumeInfo->PostponedCount[                               \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
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
        0,                                                                     \
        requestStats->PostponedCount[                                          \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
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
        0,                                                                     \
        volumeStats->VolumeInfo->PostponedCount[                               \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        volumeStats->VolumeInfo->PostponedServerCount[                         \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        volumeStats->VolumeInfo->AdvancedCount[                                \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        volumeStats->VolumeInfo->AdvancedServerCount[                          \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        requestStats->PostponedCount[                                          \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        requestStats->PostponedServerCount[                                    \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        0,                                                                     \
        requestStats->AdvancedCount[                                           \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? __VA_ARGS__                                                      \
            : 0,                                                               \
        requestStats->AdvancedServerCount[                                     \
            static_cast<size_t>(EBlockStoreRequest::name)]);                   \
// DO_TEST

        BLOCKSTORE_SERVICE(DO_TEST, requestCount);

#undef DO_TEST
    }

    Y_UNIT_TEST(ShouldUpdateCallContextWithThrottlerDelay)
    {
        auto logger = CreateServiceThrottlerLogger(
            std::make_shared<TRequestStats>(),
            CreateLoggingService("console"));

        TCallContext ctx;

        SetCyclesPerSecond(1'000);

        logger->LogPostponedRequest(
            1'000,
            ctx,
            nullptr,
            NProto::TWriteBlocksRequest(),
            TDuration::Zero());

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            ctx.Time(EProcessingStage::Postponed));

        logger->LogAdvancedRequest(
            2'250,
            ctx,
            nullptr,
            NProto::TWriteBlocksRequest());

        UNIT_ASSERT_VALUES_EQUAL(
            CyclesToDurationSafe(1'250), // 2'250 - 1'000
            ctx.Time(EProcessingStage::Postponed));

        logger->LogPostponedRequest(
            3'000,
            ctx,
            nullptr,
            NProto::TWriteBlocksRequest(),
            TDuration::Zero());

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(1'250),
            ctx.Time(EProcessingStage::Postponed));

        logger->LogAdvancedRequest(
            4'400,
            ctx,
            nullptr,
            NProto::TWriteBlocksRequest());

        UNIT_ASSERT_VALUES_EQUAL(
            CyclesToDurationSafe(2'650), // 1'250 + (4'400 - 3'000)
            ctx.Time(EProcessingStage::Postponed));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(2'650),
            ctx.Time(EProcessingStage::Postponed));
    }

    Y_UNIT_TEST(ShouldUpdateCallContextInsideThrottler)
    {
        auto volumeStats =
            std::make_shared<TTestVolumeStats<TVolumeProcessingPolicy>>();
        auto requestStats = std::make_shared<TRequestStats>();

        auto logger = CreateServiceThrottlerLogger(
            requestStats,
            CreateLoggingService("console"));

        auto policy = std::make_shared<TThrottlerPolicy>();
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

        constexpr TDuration volumeThrottlerDelay = TDuration::Seconds(10);
        const TString clientId = "test_client";
        const TString diskId = "test_disk";
        TCallContextPtr callContext = MakeIntrusive<TCallContext>();

        NProto::TVolume vol;
        vol.SetDiskId(diskId);
        volumeStats->MountVolume(vol, clientId, "test_instance");

        auto client = std::make_shared<TTestService>();
        client->WriteBlocksHandler = [volumeThrottlerDelay] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksRequest> request)
        {
            Y_UNUSED(request);
            callContext->AddTime(
                EProcessingStage::Postponed,
                volumeThrottlerDelay);
            return MakeFuture(NProto::TWriteBlocksResponse());
        };

        policy->PostponeTimeout = TDuration::Seconds(1);

        auto writeReq = std::make_shared<NProto::TWriteBlocksRequest>();
        writeReq->MutableHeaders()->SetClientId(clientId);
        writeReq->SetDiskId(diskId);
        auto future = throttler->WriteBlocks(client, callContext, writeReq);

        policy->PostponeTimeout = TDuration::Zero();

        scheduler->RunAllScheduledTasks();

        auto response = future.GetValueSync();
        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
        UNIT_ASSERT_C(
            callContext->Time(EProcessingStage::Postponed) >
                volumeThrottlerDelay,
            "Request didn't pass LogPostponedRequest() and LogAdvancedRequest() methods");
    }
}

}   // namespace NCloud::NBlockStore
