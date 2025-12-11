#include "throttling.h"

#include <cloud/blockstore/libs/diagnostics/volume_stats_test.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_logger_test.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/throttling/throttler_policy.h>
#include <cloud/blockstore/libs/throttling/throttler_tracker_test.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerPolicy final: public IThrottlerPolicy
{
public:
    bool Frozen = true;

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

        return Frozen ? TDuration::Max() : TDuration::Zero();
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const override
    {
        Y_UNUSED(ts);

        return 0.0;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeProcessingPolicy
{
    std::shared_ptr<TTestVolumeInfo<>> VolumeInfo =
        std::make_shared<TTestVolumeInfo<>>();

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId)
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);

        return false;
    }

    void UnmountVolume(const TString& diskId, const TString& clientId)
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
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);

        return VolumeInfo;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnvironment final
{
    std::shared_ptr<TTestThrottlerLogger> Logger;
    std::shared_ptr<TThrottlerPolicy> Policy;
    std::shared_ptr<TTestThrottlerTracker> Tracker;
    std::shared_ptr<TTestTimer> Timer;
    std::shared_ptr<TTestScheduler> Scheduler;
    std::shared_ptr<TTestVolumeStats<TVolumeProcessingPolicy>> VolumeStats;
    std::shared_ptr<TTestService> Service;
    IBlockStorePtr ThrottlingService;

    ui32 RequestCount[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};
    ui32 ReceivedCount[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};

    TTestEnvironment()
        : Logger(std::make_shared<TTestThrottlerLogger>())
        , Policy(std::make_shared<TThrottlerPolicy>())
        , Tracker(std::make_shared<TTestThrottlerTracker>())
        , Timer(std::make_shared<TTestTimer>())
        , Scheduler(std::make_shared<TTestScheduler>())
        , VolumeStats(
              std::make_shared<TTestVolumeStats<TVolumeProcessingPolicy>>())
        , Service(std::make_shared<TTestService>())
    {
        InitTestLogger();
        InitTestTracker();

        InitTestService();

        ThrottlingService = CreateThrottlingService(
            Service,
            CreateThrottler(
                Logger,
                CreateThrottlerMetricsStub(),
                Policy,
                Tracker,
                Timer,
                Scheduler,
                VolumeStats));
    }

    void InitTestLogger()
    {
#define SET_HANDLER(name, ...)                              \
    Logger->name##LogPostponedRequestHandler =              \
        [](ui64,                                            \
           TCallContext&,                                   \
           IVolumeInfo*,                                    \
           const NProto::T##name##Request&,                 \
           TDuration) {                                     \
        };                                                  \
                                                            \
    Logger->name##LogAdvancedRequestHandler =               \
        [](ui64,                                            \
           TCallContext&,                                   \
           IVolumeInfo*,                                    \
           const NProto::T##name##Request&) {               \
        };                                                  \
                                                            \
    Logger->name##LogErrorHandler =                         \
        [](const NProto::T##name##Request&, const TString&) \
    {                                                       \
        UNIT_ASSERT(false);                                 \
    };                                                      \
    // SET_HANDLER

        BLOCKSTORE_SERVICE(SET_HANDLER)

#undef SET_HANDLER
    }

    void InitTestTracker()
    {
#define SET_HANDLER(name, ...)                                          \
    Tracker->name##TrackReceivedRequestHandler =                        \
        [&ReceivedCount = this->ReceivedCount](                         \
            TCallContext&,                                              \
            IVolumeInfo*,                                               \
            const NProto::T##name##Request&)                            \
    {                                                                   \
        ++ReceivedCount[static_cast<size_t>(EBlockStoreRequest::name)]; \
    };                                                                  \
                                                                        \
    Tracker->name##TrackPostponedRequestHandler =                       \
        [](TCallContext&, const NProto::T##name##Request&) {            \
        };                                                              \
                                                                        \
    Tracker->name##TrackAdvancedRequestHandler =                        \
        [](TCallContext&, const NProto::T##name##Request&) {            \
        };                                                              \
    // SET_HANDLER

        BLOCKSTORE_SERVICE(SET_HANDLER)

#undef SET_HANDLER
    }

    void InitTestService()
    {
#define SET_HANDLER(name, ...)                                              \
    Service->name##Handler = [&RequestCount = this->RequestCount](          \
                                 std::shared_ptr<NProto::T##name##Request>) \
    {                                                                       \
        ++RequestCount[static_cast<size_t>(EBlockStoreRequest::name)];      \
        return MakeFuture(NProto::T##name##Response());                     \
    };                                                                      \
    // SET_HANDLER

        BLOCKSTORE_SERVICE(SET_HANDLER)

#undef SET_HANDLER
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                          \
    std::shared_ptr<NProto::T##name##Request> Create##name##Request() \
    {                                                                 \
        return std::make_shared<NProto::T##name##Request>();          \
    }                                                                 \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TThrottlingServiceTest)
{
    Y_UNIT_TEST(ShouldRequestsGoThroughThrottlingService)
    {
        TTestEnvironment env;
        constexpr auto requestCount = 7;

#define DO_REQUEST(name, ...)          \
    env.ThrottlingService->name(       \
        MakeIntrusive<TCallContext>(), \
        env.Create##name##Request());  \
    // DO_REQUEST

        for (ui32 i = 0; i < requestCount; ++i) {
            BLOCKSTORE_SERVICE(DO_REQUEST)
        }

#undef DO_REQUEST

#define TEST_REQUEST(name, throttlerReceivedCount, serviceReceivedCount)     \
    UNIT_ASSERT_VALUES_EQUAL(                                                \
        throttlerReceivedCount,                                              \
        env.ReceivedCount[static_cast<size_t>(EBlockStoreRequest::name)]);   \
    UNIT_ASSERT_VALUES_EQUAL(                                                \
        ShouldBeThrottled<NProto::T##name##Request>() ? serviceReceivedCount \
                                                      : requestCount,        \
        env.RequestCount[static_cast<size_t>(EBlockStoreRequest::name)]);    \
    // TEST_REQUEST

        BLOCKSTORE_SERVICE(TEST_REQUEST, requestCount, 0)

        env.Policy->Frozen = false;
        env.Scheduler->RunAllScheduledTasks();

        BLOCKSTORE_SERVICE(TEST_REQUEST, requestCount, requestCount)

#undef TEST_REQUEST
    }
}

}   // namespace NCloud::NBlockStore
