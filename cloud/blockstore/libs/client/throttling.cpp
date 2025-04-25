#include "throttling.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/probes.h>
#include <cloud/blockstore/libs/diagnostics/quota_metrics.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_logger.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/throttling/throttler_policy.h>
#include <cloud/blockstore/libs/throttling/throttler_tracker.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>
#include <cloud/storage/core/libs/throttling/helpers.h>
#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <cloud/blockstore/config/client.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/deque.h>
#include <util/generic/size_literals.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <type_traits>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_SERVER_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerPolicy final
    : public IThrottlerPolicy
{
private:
    TBoostedTimeBucket Bucket;
    NProto::TClientPerformanceProfile Profile;

public:
    TThrottlerPolicy(const NProto::TClientPerformanceProfile& pp)
        : Bucket(SecondsToDuration(pp.GetBurstTime() / 1'000.))
        , Profile(pp)
    {
    }

    TDuration SuggestDelay(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        size_t byteCount) override
    {
        const NProto::TClientMediaKindPerformanceProfile* mediaKindProfile
            = nullptr;

        switch (mediaKind) {
            case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
                mediaKindProfile = &Profile.GetNonreplProfile();
                break;
            }

            case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: {
                mediaKindProfile = &Profile.GetHddNonreplProfile();
                break;
            }

            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: {
                mediaKindProfile = &Profile.GetMirror2Profile();
                break;
            }

            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: {
                mediaKindProfile = &Profile.GetMirror3Profile();
                break;
            }

            case NCloud::NProto::STORAGE_MEDIA_SSD: {
                mediaKindProfile = &Profile.GetSSDProfile();
                break;
            }

            case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL:
            case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL:
                Y_DEBUG_ABORT_UNLESS(false);
                [[fallthrough]];

            default: {
                mediaKindProfile = &Profile.GetHDDProfile();
                break;
            }
        }

        ui64 maxIops = 0;
        ui64 maxBandwidth = 0;

        switch (requestType) {
            case EBlockStoreRequest::ReadBlocks:
            case EBlockStoreRequest::ReadBlocksLocal: {
                maxIops = mediaKindProfile->GetMaxReadIops();
                maxBandwidth = mediaKindProfile->GetMaxReadBandwidth();

                break;
            }

            default: {
                maxIops = mediaKindProfile->GetMaxWriteIops();
                maxBandwidth = mediaKindProfile->GetMaxWriteBandwidth();

                break;
            }
        }

        if (!maxIops || !maxBandwidth) {
            return TDuration::Zero();
        }

        return Bucket.Register(
            now,
            CostPerIO(
                CalculateThrottlerC1(maxIops, maxBandwidth),
                CalculateThrottlerC2(maxIops, maxBandwidth),
                byteCount)
        );
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const override
    {
        return Bucket.CalculateCurrentSpentBudgetShare(ts);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TThrottlerTracker final
    : public IThrottlerTracker
{
public:

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    void TrackReceivedRequest(                                                 \
        TCallContext& callContext,                                             \
        IVolumeInfo* volumeInfo,                                               \
        const NProto::T##name##Request& request) override                      \
    {                                                                          \
        if (volumeInfo) {                                                      \
            LWTRACK(                                                           \
                RequestReceived,                                               \
                callContext.LWOrbit,                                           \
                GetBlockStoreRequestName(EBlockStoreRequest::name),            \
                static_cast<ui32>(volumeInfo->GetInfo().GetStorageMediaKind()),\
                GetRequestId(request),                                         \
                GetDiskId(request));                                           \
        }                                                                      \
    }                                                                          \
                                                                               \
    void TrackPostponedRequest(                                                \
        TCallContext& callContext,                                             \
        const NProto::T##name##Request& request) override                      \
    {                                                                          \
        LWTRACK(                                                               \
            RequestPostponed,                                                  \
            callContext.LWOrbit,                                               \
            GetBlockStoreRequestName(EBlockStoreRequest::name),                \
            GetRequestId(request),                                             \
            GetDiskId(request));                                               \
   }                                                                           \
                                                                               \
    void TrackAdvancedRequest(                                                 \
        TCallContext& callContext,                                             \
        const NProto::T##name##Request& request) override                      \
    {                                                                          \
        LWTRACK(                                                               \
            RequestAdvanced,                                                   \
            callContext.LWOrbit,                                               \
            GetBlockStoreRequestName(EBlockStoreRequest::name),                \
            GetRequestId(request),                                             \
            GetDiskId(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TThrottlerLogger final
    : public IThrottlerLogger
{
private:
    const IRequestStatsPtr RequestStats;

    TLog Log;

public:
    TThrottlerLogger(
            IRequestStatsPtr requestStats,
            ILoggingServicePtr logging,
            const TString& loggerName)
        : RequestStats(std::move(requestStats))
        , Log(logging->CreateLog(loggerName))
    {}

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    void LogPostponedRequest(                                                  \
        ui64 nowCycles,                                                        \
        TCallContext& callContext,                                             \
        IVolumeInfo* volumeInfo,                                               \
        const NProto::T##name##Request& request,                               \
        TDuration postponeDelay) override                                      \
    {                                                                          \
        static constinit auto requestType = EBlockStoreRequest::name;          \
        RequestStats->RequestPostponed(requestType);                           \
        if (volumeInfo) {                                                      \
            volumeInfo->RequestPostponed(requestType);                         \
        }                                                                      \
                                                                               \
        STORAGE_DEBUG(                                                         \
            TRequestInfo(                                                      \
                requestType,                                                   \
                GetRequestId(request),                                         \
                GetDiskId(request),                                            \
                GetClientId(request))                                          \
            << GetRequestDetails(request)                                      \
            << " request postponed"                                            \
            << " (delay: " << FormatDuration(postponeDelay) << ")");           \
                                                                               \
        callContext.Postpone(nowCycles);                                       \
    }                                                                          \
                                                                               \
    void LogAdvancedRequest(                                                   \
        ui64 nowCycles,                                                        \
        TCallContext& callContext,                                             \
        IVolumeInfo* volumeInfo,                                               \
        const NProto::T##name##Request& request) override                      \
    {                                                                          \
        static constinit auto requestType = EBlockStoreRequest::name;          \
        RequestStats->RequestAdvanced(requestType);                            \
        if (volumeInfo) {                                                      \
            volumeInfo->RequestAdvanced(requestType);                          \
        }                                                                      \
                                                                               \
        STORAGE_DEBUG(                                                         \
            TRequestInfo(                                                      \
                requestType,                                                   \
                GetRequestId(request),                                         \
                GetDiskId(request),                                            \
                GetClientId(request))                                          \
            << GetRequestDetails(request)                                      \
            << " request advanced");                                           \
                                                                               \
        callContext.Advance(nowCycles);                                        \
    }                                                                          \
                                                                               \
    void LogError(                                                             \
        const NProto::T##name##Request& request,                               \
        const TString& errorMessage) override                                  \
    {                                                                          \
        static constinit auto requestType = EBlockStoreRequest::name;          \
        STORAGE_ERROR(                                                         \
            TRequestInfo(                                                      \
                requestType,                                                   \
                GetRequestId(request),                                         \
                GetDiskId(request),                                            \
                GetClientId(request))                                          \
            << GetRequestDetails(request)                                      \
            << " exception in callback: " << errorMessage);                    \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TThrottlingClient final
    : public IBlockStore
{
private:
    IThrottlerPtr Throttler;
    IBlockStorePtr Client;

public:
    TThrottlingClient(
            IBlockStorePtr client,
            IThrottlerPtr throttler)
        : Throttler(std::move(throttler))
        , Client(std::move(client))
    {}

    void Start() override
    {
        Client->Start();
    }

    void Stop() override
    {
        Client->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Client->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return Throttler->name(                                                \
            Client,                                                            \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TThrottlerProvider final
    : public IThrottlerProvider
{
    struct TThrottlerInfo
    {
        IThrottlerPtr Throttler;
        NProto::TClientPerformanceProfile PerformanceProfile;
    };

private:
    const THostPerformanceProfile HostProfile;
    const ILoggingServicePtr Logging;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const NMonitoring::TDynamicCountersPtr RootGroup;
    const IRequestStatsPtr RequestStats;
    const IVolumeStatsPtr VolumeStats;

    TMutex ThrottlerLock;
    THashMap<TString, TThrottlerInfo> Throttlers;

public:
    TThrottlerProvider(
            THostPerformanceProfile hostProfile,
            ILoggingServicePtr logging,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            NMonitoring::TDynamicCountersPtr rootGroup,
            IRequestStatsPtr requestStats,
            IVolumeStatsPtr volumeStats)
        : HostProfile(hostProfile)
        , Logging(std::move(logging))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , RootGroup(std::move(rootGroup))
        , RequestStats(std::move(requestStats))
        , VolumeStats(std::move(volumeStats))
    {}

    IThrottlerPtr GetThrottler(
        const NProto::TClientConfig& clientConfig,
        const NProto::TClientProfile& clientProfile,
        NProto::TClientPerformanceProfile performanceProfile) override
    {
        bool prepared = PreparePerformanceProfile(
            HostProfile,
            clientConfig,
            clientProfile,
            performanceProfile);

        if (!prepared) {
            return nullptr;
        }

        if (clientConfig.GetClientId().empty()) {
            return CreateClientThrottler(performanceProfile);
        }

        with_lock (ThrottlerLock) {
            DoClean();

            auto it = Throttlers.find(clientConfig.GetClientId());
            if (it != Throttlers.end()) {
                auto& info = it->second;
                if (!ProfilesEqual(info.PerformanceProfile, performanceProfile)) {
                    info.Throttler->UpdateThrottlerPolicy(
                        CreateClientThrottlerPolicy(performanceProfile));
                    info.PerformanceProfile = performanceProfile;
                }
                return info.Throttler;
            }

            auto throttler = CreateClientThrottler(performanceProfile);

            TThrottlerInfo info = {
                .Throttler = throttler,
                .PerformanceProfile = performanceProfile,
            };

            info.Throttler->Start();

            auto inserted = Throttlers.emplace(
                clientConfig.GetClientId(),
                std::move(info)).second;

            STORAGE_VERIFY(
                inserted,
                TWellKnownEntityTypes::CLIENT,
                clientConfig.GetClientId());
            return throttler;
        }
    }

    NProto::TClientPerformanceProfile GetPerformanceProfile(
        const TString& clientId) const override
    {
        with_lock (ThrottlerLock) {
            auto it = Throttlers.find(clientId);
            if (it == Throttlers.end()) {
                return NProto::TClientPerformanceProfile();
            }

            return it->second.PerformanceProfile;
        }
    }

    void Clean() override
    {
        with_lock (ThrottlerLock) {
            DoClean();
        }
    }

private:
    void DoClean()
    {
        for (auto it = Throttlers.begin(); it != Throttlers.end();) {
            if (it->second.Throttler.use_count() == 1) {
                it->second.Throttler->Stop();
                Throttlers.erase(it++);
            } else {
                ++it;
            }
        }
    }

    IThrottlerPtr CreateClientThrottler(
        NProto::TClientPerformanceProfile performanceProfile) const
    {
        auto throttlerPolicy = CreateClientThrottlerPolicy(
            std::move(performanceProfile));
        auto throttlerLogger = CreateClientThrottlerLogger(
            RequestStats,
            Logging);
        auto throttlerMetrics = CreateThrottlerMetrics(
            Timer,
            RootGroup,
            "server");
        auto throttlerTracker = CreateClientThrottlerTracker();

        return CreateThrottler(
            std::move(throttlerLogger),
            std::move(throttlerMetrics),
            std::move(throttlerPolicy),
            std::move(throttlerTracker),
            Timer,
            Scheduler,
            VolumeStats);
    }

    template <typename T>
    int GetFieldCount()
    {
        return T::GetDescriptor()->field_count();
    }

    bool ProfilesEqual(
        const NProto::TClientPerformanceProfile& lft,
        const NProto::TClientPerformanceProfile& rgt)
    {
        Y_DEBUG_ABORT_UNLESS(7 == GetFieldCount<NProto::TClientPerformanceProfile>());
        return ProfilesEqual(lft.GetHDDProfile(), rgt.GetHDDProfile())
            && ProfilesEqual(lft.GetSSDProfile(), rgt.GetSSDProfile())
            && ProfilesEqual(lft.GetNonreplProfile(), rgt.GetNonreplProfile())
            && ProfilesEqual(lft.GetHddNonreplProfile(), rgt.GetHddNonreplProfile())
            && ProfilesEqual(lft.GetMirror2Profile(), rgt.GetMirror2Profile())
            && ProfilesEqual(lft.GetMirror3Profile(), rgt.GetMirror3Profile())
            && lft.GetBurstTime() == rgt.GetBurstTime();
    }

    bool ProfilesEqual(
        const NProto::TClientMediaKindPerformanceProfile& lft,
        const NProto::TClientMediaKindPerformanceProfile& rgt)
    {
        Y_DEBUG_ABORT_UNLESS(4 ==
            GetFieldCount<NProto::TClientMediaKindPerformanceProfile>());
        return lft.GetMaxReadBandwidth() == rgt.GetMaxReadBandwidth()
            && lft.GetMaxReadIops() == rgt.GetMaxReadIops()
            && lft.GetMaxWriteBandwidth() == rgt.GetMaxWriteBandwidth()
            && lft.GetMaxWriteIops() == rgt.GetMaxWriteIops();
    }
};

////////////////////////////////////////////////////////////////////////////////

ui64 MinNonzero(ui64 l, ui64 r)
{
    if (l == 0 || r == 0) {
        return Max(l, r);
    }
    return Min(l, r);
}

template <typename... Args>
ui64 MinNonzero(ui64 a, ui64 b, Args... args)
{
    return MinNonzero(a, MinNonzero(b, args...));
}

////////////////////////////////////////////////////////////////////////////////

bool PrepareMediaKindPerformanceProfile(
    const NProto::TClientThrottlingConfig& config,
    const NProto::TClientMediaKindThrottlingConfig& mediaKindConfig,
    const NProto::TClientProfile& profile,
    ui64 maxIopsPerGuest,
    ui64 maxBandwidthPerGuest,
    const bool useLegacyFallback,
    NProto::TClientMediaKindPerformanceProfile& performanceProfile)
{
    maxIopsPerGuest *= mediaKindConfig.GetHostOvercommitPercentage() / 100.0;
    maxBandwidthPerGuest *= mediaKindConfig.GetHostOvercommitPercentage() / 100.0;

    const ui32 cpuUnitsPerCore = 100u;
    auto cpuUnitCount = Max(cpuUnitsPerCore, profile.GetCpuUnitCount());

    if (!performanceProfile.GetMaxReadIops()) {
        ui64 iopsPerCpuUnit = mediaKindConfig.GetReadIopsPerCpuUnit();
        if (!iopsPerCpuUnit && useLegacyFallback) {
            iopsPerCpuUnit = config.GetIopsPerCpuUnit();
        }

        ui64 maxIops = mediaKindConfig.GetMaxReadIops();

        const auto iops = MinNonzero(
            cpuUnitCount * iopsPerCpuUnit,
            maxIopsPerGuest,
            maxIops
        );
        performanceProfile.SetMaxReadIops(iops);
    }

    if (!performanceProfile.GetMaxWriteIops()) {
        ui64 iopsPerCpuUnit = mediaKindConfig.GetWriteIopsPerCpuUnit();
        if (!iopsPerCpuUnit && useLegacyFallback) {
            iopsPerCpuUnit = config.GetIopsPerCpuUnit();
        }

        ui64 maxIops = mediaKindConfig.GetMaxWriteIops();

        const auto iops = MinNonzero(
            cpuUnitCount * iopsPerCpuUnit,
            maxIopsPerGuest,
            maxIops
        );
        performanceProfile.SetMaxWriteIops(iops);
    }

    if (!performanceProfile.GetMaxReadBandwidth()) {
        ui64 bandwidthPerCpuUnit = mediaKindConfig.GetReadBandwidthPerCpuUnit();
        if (!bandwidthPerCpuUnit && useLegacyFallback) {
            bandwidthPerCpuUnit = config.GetBandwidthPerCpuUnit();
        }

        ui64 maxBw = mediaKindConfig.GetMaxReadBandwidth() * 1_MB;

        const auto bw = MinNonzero(
            cpuUnitCount * bandwidthPerCpuUnit * 1_MB,
            maxBandwidthPerGuest,
            maxBw
        );
        performanceProfile.SetMaxReadBandwidth(bw);
    }

    if (!performanceProfile.GetMaxWriteBandwidth()) {
        ui64 bandwidthPerCpuUnit = mediaKindConfig.GetWriteBandwidthPerCpuUnit();
        if (!bandwidthPerCpuUnit && useLegacyFallback) {
            bandwidthPerCpuUnit = config.GetBandwidthPerCpuUnit();
        }

        ui64 maxBw = mediaKindConfig.GetMaxWriteBandwidth() * 1_MB;

        const auto bw = MinNonzero(
            cpuUnitCount * bandwidthPerCpuUnit * 1_MB,
            maxBandwidthPerGuest,
            maxBw
        );
        performanceProfile.SetMaxWriteBandwidth(bw);
    }

    return performanceProfile.GetMaxReadIops()
        && performanceProfile.GetMaxWriteIops()
        && performanceProfile.GetMaxReadBandwidth()
        && performanceProfile.GetMaxWriteBandwidth();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool PreparePerformanceProfile(
    const THostPerformanceProfile& hostProfile,
    const NProto::TClientConfig& config,
    const NProto::TClientProfile& profile,
    NProto::TClientPerformanceProfile& performanceProfile)
{
    if (!profile.GetCpuUnitCount()) {
        return false;
    }

    if (profile.GetHostType() == NProto::HOST_TYPE_DEDICATED) {
        return false;
    }

    const auto& tc = config.GetThrottlingConfig();
    auto burstPercentage = tc.GetBurstPercentage();

    if (!burstPercentage) {
        burstPercentage = 100 / Max(tc.GetBurstDivisor(), 1u);
    }

    if (!performanceProfile.GetBurstTime()) {
        performanceProfile.SetBurstTime(1'000 * burstPercentage / 100);
    }

    double hostFraction = 0.0;
    if (hostProfile.CpuCount > 0) {
        hostFraction = profile.GetCpuUnitCount() / (hostProfile.CpuCount * 100.0);
    }

    ui64 networkBandwidth = (hostProfile.NetworkMbitThroughput / 8) * 1_MB
        * (tc.GetNetworkThroughputPercentage() / 100.0);

    if (!networkBandwidth) {
        networkBandwidth = Max<ui64>();
    }

    ui64 maxIopsPerGuest = hostFraction * tc.GetMaxIopsPerHost();
    ui64 maxBandwidthPerGuest = hostFraction
        * Min(tc.GetMaxBandwidthPerHost(), networkBandwidth);

    const auto init = {
        PrepareMediaKindPerformanceProfile(
            tc,
            tc.GetHDDThrottlingConfig(),
            profile,
            maxIopsPerGuest,
            maxBandwidthPerGuest,
            true,
            *performanceProfile.MutableHDDProfile()
        ),
        PrepareMediaKindPerformanceProfile(
            tc,
            tc.GetSSDThrottlingConfig(),
            profile,
            maxIopsPerGuest,
            maxBandwidthPerGuest,
            true,
            *performanceProfile.MutableSSDProfile()
        ),
        PrepareMediaKindPerformanceProfile(
            tc,
            tc.GetNonreplThrottlingConfig(),
            profile,
            maxIopsPerGuest,
            maxBandwidthPerGuest,
            false,
            *performanceProfile.MutableNonreplProfile()
        ),
        PrepareMediaKindPerformanceProfile(
            tc,
            tc.GetHddNonreplThrottlingConfig(),
            profile,
            maxIopsPerGuest,
            maxBandwidthPerGuest,
            false,
            *performanceProfile.MutableHddNonreplProfile()
        ),
        PrepareMediaKindPerformanceProfile(
            tc,
            tc.GetMirror2ThrottlingConfig(),
            profile,
            maxIopsPerGuest,
            maxBandwidthPerGuest,
            false,
            *performanceProfile.MutableMirror2Profile()
        ),
        PrepareMediaKindPerformanceProfile(
            tc,
            tc.GetMirror3ThrottlingConfig(),
            profile,
            maxIopsPerGuest,
            maxBandwidthPerGuest,
            false,
            *performanceProfile.MutableMirror3Profile()
        ),
    };

    return std::any_of(init.begin(), init.end(), [] (bool x) {return x;});
}

IThrottlerPolicyPtr CreateClientThrottlerPolicy(
    NProto::TClientPerformanceProfile performanceProfile)
{
    return std::make_shared<TThrottlerPolicy>(std::move(performanceProfile));
}

IThrottlerTrackerPtr CreateClientThrottlerTracker()
{
    return std::make_shared<TThrottlerTracker>();
}

IThrottlerLoggerPtr CreateClientThrottlerLogger(
    IRequestStatsPtr requestStats,
    ILoggingServicePtr logging)
{
    return std::make_shared<TThrottlerLogger>(
        std::move(requestStats),
        std::move(logging),
        "BLOCKSTORE_CLIENT");
}

IBlockStorePtr CreateThrottlingClient(
    IBlockStorePtr client,
    IThrottlerPtr throttler)
{
    return std::make_shared<TThrottlingClient>(
        std::move(client),
        std::move(throttler));
}

IThrottlerProviderPtr CreateThrottlerProvider(
    THostPerformanceProfile hostProfile,
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    NMonitoring::TDynamicCountersPtr rootGroup,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats)
{
    return std::make_shared<TThrottlerProvider>(
        hostProfile,
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(rootGroup),
        std::move(requestStats),
        std::move(volumeStats));
}

}   // namespace NCloud::NBlockStore::NClient
