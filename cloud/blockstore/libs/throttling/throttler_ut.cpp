#include "throttler.h"

#include "throttler_logger_test.h"
#include "throttler_metrics_test.h"
#include "throttler_policy.h"
#include "throttler_tracker_test.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats_test.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/queue.h>
#include <util/generic/scope.h>
#include <util/generic/xrange.h>
#include <util/random/entropy.h>
#include <util/random/mersenne.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/thread/factory.h>

#include <array>
#include <random>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto SHORT_TIMEOUT = UPDATE_THROTTLER_USED_QUOTA_INTERVAL / 10;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_NODISK_SERVICE(xxx, ...)                                    \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(ListVolumes,                        __VA_ARGS__)                       \
    xxx(UploadClientMetrics,                __VA_ARGS__)                       \
    xxx(DiscoverInstances,                  __VA_ARGS__)                       \
    xxx(ExecuteAction,                      __VA_ARGS__)                       \
    xxx(DescribeVolumeModel,                __VA_ARGS__)                       \
    xxx(UpdateDiskRegistryConfig,           __VA_ARGS__)                       \
    xxx(DescribeDiskRegistryConfig,         __VA_ARGS__)                       \
    xxx(CreatePlacementGroup,               __VA_ARGS__)                       \
    xxx(DestroyPlacementGroup,              __VA_ARGS__)                       \
    xxx(AlterPlacementGroupMembership,      __VA_ARGS__)                       \
    xxx(DescribePlacementGroup,             __VA_ARGS__)                       \
    xxx(ListPlacementGroups,                __VA_ARGS__)                       \
    xxx(CmsAction,                          __VA_ARGS__)                       \
    xxx(StopEndpoint,                       __VA_ARGS__)                       \
    xxx(ListEndpoints,                      __VA_ARGS__)                       \
    xxx(KickEndpoint,                       __VA_ARGS__)                       \
    xxx(ListKeyrings,                       __VA_ARGS__)                       \
    xxx(DescribeEndpoint,                   __VA_ARGS__)                       \
    xxx(RefreshEndpoint,                    __VA_ARGS__)                       \
    xxx(CancelEndpointInFlightRequests,     __VA_ARGS__)                       \
// BLOCKSTORE_NODISK_SERVICE

#define BLOCKSTORE_NOBLOCKS_SERVICE(xxx, ...)                                  \
    xxx(StartEndpoint,                      __VA_ARGS__)                       \
    xxx(CreateVolume,                       __VA_ARGS__)                       \
    xxx(DestroyVolume,                      __VA_ARGS__)                       \
    xxx(ResizeVolume,                       __VA_ARGS__)                       \
    xxx(StatVolume,                         __VA_ARGS__)                       \
    xxx(AssignVolume,                       __VA_ARGS__)                       \
    xxx(CreateCheckpoint,                   __VA_ARGS__)                       \
    xxx(DeleteCheckpoint,                   __VA_ARGS__)                       \
    xxx(AlterVolume,                        __VA_ARGS__)                       \
    xxx(GetChangedBlocks,                   __VA_ARGS__)                       \
    xxx(DescribeVolume,                     __VA_ARGS__)                       \
// BLOCKSTORE_NOBLOCKS_SERVICE

#define BLOCKSTORE_READ_SERVICE(xxx, ...)                                      \
    xxx(ReadBlocks,                         __VA_ARGS__)                       \
    xxx(ReadBlocksLocal,                    __VA_ARGS__)                       \
    xxx(ZeroBlocks,                         __VA_ARGS__)                       \
// BLOCKSTORE_READ_SERVICE

#define BLOCKSTORE_WRITE_SERVICE(xxx, ...)                                     \
    xxx(WriteBlocks,                        __VA_ARGS__)                       \
    xxx(WriteBlocksLocal,                   __VA_ARGS__)                       \
// BLOCKSTORE_WRITE_SERVICE

////////////////////////////////////////////////////////////////////////////////

class TThrottlerPolicy final
    : public IThrottlerPolicy
{
private:
    TDuration PostponeDelay = TDuration::Zero();

    TMersenne<ui32> RandomGenerator;
    TAtomic UseRandomDelay = false;

public:
    TThrottlerPolicy()
        : RandomGenerator(Seed())
    {}

    // Accessed from throttler within single thread
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

        if (AtomicGet(UseRandomDelay)) {
            TDuration delay = TDuration::Zero();

            std::uniform_int_distribution dist(0, 1);
            if (dist(RandomGenerator)) {
                std::uniform_int_distribution milliDist(1, 10'000);
                delay = TDuration::MilliSeconds(milliDist(RandomGenerator));
            }

            return delay;
        }

        return PostponeDelay;
    }

    // Accessed from throttler within single thread
    double CalculateCurrentSpentBudgetShare(TInstant ts) const override
    {
        Y_UNUSED(ts);

        return PostponeDelay.GetValue() / 1e6;
    }

    void SetPostponeDelay(TDuration delay)
    {
        PostponeDelay = delay;
    }

    void SetRandomDelay(bool enabled)
    {
        AtomicSet(UseRandomDelay, enabled);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMultiVolumeMultiClientProcessingPolicy
{
    TMap<
        TString,
        TMap<TString, std::shared_ptr<TTestVolumeInfo<>>>> VolumeInfos;
    mutable TMap<TString, TMap<TString, TAtomic>> VolumeClientCount;

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId)
    {
        Y_UNUSED(instanceId);

        if (auto volumeIt = VolumeInfos.find(volume.GetDiskId());
            volumeIt != VolumeInfos.end())
        {
            if (auto clientIt = volumeIt->second.find(clientId);
                clientIt != volumeIt->second.end())
            {
                clientIt->second->Volume = volume;
            } else {
                auto volumeStats = std::make_shared<TTestVolumeInfo<>>();
                volumeStats->Volume = volume;
                volumeIt->second.emplace(clientId, std::move(volumeStats));
            }
        } else {
            auto volumeStats = std::make_shared<TTestVolumeInfo<>>();
            volumeStats->Volume = volume;
            VolumeInfos[volume.GetDiskId()][clientId] = std::move(volumeStats);
        }
        AtomicSet(VolumeClientCount[volume.GetDiskId()][clientId], 0);
        return true;
    }

    void UnmountVolume(
        const TString& diskId,
        const TString& clientId)
    {
        if (auto volumeIt = VolumeInfos.find(diskId);
            volumeIt != VolumeInfos.end())
        {
            if (auto clientIt = volumeIt->second.find(clientId);
                clientIt != volumeIt->second.end())
            {
                volumeIt->second.erase(clientIt);
            }
            if (volumeIt->second.empty()) {
                VolumeInfos.erase(volumeIt);
            }
        }
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
        if (auto volumeIt = VolumeInfos.find(diskId);
            volumeIt != VolumeInfos.end())
        {
            if (auto clientIt = volumeIt->second.find(clientId);
                clientIt != volumeIt->second.end())
            {
                AtomicIncrement(VolumeClientCount[diskId][clientId]);
                return clientIt->second;
            }
            return nullptr;
        }
        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TVolume CreateVolumeConfig(
    const TString& diskId,
    ui32 blockSize)
{
   NProto::TVolume volume;

   volume.SetDiskId(diskId);
   volume.SetBlockSize(blockSize);

   return volume;
}

////////////////////////////////////////////////////////////////////////////////

class TThrottlingClient final
    : public IBlockStore
{
private:
    IBlockStorePtr Client;
    IThrottlerPtr Throttler;

public:
    TThrottlingClient(
            IBlockStorePtr client,
            IThrottlerPtr throttler)
        : Client(std::move(client))
        , Throttler(std::move(throttler))
    {}

    void Start() override
    {
        Client->Start();
        Throttler->Start();
    }

    void Stop() override
    {
        Throttler->Stop();
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

struct TScopedTasks
{
private:
    using TThread = THolder<IThreadFactory::IThread>;

    TVector<TThread> Workers;

    bool Started = false;
    TCondVar StartedCheck;
    TMutex WorkersLock;

public:
    ~TScopedTasks()
    {
        WaitAllTasks();
    }

    template <typename F>
    void Add(F f)
    {
        Workers.push_back(SystemThreadFactory()->Run(
            [this, f = std::move(f)]() {
                WorkersLock.Acquire();
                StartedCheck.Wait(
                    WorkersLock,
                    [&started = this->Started]() { return started; }
                );
                WorkersLock.Release();
                f();
            }
        ));
    }

    void Start()
    {
        with_lock (WorkersLock) {
            Started = true;
        }
        StartedCheck.BroadCast();
    }

    void WaitAllTasks()
    {
        for (auto& worker: Workers) {
            worker->Join();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnvironment final
{
    std::shared_ptr<
        TTestVolumeStats<TMultiVolumeMultiClientProcessingPolicy>> VolumeStats;
    std::shared_ptr<TTestThrottlerLogger> Logger;
    std::shared_ptr<TTestThrottlerMetrics> Metrics;
    std::shared_ptr<TThrottlerPolicy> Policy;
    std::shared_ptr<TTestThrottlerTracker> Tracker;
    std::shared_ptr<TTestTimer> Timer;
    ISchedulerPtr Scheduler;
    std::shared_ptr<TTestService> Service;
    IThrottlerPtr Throttler;
    std::shared_ptr<TThrottlingClient> ThrottlingClient;
    TCallContextPtr CallContext;

    ui32 Requests = 1;
    ui32 BlockCount = 1;

    TAtomic RequestCount[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};

    TAtomic LoggerPostponed[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};
    TAtomic LoggerAdvanced[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};

    ui32 TrimCount = 0;
    TAtomic UsedQuotaCount = 0;
    ui32 MaxUsedQuotaCount = 0;

    TAtomic TrackerReceived[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};
    TAtomic TrackerPostponed[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};
    TAtomic TrackerAdvanced[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};

    THashMap<TString, THashSet<TString>> ClientToVolumeMap;

    bool IsManualSchedulerStartStop = false;

    TTestEnvironment()
        : VolumeStats(
            std::make_shared<
                TTestVolumeStats<TMultiVolumeMultiClientProcessingPolicy>>())
        , Logger(std::make_shared<TTestThrottlerLogger>())
        , Metrics(std::make_shared<TTestThrottlerMetrics>())
        , Policy(std::make_shared<TThrottlerPolicy>())
        , Tracker(std::make_shared<TTestThrottlerTracker>())
        , Timer(std::make_shared<TTestTimer>())
        , Scheduler(CreateScheduler(Timer))
        , Service(std::make_shared<TTestService>())
        , Throttler(CreateThrottler(
            Logger,
            Metrics,
            Policy,
            Tracker,
            Timer,
            Scheduler,
            VolumeStats))
        , ThrottlingClient(
            std::make_shared<TThrottlingClient>(Service, Throttler))
        , CallContext(MakeIntrusive<TCallContext>())
    {
        InitTestLogger();
        InitTestMetrics();
        InitTestTracker();

        InitTestService();

        Scheduler->Start();
        ThrottlingClient->Start();
    }

    TTestEnvironment(
            std::shared_ptr<TTestTimer> timer,
            ISchedulerPtr scheduler,
            bool isManualSchedulerStop)
        : VolumeStats(
            std::make_shared<
                TTestVolumeStats<TMultiVolumeMultiClientProcessingPolicy>>())
        , Logger(std::make_shared<TTestThrottlerLogger>())
        , Metrics(std::make_shared<TTestThrottlerMetrics>())
        , Policy(std::make_shared<TThrottlerPolicy>())
        , Tracker(std::make_shared<TTestThrottlerTracker>())
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Service(std::make_shared<TTestService>())
        , Throttler(CreateThrottler(
            Logger,
            Metrics,
            Policy,
            Tracker,
            Timer,
            Scheduler,
            VolumeStats))
        , ThrottlingClient(
            std::make_shared<TThrottlingClient>(Service, Throttler))
        , CallContext(MakeIntrusive<TCallContext>())
        , IsManualSchedulerStartStop(isManualSchedulerStop)
    {
        InitTestLogger();
        InitTestMetrics();
        InitTestTracker();

        InitTestService();

        if (!IsManualSchedulerStartStop) {
            Scheduler->Start();
        }
        ThrottlingClient->Start();
    }

    ~TTestEnvironment()
    {
        if (!IsManualSchedulerStartStop) {
            Scheduler->Stop();
        }
        ThrottlingClient->Stop();
    }

    void InitTestLogger()
    {
#define SET_HANDLER(name, ...)                                                 \
    Logger->name##LogPostponedRequestHandler =                                 \
        [&PostponedCount = this->LoggerPostponed] (                            \
            ui64,                                                              \
            TCallContext&,                                                     \
            IVolumeInfo*,                                                      \
            const NProto::T##name##Request&,                                   \
            TDuration)                                                         \
    {                                                                          \
        AtomicIncrement(                                                       \
            PostponedCount[static_cast<size_t>(EBlockStoreRequest::name)]);    \
    };                                                                         \
                                                                               \
    Logger->name##LogAdvancedRequestHandler =                                  \
        [&AdvancedCount = this->LoggerAdvanced] (                              \
            ui64,                                                              \
            TCallContext&,                                                     \
            IVolumeInfo*,                                                      \
            const NProto::T##name##Request&)                                   \
    {                                                                          \
        AtomicIncrement(                                                       \
            AdvancedCount[static_cast<size_t>(EBlockStoreRequest::name)]);     \
    };                                                                         \
                                                                               \
    Logger->name##LogErrorHandler = [] (                                       \
        const NProto::T##name##Request&,                                       \
        const TString&)                                                        \
    {                                                                          \
        UNIT_ASSERT_C(false, "Error occured on " #name " request");            \
    };                                                                         \
// SET_HANDLER

        BLOCKSTORE_SERVICE(SET_HANDLER)

#undef SET_HANDLER
    }

    void InitTestMetrics()
    {
        Metrics->RegisterHandler = [&map = this->ClientToVolumeMap] (
            const TString& diskId,
            const TString& clientId)
        {
            if (auto clientIt = map.find(clientId);
                clientIt != map.end())
            {
                clientIt->second.emplace(diskId);
            } else {
                map[clientId].insert(diskId);
            }
        };

        Metrics->UnregisterHandler = [&map = this->ClientToVolumeMap] (
            const TString& diskId,
            const TString& clientId)
        {
            auto clientIt = map.find(clientId);
            auto volumeIt = clientIt->second.find(diskId);

            clientIt->second.erase(volumeIt);
            if (clientIt->second.empty()) {
                map.erase(clientIt);
            }
        };

        Metrics->TrimHandler =
            [&trimCount = this->TrimCount] (const TInstant&)
        {
            ++trimCount;
        };

        Metrics->UpdateUsedQuotaHandler =
            [&usedQuotaCount = this->UsedQuotaCount] (ui64)
        {
            AtomicIncrement(usedQuotaCount);
        };

        Metrics->UpdateMaxUsedQuotaHandler =
            [&maxUsedQuotaCount = this->MaxUsedQuotaCount] ()
        {
            ++maxUsedQuotaCount;
        };
    }

    void InitTestTracker()
    {
#define SET_HANDLER(name, ...)                                                 \
    Tracker->name##TrackReceivedRequestHandler =                               \
        [&ReceivedCount = this->TrackerReceived] (                             \
            TCallContext&,                                                     \
            IVolumeInfo*,                                                      \
            const NProto::T##name##Request&)                                   \
    {                                                                          \
        AtomicIncrement(                                                       \
            ReceivedCount[static_cast<size_t>(EBlockStoreRequest::name)]);     \
    };                                                                         \
                                                                               \
    Tracker->name##TrackPostponedRequestHandler =                              \
        [&PostponedCount = this->TrackerPostponed] (                           \
            TCallContext&,                                                     \
            const NProto::T##name##Request&)                                   \
    {                                                                          \
        AtomicIncrement(                                                       \
            PostponedCount[static_cast<size_t>(EBlockStoreRequest::name)]);    \
    };                                                                         \
                                                                               \
    Tracker->name##TrackAdvancedRequestHandler =                               \
        [&AdvancedCount = this->TrackerAdvanced] (                             \
            TCallContext&,                                                     \
            const NProto::T##name##Request&)                                   \
    {                                                                          \
        AtomicIncrement(                                                       \
            AdvancedCount[static_cast<size_t>(EBlockStoreRequest::name)]);     \
    };                                                                         \
// SET_HANDLER

        BLOCKSTORE_SERVICE(SET_HANDLER)

#undef SET_HANDLER
    }

    void InitTestService()
    {
#define SET_HANDLER(name, ...)                                                 \
    Service->name##Handler = [&RequestCount = this->RequestCount] (            \
        std::shared_ptr<NProto::T##name##Request>)                             \
    {                                                                          \
        AtomicIncrement(                                                       \
            RequestCount[static_cast<size_t>(EBlockStoreRequest::name)]);      \
        return MakeFuture(NProto::T##name##Response());                        \
    };                                                                         \
// SET_HANDLER

        BLOCKSTORE_NODISK_SERVICE(SET_HANDLER)
        BLOCKSTORE_NOBLOCKS_SERVICE(SET_HANDLER)
        BLOCKSTORE_READ_SERVICE(SET_HANDLER)
        BLOCKSTORE_WRITE_SERVICE(SET_HANDLER)

#undef SET_HANDLER

        Service->MountVolumeHandler = [
            &RequestCount = this->RequestCount,
            &VolumeStats = this->VolumeStats] (
            std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            AtomicIncrement(RequestCount[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]);

            const auto diskId = request->GetDiskId();
            const auto clientId = GetClientId(*request);

            UNIT_ASSERT_C(
                VolumeStats->MountVolume(
                    CreateVolumeConfig(diskId, 4_MB),
                    clientId,
                    ""),
                TStringBuilder() << "Volume " << diskId
                    << " for client " << clientId << " was not mounted");

            NProto::TMountVolumeResponse r;
            r.MutableVolume()->SetDiskId(diskId);

            return MakeFuture(std::move(r));
        };

        Service->UnmountVolumeHandler = [
            &RequestCount = this->RequestCount,
            &VolumeStats = this->VolumeStats] (
            std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            AtomicIncrement(RequestCount[
                static_cast<size_t>(EBlockStoreRequest::UnmountVolume)]);

            const auto diskId = request->GetDiskId();
            const auto clientId = GetClientId(*request);

            VolumeStats->UnmountVolume(diskId, clientId);

            UNIT_ASSERT_C(
                !VolumeStats->GetVolumeInfo(diskId, clientId),
                TStringBuilder() << "Volume " << diskId
                    << " for client " << clientId << " was not unmounted");

            return MakeFuture(NProto::TUnmountVolumeResponse());
        };
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    std::shared_ptr<NProto::T##name##Request> Create##name##Request(           \
        const TString& clientId)                                               \
    {                                                                          \
        NProto::T##name##Request r;                                            \
        r.MutableHeaders()->SetClientId(clientId);                             \
        return std::make_shared<NProto::T##name##Request>(std::move(r));       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_NODISK_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    std::shared_ptr<NProto::T##name##Request> Create##name##Request(           \
        const TString& clientId,                                               \
        const TString& diskId)                                                 \
    {                                                                          \
        NProto::T##name##Request r;                                            \
        r.MutableHeaders()->SetClientId(clientId);                             \
        r.SetDiskId(diskId);                                                   \
        return std::make_shared<NProto::T##name##Request>(std::move(r));       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_NOBLOCKS_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

    std::shared_ptr<NProto::TMountVolumeRequest> CreateMountVolumeRequest(
        const TString& clientId,
        const TString& diskId)
    {
        NProto::TMountVolumeRequest r;
        r.MutableHeaders()->SetClientId(clientId);
        r.SetDiskId(diskId);
        return std::make_shared<NProto::TMountVolumeRequest>(std::move(r));
    }

    std::shared_ptr<NProto::TUnmountVolumeRequest> CreateUnmountVolumeRequest(
        const TString& clientId,
        const TString& diskId)
    {
        NProto::TUnmountVolumeRequest r;
        r.MutableHeaders()->SetClientId(clientId);
        r.SetDiskId(diskId);
        return std::make_shared<NProto::TUnmountVolumeRequest>(std::move(r));
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    std::shared_ptr<NProto::T##name##Request> Create##name##Request(           \
        const TString& clientId,                                               \
        const TString& diskId,                                                 \
        ui32 blockCount)                                                       \
    {                                                                          \
        NProto::T##name##Request r;                                            \
        r.MutableHeaders()->SetClientId(clientId);                             \
        r.SetDiskId(diskId);                                                   \
        r.SetBlocksCount(blockCount);                                          \
        return std::make_shared<NProto::T##name##Request>(std::move(r));       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_READ_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

    std::shared_ptr<NProto::TWriteBlocksRequest> CreateWriteBlocksRequest(
        const TString& clientId,
        const TString& diskId,
        size_t blockCount)
    {
        NProto::TWriteBlocksRequest r;
        r.MutableHeaders()->SetClientId(clientId);
        r.SetDiskId(diskId);
        auto buffers = r.MutableBlocks()->MutableBuffers();
        for (size_t i = 0; i < blockCount; ++i) {
            buffers->Add();
        }
        return std::make_shared<NProto::TWriteBlocksRequest>(std::move(r));
    }

    std::shared_ptr<NProto::TWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
        const TString& clientId,
        const TString& diskId,
        size_t blockCount)
    {
        NProto::TWriteBlocksLocalRequest r;
        r.MutableHeaders()->SetClientId(clientId);
        r.SetDiskId(diskId);
        r.BlocksCount = blockCount;
        return std::make_shared<NProto::TWriteBlocksLocalRequest>(std::move(r));
    }

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    TVector<TFuture<NProto::T##name##Response>> Perform##name##Requests(       \
        IBlockStorePtr service,                                                \
        TCallContextPtr callContext,                                           \
        ui32 count,                                                            \
        const TString& clientId)                                               \
    {                                                                          \
        TVector<TFuture<NProto::T##name##Response>> res;                       \
        for (ui32 i = 0; i < count; ++i) {                                     \
            res.push_back(service->name(                                       \
                callContext,                                                   \
                Create##name##Request(clientId)));                             \
        }                                                                      \
        return res;                                                            \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_NODISK_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    TVector<TFuture<NProto::T##name##Response>> Perform##name##Requests(       \
        IBlockStorePtr service,                                                \
        TCallContextPtr callContext,                                           \
        ui32 count,                                                            \
        const TString& clientId,                                               \
        const TString& diskId)                                                 \
    {                                                                          \
        TVector<TFuture<NProto::T##name##Response>> res;                       \
        for (ui32 i = 0; i < count; ++i) {                                     \
            res.push_back(service->name(                                       \
                callContext,                                                   \
                Create##name##Request(clientId, diskId)));                     \
        }                                                                      \
        return res;                                                            \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_NOBLOCKS_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

    TVector<TFuture<NProto::TMountVolumeResponse>> PerformMountVolumeRequests(
            IBlockStorePtr service,
            TCallContextPtr callContext,
            ui32 count,
            const TString& clientId,
            const TString& diskId)
    {
        TVector<TFuture<NProto::TMountVolumeResponse>> res;
        for (ui32 i = 0; i < count; ++i) {
            res.push_back(service->MountVolume(
                callContext,
                CreateMountVolumeRequest(clientId, diskId)));
        }
        return res;
    }

    TVector<TFuture<NProto::TUnmountVolumeResponse>> PerformUnmountVolumeRequests(
            IBlockStorePtr service,
            TCallContextPtr callContext,
            ui32 count,
            const TString& clientId,
            const TString& diskId)
    {
        TVector<TFuture<NProto::TUnmountVolumeResponse>> res;
        for (ui32 i = 0; i < count; ++i) {
            res.push_back(service->UnmountVolume(
                callContext,
                CreateUnmountVolumeRequest(clientId, diskId)));
        }
        return res;
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    TVector<TFuture<NProto::T##name##Response>> Perform##name##Requests(       \
        IBlockStorePtr service,                                                \
        TCallContextPtr callContext,                                           \
        ui32 count,                                                            \
        const TString& clientId,                                               \
        const TString& diskId,                                                 \
        ui32 blockCount)                                                       \
    {                                                                          \
        TVector<TFuture<NProto::T##name##Response>> res;                       \
        for (ui32 i = 0; i < count; ++i) {                                     \
            res.push_back(service->name(                                       \
                callContext,                                                   \
                Create##name##Request(clientId, diskId, blockCount)));         \
        }                                                                      \
        return res;                                                            \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_READ_SERVICE(BLOCKSTORE_DECLARE_METHOD)
    BLOCKSTORE_WRITE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

    void MountVolume(
        const TString& diskId,
        const TString& clientId)
    {
        auto futuresMountVolume = PerformMountVolumeRequests(
            ThrottlingClient,
            CallContext,
            1,
            clientId,
            diskId);
        NWait::WaitAll(futuresMountVolume).Wait();
        for (const auto& future: futuresMountVolume) {
            UNIT_ASSERT_C(
                future.HasValue(),
                "MountVolume future doesn't contain value");
            auto response = future.GetValue();
            UNIT_ASSERT_C(
                !HasError(response),
                FormatError(response.GetError()));
        }
    }

    void UnmountVolume(
        const TString& diskId,
        const TString& clientId)
    {
        auto futuresUnmountVolume = PerformUnmountVolumeRequests(
            ThrottlingClient,
            CallContext,
            1,
            clientId,
            diskId);
        NWait::WaitAll(futuresUnmountVolume).Wait();
        for (const auto& future: futuresUnmountVolume) {
            UNIT_ASSERT_C(
                future.HasValue(),
                "UnmountVolume future doesn't contain value");
            auto response = future.GetValue();
            UNIT_ASSERT_C(
                !HasError(response),
                FormatError(response.GetError()));
        }
    }

////////////////////////////////////////////////////////////////////////////////

    constexpr ui32 GetThrottableRequestTypeCount() const
    {
        ui32 result = 0;
#define PERFORM_COUNT(name, ...)                                               \
    if constexpr (ShouldBeThrottled<NProto::T##name##Request>()) {             \
        ++result;                                                              \
    }                                                                          \
// PERFORM_COUNT

        BLOCKSTORE_SERVICE(PERFORM_COUNT)

#undef PERFORM_COUNT

        return result;
    }

    ui32 GetVolumeInfosCount() const
    {
        ui32 result = 0;
        for (const auto& [volumeId, clientMap]: VolumeStats->VolumeInfos) {
            result += clientMap.size();
        }

        return result;
    }

////////////////////////////////////////////////////////////////////////////////

    void CheckThroughput(
        ui32 requestCountThrottled,
        ui32 requestCount,
        ui32 receivedCountVolume,
        ui32 receivedCount,
        ui32 postponedCountThrottled,
        ui32 postponedCount,
        ui32 advancedCountThrottled,
        ui32 advancedCount) const
    {
#define DO_TEST(name, ...)                                                     \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? requestCountThrottled                                            \
            : requestCount,                                                    \
        AtomicGet(RequestCount[                                                \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? postponedCountThrottled                                          \
            : postponedCount,                                                  \
        AtomicGet(LoggerPostponed[                                             \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? advancedCountThrottled                                           \
            : advancedCount,                                                   \
        AtomicGet(LoggerAdvanced[                                              \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? postponedCountThrottled                                          \
            : postponedCount,                                                  \
        AtomicGet(TrackerPostponed[                                            \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        ShouldBeThrottled<NProto::T##name##Request>()                          \
            ? advancedCountThrottled                                           \
            : advancedCount,                                                   \
        AtomicGet(TrackerAdvanced[                                             \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
// DO_TEST

        BLOCKSTORE_NODISK_SERVICE(DO_TEST)
        BLOCKSTORE_NOBLOCKS_SERVICE(DO_TEST)
        BLOCKSTORE_READ_SERVICE(DO_TEST)
        BLOCKSTORE_WRITE_SERVICE(DO_TEST)

#undef DO_TEST

#define DO_TEST(name, ...)                                                     \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        receivedCountVolume,                                                   \
        AtomicGet(TrackerReceived[                                             \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
// DO_TEST

    BLOCKSTORE_NOBLOCKS_SERVICE(DO_TEST)
    BLOCKSTORE_READ_SERVICE(DO_TEST)
    BLOCKSTORE_WRITE_SERVICE(DO_TEST)

#undef DO_TEST

#define DO_TEST(name, ...)                                                     \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        receivedCount,                                                         \
        AtomicGet(TrackerReceived[                                             \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
// DO_TEST

    BLOCKSTORE_NODISK_SERVICE(DO_TEST)

#undef DO_TEST

        auto totalCount = GetVolumeInfosCount();

        UNIT_ASSERT_VALUES_EQUAL(
            totalCount,
            AtomicGet(RequestCount[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            totalCount,
            AtomicGet(TrackerReceived[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            AtomicGet(LoggerPostponed[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            AtomicGet(LoggerAdvanced[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            AtomicGet(TrackerPostponed[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            AtomicGet(TrackerAdvanced[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
    }

    void CheckDisks() const  {
#define DISK_REQUESTS(name, ...)                                               \
    count += AtomicGet(TrackerReceived[                                        \
        static_cast<size_t>(EBlockStoreRequest::name)]);                       \
// DISK_REQUESTS

        ui32 count = 0;
        BLOCKSTORE_NOBLOCKS_SERVICE(DISK_REQUESTS)
        BLOCKSTORE_READ_SERVICE(DISK_REQUESTS)
        BLOCKSTORE_WRITE_SERVICE(DISK_REQUESTS)

#undef DISK_REQUESTS

        ui64 totalVolumesPerClient = GetVolumeInfosCount();
        for (const auto& [diskId, clientMap]: VolumeStats->VolumeClientCount) {
            for (const auto& [clientId, counter]: clientMap) {
                UNIT_ASSERT_VALUES_EQUAL(
                    AtomicGet(counter),
                    count / totalVolumesPerClient);
            }
        }
    }

    void CheckCount() const
    {
        for (size_t i: xrange(static_cast<size_t>(EBlockStoreRequest::MAX))) {
            UNIT_ASSERT_VALUES_EQUAL(LoggerPostponed[i], LoggerAdvanced[i]);
        }
        for (size_t i: xrange(static_cast<size_t>(EBlockStoreRequest::MAX))) {
            UNIT_ASSERT_VALUES_EQUAL(LoggerAdvanced[i], TrackerAdvanced[i]);
        }
    }

    void ShootSync()
    {
#define SETUP_VECTORS(name, ...)                                               \
    TVector<TFuture<NProto::T##name##Response>> futures##name;                 \
// SETUP_VECTORS

        BLOCKSTORE_SERVICE(SETUP_VECTORS)

#undef SETUP_VECTORS

#define PERFORM_TEST(name, clientId, diskId, ...)                              \
    auto futures##name##clientId##diskId =                                     \
        Perform##name##Requests(__VA_ARGS__);                                  \
    futures##name.insert(                                                      \
        futures##name.end(),                                                   \
        futures##name##clientId##diskId.begin(),                               \
        futures##name##clientId##diskId.end());                                \
// PERFORM_TEST

        for (const auto& [diskId, clientMap]: VolumeStats->VolumeInfos) {
            for (const auto& [clientId, volumeInfo]: clientMap) {
                auto disk = diskId;
                auto client = clientId;

                BLOCKSTORE_NODISK_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client);

                BLOCKSTORE_NOBLOCKS_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk);
                BLOCKSTORE_READ_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk,
                    BlockCount);
                BLOCKSTORE_WRITE_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk,
                    BlockCount);
            }
        }

#undef PERFORM_TEST

#define WAIT_FUTURES(name, ...)                                                \
    NWait::WaitAll(futures##name).Wait();                                      \
    for (const auto& future: futures##name) {                                  \
        UNIT_ASSERT_C(                                                         \
            future.HasValue(),                                                 \
            #name " future doesn't contain value");                            \
    }                                                                          \
// WAIT_FUTURES

        BLOCKSTORE_NODISK_SERVICE(WAIT_FUTURES);

        BLOCKSTORE_NOBLOCKS_SERVICE(WAIT_FUTURES);
        BLOCKSTORE_READ_SERVICE(WAIT_FUTURES);
        BLOCKSTORE_WRITE_SERVICE(WAIT_FUTURES);

#undef WAIT_FUTURES

        ui64 totalMetrics = GetVolumeInfosCount();

        CheckThroughput(
            Requests * totalMetrics,   // requestCountThrottled
            Requests * totalMetrics,   // requestCount
            Requests * totalMetrics,   // receivedCountVolume
            Requests * totalMetrics,   // receivedCount
            0,                         // postponedCountThrottled
            0,                         // postponedCount
            0,                         // advancedCountThrottled
            0                          // advancedCount
        );
        CheckDisks();

        CheckCount();
    }

    void ShootAsync()
    {
#define SETUP_VECTORS(name, ...)                                               \
    TVector<TFuture<NProto::T##name##Response>> futures##name;                 \
// SETUP_VECTORS

        BLOCKSTORE_SERVICE(SETUP_VECTORS)

#undef SETUP_VECTORS

#define PERFORM_TEST(name, clientId, diskId, ...)                              \
    scopedTasks.Add<std::function<void()>>(                                    \
        [=, &futures##name, &requestsLock]() {                                 \
            auto futures##name##clientId##diskId =                             \
                Perform##name##Requests(__VA_ARGS__);                          \
            with_lock (requestsLock) {                                         \
                futures##name.insert(                                          \
                    futures##name.end(),                                       \
                    futures##name##clientId##diskId.begin(),                   \
                    futures##name##clientId##diskId.end());                    \
            }                                                                  \
        }                                                                      \
    );                                                                         \
// PERFORM_TEST

        TScopedTasks scopedTasks;
        TMutex requestsLock;

        for (const auto& [diskId, clientMap]: VolumeStats->VolumeInfos) {
            for (const auto& [clientId, volumeInfo]: clientMap) {
                auto disk = diskId;
                auto client = clientId;

                BLOCKSTORE_NODISK_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client);

                BLOCKSTORE_NOBLOCKS_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk);
                BLOCKSTORE_READ_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk,
                    BlockCount);
                BLOCKSTORE_WRITE_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk,
                    BlockCount);
            }
        }

        scopedTasks.Start();
        scopedTasks.WaitAllTasks();

#undef PERFORM_TEST

#define WAIT_FUTURES(name, ...)                                                \
    NWait::WaitAll(futures##name).Wait();                                      \
    for (const auto& future: futures##name) {                                  \
        UNIT_ASSERT_C(                                                         \
            future.HasValue(),                                                 \
            #name " future doesn't contain value");                            \
    }                                                                          \
// WAIT_FUTURES

        BLOCKSTORE_NODISK_SERVICE(WAIT_FUTURES);

        BLOCKSTORE_NOBLOCKS_SERVICE(WAIT_FUTURES);
        BLOCKSTORE_READ_SERVICE(WAIT_FUTURES);
        BLOCKSTORE_WRITE_SERVICE(WAIT_FUTURES);

#undef WAIT_FUTURES

        ui64 totalMetrics = GetVolumeInfosCount();

        CheckThroughput(
            Requests * totalMetrics,   // requestCountThrottled
            Requests * totalMetrics,   // requestCount
            Requests * totalMetrics,   // receivedCountVolume
            Requests * totalMetrics,   // receivedCount
            0,                         // postponedCountThrottled
            0,                         // postponedCount
            0,                         // advancedCountThrottled
            0                          // advancedCount
        );
        CheckDisks();

        CheckCount();
    }

    void ShootSyncWithPostpone()
    {
#define SETUP_VECTORS(name, ...)                                               \
    TVector<TFuture<NProto::T##name##Response>> futures##name;                 \
// SETUP_VECTORS

        BLOCKSTORE_SERVICE(SETUP_VECTORS)

#undef SETUP_VECTORS

#define PERFORM_TEST(name, clientId, diskId, ...)                              \
    auto futures##name##clientId##diskId =                                     \
        Perform##name##Requests(__VA_ARGS__);                                  \
    futures##name.insert(                                                      \
        futures##name.end(),                                                   \
        futures##name##clientId##diskId.begin(),                               \
        futures##name##clientId##diskId.end());                                \
// PERFORM_TEST

        Policy->SetPostponeDelay(TDuration::Seconds(1));

        for (const auto& [diskId, clientMap]: VolumeStats->VolumeInfos) {
            for (const auto& [clientId, volumeInfo]: clientMap) {
                auto disk = diskId;
                auto client = clientId;

                BLOCKSTORE_NODISK_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client);

                BLOCKSTORE_NOBLOCKS_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk);
                BLOCKSTORE_READ_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk,
                    BlockCount);
                BLOCKSTORE_WRITE_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    ThrottlingClient,
                    CallContext,
                    Requests,
                    client,
                    disk,
                    BlockCount);
            }
        }

#undef PERFORM_TEST

        ui64 totalMetrics = GetVolumeInfosCount();

        CheckThroughput(
            0,                         // requestCountThrottled
            Requests * totalMetrics,   // requestCount
            Requests * totalMetrics,   // receivedCountVolume
            Requests * totalMetrics,   // receivedCount
            Requests * totalMetrics,   // postponedCountThrottled
            0,                         // postponedCount
            0,                         // advancedCountThrottled
            0                          // advancedCount
        );
        CheckDisks();

        Policy->SetPostponeDelay(TDuration::Zero());
        Timer->AdvanceTime(TDuration::Seconds(1));

#define WAIT_FUTURES(name, ...)                                                \
    NWait::WaitAll(futures##name).Wait();                                      \
    for (const auto& future: futures##name) {                                  \
        UNIT_ASSERT_C(                                                         \
            future.HasValue(),                                                 \
            #name " future doesn't contain value");                            \
    }                                                                          \
// WAIT_FUTURES

        BLOCKSTORE_NODISK_SERVICE(WAIT_FUTURES);

        BLOCKSTORE_NOBLOCKS_SERVICE(WAIT_FUTURES);
        BLOCKSTORE_READ_SERVICE(WAIT_FUTURES);
        BLOCKSTORE_WRITE_SERVICE(WAIT_FUTURES);

#undef WAIT_FUTURES

        totalMetrics = GetVolumeInfosCount();

        CheckThroughput(
            Requests * totalMetrics,   // requestCountThrottled
            Requests * totalMetrics,   // requestCount
            Requests * totalMetrics,   // receivedCountVolume
            Requests * totalMetrics,   // receivedCount
            Requests * totalMetrics,   // postponedCountThrottled
            0,                         // postponedCount
            Requests * totalMetrics,   // advancedCountThrottled
            0                          // advancedCount
        );
        CheckDisks();

        CheckCount();
    }

    void ShootAsyncWithPostpone()
    {
#define SETUP_VECTORS(name, ...)                                               \
    TVector<TFuture<NProto::T##name##Response>> futures##name;                 \
// SETUP_VECTORS

        BLOCKSTORE_SERVICE(SETUP_VECTORS)

#undef SETUP_VECTORS

#define PERFORM_TEST(name, clientId, diskId, ...)                              \
    scopedTasks.Add<std::function<void()>>(                                    \
        [=, &requestsLock, &futures##name] () {                                \
            TVector<TFuture<NProto::T##name##Response>>                        \
                futures##name##clientId##diskId;                               \
            for (ui32 i = 0; i < Requests; ++i) {                              \
                futures##name##clientId##diskId.push_back(                     \
                    ThrottlingClient->name(                                    \
                        CallContext,                                           \
                        Create##name##Request(__VA_ARGS__)));                  \
            }                                                                  \
            with_lock (requestsLock) {                                         \
                futures##name.insert(                                          \
                    futures##name.end(),                                       \
                    futures##name##clientId##diskId.begin(),                   \
                    futures##name##clientId##diskId.end());                    \
            }                                                                  \
        }                                                                      \
    );                                                                         \
// PERFORM_TEST

        Policy->SetRandomDelay(true);
        TScopedTasks scopedTasks;
        TMutex requestsLock;

        for (const auto& [diskId, clientMap]: VolumeStats->VolumeInfos) {
            for (const auto& [clientId, volumeInfo]: clientMap) {
                auto disk = diskId;
                auto client = clientId;

                BLOCKSTORE_NODISK_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    client);

                BLOCKSTORE_NOBLOCKS_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    client,
                    disk);
                BLOCKSTORE_READ_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    client,
                    disk,
                    BlockCount);
                BLOCKSTORE_WRITE_SERVICE(
                    PERFORM_TEST,
                    Client,
                    Disk,
                    client,
                    disk,
                    BlockCount);
            }
        }

        scopedTasks.Start();
        scopedTasks.WaitAllTasks();

#undef PERFORM_TEST

        const ui32 MAX_DELAY_COUNT = 100'000;
        TMersenne<ui32> gen(Seed());
        TAutoEvent notifier;

        std::uniform_int_distribution milliDist(
            UPDATE_THROTTLER_USED_QUOTA_INTERVAL.MilliSeconds()
                - SHORT_TIMEOUT.MilliSeconds(),
            UPDATE_THROTTLER_USED_QUOTA_INTERVAL.MilliSeconds()
                + SHORT_TIMEOUT.MilliSeconds());
        for (ui32 i = 0; i < MAX_DELAY_COUNT; ++i) {
            auto delay = TDuration::MilliSeconds(milliDist(gen));
            Scheduler->Schedule(
                Timer->Now() + delay,
                [&] {
                    notifier.Signal();
                });
            Timer->AdvanceTime(delay);
            UNIT_ASSERT_C(notifier.Wait(), "Locked on scheduler");
        }

        Policy->SetRandomDelay(false);
        Timer->AdvanceTime(TDuration::Days(60));

#define WAIT_FUTURES(name, ...)                                                \
    NWait::WaitAll(futures##name).Wait();                                      \
    for (const auto& future: futures##name) {                                  \
        UNIT_ASSERT_C(                                                         \
            future.HasValue(),                                                 \
            #name " future doesn't contain value");                            \
    }                                                                          \
// WAIT_FUTURES

        BLOCKSTORE_NODISK_SERVICE(WAIT_FUTURES);

        BLOCKSTORE_NOBLOCKS_SERVICE(WAIT_FUTURES);
        BLOCKSTORE_READ_SERVICE(WAIT_FUTURES);
        BLOCKSTORE_WRITE_SERVICE(WAIT_FUTURES);

#undef WAIT_FUTURES


        ui64 totalMetrics = GetVolumeInfosCount();

#define DO_TEST(name, ...)                                                     \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        __VA_ARGS__,                                                           \
        AtomicGet(RequestCount[                                                \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        __VA_ARGS__,                                                           \
        AtomicGet(TrackerReceived[                                             \
            static_cast<size_t>(EBlockStoreRequest::name)]));                  \
// DO_TEST

        BLOCKSTORE_NODISK_SERVICE(
            DO_TEST,
            Requests * totalMetrics);
        BLOCKSTORE_NOBLOCKS_SERVICE(
            DO_TEST,
            Requests * totalMetrics);
        BLOCKSTORE_READ_SERVICE(
            DO_TEST,
            Requests * totalMetrics);
        BLOCKSTORE_WRITE_SERVICE(
            DO_TEST,
            Requests * totalMetrics);

#undef DO_TEST

        CheckDisks();

        UNIT_ASSERT_VALUES_EQUAL(
            totalMetrics,
            AtomicGet(RequestCount[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            totalMetrics,
            AtomicGet(TrackerReceived[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            ShouldBeThrottled<NProto::TMountVolumeRequest>()
                ? VolumeStats->VolumeInfos.size()
                : 0,
            AtomicGet(LoggerAdvanced[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            ShouldBeThrottled<NProto::TMountVolumeRequest>()
                ? VolumeStats->VolumeInfos.size()
                : 0,
            AtomicGet(TrackerPostponed[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));
        UNIT_ASSERT_VALUES_EQUAL(
            ShouldBeThrottled<NProto::TMountVolumeRequest>()
                ? VolumeStats->VolumeInfos.size()
                : 0,
            AtomicGet(TrackerAdvanced[
                static_cast<size_t>(EBlockStoreRequest::MountVolume)]));

        CheckCount();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TThrottlerTest)
{
    Y_UNIT_TEST(ShouldPropagateRequestsWithSingleThread)
    {
        TTestEnvironment env;

        const TVector<std::pair<TString, TString>> diskIds = {
            { "first_test_disk_id" , "first_client"  },   // common first_client
            { "second_test_disk_id", "first_client"  },   // common first_client
            { "second_test_disk_id", "second_client" },   // common second_test_disk_id and second_client
            { "third_test_disk_id" , "second_client" },   // common third_test_disk_id
            { "third_test_disk_id" , "third_client"  },   // common third_test_disk_id
            { "fourth_test_disk_id", "fourh_client"  }    // single disk and client
        };
        for (const auto& [diskId, clientId]: diskIds) {
            env.MountVolume(diskId, clientId);
        }

        env.Requests = 1'000;
        env.BlockCount = 10;
        env.ShootSync();
    }

    Y_UNIT_TEST(ShouldPropagateRequestsWithMultipleThreads)
    {
        TTestEnvironment env;

        const TVector<std::pair<TString, TString>> diskIds = {
            { "first_test_disk_id" , "first_client"  },   // common first_client
            { "second_test_disk_id", "first_client"  },   // common first_client
            { "second_test_disk_id", "second_client" },   // common second_test_disk_id and second_client
            { "third_test_disk_id" , "second_client" },   // common third_test_disk_id
            { "third_test_disk_id" , "third_client"  },   // common third_test_disk_id
            { "fourth_test_disk_id", "fourh_client"  }    // single disk and client
        };
        for (const auto& [diskId, clientId]: diskIds) {
            env.MountVolume(diskId, clientId);
        }

        env.Requests = 1'000;
        env.BlockCount = 10;
        env.ShootAsync();
    }

    Y_UNIT_TEST(ShouldPostponeRequestsWithSingleThread)
    {
        TTestEnvironment env;

        const TVector<std::pair<TString, TString>> diskIds = {
            { "first_test_disk_id" , "first_client"  },   // common first_client
            { "second_test_disk_id", "first_client"  },   // common first_client
            { "second_test_disk_id", "second_client" },   // common second_test_disk_id and second_client
            { "third_test_disk_id" , "second_client" },   // common third_test_disk_id
            { "third_test_disk_id" , "third_client"  },   // common third_test_disk_id
            { "fourth_test_disk_id", "fourh_client"  }    // single disk and client
        };
        for (const auto& [diskId, clientId]: diskIds) {
            env.MountVolume(diskId, clientId);
        }

        env.Requests = 1'000;
        env.BlockCount = 10;
        env.ShootSyncWithPostpone();
    }

    Y_UNIT_TEST(ShouldPostponeRequestsWithMultipleThreads)
    {
        TTestEnvironment env;

        const TVector<std::pair<TString, TString>> diskIds = {
            { "first_test_disk_id" , "first_client"  },   // common first_client
            { "second_test_disk_id", "first_client"  },   // common first_client
            { "second_test_disk_id", "second_client" },   // common second_test_disk_id and second_client
            { "third_test_disk_id" , "second_client" },   // common third_test_disk_id
            { "third_test_disk_id" , "third_client"  },   // common third_test_disk_id
            { "fourth_test_disk_id", "fourh_client"  }    // single disk and client
        };
        for (const auto& [diskId, clientId]: diskIds) {
            env.MountVolume(diskId, clientId);
        }

        env.Requests = 1'000;
        env.BlockCount = 10;
        env.ShootAsyncWithPostpone();
    }

    Y_UNIT_TEST(ShouldCorrectlyUpdateMaxQuotaWithScheduler)
    {
        TTestEnvironment env;

        // lock in main thread
        // notify from scheduler
        auto notifier = std::make_shared<TAutoEvent>();

       // Main thread:
       // 1. Schedule "lock task"
       // 2. Advance time
       // 3. Wait on notifier for signal from scheduler
       // 4. Read the MetricsCount

       // Scheduler thread:
       // 1. Execute all non-locking tasks (LogQuota)
       // 2. Notify notifier (all read tasks have already been executed)

        env.Scheduler->Schedule(
            env.Timer->Now(),
            [notifier] () {
                notifier->Signal();
            });

        // 0. Scheduler_task (Wait on condvar) <- locked here
        //       |
        // 1. Scheduler_task (MetricsUpdate)

        // Should log quota on throttler Start call
        UNIT_ASSERT_C(
            notifier->Wait(),
            "Locked on scheduler");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.MaxUsedQuotaCount);

        // Event after update
        env.Scheduler->Schedule(
            env.Timer->Now() +
                UPDATE_THROTTLER_USED_QUOTA_INTERVAL +
                SHORT_TIMEOUT,
            [notifier] () {
                notifier->Signal();
            });

        // 0. Scheduler_task (Wait on condvar)
        //       |
        // 1. Scheduler_task (MetricsUpdate)   <- here
        //       |
        // 2. Scheduler_task (Wait on condvar)

        env.Timer->AdvanceTime(
            UPDATE_THROTTLER_USED_QUOTA_INTERVAL + SHORT_TIMEOUT);

        // 0. Scheduler_task (Wait on condvar)
        //       |
        // 1. Scheduler_task (MetricsUpdate)
        //       |
        // 2. Scheduler_task (Wait on condvar) <- locked here
        //       |
        // 3. Scheduler_task (MetricsUpdate)

        // Should log quota when interval is over
        UNIT_ASSERT_C(
            notifier->Wait(),
            "Locked on scheduler");
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.MaxUsedQuotaCount);

        env.Scheduler->Schedule(
            env.Timer->Now() +
                UPDATE_THROTTLER_USED_QUOTA_INTERVAL +
                SHORT_TIMEOUT,
            [notifier] () {
                notifier->Signal();
            });

        // 0. Scheduler_task (Wait on condvar)
        //       |
        // 1. Scheduler_task (MetricsUpdate)
        //       |
        // 2. Scheduler_task (Wait on condvar)
        //       |
        // 3. Scheduler_task (MetricsUpdate)   <- here
        //       |
        // 4. Scheduler_task (Wait on condvar)

        env.Timer->AdvanceTime(
            UPDATE_THROTTLER_USED_QUOTA_INTERVAL + SHORT_TIMEOUT);

        // 0. Scheduler_task (Wait on condvar)
        //       |
        // 1. Scheduler_task (MetricsUpdate)
        //       |
        // 2. Scheduler_task (Wait on condvar)
        //       |
        // 3. Scheduler_task (MetricsUpdate)
        //       |
        // 4. Scheduler_task (Wait on condvar) <- locked here
        //       |
        // 5. Scheduler_task (MetricsUpdate)

        // To be sure that everything is fine
        UNIT_ASSERT_C(
            notifier->Wait(),
            "Locked on scheduler");
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            env.MaxUsedQuotaCount);
    }

    Y_UNIT_TEST(ShouldNotAccessThisWhenThrottlerIsDestroyed)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = CreateScheduler(timer);

        scheduler->Start();
        Y_DEFER {
            scheduler->Stop();
        };

        auto notifier = std::make_shared<TAutoEvent>();

        {
            // Schedule task on Start call
            TTestEnvironment env(timer, scheduler, true);
        }

        scheduler->Schedule(
            timer->Now() +
                Max(
                    UPDATE_THROTTLER_USED_QUOTA_INTERVAL,
                    TRIM_THROTTLER_METRICS_INTERVAL) +
                SHORT_TIMEOUT,
            [notifier] () {
                notifier->Signal();
            });

        timer->AdvanceTime(
            Max(
                UPDATE_THROTTLER_USED_QUOTA_INTERVAL,
                TRIM_THROTTLER_METRICS_INTERVAL)
                + SHORT_TIMEOUT);

        UNIT_ASSERT_C(
            notifier->Wait(),
            "Locked on scheduler");
    }

    Y_UNIT_TEST(ShouldMountAndUnmountProperly)
    {
        TTestEnvironment env;

        const TVector<std::pair<TString, TString>> diskIds = {
            { "first_test_disk_id" , "first_client"  },   // common first_client
            { "second_test_disk_id", "first_client"  },   // common first_client
            { "second_test_disk_id", "second_client" }    // common second_test_disk_id and second_client
        };

        env.MountVolume(diskIds[0].first, diskIds[0].second);
        UNIT_ASSERT_C(
            env.ClientToVolumeMap.size() == 1,
            TStringBuilder() << "Only client " << diskIds[0].second
                << " should exist");
        auto clientIt = env.ClientToVolumeMap.find(diskIds[0].second);
        UNIT_ASSERT_C(
            clientIt != env.ClientToVolumeMap.end(),
            TStringBuilder() << "Client " << diskIds[0].second
                << " was not registered on MountVolume");
        UNIT_ASSERT_C(
            clientIt->second.size() == 1,
            TStringBuilder() << "Only volume " << diskIds[0].first
                << " for client " << diskIds[0].second << " should exist");
        auto volumeIt = clientIt->second.find(diskIds[0].first);
        UNIT_ASSERT_C(
            volumeIt != clientIt->second.end(),
            TStringBuilder() << "Volume " << diskIds[0].first
                << " for client " << diskIds[0].second
                << " was not registered on MountVolume");

        env.MountVolume(diskIds[1].first, diskIds[1].second);
        UNIT_ASSERT_C(
            env.ClientToVolumeMap.size() == 1,
            TStringBuilder() << "Only client " << diskIds[1].second
                << " should exist");
        clientIt = env.ClientToVolumeMap.find(diskIds[1].second);
        UNIT_ASSERT_C(
            clientIt != env.ClientToVolumeMap.end(),
            TStringBuilder() << "Client " << diskIds[1].second
                << " was not registered on MountVolume");
        UNIT_ASSERT_C(
            clientIt->second.size() == 2,
            TStringBuilder() << "Volumes " << diskIds[0].first << " and "
                << diskIds[1].first << " for client " << diskIds[0].second
                << " should exist");
        volumeIt = clientIt->second.find(diskIds[1].first);
        UNIT_ASSERT_C(
            volumeIt != clientIt->second.end(),
            TStringBuilder() << "Volume " << diskIds[1].first
                << " for client " << diskIds[1].second
                << " was not registered on MountVolume");

        env.MountVolume(diskIds[2].first, diskIds[2].second);
        UNIT_ASSERT_C(
            env.ClientToVolumeMap.size() == 2,
            TStringBuilder() << "Clients " << diskIds[0].second
                << " and " << diskIds[2].second << " should exist");
        clientIt = env.ClientToVolumeMap.find(diskIds[2].second);
        UNIT_ASSERT_C(
            clientIt != env.ClientToVolumeMap.end(),
            TStringBuilder() << "Client " << diskIds[2].second
                << " was not registered on MountVolume");
        UNIT_ASSERT_C(
            clientIt->second.size() == 1,
            TStringBuilder() << "Only volume " << diskIds[2].first
                << " for client " << diskIds[2].second << " should exist");
        volumeIt = clientIt->second.find(diskIds[2].first);
        UNIT_ASSERT_C(
            volumeIt != clientIt->second.end(),
            TStringBuilder() << "Volume " << diskIds[2].first
                << " for client " << diskIds[2].second
                << " was not registered on MountVolume");

        env.UnmountVolume(diskIds[1].first, diskIds[1].second);
        UNIT_ASSERT_C(
            env.ClientToVolumeMap.size() == 2,
            TStringBuilder() << "Clients " << diskIds[0].second
                << " and " << diskIds[2].second << " should exist");
        clientIt = env.ClientToVolumeMap.find(diskIds[1].second);
        UNIT_ASSERT_C(
            clientIt != env.ClientToVolumeMap.end(),
            TStringBuilder() << "Client " << diskIds[1].second
                << " should exist with volume " << diskIds[0].first);
        UNIT_ASSERT_C(
            clientIt->second.size() == 1,
            TStringBuilder() << "Only volume " << diskIds[0].first
                << " for client " << diskIds[0].second << " should exist");
        volumeIt = clientIt->second.find(diskIds[1].first);
        UNIT_ASSERT_C(
            volumeIt == clientIt->second.end(),
            TStringBuilder() << "Volume " << diskIds[1].first
                << " for client " << diskIds[1].second
                << " was not unregistered on UnmountVolume");

        env.UnmountVolume(diskIds[0].first, diskIds[0].second);
        UNIT_ASSERT_C(
            env.ClientToVolumeMap.size() == 1,
            TStringBuilder() << "Only client " << diskIds[2].second
                << " should exist");
        clientIt = env.ClientToVolumeMap.find(diskIds[0].second);
        UNIT_ASSERT_C(
            clientIt == env.ClientToVolumeMap.end(),
            TStringBuilder() << "Client " << diskIds[0].second
                << " should not exist after UnmountVolume");
        clientIt = env.ClientToVolumeMap.find(diskIds[2].second);
        UNIT_ASSERT_C(
            clientIt != env.ClientToVolumeMap.end(),
            TStringBuilder() << "Client " << diskIds[2].second
                << " should exist");
        UNIT_ASSERT_C(
            clientIt->second.size() == 1,
            TStringBuilder() << "Only volume " << diskIds[2].first
                << " for client " << diskIds[2].second << " should exist");
        volumeIt = clientIt->second.find(diskIds[2].first);
        UNIT_ASSERT_C(
            volumeIt != clientIt->second.end(),
            TStringBuilder() << "Volume " << diskIds[2].first
                << " for client " << diskIds[2].second << " should exist");
    }

    Y_UNIT_TEST(ShouldCorrectlyTrimMetricsWithScheduler)
    {
        TTestEnvironment env;
        auto notifier = std::make_shared<TAutoEvent>();

        env.Scheduler->Schedule(
            env.Timer->Now(),
            [notifier] () {
                notifier->Signal();
            });

        UNIT_ASSERT_C(
            notifier->Wait(),
            "Locked on scheduler");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.TrimCount);

        env.Scheduler->Schedule(
            env.Timer->Now() +
                TRIM_THROTTLER_METRICS_INTERVAL +
                SHORT_TIMEOUT,
            [notifier] () {
                notifier->Signal();
            });

        env.Timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL + SHORT_TIMEOUT);

        UNIT_ASSERT_C(
            notifier->Wait(),
            "Locked on scheduler");
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.TrimCount);

        env.Scheduler->Schedule(
            env.Timer->Now() +
                TRIM_THROTTLER_METRICS_INTERVAL +
                SHORT_TIMEOUT,
            [notifier] () {
                notifier->Signal();
            });

        env.Timer->AdvanceTime(TRIM_THROTTLER_METRICS_INTERVAL + SHORT_TIMEOUT);

        UNIT_ASSERT_C(
            notifier->Wait(),
            "Locked on scheduler");
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            env.TrimCount);
    }
}

}   // namespace NCloud::NBlockStore
