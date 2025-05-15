#include "volume_stats.h"

#include "config.h"
#include "stats_helpers.h"
#include "user_counter.h"
#include "volume_perf.h"

#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/busy_idle_calculator.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/postpone_time_predictor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/rwlock.h>

#include <unordered_map>

namespace NCloud::NBlockStore {

using namespace NMonitoring;
using namespace NCloud::NStorage::NUserStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPostponeTimePredictorStats final
{
    TDynamicCounters::TCounterPtr MaxPredictedPostponeTimeCounter;

    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxPredictedPostponeTimeCalc;

public:
    TPostponeTimePredictorStats(
            TDynamicCountersPtr volumeGroup,
            ITimerPtr timer)
        : MaxPredictedPostponeTimeCounter(
            volumeGroup->GetCounter("MaxPredictedPostponeTime"))
        , MaxPredictedPostponeTimeCalc(std::move(timer))
    {}

    void OnRequestStarted(ui64 predictedPostponeTime)
    {
        MaxPredictedPostponeTimeCalc.Add(predictedPostponeTime);
    }

    void OnUpdateStats()
    {
        *MaxPredictedPostponeTimeCounter =
            MaxPredictedPostponeTimeCalc.NextValue();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDowntimeCalculator
{
private:
    using TMaxTimeCalculator = TMaxCalculator<DEFAULT_BUCKET_COUNT>;

    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const NProto::EStorageMediaKind MediaKind;

    TMaxTimeCalculator Read;
    TMaxTimeCalculator Write;
    TMaxTimeCalculator Zero;

public:
    TDowntimeCalculator(
            TDiagnosticsConfigPtr diagnosticsConfig,
            const NProto::TVolume& volume,
            ITimerPtr timer)
        : DiagnosticsConfig(std::move(diagnosticsConfig))
        , MediaKind(volume.GetStorageMediaKind())
        , Read(timer)
        , Write(timer)
        , Zero(timer)
    {}

    void AddIncompleteStats(
        EBlockStoreRequest requestType,
        TDuration requestTime)
    {
        if (!IsReadWriteRequest(requestType)) {
            return;
        }

        auto& calc = GetCalculator(requestType);

        calc.Add(requestTime.MicroSeconds());
    }

    void RequestCompleted(
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime)
    {
        if (!IsReadWriteRequest(requestType)) {
            return;
        }

        auto& calc = GetCalculator(requestType);

        auto requestTime =
            CyclesToDurationSafe(GetCycleCount() - requestStarted);

        calc.Add((requestTime - postponedTime).MicroSeconds());
    }

    bool OnUpdateStats()
    {
        auto readTime = Read.NextValue();
        auto writeTime = Write.NextValue();
        auto zeroTime = Zero.NextValue();

        auto maxTime = Max(readTime, Max(writeTime, zeroTime));

        return GetDowntimeThreshold(*DiagnosticsConfig, MediaKind) <=
               TDuration::MicroSeconds(maxTime);
    }

private:
    TMaxTimeCalculator& GetCalculator(EBlockStoreRequest requestType)
    {
        switch (requestType) {
            case EBlockStoreRequest::ReadBlocks: {
                return Read;
            }
            case EBlockStoreRequest::WriteBlocks: {
                return Write;
            }
            case EBlockStoreRequest::ZeroBlocks: {
                return Zero;
            }
            default: {
                Y_DEBUG_ABORT_UNLESS(0, "Unexpected requestType %d", requestType);
                return Read;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeInfoBase
{
    const ITimerPtr Timer;
    const NProto::TVolume Volume;
    TBusyIdleTimeCalculatorDynamicCounters BusyIdleCalc;
    TVolumePerformanceCalculator PerfCalc;
    IPostponeTimePredictorPtr PostponeTimePredictor;
    TPostponeTimePredictorStats PostponeTimePredictorStats;
    TDowntimeCalculator DowntimeCalculator;
    TDowntimeHistoryHolder DowntimeHistory;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> ThrottlerRejects;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> CheckpointRejects;
    TDynamicCounters::TCounterPtr HasStorageConfigPatchCounter;

    TVolumeInfoBase(
            NProto::TVolume volume,
            TDiagnosticsConfigPtr diagnosticsConfig,
            IPostponeTimePredictorPtr postponeTimePredictor,
            TDynamicCountersPtr volumeGroup,
            ITimerPtr timer)
        : Timer(timer)
        , Volume(std::move(volume))
        , PerfCalc(Volume, diagnosticsConfig)
        , PostponeTimePredictor(std::move(postponeTimePredictor))
        , PostponeTimePredictorStats(volumeGroup, timer)
        , DowntimeCalculator(diagnosticsConfig, Volume, timer)
        , ThrottlerRejects(timer)
        , CheckpointRejects(timer)
        , HasStorageConfigPatchCounter(
            volumeGroup->GetCounter("HasStorageConfigPatch"))
    {
        BusyIdleCalc.Register(volumeGroup);
        PerfCalc.Register(*volumeGroup, Volume);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRealInstanceId
{
private:
    const TString ClientId;
    const TString InstanceId;
    const TString RealInstanceId;

public:
    TRealInstanceId(TString clientId, TString instanceId)
        : ClientId(std::move(clientId))
        , InstanceId(std::move(instanceId))
        // in case of multi mount for empty instance, centers override itself
        // to avoid it use client ID for subgroup
        , RealInstanceId(InstanceId.empty()
            ? ClientId
            : InstanceId)
    {}

    const TString& GetClientId() const
    {
        return ClientId;
    }

    const TString& GetInstanceId() const
    {
        return InstanceId;
    }

    const TString& GetRealInstanceId() const
    {
        return RealInstanceId;
    }
};

struct TRealInstanceKeyHash
{
    std::size_t operator()(const TRealInstanceId& instance) const
    {
        return std::hash<TString>{}(instance.GetRealInstanceId());
    }
};

struct TRealInstanceKeyEqual
{
    bool operator()(const TRealInstanceId& lhs, const TRealInstanceId& rhs) const
    {
       return lhs.GetRealInstanceId() == rhs.GetRealInstanceId();
    }
};

class TVolumeInfo final
    : public IVolumeInfo
{
    friend class TVolumeStats;

private:
    const std::shared_ptr<TVolumeInfoBase> VolumeBase;
    const TRealInstanceId RealInstanceId;

    TRequestCounters RequestCounters;
    TDynamicCounters::TCounterPtr HasDowntimeCounter;

    TDuration InactivityTimeout;
    TInstant LastRemountTime;

    static TRequestCounters::EOptions GetRequestCountersOptions(
        const TVolumeInfoBase& volumeBase)
    {
        TRequestCounters::EOptions options =
            TRequestCounters::EOption::OnlyReadWriteRequests;

        auto mediaKind = volumeBase.Volume.GetStorageMediaKind();
        if (IsDiskRegistryMediaKind(mediaKind)) {
            options |= TRequestCounters::EOption::AddSpecialCounters;
        }

        return options;
    }

public:
    TVolumeInfo(
            std::shared_ptr<TVolumeInfoBase> volumeBase,
            ITimerPtr timer,
            TRealInstanceId realInstanceId,
            EHistogramCounterOptions histogramCounterOptions)
        : VolumeBase(std::move(volumeBase))
        , RealInstanceId(std::move(realInstanceId))
        , RequestCounters(MakeRequestCounters(
            std::move(timer),
            GetRequestCountersOptions(*VolumeBase),
            histogramCounterOptions))
    {}

    const NProto::TVolume& GetInfo() const override
    {
        return VolumeBase->Volume;
    }

    TDuration GetPossiblePostponeDuration() const override
    {
        return VolumeBase->PostponeTimePredictor->GetPossiblePostponeDuration();
    }

    ui64 RequestStarted(
        EBlockStoreRequest requestType,
        ui32 requestBytes) override
    {
        VolumeBase->PostponeTimePredictorStats.OnRequestStarted(
            GetPossiblePostponeDuration().MilliSeconds());
        VolumeBase->BusyIdleCalc.OnRequestStarted();
        return RequestCounters.RequestStarted(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            requestBytes);
    }

    TDuration RequestCompleted(
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        ui32 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ui64 responseSent) override
    {
        VolumeBase->BusyIdleCalc.OnRequestCompleted();
        VolumeBase->PerfCalc.OnRequestCompleted(
            TranslateLocalRequestType(requestType),
            requestStarted,
            GetCycleCount(),
            DurationToCyclesSafe(postponedTime),
            requestBytes);
        VolumeBase->PostponeTimePredictor->Register(postponedTime);
        VolumeBase->DowntimeCalculator.RequestCompleted(
            TranslateLocalRequestType(requestType),
            requestStarted,
            postponedTime);

        if (errorKind == EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint) {
            VolumeBase->CheckpointRejects.Add(1);
        } else if (errorKind == EDiagnosticsErrorKind::ErrorThrottling) {
            VolumeBase->ThrottlerRejects.Add(1);
        }

        return RequestCounters.RequestCompleted(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            requestStarted,
            postponedTime,
            requestBytes,
            errorKind,
            errorFlags,
            unaligned,
            ECalcMaxTime::ENABLE,
            responseSent);
    }

    void AddIncompleteStats(
        EBlockStoreRequest requestType,
        TRequestTime requestTime) override
    {
        RequestCounters.AddIncompleteStats(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            requestTime.ExecutionTime,
            requestTime.TotalTime,
            ECalcMaxTime::ENABLE);
        VolumeBase->DowntimeCalculator.AddIncompleteStats(
            TranslateLocalRequestType(requestType),
            requestTime.ExecutionTime);
    }

    void AddRetryStats(
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) override
    {
        if (errorKind == EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint) {
            VolumeBase->CheckpointRejects.Add(1);
        } else if (errorKind == EDiagnosticsErrorKind::ErrorThrottling) {
            VolumeBase->ThrottlerRejects.Add(1);
        }

        RequestCounters.AddRetryStats(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            errorKind,
            errorFlags);
    }

    void RequestPostponed(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestPostponed(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestPostponedServer(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestPostponedServer(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestAdvanced(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestAdvanced(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestAdvancedServer(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestFastPathHit(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestFastPathHit(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void BatchCompleted(
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        return RequestCounters.BatchCompleted(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            count,
            bytes,
            errors,
            timeHist,
            sizeHist);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeStats final
    : public IVolumeStats
{
    using TVolumeBasePtr = std::shared_ptr<TVolumeInfoBase>;
    using TVolumeInfoPtr = std::shared_ptr<TVolumeInfo>;
    using TVolumeMap = std::unordered_map<
        TRealInstanceId,
        TVolumeInfoPtr,
        TRealInstanceKeyHash,
        TRealInstanceKeyEqual>;

    struct TVolumeInfoHolder
    {
        TVolumeBasePtr VolumeBase;
        TVolumeMap VolumeInfos;
    };

    using TVolumeHolderMap = std::unordered_map<TString, TVolumeInfoHolder>;

private:
    const IMonitoringServicePtr Monitoring;
    const TDuration InactiveClientsTimeout;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const EVolumeStatsType Type;
    const ITimerPtr Timer;
    const THashSet<TString> CloudIdsWithStrictSLA;

    TDynamicCountersPtr Counters;
    std::shared_ptr<NUserCounter::IUserCounterSupplier> UserCounters;
    std::unique_ptr<TSufferCounters> SufferCounters;
    std::unique_ptr<TSufferCounters> SmoothSufferCounters;
    std::unique_ptr<TSufferCounters> StrictSLASufferCounters;
    std::unique_ptr<TSufferCounters> CriticalSufferCounters;

    std::unordered_map<TString, TRealInstanceId> ClientToRealInstance;
    TVolumeHolderMap Volumes;
    TRWMutex Lock;

    using TDownDisksCounters = std::array<
        TDynamicCounters::TCounterPtr,
        NProto::EStorageMediaKind_ARRAYSIZE
    >;
    TDownDisksCounters DownDisksCounters;
    TDynamicCounters::TCounterPtr TotalDownDisksCounter;

public:
    TVolumeStats(
            IMonitoringServicePtr monitoring,
            TDuration inactiveClientsTimeout,
            TDiagnosticsConfigPtr diagnosticsConfig,
            EVolumeStatsType type,
            ITimerPtr timer)
        : Monitoring(std::move(monitoring))
        , InactiveClientsTimeout(inactiveClientsTimeout)
        , DiagnosticsConfig(std::move(diagnosticsConfig))
        , Type(type)
        , Timer(std::move(timer))
        , CloudIdsWithStrictSLA([] (const TVector<TString>& v) {
            return THashSet<TString>(v.begin(), v.end());
        }(DiagnosticsConfig->GetCloudIdsWithStrictSLA()))
        , UserCounters(CreateUserCounterSupplier())
    {
    }

    bool MountVolumeImpl(
        const NProto::TVolume& volume,
        const TRealInstanceId& realInstanceId)
    {
        bool inserted = false;

        auto volumeIt = Volumes.find(volume.GetDiskId());
        if (volumeIt == Volumes.end()) {
            volumeIt = Volumes.emplace(
                volume.GetDiskId(),
                RegisterVolume(volume)).first;
        }

        TVolumeMap& infos = volumeIt->second.VolumeInfos;

        auto instanceIt = infos.find(realInstanceId);
        if (instanceIt == infos.end()) {
            instanceIt = infos.emplace(
                realInstanceId,
                RegisterInstance(
                    volumeIt->second.VolumeBase,
                    realInstanceId)).first;
            inserted = true;
        }

        instanceIt->second->LastRemountTime = Timer->Now();
        instanceIt->second->InactivityTimeout = InactiveClientsTimeout;

        if (!inserted) {
            AlterVolumeImpl(
                volume.GetDiskId(),
                volume.GetCloudId(),
                volume.GetFolderId());
        }

        return inserted;
    }

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override
    {
        TWriteGuard guard(Lock);

        auto [itr, result] = ClientToRealInstance.try_emplace(
            clientId,
            clientId,
            instanceId);

        return MountVolumeImpl(volume, itr->second);
    }

    void UnmountVolume(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(clientId);
        Y_UNUSED(diskId);
    }

    void AlterVolumeImpl(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId)
    {
        const auto volumeIt = Volumes.find(diskId);
        if (volumeIt == Volumes.end()) {
            return;
        }

        NProto::TVolume volumeConfig = volumeIt->second.VolumeBase->Volume;
        if (volumeConfig.GetCloudId() == cloudId &&
            volumeConfig.GetFolderId() == folderId)
        {
            return;
        }

        volumeConfig.SetCloudId(cloudId);
        volumeConfig.SetFolderId(folderId);

        TVolumeInfoHolder holder = std::move(volumeIt->second);
        Volumes.erase(volumeIt);

        for (const auto& item: holder.VolumeInfos) {
            const TVolumeInfo& info = *item.second;
            UnregisterInstance(info.VolumeBase, info.RealInstanceId);
        }
        UnregisterVolume(holder.VolumeBase);

        for (const auto& item: holder.VolumeInfos) {
            const TVolumeInfo& info = *item.second;
            MountVolumeImpl(volumeConfig, info.RealInstanceId);
        }
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override
    {
        TWriteGuard guard(Lock);
        return AlterVolumeImpl(diskId, cloudId, folderId);
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const override
    {
        TReadGuard guard(Lock);

        const auto volumeIt = Volumes.find(diskId);
        if (volumeIt == Volumes.end()) {
            return nullptr;
        }

        const TVolumeMap& infos = volumeIt->second.VolumeInfos;
        const auto realInstanceIt = ClientToRealInstance.find(clientId);
        if (realInstanceIt == ClientToRealInstance.end()) {
            return nullptr;
        }
        const auto infoIt = infos.find(realInstanceIt->second);
        if (infoIt == infos.end()) {
            return nullptr;
        }
        return infoIt->second;
    }

    NProto::EStorageMediaKind GetStorageMediaKind(
        const TString& diskId) const override
    {
        TReadGuard guard(Lock);

        const auto volumeIt = Volumes.find(diskId);
        return volumeIt != Volumes.end()
                   ? volumeIt->second.VolumeBase->Volume.GetStorageMediaKind()
                   : NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    }

    ui32 GetBlockSize(const TString& diskId) const override
    {
        TWriteGuard guard(Lock);

        const auto volumeIt = Volumes.find(diskId);
        return volumeIt != Volumes.end()
            ? volumeIt->second.VolumeBase->Volume.GetBlockSize()
            : DefaultBlockSize;
    }

    bool TrimInstance(TInstant now, TVolumeMap& infos)
    {
        std::erase_if(infos, [this, now] (const auto& item){
            const TVolumeInfo& info = *item.second;
            if (info.InactivityTimeout &&
                now - info.LastRemountTime > info.InactivityTimeout)
            {
                UnregisterInstance(
                    info.VolumeBase,
                    info.RealInstanceId);
                std::erase_if(ClientToRealInstance, [&info](const auto& client){
                    return TRealInstanceKeyEqual().operator()(
                        client.second,
                        info.RealInstanceId);
                });
                return true;
            }
            return false;
        });
        return infos.empty();
    }

    void TrimVolumes() override
    {
        TWriteGuard guard(Lock);

        const auto now = Timer->Now();

        std::erase_if(Volumes, [this, now] (auto& item) {
            TVolumeInfoHolder& holder = item.second;
            if (TrimInstance(now, holder.VolumeInfos)) {
                UnregisterVolume(holder.VolumeBase);
                return true;
            }
            return false;
        });
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        TReadGuard guard(Lock);

        ui32 totalDownDisks = 0;
        std::array<ui32, NProto::EStorageMediaKind_ARRAYSIZE> downDisksCounters{};

        for (auto& item: Volumes) {
            TVolumeInfoHolder& holder = item.second;
            TVolumeInfoBase& volumeBase = *holder.VolumeBase;

            volumeBase.PostponeTimePredictorStats.OnUpdateStats();
            volumeBase.BusyIdleCalc.OnUpdateStats();
            volumeBase.PerfCalc.UpdateStats();

            const auto hasDowntime = volumeBase.DowntimeCalculator.OnUpdateStats();
            if (hasDowntime) {
                ++totalDownDisks;
                ++downDisksCounters[volumeBase.Volume.GetStorageMediaKind()];
            }

            if (updateIntervalFinished) {
                volumeBase.DowntimeHistory.PushBack(
                    Timer->Now(),
                    hasDowntime
                    ? EDowntimeStateChange::DOWN
                    : EDowntimeStateChange::UP);
            }

            for (auto& [key, instance]: holder.VolumeInfos) {
                instance->RequestCounters.UpdateStats(updateIntervalFinished);
                if (updateIntervalFinished) {
                    Y_DEBUG_ABORT_UNLESS(instance->HasDowntimeCounter);
                    if (instance->HasDowntimeCounter) {
                        *instance->HasDowntimeCounter = hasDowntime;
                    }
                }
            }
            if (SufferCounters &&
                volumeBase.PerfCalc.IsSuffering())
            {
                SufferCounters->OnDiskSuffer(
                    volumeBase.Volume.GetStorageMediaKind());
            }
            if (SmoothSufferCounters &&
                volumeBase.PerfCalc.IsSufferingSmooth())
            {
                SmoothSufferCounters->OnDiskSuffer(
                    volumeBase.Volume.GetStorageMediaKind());

                const auto& cloudId = volumeBase.Volume.GetCloudId();
                if (StrictSLASufferCounters
                        && CloudIdsWithStrictSLA.contains(cloudId))
                {
                    StrictSLASufferCounters->OnDiskSuffer(
                        volumeBase.Volume.GetStorageMediaKind());
                }
            }
            if (CriticalSufferCounters &&
                volumeBase.PerfCalc.IsSufferingCritically())
            {
                CriticalSufferCounters->OnDiskSuffer(
                    volumeBase.Volume.GetStorageMediaKind());
            }
        }

        if (SufferCounters) {
            SufferCounters->PublishCounters();
        }
        if (SmoothSufferCounters) {
            SmoothSufferCounters->PublishCounters();
        }
        if (StrictSLASufferCounters) {
            StrictSLASufferCounters->PublishCounters();
        }
        if (CriticalSufferCounters) {
            CriticalSufferCounters->PublishCounters();
        }

        if (updateIntervalFinished) {
            if (TotalDownDisksCounter) {
                *TotalDownDisksCounter = totalDownDisks;
            }

            ui32 mk = NProto::EStorageMediaKind_MIN;
            while (mk < NProto::EStorageMediaKind_ARRAYSIZE) {
                if (DownDisksCounters[mk]) {
                    *DownDisksCounters[mk] = downDisksCounters[mk];
                }
                ++mk;
            }
        }
    }

    TVolumePerfStatuses GatherVolumePerfStatuses() override
    {
        TReadGuard guard(Lock);
        TVolumePerfStatuses ans(Reserve(Volumes.size()));

        for (const auto& item: Volumes) {
            const TVolumeInfoBase& volumeBase = *item.second.VolumeBase;
            ans.emplace_back(
                volumeBase.Volume.GetDiskId(),
                volumeBase.PerfCalc.GetSufferCount());
        }
        return ans;
    }

    NCloud::NStorage::IUserMetricsSupplierPtr GetUserCounters() const override
    {
        return UserCounters;
    }

    TDowntimeHistory GetDowntimeHistory(const TString& diskId) const override
    {
        TReadGuard guard(Lock);

        const auto volumeIt = Volumes.find(diskId);
        if (volumeIt == Volumes.end()) {
            return {};
        }

        return volumeIt->second.VolumeBase->DowntimeHistory.RecentEvents(
            Timer->Now());
    }

    bool HasStorageConfigPatch(const TString& diskId) const override
    {
        TReadGuard guard(Lock);

        const auto volumeIt = Volumes.find(diskId);
        if (volumeIt == Volumes.end()) {
            return {};
        }

        return volumeIt->second.VolumeBase->HasStorageConfigPatchCounter->Val();
    }

private:
    const TString& SelectInstanceId(
        const TString& clientId,
        const TString& instanceId)
    {
        // in case of multi mount for empty instance, centers override itself
        // to avoid it use client ID for subgroup
        return instanceId.empty()
            ? clientId
            : instanceId;
    }

    TVolumeInfoHolder RegisterVolume(NProto::TVolume volume)
    {
        if (!Counters) {
            InitCounters();
        }

        auto volumeGroup = Counters->GetSubgroup("volume", volume.GetDiskId());

        auto volumeBase = std::make_shared<TVolumeInfoBase>(
            std::move(volume),
            DiagnosticsConfig,
            CreatePostponeTimePredictor(
                Timer,
                DiagnosticsConfig->GetPostponeTimePredictorInterval(),
                DiagnosticsConfig->GetPostponeTimePredictorPercentage(),
                DiagnosticsConfig->GetPostponeTimePredictorMaxTime()),
            volumeGroup,
            Timer);

        return TVolumeInfoHolder{
            .VolumeBase = std::move(volumeBase),
            .VolumeInfos = {}};
    }

    TVolumeInfoPtr RegisterInstance(
        TVolumeBasePtr volumeBase,
        const TRealInstanceId& realInstanceId)
    {
        auto info = std::make_shared<TVolumeInfo>(
            volumeBase,
            Timer,
            realInstanceId,
            DiagnosticsConfig->GetHistogramCounterOptions());

        if (!Counters) {
            InitCounters();
        }

        const NProto::TVolume& volumeConfig = volumeBase->Volume;

        auto volumeGroup = Counters->GetSubgroup(
            "volume",
            volumeConfig.GetDiskId());
        auto countersGroup =
            volumeGroup
                ->GetSubgroup("instance", realInstanceId.GetRealInstanceId())
                ->GetSubgroup("cloud", volumeConfig.GetCloudId())
                ->GetSubgroup("folder", volumeConfig.GetFolderId());
        info->RequestCounters.Register(*countersGroup);
        info->HasDowntimeCounter = countersGroup->GetCounter("HasDowntime");

        NUserCounter::RegisterServerVolumeInstance(
            *UserCounters,
            volumeConfig.GetCloudId(),
            volumeConfig.GetFolderId(),
            volumeConfig.GetDiskId(),
            realInstanceId.GetInstanceId(),
            countersGroup);

        return info;
    }

    void UnregisterInstance(
        TVolumeBasePtr volumeBase,
        const TRealInstanceId& realInstanceId)
    {
        if (!Counters) {
            InitCounters();
        }

        Counters->GetSubgroup("volume", volumeBase->Volume.GetDiskId())->
            RemoveSubgroup("instance", realInstanceId.GetRealInstanceId());

        NUserCounter::UnregisterServerVolumeInstance(
            *UserCounters,
            volumeBase->Volume.GetCloudId(),
            volumeBase->Volume.GetFolderId(),
            volumeBase->Volume.GetDiskId(),
            realInstanceId.GetInstanceId());
    }

    void UnregisterVolume(TVolumeBasePtr volumeBase)
    {
        if (!Counters) {
            InitCounters();
        }

        Counters->RemoveSubgroup("volume", volumeBase->Volume.GetDiskId());
    }

    void InitCounters()
    {
        Counters =
            Monitoring->GetCounters()->GetSubgroup("counters", "blockstore");

        switch (Type) {
            case EVolumeStatsType::EServerStats: {
                SufferCounters = std::make_unique<TSufferCounters>(
                    "DisksSuffer",
                    Counters->GetSubgroup("component", "server"));

                SmoothSufferCounters = std::make_unique<TSufferCounters>(
                    "SmoothDisksSuffer",
                    Counters->GetSubgroup("component", "server"));

                StrictSLASufferCounters = std::make_unique<TSufferCounters>(
                    "StrictSLADisksSuffer",
                    Counters->GetSubgroup("component", "server"));

                CriticalSufferCounters = std::make_unique<TSufferCounters>(
                    "CriticalDisksSuffer",
                    Counters->GetSubgroup("component", "server"));

                TotalDownDisksCounter =
                    Counters->GetSubgroup("component", "server")
                        ->GetCounter("DownDisks");

                ui32 mk = NProto::EStorageMediaKind_MIN;
                while (mk < NProto::EStorageMediaKind_ARRAYSIZE) {
                    DownDisksCounters[mk] =
                        Counters->GetSubgroup("component", "server")
                            ->GetSubgroup("type", MediaKindToStatsString(
                                static_cast<NProto::EStorageMediaKind>(mk)))
                            ->GetCounter("DownDisks");
                    ++mk;
                }

                Counters = Counters->GetSubgroup("component", "server_volume");
                break;
            }
            case EVolumeStatsType::EClientStats: {
                Counters = Counters->GetSubgroup("component", "client_volume");
                break;
            }
        }

        Counters = Counters->GetSubgroup("host", "cluster");
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeStatsStub final
    : public IVolumeStats
{
    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);

        return true;
    }

    void UnmountVolume(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(clientId);
        Y_UNUSED(diskId);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);

        return nullptr;
    }

    NProto::EStorageMediaKind GetStorageMediaKind(
        const TString& diskId) const override
    {
        Y_UNUSED(diskId);

        return NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    }

    ui32 GetBlockSize(const TString& diskId) const override
    {
        Y_UNUSED(diskId);

        return DefaultBlockSize;
    }

    void TrimVolumes() override
    {
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }

    TVolumePerfStatuses GatherVolumePerfStatuses() override
    {
        return {};
    }

    TDowntimeHistory GetDowntimeHistory(const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return {};
    }

    bool HasStorageConfigPatch(const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return {};
    }

};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IVolumeStatsPtr CreateVolumeStats(
    IMonitoringServicePtr monitoring,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TDuration inactiveClientsTimeout,
    EVolumeStatsType type,
    ITimerPtr timer)
{
    Y_DEBUG_ABORT_UNLESS(diagnosticsConfig);
    return std::make_shared<TVolumeStats>(
        std::move(monitoring),
        inactiveClientsTimeout,
        std::move(diagnosticsConfig),
        type,
        std::move(timer));
}

IVolumeStatsPtr CreateVolumeStats(
    IMonitoringServicePtr monitoring,
    TDuration inactiveClientsTimeout,
    EVolumeStatsType type,
    ITimerPtr timer)
{
    NProto::TDiagnosticsConfig diagnosticsConfig;
    return std::make_shared<TVolumeStats>(
        std::move(monitoring),
        inactiveClientsTimeout,
        std::make_shared<TDiagnosticsConfig>(diagnosticsConfig),
        type,
        std::move(timer));
}


IVolumeStatsPtr CreateVolumeStatsStub()
{
    return std::make_shared<TVolumeStatsStub>();
}

}   // namespace NCloud::NBlockStore
