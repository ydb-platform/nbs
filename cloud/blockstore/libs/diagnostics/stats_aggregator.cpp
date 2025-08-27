#include "stats_aggregator.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THistogramPercentiles
{
    TVector<ui64> Prev;

    void Update(const TVector<TBucketInfo>& update, TDynamicCounterPtr counters)
    {
        if (!counters) {
            return;
        }

        if (Prev.size() < update.size()) {
            Prev.resize(update.size());
        }

        TVector<TBucketInfo> delta(Reserve(update.size()));
        for (ui32 i = 0; i < update.size(); ++i) {
            delta.emplace_back(
                update[i].first,
                update[i].second - Prev[i]);
            Prev[i] = update[i].second;
        }

        auto percentiles = GetDefaultPercentiles();
        auto result = CalculateWeightedPercentiles(
            delta,
            percentiles);

        for (ui32 i = 0; i < result.size(); ++i) {
            auto c = counters->GetCounter(percentiles[i].second, false);
            *c = std::lround(result[i]);
        }
    }

    void Update(
        TLog& Log,
        TDynamicCounterPtr from,
        TDynamicCounterPtr to,
        const TString& name)
    {
        if (!from || !to) {
            return;
        }
        auto buckets = ReadSolomonHistogram(Log, from);
        if (buckets) {
            Update(buckets, to->GetSubgroup("percentiles", name));
        }
    }

    TVector<TBucketInfo> ReadSolomonHistogram(TLog& Log, TDynamicCounterPtr counters)
    {
        if (!counters) {
            return {};
        }
        auto bucketCounters = counters->ReadSnapshot();
        if (bucketCounters.empty()) {
            return {};
        }

        TVector<TBucketInfo> buckets;
        for (const auto& [counterName, bucket] : bucketCounters) {
            auto counter = counters->FindCounter(counterName.LabelValue);
            if (!counter) {
                STORAGE_WARN("cannot create counter for " << counterName.LabelValue);
                return {};
            }
            const double limit = counterName.LabelValue == "Inf"
                ? Max()
                : StrToD(counterName.LabelValue.c_str(), nullptr);
            buckets.emplace_back(limit, *counter);
        }
        Sort(buckets);
        return buckets;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestPercentiles
{
    THistogramPercentiles Time;
    THistogramPercentiles Size;

    void BuildPercentiles(TLog& Log, TDynamicCounterPtr counters)
    {
        if (!counters) {
            return;
        }

        auto timeCounters = counters->FindSubgroup("histogram", "Time");
        if (timeCounters) {
            if (auto counters = timeCounters->FindSubgroup("units", "usec")) {
                timeCounters = counters;
            }
            Time.Update(Log, timeCounters, counters, "Time");
        }

        auto sizeCounters = counters->FindSubgroup("histogram", "Size");
        if (sizeCounters) {
            if (auto counters = sizeCounters->FindSubgroup("units", "KB")) {
                sizeCounters = counters;
            }
            Size.Update(Log, sizeCounters, counters, "Size");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TClientRequestPercentiles
{
    TRequestPercentiles Total;

    void BuildPercentiles(TLog& Log, TDynamicCounterPtr counters)
    {
        if (!counters) {
            return;
        }
        Total.BuildPercentiles(Log, counters);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TClientRequests
{
    TClientRequestPercentiles Read;
    TClientRequestPercentiles Write;
    TClientRequestPercentiles Zero;

    void BuildPercentiles(TLog& Log, TDynamicCounterPtr counters)
    {
        if (!counters) {
            return;
        }
        Read.BuildPercentiles(Log, counters->FindSubgroup("request", "ReadBlocks"));
        Write.BuildPercentiles(Log, counters->FindSubgroup("request", "WriteBlocks"));
        Zero.BuildPercentiles(Log, counters->FindSubgroup("request", "ZeroBlocks"));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TClientPercentileCalculator
    : public IClientPercentileCalculator
{
    const ILoggingServicePtr Logging;

    TClientRequests Total;
    TClientRequests Ssd;
    TClientRequests Hdd;
    TClientRequests SsdNonrepl;
    TClientRequests SsdMirror2;
    TClientRequests SsdMirror3;
    TClientRequests HddNonrepl;

    TLog Log;

    explicit TClientPercentileCalculator(ILoggingServicePtr logging)
        : Logging(std::move(logging))
    {
    }

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_TRACE");
    }

    void Stop() override
    {
    }

    void CalculatePercentiles(NMonitoring::TDynamicCountersPtr updatedCounters) override
    {
        auto updatedRootGroup =
            updatedCounters->FindSubgroup("counters", "blockstore");

        if (updatedRootGroup) {
            auto clientGroup = updatedRootGroup->FindSubgroup("component", "client");
            if (clientGroup) {
                Total.BuildPercentiles(Log, clientGroup);
                Ssd.BuildPercentiles(
                    Log,
                    clientGroup->FindSubgroup("type", "ssd")
                );
                Hdd.BuildPercentiles(
                    Log,
                    clientGroup->FindSubgroup("type", "hdd")
                );
                SsdNonrepl.BuildPercentiles(
                    Log,
                    clientGroup->FindSubgroup("type", "ssd_nonrepl")
                );
                SsdMirror2.BuildPercentiles(
                    Log,
                    clientGroup->FindSubgroup("type", "ssd_mirror2")
                );
                SsdMirror3.BuildPercentiles(
                    Log,
                    clientGroup->FindSubgroup("type", "ssd_mirror3")
                );
                HddNonrepl.BuildPercentiles(
                    Log,
                    clientGroup->FindSubgroup("type", "hdd_nonrepl")
                );
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStatsParser final
    : public IMetricConsumer
{
    TDynamicCounterPtr Counters;
    TVector<std::pair<TString, TString>> SubgroupLabels;
    TDynamicCounters::TCounterPtr CurrentCounter;
    TString CurrentCounterName;
    bool CounterIsDerivative = false;
    bool HostLabelSeen = false;

public:
    TStatsParser(TDynamicCounterPtr counters)
        : Counters(std::move(counters))
    {}

    void OnStreamBegin() override
    {}

    void OnStreamEnd() override
    {}

    void OnCommonTime(TInstant time) override
    {
        Y_UNUSED(time);
    }

    void OnMetricBegin(EMetricType kind) override
    {
        // When dynamic counters are encoded into spack format, derivative
        // counters become EMetricType::RATE and non-derivative ones -
        // EMetricType::GAUGE
        CounterIsDerivative = (kind == EMetricType::RATE);
    }

    void OnMetricEnd() override
    {}

    void OnLabelsBegin() override
    {
        SubgroupLabels.clear();
        CurrentCounter = nullptr;
        CurrentCounterName = "";
        HostLabelSeen = false;
    }

    void OnLabelsEnd() override
    {
        if (!SubgroupLabels.empty()) {
            auto pair = std::move(SubgroupLabels.back());
            SubgroupLabels.pop_back();

            auto c = Counters;
            for (const auto& labels: SubgroupLabels) {
                c = c->GetSubgroup(labels.first, labels.second);

                if (labels.first == "component" &&
                    labels.second == "client_volume" &&
                    !HostLabelSeen)
                {
                    c = c->GetSubgroup("host", "cluster");
                }
            }

            if (pair.first == "version") {
                c = c->GetSubgroup(
                    "revision",
                    pair.second);
                CurrentCounter = c->GetCounter(
                    "version",
                    CounterIsDerivative);
            } else {
                CurrentCounter = c->GetNamedCounter(
                    pair.first,
                    pair.second,
                    CounterIsDerivative);
            }
            CurrentCounterName = pair.second;
        }
    }

    void OnLabel(const TStringBuf name, const TStringBuf value) override
    {
        if (name == "host") {
            HostLabelSeen = true;
        }
        SubgroupLabels.emplace_back(name, value);
    }

    void OnDouble(TInstant time, double value) override
    {
        Y_UNUSED(time);

        UpdateCounterValue(value);
    }

    void OnInt64(TInstant time, i64 value) override
    {
        Y_UNUSED(time);

        UpdateCounterValue(value);
    }

    void OnUint64(TInstant time, ui64 value) override
    {
        Y_UNUSED(time);

        UpdateCounterValue(value);
    }

    void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override
    {
        // Histogram is not used by request stats at the moment
        Y_UNUSED(time);
        Y_UNUSED(snapshot);
    }

    void OnLogHistogram(TInstant, TLogHistogramSnapshotPtr) override {
    }

    void OnSummaryDouble(TInstant, ISummaryDoubleSnapshotPtr) override {
    }

private:
    template <typename T>
    void UpdateCounterValue(T value)
    {
        if (CurrentCounter) {
            if (CurrentCounterName.StartsWith("Max")) {
                *CurrentCounter = Max<T>(*CurrentCounter, value);
            } else {
                *CurrentCounter += value;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStatsAggregator final
    : public IStatsAggregator
    , public std::enable_shared_from_this<TStatsAggregator>
{
    struct TStatsInfo
    {
        TString EncodedStats;
        TInstant LastActivityTime;
    };

    using TStatsMap = THashMap<TString, TStatsInfo>;

private:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const TCommitCallback CommitCallback;

    TLog Log;

    TAdaptiveLock Lock;
    TStatsMap Stats;

    TAtomic ShouldStop = 0;

public:
    TStatsAggregator(
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            TCommitCallback commitCallback)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , CommitCallback(std::move(commitCallback))
    {}

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_TRACE");

        ScheduleUpdateStats();
    }

    void Stop() override
    {
        AtomicSet(ShouldStop, 1);
    }

    void AddStats(const TString& id, const TString& stats, TInstant now) override
    {
        if (AtomicGet(ShouldStop)) {
            // Don't add new client stats if stopping
            return;
        }

        with_lock(Lock) {
            Stats[id] = {stats, now};
        }
    }

private:
    void ScheduleUpdateStats()
    {
        if (AtomicGet(ShouldStop)) {
            return;
        }

        auto weak_ptr = weak_from_this();

        Scheduler->Schedule(
            Timer->Now() + UpdateCountersInterval,
            [weak_ptr = std::move(weak_ptr)] {
                if (auto p = weak_ptr.lock()) {
                    p->UpdateStats();
                    p->ScheduleUpdateStats();
                }
            });
    }

    void UpdateStats()
    {
        TStatsMap stats;
        with_lock(Lock) {
            stats = std::move(Stats);
        }

        TDynamicCounterPtr counters = new TDynamicCounters();

        auto now = TInstant::Now();
        for (auto it = stats.begin(); it != stats.end(); ) {
            const auto& statsInfo = it->second;
            if (now - statsInfo.LastActivityTime > 2*UpdateCountersInterval) {
                stats.erase(it++);
                continue;
            }

            TStatsParser parser(counters);
            TStringInput in(statsInfo.EncodedStats);

            try {
                DecodeSpackV1(&in, &parser);
            } catch(...) {
                STORAGE_WARN("Failed to decode stats from spack: "
                    << CurrentExceptionMessage());
                stats.erase(it++);
                continue;
            }

            ++it;
        }

        CommitCallback(std::move(counters), Monitoring->GetCounters());

        with_lock(Lock) {
            // Insert back unless newer value is already in place
            for (auto& pair: stats) {
                Stats.emplace(std::move(pair));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStatsAggregatorStub final
    : public IStatsAggregator
{
    void Start() override
    {
    }

    void Stop() override
    {
    }

    void AddStats(const TString& id, const TString& stats, TInstant now) override
    {
        Y_UNUSED(id);
        Y_UNUSED(stats);
        Y_UNUSED(now);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsAggregatorPtr CreateStatsAggregator(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IStatsAggregator::TCommitCallback commitCallback)
{
    return std::make_shared<TStatsAggregator>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(commitCallback));
}

IStatsAggregatorPtr CreateStatsAggregatorStub()
{
    return std::make_shared<TStatsAggregatorStub>();
}

IClientPercentileCalculatorPtr CreateClientPercentileCalculator(
    ILoggingServicePtr logging)
{
    return std::make_shared<TClientPercentileCalculator>(std::move(logging));
}

void UpdateClientStats(
    NMonitoring::TDynamicCountersPtr updatedCounters,
    NMonitoring::TDynamicCountersPtr baseCounters)
{
    auto baseRootGroup = baseCounters->GetSubgroup("counters", "blockstore");
    auto updatedRootGroup = updatedCounters->GetSubgroup("counters", "blockstore");

    auto clientGroup = updatedRootGroup->FindSubgroup("component", "client");
    if (clientGroup) {
        // Ensure the existence of subgroup - required for ReplaceSubgroup
        baseRootGroup->GetSubgroup("component", "client");
        baseRootGroup->ReplaceSubgroup("component", "client", clientGroup);
    } else {
        baseRootGroup->RemoveSubgroup("component", "client");
    }

    auto clientVolumeGroup = updatedRootGroup->FindSubgroup("component", "client_volume");
    if (clientVolumeGroup) {

        // Ensure the existence of subgroup - required for ReplaceSubgroup
        baseRootGroup->GetSubgroup("component", "client_volume");
        baseRootGroup->ReplaceSubgroup("component", "client_volume", clientVolumeGroup);
    } else {
        baseRootGroup->RemoveSubgroup("component", "client_volume");
    }
}

IMetricConsumerPtr CreateStatsParser(TDynamicCountersPtr counters)
{
    return std::make_shared<TStatsParser>(std::move(counters));
}

}   // namespace NCloud::NBlockStore
