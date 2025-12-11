#include "registry.h"

#include "aggregator.h"
#include "label.h"
#include "metric.h"
#include "visitor.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/synchronized/synchronized.h>

#include <util/generic/hash.h>
#include <util/generic/stack.h>
#include <util/system/rwlock.h>
#include <util/system/spinlock.h>

#include <utility>

namespace NCloud::NFileStore::NMetrics {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TDynamicCountersPtr = NMonitoring::TDynamicCountersPtr;
using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

struct TAggregatorStorage
{
    using TAggregatorStorageRef = std::reference_wrapper<TAggregatorStorage>;

    TLabels Labels;

    TStack<TDynamicCountersPtr> Subgroups;
    TCounterPtr Counter;

    NImpl::TAggregator Aggregator;
    typename TList<TAggregatorStorageRef>::iterator Item;

    TAggregatorStorage(const TAggregatorStorage&) = delete;
    TAggregatorStorage& operator=(const TAggregatorStorage&) = delete;

    TAggregatorStorage(TAggregatorStorage&&) noexcept = delete;
    TAggregatorStorage& operator=(TAggregatorStorage&&) = delete;

    TAggregatorStorage(
        TDynamicCountersPtr root,
        const TLabels& labels,
        EAggregationType aggrType,
        EMetricType metrType)
        : Aggregator(aggrType, metrType)
    {
        Y_ABORT_UNLESS(!labels.empty());

        // This check is needed to be sure, that we don't keep copy of TLabels
        // in the object inside of THashMap, because we have already saved
        // TLabels as a key.
        Y_ABORT_UNLESS(Labels.empty());

        auto currentSubgroup = std::move(root);
        Subgroups.push(currentSubgroup);

        for (ui32 i = 0; i < labels.size() - 1; ++i) {
            currentSubgroup = currentSubgroup->GetSubgroup(
                labels[i].GetName(),
                labels[i].GetValue());
            Subgroups.push(currentSubgroup);
        }

        Counter = currentSubgroup->GetNamedCounter(
            labels.back().GetName(),
            labels.back().GetValue(),
            metrType == EMetricType::MT_DERIVATIVE);
    }

    ~TAggregatorStorage()
    {
        // Before destruction we should store labels to Labels, because we need
        // them to unregister subgroups.
        Y_ABORT_UNLESS(!Labels.empty());

        auto currentSubgroup = std::move(Subgroups.top());
        Subgroups.pop();

        bool isLastEmpty = currentSubgroup->RemoveNamedCounter(
            Labels.back().GetName(),
            Labels.back().GetValue());

        while (isLastEmpty && !Subgroups.empty()) {
            const auto& currentLabel = Labels[Subgroups.size() - 1];
            currentSubgroup = std::move(Subgroups.top());
            Subgroups.pop();

            isLastEmpty = currentSubgroup->RemoveSubgroup(
                currentLabel.GetName(),
                currentLabel.GetValue());
        }
    }
};

// NOTE: TAggregatorStorage must not be copyable or movable, because we use
// references to objects of this type in TList (TAggregatorStorageRef). In this
// case we rely that address of objects in THashMap do not change after
// insertion, deletion and rehashing. Otherwise we will face dangling reference.
static_assert(!std::is_copy_constructible_v<TAggregatorStorage>);
static_assert(!std::is_copy_assignable_v<TAggregatorStorage>);
static_assert(!std::is_move_constructible_v<TAggregatorStorage>);
static_assert(!std::is_move_assignable_v<TAggregatorStorage>);

using TAggregatorStoragesList =
    TList<TAggregatorStorage::TAggregatorStorageRef>;

struct TMetricKeyStorage
{
    const TLabels Aggregator;
    const TMetricKey Metric;

    TMetricKeyStorage(TLabels aggregator, TMetricKey metric)
        : Aggregator(std::move(aggregator))
        , Metric(metric)
    {}
};

using TAggregatorsMap = THashMap<TLabels, TAggregatorStorage>;
using TMetricKeyStoragesMap = THashMap<TMetricKey, TMetricKeyStorage>;

////////////////////////////////////////////////////////////////////////////////

TLabels CreateLabels(TLabels labels, const TLabels& tail)
{
    labels.insert(labels.end(), tail.begin(), tail.end());
    return labels;
}

////////////////////////////////////////////////////////////////////////////////

class TMainMetricsRegistry
    : public IMainMetricsRegistry
    , private TMetricNextFreeKey
{
private:
    const TLabels CommonLabels;
    const NMonitoring::TDynamicCountersPtr Root;

    TAggregatorsMap Aggregators;
    TAggregatorStoragesList AggregatorStoragesOrder;

    TMetricKeyStoragesMap Keys;
    mutable TRWMutex AggregatorsLock;

public:
    TMainMetricsRegistry(
        TLabels commonLabels,
        NMonitoring::TDynamicCountersPtr rootCounters)
        : CommonLabels(std::move(commonLabels))
        , Root(std::move(rootCounters))
    {}

    ~TMainMetricsRegistry()
    {
        const auto _ = TWriteGuard(AggregatorsLock);

        while (!Keys.empty()) {
            UnregisterImpl(Keys.begin());
        }
    }

    // IMetricsRegistry
    TMetricKey Register(
        const TLabels& labels,
        IMetricPtr metric,
        EAggregationType aggrType,
        EMetricType metrType) override
    {
        auto aggregatorLabels = CreateLabels(CommonLabels, labels);

        const auto _ = TWriteGuard(AggregatorsLock);

        auto it = Aggregators.find(aggregatorLabels);
        if (it == Aggregators.end()) {
            it = Aggregators
                     .try_emplace(
                         aggregatorLabels,
                         Root,
                         aggregatorLabels,
                         aggrType,
                         metrType)
                     .first;
            AggregatorStoragesOrder.push_front(it->second);
            it->second.Item = AggregatorStoragesOrder.begin();
        }

        Y_ABORT_UNLESS(aggrType == it->second.Aggregator.GetAggregationType());
        Y_ABORT_UNLESS(metrType == it->second.Aggregator.GetMetricType());

        TMetricKey metricKey =
            it->second.Aggregator.Register(std::move(metric));
        TMetricKeyStorage keys(std::move(aggregatorLabels), metricKey);

        const TMetricKey key(this, GenerateNextFreeKey());
        const bool inserted = Keys.try_emplace(key, std::move(keys)).second;
        Y_ABORT_UNLESS(inserted);

        return key;
    }

    void Unregister(const TMetricKey& key) override
    {
        const auto _ = TWriteGuard(AggregatorsLock);

        auto it = Keys.find(key);
        if (it == Keys.end()) {
            return;
        }

        UnregisterImpl(it);
    }

    void Visit(TInstant time, IRegistryVisitor& visitor) override
    {
        const auto _ = TReadGuard(AggregatorsLock);

        visitor.OnStreamBegin();

        for (const auto& [labels, storage]: Aggregators) {
            visitor.OnMetricBegin(
                time,
                storage.Aggregator.GetAggregationType(),
                storage.Aggregator.GetMetricType());

            visitor.OnLabelsBegin();
            for (const auto& label: labels) {
                visitor.OnLabel(label.GetName(), label.GetValue());
            }
            visitor.OnLabelsEnd();

            visitor.OnValue(storage.Aggregator.Aggregate(time));

            visitor.OnMetricEnd();
        }

        visitor.OnStreamEnd();
    }

    void Update(TInstant time) override
    {
        const auto _ = TReadGuard(AggregatorsLock);

        for (auto& aggregatorStorage: AggregatorStoragesOrder) {
            const auto& storage = aggregatorStorage.get();
            storage.Counter->Set(storage.Aggregator.Aggregate(time));
        }
    }

private:
    void UnregisterImpl(typename TMetricKeyStoragesMap::iterator it)
    {
        const auto aggregatorIt = Aggregators.find(it->second.Aggregator);
        Y_ABORT_UNLESS(aggregatorIt != Aggregators.end());

        auto& aggregatorStorage = aggregatorIt->second;
        const bool isLast =
            aggregatorStorage.Aggregator.Unregister(it->second.Metric);

        if (isLast) {
            AggregatorStoragesOrder.erase(aggregatorStorage.Item);

            // Don't forget to copy labels before destruction.
            aggregatorStorage.Labels = aggregatorIt->first;
            Aggregators.erase(aggregatorIt);
        }

        Keys.erase(it);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TScopedMetricsRegistry
    : public IMetricsRegistry
    , private TMetricNextFreeKey
{
    using TSubMetricKeysMap = THashMap<TMetricKey, TVector<TMetricKey>>;

private:
    const TLabels CommonLabels;
    const TVector<IMetricsRegistryPtr> SubRegistries;

    TSubMetricKeysMap Keys;
    TAdaptiveLock KeysLock;

public:
    TScopedMetricsRegistry(
        TLabels commonLabels,
        TVector<IMetricsRegistryPtr> subRegistries)
        : CommonLabels(std::move(commonLabels))
        , SubRegistries(std::move(subRegistries))
    {}

    ~TScopedMetricsRegistry()
    {
        const auto _ = Guard(KeysLock);

        while (!Keys.empty()) {
            UnregisterImpl(Keys.begin());
        }
    }

    // IMetricsRegistry
    TMetricKey Register(
        const TLabels& labels,
        IMetricPtr metric,
        EAggregationType aggrType,
        EMetricType metrType) override
    {
        TVector<TMetricKey> keys(Reserve(SubRegistries.size()));
        for (size_t i = 0; i < SubRegistries.size(); ++i) {
            keys.push_back(SubRegistries[i]->Register(
                CreateLabels(CommonLabels, labels),
                metric,
                aggrType,
                metrType));
        }

        const auto _ = Guard(KeysLock);

        const TMetricKey key(this, GenerateNextFreeKey());
        const bool inserted = Keys.try_emplace(key, std::move(keys)).second;
        Y_ABORT_UNLESS(inserted);

        return key;
    }

    void Unregister(const TMetricKey& key) override
    {
        const auto _ = Guard(KeysLock);

        auto it = Keys.find(key);
        if (it == Keys.end()) {
            return;
        }

        UnregisterImpl(it);
    }

private:
    void UnregisterImpl(TSubMetricKeysMap::iterator it)
    {
        for (size_t i = 0; i < it->second.size(); ++i) {
            SubRegistries[i]->Unregister(it->second[i]);
        }

        Keys.erase(it);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMetricsRegistryStub: public IMetricsRegistry
{
public:
    // IMetricsRegistry
    TMetricKey Register(
        const TLabels& /*labels*/,
        IMetricPtr /*metric*/,
        EAggregationType /*aggrType*/,
        EMetricType /*metrType*/) override
    {
        return TMetricKey(this, 0);
    }

    void Unregister(const TMetricKey& /*key*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMainMetricsRegistryPtr CreateMetricsRegistry(
    TLabels commonLabels,
    NMonitoring::TDynamicCountersPtr rootCounters)
{
    return std::make_shared<TMainMetricsRegistry>(
        std::move(commonLabels),
        std::move(rootCounters));
}

IMetricsRegistryPtr CreateScopedMetricsRegistry(
    TLabels commonLabels,
    IMetricsRegistryPtr subRegistry)
{
    return std::make_shared<TScopedMetricsRegistry>(
        std::move(commonLabels),
        TVector{std::move(subRegistry)});
}

IMetricsRegistryPtr CreateScopedMetricsRegistry(
    TLabels commonLabels,
    TVector<IMetricsRegistryPtr> subRegistries)
{
    return std::make_shared<TScopedMetricsRegistry>(
        std::move(commonLabels),
        std::move(subRegistries));
}

IMetricsRegistryPtr CreateMetricsRegistryStub()
{
    return std::make_shared<TMetricsRegistryStub>();
}

}   // namespace NCloud::NFileStore::NMetrics
