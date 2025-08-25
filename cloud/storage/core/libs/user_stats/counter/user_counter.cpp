#include "user_counter.h"

#include <cloud/storage/core/libs/diagnostics/histogram_types.h>

namespace NCloud::NStorage::NUserStats {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUserCounterSupplier
    : public IUserCounterSupplier
{
private:
    TRWMutex Lock;
    THashMap<TLabels, TUserCounter> Metrics;

public:
    // NMonitoring::IMetricSupplier
    void Accept(TInstant time, IMetricConsumer* consumer) const override
    {
        if (!consumer) {
            return;
        }

        consumer->OnStreamBegin();
        {
            TReadGuard g{Lock};
            for (const auto& it: Metrics) {
                it.second.Accept(it.first, time, consumer);
            }
        }
        consumer->OnStreamEnd();
    }

    void Append(TInstant time, IMetricConsumer* consumer) const override
    {
        TReadGuard g{Lock};
        for (const auto& it: Metrics) {
            it.second.Accept(it.first, time, consumer);
        }
    }

    // IUserCounterSupplier
    void AddUserMetric(
        TLabels labels,
        TStringBuf name,
        TUserCounter metric) override
    {
        labels.Add("name", name);

        TWriteGuard g{Lock};
        Metrics.emplace(std::move(labels), std::move(metric));
    }

    void RemoveUserMetric(TLabels labels, TStringBuf name) override
    {
        labels.Add("name", name);

        TWriteGuard g{Lock};
        Metrics.erase(labels);
    }
};

class TUserCounterSupplierStub
    : public IUserCounterSupplier
{
public:
    // NMonitoring::IMetricSupplier
    void Accept(TInstant /*time*/, IMetricConsumer* /*consumer*/) const override
    {}

    void Append(TInstant /*time*/, IMetricConsumer* /*consumer*/) const override
    {}

    // IUserCounterSupplier
    void AddUserMetric(
        TLabels /*labels*/,
        TStringBuf /*name*/,
        TUserCounter /*metric*/) override
    {}

    void RemoveUserMetric(TLabels /*labels*/, TStringBuf /*name*/) override
    {}
};

template <typename THistogramType>
TBuckets MakeBuckets(auto convertBound)
{
    static_assert(BUCKETS_COUNT == THistogramType::BUCKETS_COUNT, "");

    TBuckets result;
    const auto names = THistogramType::MakeNames();
    for (size_t i = 0; i < names.size(); ++i) {
        result[i].Bound = convertBound(THistogramType::Buckets[i]);
        result[i].Name = names[i];
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TUserCounter::TUserCounter(std::shared_ptr<IUserCounter> counter)
    : Counter(std::move(counter))
{}

void TUserCounter::Accept(
    const TLabels& baseLabels,
    TInstant time,
    IMetricConsumer* consumer) const
{
    if (!Counter || !consumer) {
        return;
    }

    Counter->GetType(consumer);

    consumer->OnLabelsBegin();

    for (const auto& label: baseLabels) {
        consumer->OnLabel(label.Name(), label.Value());
    }

    consumer->OnLabelsEnd();

    Counter->GetValue(time, consumer);

    consumer->OnMetricEnd();
}

///////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IUserCounterSupplier> CreateUserCounterSupplier()
{
    return std::make_shared<TUserCounterSupplier>();
}

std::shared_ptr<IUserCounterSupplier> CreateUserCounterSupplierStub()
{
    return std::make_shared<TUserCounterSupplierStub>();
}

///////////////////////////////////////////////////////////////////////////////

TBucketsWithUnits GetMsBuckets()
{
    constexpr auto Identity = [](double data) { return data; };
    static const auto Buckets = MakeBuckets<TRequestMsTimeBuckets>(Identity);
    return {Buckets, "msec"};
}

TBucketsWithUnits GetUsBuckets()
{
    constexpr auto UsToMs = [](double data) {
        return data == std::numeric_limits<double>::max() ? data : data / 1000.;
    };
    static const auto Buckets = MakeBuckets<TRequestUsTimeBuckets>(UsToMs);
    return {Buckets, "usec"};
}

///////////////////////////////////////////////////////////////////////////////

class TUserSumCounterWrapper
    : public IUserCounter
{
private:
    TVector<TIntrusivePtr<NMonitoring::TCounterForPtr>> Counters;
    EMetricType Type = EMetricType::UNKNOWN;

public:
    explicit TUserSumCounterWrapper(
        const TVector<TBaseDynamicCounters>& baseCounters)
    {
        for (const auto& [baseCounter, name]: baseCounters) {
            if (baseCounter) {
                if (auto countSub = baseCounter->FindCounter(name)) {
                    Counters.push_back(countSub);
                    Type = countSub->ForDerivative() ? EMetricType::RATE
                                                     : EMetricType::GAUGE;
                }
            }
        }
    }

    EMetricType GetType() const
    {
        return Type;
    }

    void GetType(NMonitoring::IMetricConsumer* consumer) const override
    {
        consumer->OnMetricBegin(Type);
    }

    void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override
    {
        int64_t sum = 0;

        for (const auto& counter: Counters) {
            sum += counter->Val();
        }

        consumer->OnInt64(time, sum);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TUserSumHistogramWrapper
    : public IUserCounter
{
    using TExplicitHistogramSnapshot = NMonitoring::TExplicitHistogramSnapshot;

private:
    static constexpr size_t IgnoreBucketCount = 10;

    TVector<TIntrusivePtr<NMonitoring::TDynamicCounters>> Counters;
    const TBuckets Buckets;
    const TString Units;
    TIntrusivePtr<TExplicitHistogramSnapshot> Histogram;
    EMetricType Type = EMetricType::UNKNOWN;

public:
    TUserSumHistogramWrapper(
        const TBucketsWithUnits& buckets,
        const TVector<TBaseDynamicCounters>& baseCounters)
        : Buckets(buckets.first)
        , Units(buckets.second)
        , Histogram(TExplicitHistogramSnapshot::New(
              Buckets.size() - IgnoreBucketCount))
        , Type(EMetricType::HIST_RATE)
    {
        for (size_t i = IgnoreBucketCount; i < Buckets.size(); ++i) {
            (*Histogram)[i - IgnoreBucketCount].first = Buckets[i].Bound;
        }

        for (const auto& [baseCounter, name]: baseCounters) {
            if (!baseCounter) {
                continue;
            }
            auto histSubgroup = baseCounter->FindSubgroup("histogram", name);
            if (!histSubgroup) {
                continue;
            }
            auto unitsSubgroup = histSubgroup->FindSubgroup("units", Units);
            if (!unitsSubgroup) {
                continue;
            }
            Counters.push_back(unitsSubgroup);
        }
    }

    void Clear() const
    {
        for (size_t i = IgnoreBucketCount; i < Buckets.size(); ++i) {
            (*Histogram)[i - IgnoreBucketCount].second = 0;
        }
    }

    void GetType(NMonitoring::IMetricConsumer* consumer) const override
    {
        consumer->OnMetricBegin(Type);
    }

    void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override
    {
        Clear();

        for (const auto& counter: Counters) {
            for (size_t i = 0; i < Buckets.size(); ++i) {
                if (auto countSub = counter->GetCounter(Buckets[i].Name)) {
                    size_t id =
                        i < IgnoreBucketCount ? 0 : i - IgnoreBucketCount;
                    (*Histogram)[id].second += countSub->Val();
                }
            }
        }
        consumer->OnHistogram(time, Histogram);
    }
};

////////////////////////////////////////////////////////////////////////////////

void AddUserMetric(
    IUserCounterSupplier& dsc,
    const NMonitoring::TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName)
{
    auto wrapper = std::make_shared<TUserSumCounterWrapper>(baseCounters);

    if (wrapper->GetType() != EMetricType::UNKNOWN) {
        dsc.AddUserMetric(commonLabels, newName, TUserCounter(wrapper));
    }
}

void AddHistogramUserMetric(
    const TBucketsWithUnits& buckets,
    IUserCounterSupplier& dsc,
    const TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName)
{
    auto wrapper =
        std::make_shared<TUserSumHistogramWrapper>(buckets, baseCounters);

    dsc.AddUserMetric(commonLabels, newName, TUserCounter(wrapper));
}

}   // namespace NCloud::NStorage::NUserStats
