#include "user_counter.h"

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

TBuckets GetMsBuckets() {
    constexpr auto Identity = [](double data) { return data; };
    static const auto Buckets = MakeBuckets<TRequestMsTimeBuckets>(Identity);
    return Buckets;
}

TBuckets GetUsBuckets() {
    constexpr auto UsToMs = [](double data) {
        return data == std::numeric_limits<double>::max() ? data : data / 1000.;
    };
    static const auto Buckets = MakeBuckets<TRequestUsTimeBuckets>(UsToMs);
    return Buckets;
}

///////////////////////////////////////////////////////////////////////////////

TUserSumCounterWrapper::TUserSumCounterWrapper(
    const TVector<TBaseDynamicCounters>& baseCounters)
{
    for (const auto& [baseCounter, name]: baseCounters) {
        if (baseCounter) {
            if (auto countSub = baseCounter->FindCounter(name)) {
                Counters.push_back(countSub);
                Type = countSub->ForDerivative()
                           ? NMonitoring::EMetricType::RATE
                           : EMetricType::GAUGE;
            }
        }
    }
}

NMonitoring::EMetricType TUserSumCounterWrapper::GetType() const
{
    return Type;
}

void TUserSumCounterWrapper::GetType(
    NMonitoring::IMetricConsumer* consumer) const
{
    consumer->OnMetricBegin(Type);
}

void TUserSumCounterWrapper::GetValue(
    TInstant time,
    NMonitoring::IMetricConsumer* consumer) const
{
    int64_t sum = 0;

    for (const auto& counter: Counters) {
        sum += counter->Val();
    }

    consumer->OnInt64(time, sum);
}

///////////////////////////////////////////////////////////////////////////////

TUserSumHistogramWrapper::TUserSumHistogramWrapper(
    const TBuckets& buckets,
    const TVector<TBaseDynamicCounters>& baseCounters)
    : Histogram(
          TExplicitHistogramSnapshot::New(buckets.size() - IgnoreBucketCount))
    , Buckets(buckets)
    , Type(EMetricType::HIST_RATE)
{
    for (size_t i = IgnoreBucketCount; i < Buckets.size(); ++i) {
        (*Histogram)[i - IgnoreBucketCount].first = Buckets[i].Bound;
    }

    for (const auto& [baseCounter, name]: baseCounters) {
        if (baseCounter) {
            if (auto hist = baseCounter->FindSubgroup("histogram", name)) {
                Counters.push_back(hist);
            }
        }
    }
}

void TUserSumHistogramWrapper::Clear() const
{
    for (size_t i = IgnoreBucketCount; i < Buckets.size(); ++i) {
        (*Histogram)[i - IgnoreBucketCount].second = 0;
    }
}

void TUserSumHistogramWrapper::GetType(
    NMonitoring::IMetricConsumer* consumer) const
{
    consumer->OnMetricBegin(Type);
}

void TUserSumHistogramWrapper::GetValue(
    TInstant time,
    NMonitoring::IMetricConsumer* consumer) const
{
    Clear();

    for (const auto& counter: Counters) {
        for (size_t i = 0; i < Buckets.size(); ++i) {
            if (auto countSub = counter->GetCounter(Buckets[i].Name)) {
                size_t id = i < IgnoreBucketCount ? 0 : i - IgnoreBucketCount;
                (*Histogram)[id].second += countSub->Val();
            }
        }
    }
    consumer->OnHistogram(time, Histogram);
}

void AddUserMetric(
    IUserCounterSupplier& dsc,
    const NMonitoring::TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName)
{
    auto wrapper = std::make_shared<TUserSumCounterWrapper>(baseCounters);

    if (wrapper->GetType() != NMonitoring::EMetricType::UNKNOWN) {
        dsc.AddUserMetric(commonLabels, newName, TUserCounter(wrapper));
    }
}

void AddHistogramUserMetric(
    const TBuckets& buckets,
    IUserCounterSupplier& dsc,
    const TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName)
{
    auto wrapper =
        std::make_shared<TUserSumHistogramWrapper>(buckets, baseCounters);

    dsc.AddUserMetric(commonLabels, newName, TUserCounter(wrapper));
}

}  // namespace NCloud::NStorage::NUserStats
