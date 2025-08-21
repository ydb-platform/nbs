#pragma once

#include <cloud/storage/core/libs/diagnostics/histogram_types.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NCloud::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

class IUserCounter
{
public:
    virtual ~IUserCounter() = default;

    virtual void GetType(
        NMonitoring::IMetricConsumer* consumer) const = 0;
    virtual void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const = 0;
};

class TUserCounter
{
private:
    std::shared_ptr<IUserCounter> Counter;

public:
    TUserCounter(std::shared_ptr<IUserCounter> counter);

    void Accept(
        const NMonitoring::TLabels& baseLabels,
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const;
};

////////////////////////////////////////////////////////////////////////////////

class IUserCounterSupplier
    : public NMonitoring::IMetricSupplier
{
public:
    virtual ~IUserCounterSupplier() = default;

    virtual void AddUserMetric(
        NMonitoring::TLabels labels,
        TStringBuf name,
        TUserCounter metric) = 0;
    virtual void RemoveUserMetric(
        NMonitoring::TLabels labels,
        TStringBuf name) = 0;
};

std::shared_ptr<IUserCounterSupplier> CreateUserCounterSupplier();
std::shared_ptr<IUserCounterSupplier> CreateUserCounterSupplierStub();

////////////////////////////////////////////////////////////////////////////////

struct TBucket
{
    NMonitoring::TBucketBound Bound;
    TString Name;
};

static constexpr size_t BUCKETS_COUNT = 25;

using TBuckets = std::array<TBucket, BUCKETS_COUNT>;

TBuckets GetMsBuckets();
TBuckets GetUsBuckets();

////////////////////////////////////////////////////////////////////////////////

using TBaseDynamicCounters =
    std::pair<NMonitoring::TDynamicCounterPtr, TString>;

class TUserSumCounterWrapper
    : public IUserCounter
{
private:
    TVector<TIntrusivePtr<NMonitoring::TCounterForPtr>> Counters;
    NMonitoring::EMetricType Type = NMonitoring::EMetricType::UNKNOWN;

public:
    explicit TUserSumCounterWrapper(
        const TVector<TBaseDynamicCounters>& baseCounters);

    NMonitoring::EMetricType GetType() const;
    void GetType(NMonitoring::IMetricConsumer* consumer) const override;

    void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TUserSumHistogramWrapper
    : public IUserCounter
{
    using TExplicitHistogramSnapshot = NMonitoring::TExplicitHistogramSnapshot;
    using EMetricType = NMonitoring::EMetricType;

private:
    static constexpr size_t IgnoreBucketCount = 10;

    TVector<TIntrusivePtr<NMonitoring::TDynamicCounters>> Counters;
    TIntrusivePtr<TExplicitHistogramSnapshot> Histogram;
    const TBuckets Buckets;
    EMetricType Type = EMetricType::UNKNOWN;

public:
    explicit TUserSumHistogramWrapper(
        const TBuckets& buckets,
        const TVector<TBaseDynamicCounters>& baseCounters);

    void Clear() const;

    void GetType(NMonitoring::IMetricConsumer* consumer) const override;

    void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override;
};

////////////////////////////////////////////////////////////////////////////////

void AddUserMetric(
    IUserCounterSupplier& dsc,
    const NMonitoring::TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName);

void AddHistogramUserMetric(
    const TBuckets& buckets,
    IUserCounterSupplier& dsc,
    const NMonitoring::TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName);

}  // namespace NCloud::NStorage::NUserStats
