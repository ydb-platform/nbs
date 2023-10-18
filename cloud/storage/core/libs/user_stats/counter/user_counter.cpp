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

}   // NCloud::NStorage::NUserStats
