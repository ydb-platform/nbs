#include "user_counter.h"

namespace NCloud::NStorage::NUserStats {

using namespace NMonitoring;

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

void TUserCounterSupplier::Accept(
    TInstant time,
    IMetricConsumer* consumer) const
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

void TUserCounterSupplier::Append(
    TInstant time,
    IMetricConsumer* consumer) const
{
    TReadGuard g{Lock};
    for (const auto& it: Metrics) {
        it.second.Accept(it.first, time, consumer);
    }
}

void TUserCounterSupplier::AddUserMetric(
    TLabels labels,
    TStringBuf name,
    TUserCounter metric)
{
    labels.Add("name", name);

    TWriteGuard g{Lock};
    Metrics.emplace(std::move(labels), std::move(metric));
}

void TUserCounterSupplier::RemoveUserMetric(
    TLabels labels,
    TStringBuf name)
{
    labels.Add("name", name);

    TWriteGuard g{Lock};
    Metrics.erase(labels);
}

}   // NCloud::NStorage::NUserStats
