#pragma once

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

}   // NCloud::NStorage::NUserStats
