#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_registry.h>

#include <util/generic/hash_multi_map.h>

namespace NCloud::NBlockStore::NUserCounter {

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

class TUserCounterSupplier
    : public NMonitoring::IMetricSupplier
{
private:
    TRWMutex Lock;
    THashMap<NMonitoring::TLabels, TUserCounter> Metrics;

public:
    // NMonitoring::IMetricSupplier
    void Accept(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override;
    void Append(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override;

    void AddUserMetric(
        NMonitoring::TLabels labels,
        TStringBuf name,
        TUserCounter metric);
    void RemoveUserMetric(
        NMonitoring::TLabels labels,
        TStringBuf name);
};

////////////////////////////////////////////////////////////////////////////////

void RegisterServiceVolume(
    TUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    NMonitoring::TDynamicCounterPtr src);

void UnregisterServiceVolume(
    TUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId);

void RegisterServerVolumeInstance(
    TUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId,
    NMonitoring::TDynamicCounterPtr src);

void UnregisterServerVolumeInstance(
    TUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId);

} // NCloud::NBlockStore::NUserCounter
