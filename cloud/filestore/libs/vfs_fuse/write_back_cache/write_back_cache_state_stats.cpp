#include "write_back_cache_state_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheStateStats
    : public std::enable_shared_from_this<TWriteBackCacheStateStats>
    , public IWriteBackCacheStateStats
{
private:
    TRelaxedEventCounter<> FlushEventCounter;
    TRelaxedCounter FailedFlushCounter;
    TRelaxedCounter WriteDataRequestDroppedCounter;
    TRelaxedEventCounterWithTimeStats<> BarrierEventCounter;

    TRelaxedExtendedEventCounterWithTimeStats<> FlushRequestCounter;
    TRelaxedExtendedEventCounterWithTimeStats<> FlushAllRequestCounter;
    TRelaxedExtendedEventCounterWithTimeStats<> ReleaseHandleRequestCounter;
    TRelaxedExtendedEventCounterWithTimeStats<> AcquireBarrierRequestCounter;

public:
    void FlushStarted() override
    {
        FlushEventCounter.Started();
    }

    void FlushCompleted() override
    {
        FlushEventCounter.Completed();
    }

    void FlushFailed() override
    {
        FailedFlushCounter.Inc();
    }

    void WriteDataRequestDropped() override
    {
        WriteDataRequestDroppedCounter.Inc();
    }

    void BarrierAcquired() override
    {
        BarrierEventCounter.Started();
    }

    void BarrierReleased(TDuration duration) override
    {
        BarrierEventCounter.Completed(duration);
    }

    void RequestAdded(ERequestType type) override
    {
        AccessRequestCounter(type).Started();
    }

    void RequestCompleted(ERequestType type, TDuration duration) override
    {
        AccessRequestCounter(type).Completed(duration);
    }

    void RequestCompletedImmediately(ERequestType type) override
    {
        AccessRequestCounter(type).CompletedImmediately();
    }

    void RequestFailed(ERequestType type, TDuration duration) override
    {
        AccessRequestCounter(type).Failed(duration);
    }

    TWriteBackCacheStateMetrics CreateMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .Flush =
                {.InProgressCount = CreateMetric(
                     [self]
                     { return self->FlushEventCounter.GetActiveCount(); }),
                 .InProgressMaxCount = CreateMetric(
                     [self]
                     { return self->FlushEventCounter.GetActiveMaxCount(); }),
                 .CompletedCount = CreateMetric(
                     [self]
                     { return self->FlushEventCounter.GetCompletedCount(); }),
                 .FailedCount = CreateMetric(
                     [self] { return self->FailedFlushCounter.Get(); })},
            .WriteDataRequestDroppedCount = CreateMetric(
                [self] { return self->WriteDataRequestDroppedCounter.Get(); }),
            .Barriers =
                {
                    .ActiveCount = CreateMetric(
                        [self]
                        { return self->BarrierEventCounter.GetActiveCount(); }),
                    .ActiveMaxCount = CreateMetric(
                        [self]
                        {
                            return self->BarrierEventCounter
                                .GetActiveMaxCount();
                        }),
                    .ReleasedCount = CreateMetric(
                        [self]
                        {
                            return self->BarrierEventCounter
                                .GetCompletedCount();
                        }),
                    .ReleasedTime = CreateMetric(
                        [self]
                        {
                            return self->BarrierEventCounter.GetCompletedTime();
                        }),
                    .MaxTime = CreateMetric(
                        [self]
                        { return self->BarrierEventCounter.GetMaxTime(); }),

                },
            .FlushRequests = CreateMetrics(
                [self]() -> const auto&
                { return self->FlushRequestCounter; }),
            .FlushAllRequests = CreateMetrics(
                [self]() -> const auto&
                { return self->FlushAllRequestCounter; }),
            .ReleaseHandleRequests = CreateMetrics(
                [self]() -> const auto&
                { return self->ReleaseHandleRequestCounter; }),
            .AcquireBarrierRequests = CreateMetrics(
                [self]() -> const auto&
                { return self->AcquireBarrierRequestCounter; }),
        };
    }

    void UpdateStats(const TMaxInProgressDurations& values) override
    {
        FlushEventCounter.Update();
        BarrierEventCounter.Update(values.ActiveBarrier);
        FlushRequestCounter.Update(values.FlushRequest);
        FlushAllRequestCounter.Update(values.FlushAllRequest);
        ReleaseHandleRequestCounter.Update(values.ReleaseHandleRequest);
        AcquireBarrierRequestCounter.Update(values.AcquireBarrierRequest);
    }

private:
    TRelaxedExtendedEventCounterWithTimeStats<>& AccessRequestCounter(
        ERequestType type)
    {
        switch (type) {
            case ERequestType::Flush:
                return FlushRequestCounter;
            case ERequestType::FlushAll:
                return FlushAllRequestCounter;
            case ERequestType::ReleaseHandle:
                return ReleaseHandleRequestCounter;
            case ERequestType::AcquireBarrier:
                return AcquireBarrierRequestCounter;
        }
    }

    // f: () -> const& TRelaxedExtendedEventCounter<>
    TWriteBackCacheStateMetrics::TRequestMetrics CreateMetrics(auto&& f) const
    {
        return {
            .InProgressCount =
                CreateMetric([f] { return f().GetActiveCount(); }),
            .InProgressMaxCount =
                CreateMetric([f] { return f().GetActiveMaxCount(); }),
            .CompletedCount =
                CreateMetric([f] { return f().GetCompletedCount(); }),
            .CompletedTime =
                CreateMetric([f] { return f().GetCompletedTime(); }),
            .MaxTime = CreateMetric([f] { return f().GetMaxTime(); }),
            .CompletedImmediately =
                CreateMetric([f] { return f().GetCompletedImmediately(); }),
            .FailedCount = CreateMetric([f] { return f().GetFailedCount(); }),
        };
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TWriteBackCacheStateMetrics::Register(
    IMetricsRegistry& localMetricsRegistry,
    IMetricsRegistry& aggregatableMetricsRegistry) const
{
    localMetricsRegistry.Register(
        {CreateSensor("Flush_InProgressCount")},
        Flush.InProgressCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Flush_InProgressMaxCount")},
        Flush.InProgressMaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Flush_CompletedCount")},
        Flush.CompletedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("Flush_FailedCount")},
        Flush.FailedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("WriteDataRequest_DroppedCount")},
        WriteDataRequestDroppedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    // Barriers

    localMetricsRegistry.Register(
        {CreateSensor("Barriers_ActiveCount")},
        Barriers.ActiveCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Barriers_ActiveMaxCount")},
        Barriers.ActiveMaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Barriers_ReleasedCount")},
        Barriers.ReleasedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    localMetricsRegistry.Register(
        {CreateSensor("Barriers_ReleasedTime")},
        Barriers.ReleasedTime,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("Barriers_MaxTime")},
        Barriers.MaxTime,
        EAggregationType::AT_MAX,
        EMetricType::MT_ABSOLUTE);

    auto helper = [&](const TString& p, const auto& m)
    {
        localMetricsRegistry.Register(
            {CreateSensor(p + "_InProgressCount")},
            m.InProgressCount,
            EAggregationType::AT_SUM,
            EMetricType::MT_ABSOLUTE);

        localMetricsRegistry.Register(
            {CreateSensor(p + "_InProgressMaxCount")},
            m.InProgressMaxCount,
            EAggregationType::AT_SUM,
            EMetricType::MT_ABSOLUTE);

        localMetricsRegistry.Register(
            {CreateSensor(p + "_CompletedCount")},
            m.CompletedCount,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        localMetricsRegistry.Register(
            {CreateSensor(p + "_CompletedTime")},
            m.CompletedTime,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        aggregatableMetricsRegistry.Register(
            {CreateSensor(p + "_MaxTime")},
            m.MaxTime,
            EAggregationType::AT_MAX,
            EMetricType::MT_ABSOLUTE);

        localMetricsRegistry.Register(
            {CreateSensor(p + "_CompletedImmediately")},
            m.CompletedImmediately,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        localMetricsRegistry.Register(
            {CreateSensor(p + "_FailedCount")},
            m.FailedCount,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);
    };

    helper("FlushRequests", FlushRequests);
    helper("FlushAllRequests", FlushAllRequests);
    helper("ReleaseHandleRequests", ReleaseHandleRequests);
    helper("AcquireBarrierRequests", AcquireBarrierRequests);
}

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStateStatsPtr CreateWriteBackCacheStateStats()
{
    return std::make_shared<TWriteBackCacheStateStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
