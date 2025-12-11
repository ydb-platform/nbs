#pragma once

#include "public.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class ECounterExpirationPolicy
{
    Permanent,
    Expiring
};

////////////////////////////////////////////////////////////////////////////////

class TSolomonValueHolder
{
private:
    ECounterExpirationPolicy ExpirationPolicy =
        ECounterExpirationPolicy::Permanent;
    TInstant LastNonZeroValueTs;

    TString CounterName;
    bool Derivative = false;
    NMonitoring::TDynamicCountersPtr Parent;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonValue;

public:
    TSolomonValueHolder() = default;

    TSolomonValueHolder(ECounterExpirationPolicy policy)
        : ExpirationPolicy(policy){};

    void Init(
        NMonitoring::TDynamicCountersPtr counters,
        const TString& counterName,
        bool derivative = false);

    void SetCounterValue(TInstant now, ui64 value);

    ui64 GetValue() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TSimpleCounter
{
    enum class ECounterType
    {
        Generic,
        Max,
        Min
    };

    ECounterType Type = ECounterType::Generic;
    ui64 Value = 0;
    ui64 MinValue = Max();
    ui64 MaxValue = 0;
    ui64 Sum = 0;

    TSolomonValueHolder SolomonValue;

    TSimpleCounter() = default;

    TSimpleCounter(ECounterType type, ECounterExpirationPolicy policy)
        : Type(type)
        , SolomonValue(policy)
    {}

    void Add(const TSimpleCounter& source);
    void AggregateWith(const TSimpleCounter& source);

    void Increment(ui64 value);
    void Set(ui64 value);

    void Reset();

    void Register(
        NMonitoring::TDynamicCountersPtr counters,
        const TString& counterName);

    void Publish(TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

struct TCumulativeCounter: public TSimpleCounter
{
    TCumulativeCounter() = default;

    TCumulativeCounter(ECounterType type, ECounterExpirationPolicy policy)
        : TSimpleCounter(type, policy)
    {}

    void Add(const TCumulativeCounter& source);
    void AggregateWith(const TCumulativeCounter& source);
    void Register(
        NMonitoring::TDynamicCountersPtr counters,
        const TString& counterName);
    void Publish(TInstant now);
};

}   // namespace NCloud
