#include "solomon_counters.h"

namespace NCloud {

void TSolomonValueHolder::Init(
    NMonitoring::TDynamicCountersPtr counters,
    const TString& counterName,
    bool derivative)
{
    Parent = counters;
    CounterName = counterName;
    Derivative = derivative;
    if (ExpirationPolicy == ECounterExpirationPolicy::Permanent) {
        SolomonValue = counters->GetExpiringCounter(counterName, Derivative);
    }
}

void TSolomonValueHolder::SetCounterValue(TInstant now, ui64 value)
{
    if (ExpirationPolicy == ECounterExpirationPolicy::Expiring) {
        if (value == 0 && now - LastNonZeroValueTs >= TDuration::Hours(1)) {
            SolomonValue.Reset();
        }
        if (value > 0) {
            LastNonZeroValueTs = now;
            if (!SolomonValue && Parent) {
                SolomonValue =
                    Parent->GetExpiringCounter(CounterName, Derivative);
            }
        }
    }

    if (SolomonValue) {
        *SolomonValue = value;
    }
}

ui64 TSolomonValueHolder::GetValue() const
{
    if (SolomonValue) {
        return *SolomonValue;
    }
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleCounter::Add(const TSimpleCounter& source)
{
    Value = source.Value;
    MinValue = Min(MinValue, source.MinValue);
    MaxValue = Max(MaxValue, source.MaxValue);
    Sum += source.Sum;
}

void TSimpleCounter::AggregateWith(const TSimpleCounter& source)
{
    Value += source.Value;
    MinValue = Min(MinValue, source.MinValue);
    MaxValue = Max(MaxValue, source.MaxValue);
    Sum += source.Sum;
}

void TSimpleCounter::Increment(ui64 value)
{
    Value += value;
    MinValue = Min(MinValue, Value);
    MaxValue = Max(MaxValue, Value);
    Sum += value;
}

void TSimpleCounter::Set(ui64 value)
{
    Value = value;
    MinValue = value;
    MaxValue = value;
    Sum = value;
}

void TSimpleCounter::Reset()
{
    Value = 0;
    MinValue = Max();
    MaxValue = 0;
    Sum = 0;
}

void TSimpleCounter::Register(
    NMonitoring::TDynamicCountersPtr counters,
    const TString& counterName)
{
    SolomonValue.Init(std::move(counters), counterName);
}

void TSimpleCounter::Publish(TInstant now)
{
    ui64 valueToReport = 0;

    switch (Type) {
        case ECounterType::Generic: {
            valueToReport = Value;
            break;
        }
        case ECounterType::Max: {
            valueToReport = MaxValue;
            break;
        }
        case ECounterType::Min: {
            valueToReport = MinValue;
            break;
        }
    }

    SolomonValue.SetCounterValue(now, valueToReport);
}

/////////////////////////////////////////////////////////////////////////////

void TCumulativeCounter::Add(const TCumulativeCounter& source)
{
    Value += source.Value;
    MinValue = Min(MinValue, source.Value);
    MaxValue = Max(MaxValue, source.Value);
    Sum += source.Sum;
}

void TCumulativeCounter::AggregateWith(const TCumulativeCounter& source)
{
    Add(source);
}

void TCumulativeCounter::Register(
    NMonitoring::TDynamicCountersPtr counters,
    const TString& counterName)
{
    SolomonValue.Init(std::move(counters), counterName, true);
}

void TCumulativeCounter::Publish(TInstant now)
{
    SolomonValue.SetCounterValue(now, SolomonValue.GetValue() + Value);
}

}   // namespace NCloud
