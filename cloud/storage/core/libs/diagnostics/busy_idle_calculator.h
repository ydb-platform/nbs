#pragma once

#include "public.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>

namespace NCloud {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

enum EState
{
    IDLE,
    BUSY,
    MAX
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept TBusyIdleTimeStorage = requires(T t) {
    {
        t.IncrementState(std::declval<ui64>(), std::declval<EState>())
    } -> std::same_as<void>;
} && std::is_default_constructible<T>::value;

////////////////////////////////////////////////////////////////////////////////

class TDynamicCountersStorage
{
    TIntrusivePtr<TCounterForPtr> BusyTime;
    TIntrusivePtr<TCounterForPtr> IdleTime;

public:
    void Register(TDynamicCountersPtr counters)
    {
        IdleTime = counters->GetCounter("IdleTime", true);
        BusyTime = counters->GetCounter("BusyTime", true);
    }

    void IncrementState(ui64 value, EState state)
    {
        switch (state) {
            case IDLE:
                if (Y_LIKELY(IdleTime)) {
                    IdleTime->Add(value);
                }
                break;
            case BUSY:
                if (Y_LIKELY(BusyTime)) {
                    BusyTime->Add(value);
                }
                break;
            case MAX:
                break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAtomicsStorage
{
    std::atomic<i64>* BusyTime = nullptr;
    std::atomic<i64>* IdleTime = nullptr;

public:
    void Register(std::atomic<i64>* busyTime, std::atomic<i64>* idleTime)
    {
        BusyTime = busyTime;
        IdleTime = idleTime;
    }

    void IncrementState(ui64 value, EState state)
    {
        switch (state) {
            case IDLE:
                if (Y_LIKELY(IdleTime)) {
                    IdleTime->fetch_add(value, std::memory_order_relaxed);
                }
                break;
            case BUSY:
                if (Y_LIKELY(BusyTime)) {
                    BusyTime->fetch_add(value, std::memory_order_relaxed);
                }
                break;
            case MAX:
                break;
        }
    }
};
}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <TBusyIdleTimeStorage T>
class TBusyIdleTimeCalculator
{
    std::array<TAtomic, EState::MAX> State;
    TAtomic InflightCount = 0;
    T Storage;

public:
    TBusyIdleTimeCalculator()
    {
        for (auto& e: State) {
            AtomicSet(e, 0);
        }
        StartState(IDLE);
    }

    template <typename... Args>
    void Register(Args&&... args)
    {
        Storage.Register(std::forward<Args>(args)...);
    }

    void OnRequestStarted()
    {
        if (AtomicIncrement(InflightCount) == 1) {
            FinishState(IDLE);
            StartState(BUSY);
        }
    }

    void OnRequestCompleted()
    {
        if (AtomicDecrement(InflightCount) == 0) {
            FinishState(BUSY);
            StartState(IDLE);
        }
    }

    void OnUpdateStats()
    {
        UpdateProgress(IDLE);
        UpdateProgress(BUSY);
    }

private:
    void FinishState(EState state)
    {
        ui64 val = MicroSeconds() - AtomicSwap(&State[state], 0);
        Storage.IncrementState(val, state);
    }

    void StartState(EState state)
    {
        AtomicSet(State[state], MicroSeconds());
    }

    void UpdateProgress(EState state)
    {
        for (;;) {
            ui64 started = AtomicGet(State[state]);
            if (!started) {
                return;
            }

            auto now = MicroSeconds();
            if (AtomicCas(&State[state], now, started)) {
                ui64 value = now - started;
                Storage.IncrementState(value, state);
                return;
            }
        }
    }
};

using TBusyIdleTimeCalculatorDynamicCounters =
    TBusyIdleTimeCalculator<TDynamicCountersStorage>;

using TBusyIdleTimeCalculatorAtomics = TBusyIdleTimeCalculator<TAtomicsStorage>;

}   // namespace NCloud
