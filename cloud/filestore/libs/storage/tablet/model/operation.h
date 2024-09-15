#pragma once

#include "public.h"

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EOperationState
{
    Idle,
    Enqueued,
    Started,
};

////////////////////////////////////////////////////////////////////////////////

class TOperationState
{
private:
    EOperationState State = EOperationState::Idle;
    TDuration Backoff;
    TInstant StateChanged;
    size_t Completed = 0;
    size_t Failed = 0;

public:
    bool Enqueue()
    {
        if (State == EOperationState::Idle) {
            ChangeState(EOperationState::Enqueued);
            return true;
        }
        return false;
    }

    bool Start()
    {
        if (State != EOperationState::Started) {
            ChangeState(EOperationState::Started);
            return true;
        }
        return false;
    }

    void Complete()
    {
        Y_ABORT_UNLESS(State != EOperationState::Idle);
        ChangeState(EOperationState::Idle);

        Backoff = TDuration::Zero();
        ++Completed;
    }

    void Fail()
    {
        Y_ABORT_UNLESS(State != EOperationState::Idle);
        ChangeState(EOperationState::Idle);

        Backoff = Min(
            Backoff + TDuration::MilliSeconds(100),
            TDuration::Seconds(5));
        ++Failed;
    }

    TDuration GetBackoffTimeout() const
    {
        return Backoff;
    }

    // for monpages

    EOperationState GetOperationState() const
    {
        return State;
    }

    TInstant GetStateChanged() const
    {
        return StateChanged;
    }

    size_t GetCompleted() const
    {
        return Completed;
    }

    size_t GetFailed() const
    {
        return Failed;
    }

private:
    void ChangeState(EOperationState state)
    {
        State = state;
        StateChanged = TInstant::Now();
    }
};

////////////////////////////////////////////////////////////////////////////////

enum class EBlobIndexOp
{
    Compaction,
    Cleanup,
    FlushBytes,
    Max,
};

////////////////////////////////////////////////////////////////////////////////

class TBlobIndexOpQueue
{
private:
    static const auto Capacity = static_cast<size_t>(EBlobIndexOp::Max);
    EBlobIndexOp Ops[Capacity] = {};
    ui32 Begin = 0;
    ui32 Count = 0;

public:
    void Push(EBlobIndexOp op)
    {
        for (ui32 i = 0; i < Count; ++i) {
            if (Ops[Index(i)] == op) {
                return;
            }
        }

        Ops[Index(Count)] = op;
        ++Count;

        Y_ABORT_UNLESS(Count <= Capacity);
    }

    bool Empty() const
    {
        return Count == 0;
    }

    EBlobIndexOp Pop()
    {
        Y_ABORT_UNLESS(Count);

        auto op = Ops[Index(0)];
        Begin = Index(1);
        --Count;

        return op;
    }

private:
    ui32 Index(ui32 i) const
    {
        return (Begin + i) % Capacity;
    }
};

}   // namespace NCloud::NFileStore::NStorage
