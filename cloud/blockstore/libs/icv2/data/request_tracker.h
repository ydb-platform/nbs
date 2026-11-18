#pragma once

#include "config.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <atomic>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Create with make_shared; retain a reservation until its request finishes.
class TRequestTracker final
    : public std::enable_shared_from_this<TRequestTracker>
{
public:
    class TReservation
    {
        const std::shared_ptr<TRequestTracker> Owner;
        const ui64 Bytes;

    public:
        TReservation(std::shared_ptr<TRequestTracker> owner, ui64 bytes);
        TReservation(const TReservation&) = delete;
        TReservation& operator=(const TReservation&) = delete;
        ~TReservation();
    };

private:
    enum class EState : ui32
    {
        Running,
        Stopping,
        Stopped,
    };

    struct TState
    {
        ui32 Requests;
        EState Phase;
    };

    static_assert(std::atomic<TState>::is_always_lock_free);

    const TDataConfig Config;

    // Admit requests and enter shutdown atomically.
    std::atomic<TState> State{TState{
        .Requests = 0,
        .Phase = EState::Running,
    }};
    std::atomic<ui64> Bytes = 0;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    explicit TRequestTracker(TDataConfig config);
    TResultOrError<std::shared_ptr<TReservation>> Acquire(ui64 bytes);
    NProto::TError CheckRunning() const;
    void RequestStop();

    // Waits for all reservations; do not call while holding one.
    void Stop();

private:
    NProto::TError ReserveBytes(ui64 bytes);
    void Release(ui64 bytes);
};

}   // namespace NCloud::NBlockStore::NIcV2
