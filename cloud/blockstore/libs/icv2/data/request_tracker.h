#pragma once

#include "config.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/system/mutex.h>

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
        ui64 Bytes;

    public:
        TReservation(std::shared_ptr<TRequestTracker> owner, ui64 bytes);
        TReservation(const TReservation&) = delete;
        TReservation& operator=(const TReservation&) = delete;
        ~TReservation();

        // Zero bytes checks for shutdown. Failure leaves the budget unchanged.
        NProto::TError AddBytes(ui64 bytes);
    };

private:
    const TDataConfig Config;
    TMutex Mutex;
    ui32 Requests = 0;
    ui64 Bytes = 0;
    bool Stopping = false;
    bool Published = false;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    explicit TRequestTracker(TDataConfig config);
    TResultOrError<std::shared_ptr<TReservation>> Acquire(ui64 bytes);
    void RequestStop();

    // Waits for all reservations; do not call while holding one.
    void Stop();

private:
    NProto::TError AddBytes(ui64 bytes);
    void Release(ui64 bytes);
    void PublishClosed();
};

}   // namespace NCloud::NBlockStore::NIcV2
