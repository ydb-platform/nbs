#pragma once

#include "data.h"

#include <mutex>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Internal shared admission and shutdown accounting for an endpoint or target.
class TRequestTracker: public std::enable_shared_from_this<TRequestTracker>
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
        NProto::TError AddBytes(ui64 bytes);
    };

private:
    const TDataConfig Config;
    std::mutex Mutex;
    ui32 Requests = 0;
    ui64 Bytes = 0;
    bool Stopping = false;
    bool Published = false;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    explicit TRequestTracker(TDataConfig config);
    TResultOrError<std::shared_ptr<TReservation>> Acquire(ui64 bytes);
    void RequestStop();
    void Stop();

private:
    NProto::TError AddBytes(ui64 bytes);
    void Release(ui64 bytes);
    void PublishClosed();
};

bool ValidateDataConfig(const TDataConfig& config);
ui64 GetDataReservationBytes(ui64 payloadBytes, ui32 blocksCount);
NProto::TError ValidateRange(const TString& diskId, ui64 start, ui32 count);
TResultOrError<ui64> ValidateDataRange(const TString& diskId, ui64 start,
                                       ui32 count, ui32 blockSize,
                                       const TDataConfig& config);

}   // namespace NCloud::NBlockStore::NIcV2
