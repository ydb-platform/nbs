#include "request_tracker.h"

#include <limits>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

TRequestTracker::TReservation::TReservation(
    std::shared_ptr<TRequestTracker> owner, ui64 bytes)
    : Owner(std::move(owner))
    , Bytes(bytes)
{}

TRequestTracker::TReservation::~TReservation()
{
    Owner->Release(Bytes);
}

NProto::TError TRequestTracker::TReservation::AddBytes(ui64 bytes)
{
    if (auto error = Owner->AddBytes(bytes); HasError(error)) {
        return error;
    }
    Bytes += bytes;
    return {};
}

TRequestTracker::TRequestTracker(TDataConfig config)
    : Config(config)
{}

TResultOrError<std::shared_ptr<TRequestTracker::TReservation>>
TRequestTracker::Acquire(ui64 bytes)
{
    std::lock_guard guard(Mutex);
    if (Stopping) {
        return MakeError(E_CANCELLED, "IC V2 data owner is stopping");
    }
    if (Requests >= Config.MaxInflightRequests ||
        bytes > Config.MaxInflightBytes - Bytes)
    {
        return MakeError(E_REJECTED, "IC V2 data admission limit reached");
    }
    ++Requests;
    Bytes += bytes;
    return std::make_shared<TReservation>(shared_from_this(), bytes);
}

NProto::TError TRequestTracker::AddBytes(ui64 bytes)
{
    std::lock_guard guard(Mutex);
    if (Stopping) {
        return MakeError(E_CANCELLED, "IC V2 data owner is stopping");
    }
    if (bytes > Config.MaxInflightBytes - Bytes) {
        return MakeError(E_REJECTED, "IC V2 data byte budget exceeded");
    }
    Bytes += bytes;
    return {};
}

void TRequestTracker::Release(ui64 bytes)
{
    {
        std::lock_guard guard(Mutex);
        Y_ABORT_UNLESS(Requests && Bytes >= bytes);
        --Requests;
        Bytes -= bytes;
    }
    PublishClosed();
}

void TRequestTracker::RequestStop()
{
    {
        std::lock_guard guard(Mutex);
        Stopping = true;
    }
    PublishClosed();
}

void TRequestTracker::Stop()
{
    RequestStop();
    Closed.GetFuture().GetValueSync();
}

void TRequestTracker::PublishClosed()
{
    {
        std::lock_guard guard(Mutex);
        if (!Stopping || Requests || Published) {
            return;
        }
        Y_ABORT_UNLESS(!Bytes);
        Published = true;
    }
    Closed.SetValue();
}

bool ValidateDataConfig(const TDataConfig& config)
{
    return config.MaxRequestBytes &&
           config.MaxRequestBytes <= std::numeric_limits<int>::max() &&
           config.MaxInflightRequests && config.MaxInflightBytes;
}

ui64 GetDataReservationBytes(ui64 payloadBytes, ui32 blocksCount)
{
    return payloadBytes +
           static_cast<ui64>(blocksCount) * sizeof(TBlockDataRef);
}

NProto::TError ValidateRange(const TString& diskId, ui64 start, ui32 count)
{
    if (diskId.empty() || !count ||
        start > std::numeric_limits<ui64>::max() - (count - 1))
    {
        return MakeError(E_ARGUMENT, "invalid IC V2 data range");
    }
    return {};
}

TResultOrError<ui64> ValidateDataRange(const TString& diskId, ui64 start,
                                       ui32 count, ui32 blockSize,
                                       const TDataConfig& config)
{
    if (auto error = ValidateRange(diskId, start, count); HasError(error)) {
        return error;
    }
    const ui64 bytes = static_cast<ui64>(count) * blockSize;
    if (!blockSize || bytes > config.MaxRequestBytes ||
        bytes > std::numeric_limits<size_t>::max())
    {
        return MakeError(E_ARGUMENT, "invalid IC V2 data size");
    }
    return bytes;
}

}   // namespace NCloud::NBlockStore::NIcV2
