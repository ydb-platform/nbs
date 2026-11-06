#include "request_tracker.h"

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

TRequestTracker::TReservation::TReservation(
    std::shared_ptr<TRequestTracker> owner,
    ui64 bytes)
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
    TGuard<TMutex> guard(Mutex);
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
    TGuard<TMutex> guard(Mutex);
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
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(Requests && Bytes >= bytes);

        --Requests;
        Bytes -= bytes;
    }
    PublishClosed();
}

void TRequestTracker::RequestStop()
{
    {
        TGuard<TMutex> guard(Mutex);
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
        TGuard<TMutex> guard(Mutex);
        if (!Stopping || Requests || Published) {
            return;
        }
        Y_ABORT_UNLESS(!Bytes);

        Published = true;
    }
    Closed.SetValue();
}

}   // namespace NCloud::NBlockStore::NIcV2
