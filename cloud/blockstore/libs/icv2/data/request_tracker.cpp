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

TRequestTracker::TRequestTracker(TDataConfig config)
    : Config(config)
{}

TResultOrError<std::shared_ptr<TRequestTracker::TReservation>>
TRequestTracker::Acquire(ui64 bytes)
{
    auto state = State.load();
    for (;;) {
        if (state.Phase != EState::Running) {
            return MakeError(E_CANCELLED, "IC V2 data owner is stopping");
        }
        if (state.Requests >= Config.MaxInflightRequests) {
            return MakeError(E_REJECTED, "IC V2 data admission limit reached");
        }
        auto next = state;
        ++next.Requests;
        if (State.compare_exchange_weak(state, next)) {
            break;
        }
    }

    if (auto error = ReserveBytes(bytes); HasError(error)) {
        Release(0);
        return error;
    }
    return std::make_shared<TReservation>(shared_from_this(), bytes);
}

NProto::TError TRequestTracker::ReserveBytes(ui64 bytes)
{
    if (auto error = CheckRunning(); HasError(error)) {
        return error;
    }
    if (!bytes) {
        return {};
    }
    auto current = Bytes.load();
    for (;;) {
        if (bytes > Config.MaxInflightBytes - current) {
            return MakeError(E_REJECTED, "IC V2 data byte budget exceeded");
        }
        if (Bytes.compare_exchange_weak(current, current + bytes)) {
            return {};
        }
    }
}

void TRequestTracker::Release(ui64 bytes)
{
    const auto previousBytes = Bytes.fetch_sub(bytes);
    Y_DEBUG_ABORT_UNLESS(previousBytes >= bytes);

    auto state = State.load();
    for (;;) {
        Y_DEBUG_ABORT_UNLESS(state.Requests);

        auto next = state;
        --next.Requests;
        if (next.Phase == EState::Stopping && !next.Requests) {
            next.Phase = EState::Stopped;
        }
        if (State.compare_exchange_weak(state, next)) {
            if (next.Phase == EState::Stopped) {
                Y_DEBUG_ABORT_UNLESS(!Bytes.load());

                Closed.SetValue();
            }
            return;
        }
    }
}

NProto::TError TRequestTracker::CheckRunning() const
{
    if (State.load().Phase != EState::Running) {
        return MakeError(E_CANCELLED, "IC V2 data owner is stopping");
    }
    return {};
}

void TRequestTracker::RequestStop()
{
    auto state = State.load();
    while (state.Phase == EState::Running) {
        auto next = state;
        next.Phase = next.Requests ? EState::Stopping : EState::Stopped;
        if (State.compare_exchange_weak(state, next)) {
            if (next.Phase == EState::Stopped) {
                Y_DEBUG_ABORT_UNLESS(!Bytes.load());

                Closed.SetValue();
            }
            return;
        }
    }
}

void TRequestTracker::Stop()
{
    RequestStop();
    Closed.GetFuture().GetValueSync();
}

}   // namespace NCloud::NBlockStore::NIcV2
