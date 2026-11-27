#include "peer_request_sender.h"

#include <util/generic/scope.h>
#include <util/system/yassert.h>

#include <atomic>

namespace NCloud::NBlockStore::NIcV2 {

using namespace NThreading;

using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::TResponseFuture;
using NCloud::NIcV2::TResponseResult;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError RejectTransportError(NProto::TError error)
{
    error.SetCode(E_REJECTED);
    return error;
}

TDataResponseResult ClassifyChannelResult(
    TResponseResult result,
    std::weak_ptr<IRequestChannel> source)
{
    const auto& error = result.GetError();
    if (HasError(error)) {
        const auto code = error.GetCode();
        if (code == E_REJECTED || code == E_ARGUMENT || code == E_TIMEOUT) {
            return error;
        }
        return RejectTransportError(error);
    }
    return TDataResponse{
        .Event = result.ExtractResult(),
        .SourceChannel = std::move(source),
    };
}

////////////////////////////////////////////////////////////////////////////////

class TPeerRequestSender final
    : public IDataRequestSender
    , public std::enable_shared_from_this<TPeerRequestSender>
{
    enum class EPhase : ui32
    {
        Running,
        Stopping,
        Stopped,
    };

    struct TState
    {
        ui32 Operations = 0;
        EPhase Phase = EPhase::Running;
    };

    static_assert(std::atomic<TState>::is_always_lock_free);
    const IPeerPtr Peer;

    std::atomic<TState> State{TState{}};
    TPromise<void> Closed = NewPromise<void>();

public:
    explicit TPeerRequestSender(IPeerPtr peer)
        : Peer(std::move(peer))
    {}

    TDataResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        Y_DEBUG_ABORT_UNLESS(event);

        const auto keepAlive = shared_from_this();
        if (!TryReserveRequest()) {
            return MakeFuture<TDataResponseResult>(MakeClosedError());
        }
        Y_DEFER
        {
            ReleaseOperation();
        };

        auto promise = NewPromise<TDataResponseResult>();
        auto response = promise.GetFuture();
        const auto channelFuture = Peer->GetRequestChannel();
        if (!IsRunning()) {
            CompleteRequest(std::move(promise), MakeClosedError());
            return response;
        }
        if (!channelFuture.HasValue()) {
            CompleteRequest(
                std::move(promise),
                MakeError(E_UNAVAILABLE, "IC V2 peer is connecting"));
            return response;
        }

        const auto& channelResult = channelFuture.GetValue();
        if (const auto& error = channelResult.GetError(); HasError(error)) {
            CompleteRequest(
                std::move(promise),
                IsConnectionError(error) ? error : RejectTransportError(error));
            return response;
        }

        const auto& channel = channelResult.GetResult();
        channel->Request(std::move(event), timeout)
            .Subscribe(
                [self = keepAlive,
                 promise = std::move(promise),
                 source = std::weak_ptr<IRequestChannel>(channel)]   //
                (TResponseFuture future) mutable
                {
                    self->CompleteRequest(
                        std::move(promise),
                        ClassifyChannelResult(
                            future.ExtractValue(),
                            std::move(source)));
                });
        return response;
    }

    TFuture<void> Close() override
    {
        auto closed = Closed.GetFuture();
        if (!TryBeginStop()) {
            return closed;
        }

        const auto keepAlive = shared_from_this();
        Peer->Close();
        ReleaseOperation();
        return closed;
    }

private:
    static NProto::TError MakeClosedError()
    {
        return MakeError(E_REJECTED, "IC V2 peer request channel is closed");
    }

    void CompleteRequest(
        TPromise<TDataResponseResult> promise,
        TDataResponseResult result)
    {
        promise.SetValue(std::move(result));
        ReleaseOperation();
    }

    bool TryReserveRequest()
    {
        auto state = State.load();
        while (state.Phase == EPhase::Running) {
            auto next = state;
            // One operation for Request(), one until result delivery finishes.
            next.Operations += 2;
            if (State.compare_exchange_weak(state, next)) {
                return true;
            }
        }
        return false;
    }

    bool TryBeginStop()
    {
        auto state = State.load();
        while (state.Phase == EPhase::Running) {
            auto next = state;
            next.Phase = EPhase::Stopping;
            ++next.Operations;
            if (State.compare_exchange_weak(state, next)) {
                return true;
            }
        }
        return false;
    }

    bool IsRunning() const
    {
        return State.load().Phase == EPhase::Running;
    }

    void ReleaseOperation()
    {
        auto state = State.load();
        for (;;) {
            Y_ABORT_UNLESS(state.Operations);

            auto next = state;
            --next.Operations;
            if (next.Phase == EPhase::Stopping && !next.Operations) {
                next.Phase = EPhase::Stopped;
            }
            if (State.compare_exchange_weak(state, next)) {
                if (next.Phase == EPhase::Stopped) {
                    Closed.SetValue();
                }
                return;
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDataRequestSenderPtr CreateDataRequestSender(IPeerPtr peer)
{
    Y_DEBUG_ABORT_UNLESS(peer);

    return std::make_shared<TPeerRequestSender>(std::move(peer));
}

}   // namespace NCloud::NBlockStore::NIcV2
