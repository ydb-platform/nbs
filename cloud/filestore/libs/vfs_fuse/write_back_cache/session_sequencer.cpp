#include "session_sequencer.h"

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TSessionSequencer::TSessionSequencer(IFileStorePtr session)
    : Session(std::move(session))
{}

TFuture<NProto::TReadDataResponse> TSessionSequencer::ReadData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadDataRequest> protoRequest)
{
    const auto handle = protoRequest->GetHandle();
    const auto offset = protoRequest->GetOffset();
    const auto length = protoRequest->GetLength();

    using TResponse = NProto::TReadDataResponse;

    struct TState
    {
        TPromise<TResponse> Promise = NewPromise<TResponse>();
    };
    auto state = std::make_shared<TState>();

    auto execute = [
        session = Session,
        callContext = std::move(callContext),
        protoRequest = std::move(protoRequest),
        state] ()
    {
        session->ReadData(
            std::move(callContext),
            std::move(protoRequest))
                .Apply([state] (auto future)
                    {
                        state->Promise.SetValue(future.ExtractValue());
                    });
    };

    const auto id = ++NextRequestId;

    auto future = state->Promise.GetFuture();
    future.Subscribe([ptr = weak_from_this(), id] (auto) {
        if (auto self = ptr.lock()) {
            self->OnRequestFinished(id);
        }
    });

    TRequest request = {
        .Handle = handle,
        .Offset = offset,
        .Length = length,

        .Id = id,
        .Execute = std::move(execute),
        .IsRead = true
    };
    QueueOrExecuteRequest(std::move(request));
    return future;
}

TFuture<NProto::TWriteDataResponse> TSessionSequencer::WriteData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDataRequest> protoRequest)
{
    const auto handle = protoRequest->GetHandle();
    const auto offset = protoRequest->GetOffset();
    const auto length = protoRequest->GetBuffer().length();

    using TResponse = NProto::TWriteDataResponse;

    struct TState
    {
        TPromise<TResponse> Promise = NewPromise<TResponse>();
    };
    auto state = std::make_shared<TState>();

    auto execute = [
        session = Session,
        callContext = std::move(callContext),
        protoRequest = std::move(protoRequest),
        state] ()
    {
        session->WriteData(
            std::move(callContext),
            std::move(protoRequest))
                .Apply([state] (auto future)
                    {
                        state->Promise.SetValue(future.ExtractValue());
                    });
    };

    const auto id = ++NextRequestId;

    auto future = state->Promise.GetFuture();
    future.Subscribe([ptr = weak_from_this(), id] (auto) {
        if (auto self = ptr.lock()) {
            self->OnRequestFinished(id);
        }
    });

    TRequest request = {
        .Handle = handle,
        .Offset = offset,
        .Length = length,

        .Id = id,
        .Execute = std::move(execute),
        .IsRead = false
    };
    QueueOrExecuteRequest(std::move(request));
    return future;
}

// should be protected by |Lock|
bool TSessionSequencer::CanExecuteRequest(const TRequest& request) const
{
    for (const auto& [_, otherRequest]: InFlightRequests) {
        if (request.Handle != otherRequest.Handle) {
            // TODO(svartmetal): optimise, use separate buckets for
            // different handles
            //
            // requests to different handles should not affect each other
            continue;
        }

        if (request.IsRead && otherRequest.IsRead) {
            // skip this overlapping as it does not cause inconsistency
            continue;
        }

        const auto offset = Max(request.Offset, otherRequest.Offset);
        const auto end = Min(
            request.Offset + request.Length,
            otherRequest.Offset + otherRequest.Length);

        if (offset < end) {
            // overlaps with one of the requests in-flight
            return false;
        }
    }

    return true;
}

// should be protected by |Lock|
auto TSessionSequencer::TakeNextRequestToExecute() -> std::optional<TRequest>
{
    const auto begin = WaitingRequests.begin();
    const auto end = WaitingRequests.end();

    for (auto it = begin; it != end; it++) {
        if (CanExecuteRequest(*it)) {
            auto taken = std::move(*it);
            WaitingRequests.erase(it);
            return std::move(taken);
        }
    }

    return std::nullopt;
}

void TSessionSequencer::OnRequestFinished(ui64 id)
{
    TVector<TRequest> nextRequests;

    with_lock (Lock) {
        Y_DEBUG_ABORT_UNLESS(InFlightRequests.count(id) == 1);
        InFlightRequests.erase(id);

        while (auto request = TakeNextRequestToExecute()) {
            InFlightRequests[request->Id] = *request;
            nextRequests.push_back(std::move(*request));
        }
    }

    for (const auto& request: nextRequests) {
        request.Execute();
    }
}

void TSessionSequencer::QueueOrExecuteRequest(TRequest request)
{
    bool shouldExecute = false;

    with_lock (Lock) {
        if (CanExecuteRequest(request)) {
            InFlightRequests[request.Id] = request;
            shouldExecute = true;
        } else {
            WaitingRequests.push_back(std::move(request));
        }
    }

    if (shouldExecute) {
        request.Execute();
    }
}

////////////////////////////////////////////////////////////////////////////////

TSessionSequencerPtr CreateSessionSequencer(IFileStorePtr session)
{
    return std::make_shared<TSessionSequencer>(std::move(session));
}

}   // namespace NCloud::NFileStore::NFuse
