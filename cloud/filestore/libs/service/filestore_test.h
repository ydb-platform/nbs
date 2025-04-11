#pragma once

#include "public.h"

#include "filestore.h"

#include <cloud/filestore/libs/service/context.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <functional>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TFileStoreTest
    : public IFileStoreService
{
    TMutex HandlerMutex;
    void Start() override
    {}

    void Stop() override
    {}

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    using T##name##Handler = std::function<                                    \
        NThreading::TFuture<NProto::T##name##Response>(                        \
            TCallContextPtr callContext,                                       \
            std::shared_ptr<NProto::T##name##Request>)                         \
        >;                                                                     \
                                                                               \
    T##name##Handler name##Handler;                                            \
                                                                               \
    NThreading::TFuture<NProto::T##name##Response> name(                       \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        with_lock(HandlerMutex) {                                              \
            return name##Handler(std::move(callContext), std::move(request));  \
        }                                                                      \
    }                                                                          \
                                                                               \
    void SetHandler##name(const std::function<                                 \
        NThreading::TFuture<NProto::T##name##Response>(                        \
            TCallContextPtr callContext,                                       \
            std::shared_ptr<NProto::T##name##Request>)                         \
        >& funcHandler)                                                        \
    {                                                                          \
        with_lock(HandlerMutex) {                                              \
            name##Handler = funcHandler;                                       \
        }                                                                      \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_SERVICE(FILESTORE_IMPLEMENT_METHOD)
    FILESTORE_IMPLEMENT_METHOD(ReadDataLocal)
    FILESTORE_IMPLEMENT_METHOD(WriteDataLocal)

#undef FILESTORE_IMPLEMENT_METHOD

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    using T##name##StreamHandler = std::function<void(                         \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request>,                             \
        IResponseHandlerPtr<NProto::T##name##Response> responseHandler)        \
    >;                                                                         \
                                                                               \
    T##name##StreamHandler name##StreamHandler;                                \
                                                                               \
    void name##Stream(                                                         \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request,                     \
    IResponseHandlerPtr<NProto::T##name##Response> responseHandler) override   \
    {                                                                          \
        return name##StreamHandler(                                            \
            std::move(callContext),                                            \
            std::move(request),                                                \
            std::move(responseHandler));                                       \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_IMPLEMENT_METHOD(GetSessionEvents)

#undef FILESTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TResponseHandler final
    : public IResponseHandler<NProto::TGetSessionEventsResponse>
{
private:
    TVector<NProto::TSessionEvent> Events;
    TMaybe<NProto::TError> Error;
    TMutex Lock;

public:
    void HandleResponse(const NProto::TGetSessionEventsResponse& response) override
    {
        with_lock (Lock) {
            for (const auto& event: response.GetEvents()) {
                Events.push_back(event);
            }
        }
    }

    bool GotResponse() const
    {
        with_lock (Lock) {
            return !Events.empty();
        }
    }

    void HandleCompletion(const NProto::TError& error) override
    {
        with_lock (Lock) {
            Error = error;
        }
    }

    bool GotCompletion() const
    {
        with_lock (Lock) {
            return !Error.Empty();
        }
    }
};

}   // namespace NCloud::NFileStore
