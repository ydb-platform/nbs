#pragma once

#include "public.h"

#include "filestore.h"

#include <cloud/filestore/libs/service/context.h>

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

#undef FILESTORE_IMPLEMENT_METHOD
};

}   // namespace NCloud::NFileStore
