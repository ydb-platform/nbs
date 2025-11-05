#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/event.h>
#include <contrib/ydb/library/actors/core/event_local.h>

#include <util/generic/string.h>
#include <util/generic/typetraits.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TEmpty
{
    static TString Name()
    {
        return "Empty";
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TRequestEvent
    : public NActors::TEventLocal<TRequestEvent<TArgs, EventId>, EventId>
    , public TArgs
{
    TRequestEvent() = default;

    template <typename ...Args>
    TRequestEvent(Args&& ...args)
        : TArgs(std::forward<Args>(args)...)
    {
    }

    static TString Name()
    {
        return TypeName<TArgs>();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TResponseEvent
    : public NActors::TEventLocal<TResponseEvent<TArgs, EventId>, EventId>
    , public TArgs
{
    const NProto::TError Error;

    TResponseEvent() = default;

    template <
        typename T1,
        typename = std::enable_if_t<
            !std::is_convertible<T1, NProto::TError>::value &&
            !std::is_same<std::decay_t<T1>, TResponseEvent<TArgs, EventId>>::
                value>,
        typename... Args>
    explicit TResponseEvent(T1&& a1, Args&&... args)
        : TArgs(std::forward<T1>(a1), std::forward<Args>(args)...)
    {}

    template <typename... Args>
    explicit TResponseEvent(NProto::TError error, Args&&... args)
        : TArgs(std::forward<Args>(args)...)
        , Error(std::move(error))
    {}

    const NProto::TError& GetError() const
    {
        return Error;
    }

    ui32 GetStatus() const
    {
        return GetError().GetCode();
    }

    const TString& GetErrorReason() const
    {
        return GetError().GetMessage();
    }

    static TString Name()
    {
        return TypeName<TArgs>();
    }
};

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_DECLARE_EVENT_IDS(name, ...)                                   \
        Ev##name##Request,                                                     \
        Ev##name##Response,                                                    \
// BLOCKSTORE_DECLARE_EVENT_IDS

#define STORAGE_DECLARE_REQUEST(name, ...)                                     \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr const char* Name = #name;                             \
                                                                               \
        using TRequest = TEv##name##Request;                                   \
        using TResponse = TEv##name##Response;                                 \
    };                                                                         \
// STORAGE_DECLARE_REQUEST

#define STORAGE_DECLARE_EVENTS(name, ...)                                      \
    using TEv##name##Request = TRequestEvent<                                  \
        T##name##Request,                                                      \
        Ev##name##Request                                                      \
    >;                                                                         \
                                                                               \
    using TEv##name##Response = TResponseEvent<                                \
        T##name##Response,                                                     \
        Ev##name##Response                                                     \
    >;                                                                         \
                                                                               \
    STORAGE_DECLARE_REQUEST(name, __VA_ARGS__)                                 \
// STORAGE_DECLARE_EVENTS

}   // namespace NCloud
