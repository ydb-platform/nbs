#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEmpty
{
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TRequestEvent
    : public NActors::TEventLocal<TRequestEvent<TArgs, EventId>, EventId>
    , public TArgs
{
    TCallContextPtr CallContext = MakeIntrusive<TCallContext>(RandomNumber<ui64>());

    TRequestEvent() = default;

    template <typename ...Args>
    TRequestEvent(TCallContextPtr callContext, Args&& ...args)
        : TArgs(std::forward<Args>(args)...)
        , CallContext(std::move(callContext))
    {
        Y_DEBUG_ABORT_UNLESS(CallContext);
    }

    template <typename ...Args>
    TRequestEvent(Args&& ...args)
        : TArgs(std::forward<Args>(args)...)
    {
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

    template <typename T1, typename = std::enable_if_t<!std::is_convertible<T1, NProto::TError>::value>, typename ...Args>
    TResponseEvent(T1&& a1, Args&& ...args)
        : TArgs(std::forward<T1>(a1), std::forward<Args>(args)...)
    {}

    template <typename ...Args>
    TResponseEvent(const NProto::TError& error, Args&& ...args)
        : TArgs(std::forward<Args>(args)...)
        , Error(error)
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
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TProtoRequestEvent
    : public NActors::TEventPB<TProtoRequestEvent<TArgs, EventId>, TArgs, EventId>
{
    TCallContextPtr CallContext = MakeIntrusive<TCallContext>();

    TProtoRequestEvent() = default;

    TProtoRequestEvent(TCallContextPtr callContext)
        : CallContext(std::move(callContext))
    {
        Y_DEBUG_ABORT_UNLESS(CallContext);
    }

    template <typename T>
    TProtoRequestEvent(TCallContextPtr callContext, T&& proto)
        : CallContext(std::move(callContext))
    {
        Y_DEBUG_ABORT_UNLESS(CallContext);
        TProtoRequestEvent::Record = std::forward<T>(proto);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TProtoResponseEvent
    : public NActors::TEventPB<TProtoResponseEvent<TArgs, EventId>, TArgs, EventId>
{
    TProtoResponseEvent() = default;

    template <typename T, typename = std::enable_if_t<!std::is_convertible<T, NProto::TError>::value>>
    TProtoResponseEvent(T&& proto)
    {
        TProtoResponseEvent::Record = std::forward<T>(proto);
    }

    TProtoResponseEvent(const NProto::TError& error)
    {
        if (error.GetCode() != 0 || !error.GetMessage().empty()) {
            *TProtoResponseEvent::Record.MutableError() = error;
        }
    }

    const NProto::TError& GetError() const
    {
        return TProtoResponseEvent::Record.GetError();
    }

    ui32 GetStatus() const
    {
        return GetError().GetCode();
    }

    const TString& GetErrorReason() const
    {
        return GetError().GetMessage();
    }
};

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_EVENT_IDS(name, ...)                                 \
        Ev##name##Request,                                                     \
        Ev##name##Response,                                                    \
// FILESTORE_DECLARE_EVENT_IDS

#define FILESTORE_DECLARE_REQUEST(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr const char* Name = #name;                             \
                                                                               \
        using TRequest = TEv##name##Request;                                   \
        using TResponse = TEv##name##Response;                                 \
    };                                                                         \
// FILESTORE_DECLARE_REQUEST

#define FILESTORE_DECLARE_EVENTS(name, ...)                                    \
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
    FILESTORE_DECLARE_REQUEST(name, __VA_ARGS__)                               \
// FILESTORE_DECLARE_EVENTS

#define FILESTORE_DECLARE_PROTO_EVENTS(name, ns, ...)                          \
    using TEv##name##Request = TProtoRequestEvent<                             \
        ns::T##name##Request,                                                  \
        Ev##name##Request                                                      \
    >;                                                                         \
                                                                               \
    using TEv##name##Response = TProtoResponseEvent<                           \
        ns::T##name##Response,                                                 \
        Ev##name##Response                                                     \
    >;                                                                         \
                                                                               \
    FILESTORE_DECLARE_REQUEST(name, __VA_ARGS__)                               \
// FILESTORE_DECLARE_PROTO_EVENTS

#define FILESTORE_IMPLEMENT_REQUEST(name, ns)                                  \
    void Handle##name(                                                         \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const NActors::TActorContext& ctx);                                    \
                                                                               \
    void Reject##name(                                                         \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const NActors::TActorContext& ctx)                                     \
    {                                                                          \
        auto response = std::make_unique<ns::TEv##name##Response>(             \
            MakeError(E_REJECTED, #name " request rejected"));                 \
        NCloud::Reply(ctx, *ev, std::move(response));                          \
    }                                                                          \
                                                                               \
    void Reject##name##ByBrokenTablet(                                         \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const NActors::TActorContext& ctx)                                     \
    {                                                                          \
        auto response = std::make_unique<ns::TEv##name##Response>(             \
            MakeError(E_REJECTED, #name " request rejected: broken tablet"));  \
        NCloud::Reply(ctx, *ev, std::move(response));                          \
    }                                                                          \
// FILESTORE_IMPLEMENT_REQUEST

#define FILESTORE_HANDLE_REQUEST(name, ns)                                     \
    HFunc(ns::TEv##name##Request, Handle##name);                               \
// FILESTORE_HANDLE_REQUEST

#define FILESTORE_HANDLE_RESPONSE(name, ns)                                    \
    HFunc(ns::TEv##name##Response, Handle##name);                              \
// FILESTORE_HANDLE_RESPONSE

#define FILESTORE_REJECT_REQUEST(name, ns)                                     \
    HFunc(ns::TEv##name##Request, Reject##name);                               \
// FILESTORE_REJECT_REQUEST

#define FILESTORE_REJECT_REQUEST_BY_BROKEN_TABLET(name, ns)                    \
    HFunc(ns::TEv##name##Request, Reject##name##ByBrokenTablet);               \
// FILESTORE_REJECT_REQUEST_BY_BROKEN_TABLET

}   // namespace NCloud::NFileStore::NStorage
