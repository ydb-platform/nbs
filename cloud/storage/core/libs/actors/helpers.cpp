#include "helpers.h"

#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString EventInfo(IEventHandle& ev)
{
    return ev.GetTypeName();
}

void LogUnexpectedEvent(
    IEventHandle& ev,
    int component)
{
    LOG_ERROR(*TlsActivationContext, component,
        "Unexpected event: (0x%08X) %s",
        ev.GetTypeRewrite(),
        EventInfo(ev).c_str());
}

#ifndef NDEBUG
[[maybe_unused]]
#endif
void LogUnexpectedEvent(
    IEventHandle& ev,
    int component,
    const TString& location)
{
    LOG_ERROR(
        *TlsActivationContext,
        component,
        "Unexpected event: (0x%08X) %s, %s",
        ev.GetTypeRewrite(),
        EventInfo(ev).c_str(),
        location.c_str());
}

void HandleUnexpectedEvent(
    IEventHandle& ev,
    int component)
{
#if defined(NDEBUG)
    LogUnexpectedEvent(ev, component);
#else
    Y_ABORT(
        "[%s] Unexpected event: (0x%08X) %s",
        TlsActivationContext->LoggerSettings()->ComponentName(component),
        ev.GetTypeRewrite(),
        EventInfo(ev).c_str());
#endif
}

void HandleUnexpectedEvent(
    IEventHandle& ev,
    int component,
    TSourceLocation location)
{
#if defined(NDEBUG)
    LogUnexpectedEvent(ev, component, ToString(location));
#else
    Y_ABORT(
        "[%s] Unexpected event: (0x%08X) %s, %s",
        TlsActivationContext->LoggerSettings()->ComponentName(component),
        ev.GetTypeRewrite(),
        EventInfo(ev).c_str(),
        ToString(location).c_str());
#endif
}


}   // namespace

////////////////////////////////////////////////////////////////////////////////

void HandleUnexpectedEvent(
    TAutoPtr<IEventHandle>& ev,
    int component)
{
    HandleUnexpectedEvent(*ev, component);
}

void HandleUnexpectedEvent(
    NActors::IEventHandlePtr& ev,
    int component)
{
    HandleUnexpectedEvent(*ev, component);
}

void HandleUnexpectedEvent(
    TAutoPtr<IEventHandle>& ev,
    int component,
    const TSourceLocation& location)
{
    HandleUnexpectedEvent(*ev, component, location);
}

void HandleUnexpectedEvent(
    NActors::IEventHandlePtr& ev,
    int component,
    const TSourceLocation& location)
{
    HandleUnexpectedEvent(*ev, component, location);
}

void LogUnexpectedEvent(
    TAutoPtr<IEventHandle>& ev,
    int component)
{
    LogUnexpectedEvent(*ev, component);
}

}   // namespace NCloud
