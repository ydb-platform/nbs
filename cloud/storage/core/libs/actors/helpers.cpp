#include "helpers.h"

#include <library/cpp/actors/core/log.h>

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

void LogUnexpectedEvent(
    TAutoPtr<IEventHandle>& ev,
    int component)
{
    LogUnexpectedEvent(*ev, component);
}

}   // namespace NCloud
