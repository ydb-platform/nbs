#include "helpers.h"

#include <library/cpp/actors/core/log.h>

namespace NCloud {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString EventInfo(TAutoPtr<IEventHandle>& ev)
{
    return ev->GetTypeName();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void HandleUnexpectedEvent(
    TAutoPtr<IEventHandle>& ev,
    int component)
{
#if defined(NDEBUG)
    LogUnexpectedEvent(ev, component);
#else
    Y_FAIL(
        "[%s] Unexpected event: (0x%08X) %s",
        TlsActivationContext->LoggerSettings()->ComponentName(component),
        ev->GetTypeRewrite(),
        EventInfo(ev).c_str());
#endif
}

void LogUnexpectedEvent(
    TAutoPtr<IEventHandle>& ev,
    int component)
{
    LOG_ERROR(*TlsActivationContext, component,
        "Unexpected event: (0x%08X) %s",
        ev->GetTypeRewrite(),
        EventInfo(ev).c_str());
}

}   // namespace NCloud
