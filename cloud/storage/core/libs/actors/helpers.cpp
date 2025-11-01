#include "helpers.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString EventInfo(const IEventHandle& ev)
{
    return ev.GetTypeName();
}

void LogUnexpectedEvent(
    const IEventHandle& ev,
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
    const IEventHandle& ev,
    int component,
    const TString& location)
{
    ReportUnexpectedEvent(Sprintf(
        "[%s] Unexpected event: (0x%08X) %s, %s",
        TlsActivationContext->LoggerSettings()->ComponentName(component),
        ev.GetTypeRewrite(),
        EventInfo(ev).c_str(),
        location.c_str()));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////


void HandleUnexpectedEvent(
    const TAutoPtr<IEventHandle>& ev,
    int component,
    const TString& location)
{
    HandleUnexpectedEvent(*ev, component, location);
}

void HandleUnexpectedEvent(
    const NActors::IEventHandlePtr& ev,
    int component,
    const TString& location)
{
    HandleUnexpectedEvent(*ev, component, location);
}

void LogUnexpectedEvent(
    const TAutoPtr<IEventHandle>& ev,
    int component,
    const TString& location)
{
    LogUnexpectedEvent(*ev, component, location);
}

}   // namespace NCloud
