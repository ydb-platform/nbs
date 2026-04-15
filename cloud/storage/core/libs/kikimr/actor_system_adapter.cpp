#include "actor_system_adapter.h"

#include <library/cpp/monlib/service/pages/mon_page.h>

using namespace NActors;

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TActorSystemAdapter::TActorSystemAdapter(NActors::TActorSystem* sys)
    : ActorSystem(sys)
{}

void TActorSystemAdapter::Start()
{}

void TActorSystemAdapter::Stop()
{}

TLog TActorSystemAdapter::CreateLog(const TString& component)
{
    Y_UNUSED(component);
    return {};
}

NMonitoring::IMonPagePtr TActorSystemAdapter::RegisterIndexPage(
    const TString& path,
    const TString& title)
{
    Y_UNUSED(path);
    Y_UNUSED(title);
    return {};
}

void TActorSystemAdapter::RegisterMonPage(NMonitoring::IMonPagePtr page)
{
    Y_UNUSED(page);
}

NMonitoring::IMonPagePtr TActorSystemAdapter::GetMonPage(const TString& path)
{
    Y_UNUSED(path);
    return {};
}

NMonitoring::TDynamicCountersPtr TActorSystemAdapter::GetCounters()
{
    return {};
}

NActors::TActorId TActorSystemAdapter::Register(
    NActors::IActorPtr actor,
    TStringBuf executorName)
{
    Y_UNUSED(executorName);
    return ActorSystem->Register(actor.release());
}

bool TActorSystemAdapter::Send(
    const NActors::TActorId& recipient,
    NActors::IEventBasePtr event)
{
    return ActorSystem->Send(recipient, event.release());
}

bool TActorSystemAdapter::Send(NActors::IEventHandlePtr ev)
{
    return ActorSystem->Send(ev.release());
}

bool TActorSystemAdapter::Send(TAutoPtr<NActors::IEventHandle> ev)
{
    return ActorSystem->Send(ev);
}

void TActorSystemAdapter::Schedule(
    TDuration delta,
    std::unique_ptr<NActors::IEventHandle> ev,
    NActors::ISchedulerCookie* cookie)
{
    ActorSystem->Schedule(delta, ev.release(), cookie);
}

NActors::NLog::TSettings* TActorSystemAdapter::LoggerSettings() const
{
    return ActorSystem->LoggerSettings();
}

TProgramShouldContinue& TActorSystemAdapter::GetProgramShouldContinue()
{
    Y_ABORT("Unimplemented");
    return ProgramShouldContinue;
}

void TActorSystemAdapter::DeferPreStop(std::function<void()> fn)
{
    ActorSystem->DeferPreStop(std::move(fn));
}

}   // namespace NCloud
