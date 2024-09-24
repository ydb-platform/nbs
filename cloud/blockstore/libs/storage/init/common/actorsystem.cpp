#include "actorsystem.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoring;

using namespace NKikimr;

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

TActorSystem::TActorSystem(const TActorSystemArgs& args)
    : TKikimrRunner(args.ModuleFactories)
    , Args(args)
    , Running(false)
{}

////////////////////////////////////////////////////////////////////////////////

void TActorSystem::Init()
{
    TKikimrRunConfig runConfig(
        *Args.AppConfig,
        Args.NodeId,
        TKikimrScopeId(Args.ScopeId));

    runConfig.AppConfig.MutableMonitoringConfig()->SetRedirectMainPageTo("");

    InitializeRegistries(runConfig);
    InitializeMonitoring(runConfig);
    InitializeAppData(runConfig);
    InitializeLogSettings(runConfig);

    Args.PrepareKikimrRunConfig(runConfig);

    LogSettings->Append(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    auto servicesMask = Args.ServicesMask;

    if (Args.AppConfig->HasAuthConfig()) {
        servicesMask.EnableSecurityServices = 1;
    }

#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
    servicesMask.EnableStatsCollector = 1;
#endif

    auto services = CreateServiceInitializersList(runConfig, servicesMask);

    Args.OnInitialize(runConfig, *services);

    InitializeActorSystem(runConfig, services, servicesMask);
}

void TActorSystem::Start()
{
    KikimrStart();
    Running = true;
    Args.OnStart(*this);
}

void TActorSystem::Stop()
{
    if (Running) {
        Send(MakeDiskAgentServiceId(Args.NodeId),
            std::make_unique<TEvents::TEvPoisonPill>());

        KikimrStop(false);
    }
}

TActorId TActorSystem::Register(IActorPtr actor, TStringBuf executorName)
{
    ui32 id = AppData->UserPoolId;
    if (executorName) {
        if (auto it = AppData->ServicePools.find(executorName);
            it != AppData->ServicePools.end())
        {
            id = it->second;
        }
    }

    return ActorSystem->Register(
        actor.release(),
        TMailboxType::Simple,
        id);
}

bool TActorSystem::Send(const TActorId& recipient, IEventBasePtr event)
{
    return ActorSystem->Send(recipient, event.release());
}

TLog TActorSystem::CreateLog(const TString& componentName)
{
    if (LogBackend) {
        TLogSettings logSettings;
        logSettings.UseLocalTimestamps = LogSettings->UseLocalTimestamps;
        logSettings.SuppressNewLine = true;   // kikimr will do it for us

        auto component = LogSettings->FindComponent(componentName);
        if (component != NLog::InvalidComponent) {
            auto settings = LogSettings->GetComponentSettings(component);
            logSettings.FiltrationLevel =
                static_cast<ELogPriority>(settings.Raw.X.Level);
        } else {
            logSettings.FiltrationLevel =
                static_cast<ELogPriority>(LogSettings->DefPriority);
        }

        return CreateComponentLog(
            componentName,
            LogBackend,
            Args.AsyncLogger,
            logSettings);
    }

    return {};
}

IMonPagePtr TActorSystem::RegisterIndexPage(
    const TString& path,
    const TString& title)
{
    if (Monitoring) {
        return Monitoring->RegisterIndexPage(path, title);
    } else {
        return IndexMonPage->RegisterIndexPage(path, title);
    }
}

void TActorSystem::RegisterMonPage(IMonPagePtr page)
{
    if (Monitoring) {
        Monitoring->Register(page.Release());
    } else {
        IndexMonPage->Register(page.Release());
    }
}

IMonPagePtr TActorSystem::GetMonPage(const TString& path)
{
    if (Monitoring) {
        return Monitoring->FindPage(path);
    } else {
        return IndexMonPage->FindPage(path);
    }
}

TDynamicCountersPtr TActorSystem::GetCounters()
{
    return Counters;
}

TProgramShouldContinue& TActorSystem::GetProgramShouldContinue()
{
    return TKikimrRunner::KikimrShouldContinue;
}

}   // namespace NCloud::NBlockStore::NStorage
