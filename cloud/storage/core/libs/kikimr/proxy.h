#pragma once

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TLoggingProxy final: public ILoggingService
{
private:
    IActorSystemPtr ActorSystem;

public:
    void Init(IActorSystemPtr actorSystem)
    {
        ActorSystem = std::move(actorSystem);
    }

    void Start() override
    {
        Y_ABORT_UNLESS(ActorSystem);
    }

    void Stop() override
    {
        ActorSystem.Reset();
    }

    TLog CreateLog(const TString& component) override;
};

////////////////////////////////////////////////////////////////////////////////

class TMonitoringProxy final: public IMonitoringService
{
private:
    IActorSystemPtr ActorSystem;

public:
    void Init(IActorSystemPtr actorSystem)
    {
        ActorSystem = std::move(actorSystem);
    }

    void Start() override
    {
        Y_ABORT_UNLESS(ActorSystem);
    }

    void Stop() override
    {
        ActorSystem.Reset();
    }

    void RegisterMonPage(NMonitoring::IMonPagePtr page) override;
    NMonitoring::TDynamicCountersPtr GetCounters() override;
    NMonitoring::IMonPagePtr GetMonPage(const TString& path) override;
    NMonitoring::IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override;
};

}   // namespace NCloud::NStorage
