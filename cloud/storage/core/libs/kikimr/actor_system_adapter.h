#pragma once

#include "actorsystem.h"

#include <contrib/ydb/library/actors/core/actorsystem.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TActorSystemAdapter final: public NCloud::IActorSystem
{
    NActors::TActorSystem* const ActorSystem;
    TProgramShouldContinue ProgramShouldContinue;

public:
    explicit TActorSystemAdapter(NActors::TActorSystem* sys);

    void Start() override;

    void Stop() override;

    TLog CreateLog(const TString& component) override;

    NMonitoring::IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override;

    void RegisterMonPage(NMonitoring::IMonPagePtr page) override;
    NMonitoring::IMonPagePtr GetMonPage(const TString& path) override;

    NMonitoring::TDynamicCountersPtr GetCounters() override;

    NActors::TActorId Register(
        NActors::IActorPtr actor,
        TStringBuf executorName) override;

    bool Send(
        const NActors::TActorId& recipient,
        NActors::IEventBasePtr event) override;

    bool Send(NActors::IEventHandlePtr ev) override;

    bool Send(TAutoPtr<NActors::IEventHandle> ev) override;

    void Schedule(
        TDuration delta,
        std::unique_ptr<NActors::IEventHandle> ev,
        NActors::ISchedulerCookie* cookie) override;

    NActors::NLog::TSettings* LoggerSettings() const override;

    TProgramShouldContinue& GetProgramShouldContinue() override;

    void DeferPreStop(std::function<void()> fn) override;
};

}   // namespace NCloud
