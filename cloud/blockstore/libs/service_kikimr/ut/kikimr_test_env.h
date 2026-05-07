#pragma once

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TTestActorSystem final
    : public IActorSystem
{
private:
    std::unique_ptr<NKikimr::TTestBasicRuntime> Runtime;
    NActors::TActorId Sender;

    TProgramShouldContinue ProgramShouldContinue;

public:
    TTestActorSystem();

    //
    // IStartable
    //

    void Start() override;
    void Stop() override;

    //
    // ILoggingService
    //

    TLog CreateLog(const TString& component) override;

    //
    // IMonitoringService
    //

    NMonitoring::IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override;

    void RegisterMonPage(NMonitoring::IMonPagePtr page) override;
    NMonitoring::IMonPagePtr GetMonPage(const TString& path) override;

    NMonitoring::TDynamicCountersPtr GetCounters() override;

    //
    // IActorSystem
    //

    NActors::TActorId Register(
        NActors::IActorPtr actor,
        TStringBuf executorName = {}) override;
    bool Send(
        const NActors::TActorId& recipient,
        NActors::IEventBasePtr event) override;
    bool Send(NActors::IEventHandlePtr event) override;
    bool Send(TAutoPtr<NActors::IEventHandle> ev) override;
    void Schedule(
        TDuration delta,
        std::unique_ptr<NActors::IEventHandle> ev,
        NActors::ISchedulerCookie* cookie) override;
    NActors::NLog::TSettings* LoggerSettings() const override;
    TProgramShouldContinue& GetProgramShouldContinue() override;

    //
    // Helpers
    //

    void DispatchEvents(TDuration timeout);

    void RegisterTestService(NActors::IActorPtr serviceActor);
    void RegisterTestAuthorizer(NActors::IActorPtr authorizer);
};

}   // namespace NCloud::NBlockStore::NServer
