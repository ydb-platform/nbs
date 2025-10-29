#pragma once

#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <ydb/core/testlib/basics/runtime.h>

namespace NCloud::NFileStore {

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

    TProgramShouldContinue& GetProgramShouldContinue() override;

    //
    // Helpers
    //

    void DispatchEvents(TDuration timeout);

    void RegisterTestService(NActors::IActorPtr serviceActor);
    void RegisterTestAuthorizer(NActors::IActorPtr authorizer);
};

}   // namespace NCloud::NFileStore
