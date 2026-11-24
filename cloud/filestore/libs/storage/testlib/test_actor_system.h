#pragma once

#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/event.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// IActorSystem of one node of the test runtime.
class TTestActorSystem final
    : public IActorSystem
{
private:
    NActors::TTestActorRuntime& Runtime;
    const ui32 NodeIdx;

    TProgramShouldContinue ProgramShouldContinue;

public:
    TTestActorSystem(NActors::TTestActorRuntime& runtime, ui32 nodeIdx)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TLog CreateLog(const TString& component) override
    {
        Y_UNUSED(component);
        return {};
    }

    NMonitoring::IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override
    {
        Y_UNUSED(path);
        Y_UNUSED(title);
        return {};
    }

    void RegisterMonPage(NMonitoring::IMonPagePtr page) override
    {
        Y_UNUSED(page);
    }

    NMonitoring::IMonPagePtr GetMonPage(const TString& path) override
    {
        Y_UNUSED(path);
        return {};
    }

    NMonitoring::TDynamicCountersPtr GetCounters() override
    {
        return {};
    }

    NActors::TActorId Register(
        NActors::IActorPtr actor,
        TStringBuf executorName) override
    {
        Y_UNUSED(executorName);

        const auto actorId = Runtime.Register(actor.release(), NodeIdx);
        Runtime.EnableScheduleForActor(actorId);
        return actorId;
    }

    bool Send(
        const NActors::TActorId& recipient,
        NActors::IEventBasePtr event) override
    {
        return Send(std::make_unique<NActors::IEventHandle>(
            recipient,
            NActors::TActorId(),
            event.release()));
    }

    bool Send(NActors::IEventHandlePtr event) override
    {
        return Runtime.GetActorSystem(NodeIdx)->Send(event.release());
    }

    void Schedule(
        TDuration delta,
        NActors::IEventHandlePtr event,
        NActors::ISchedulerCookie* cookie) override
    {
        Runtime.GetActorSystem(NodeIdx)->Schedule(
            delta,
            event.release(),
            cookie);
    }

    TProgramShouldContinue& GetProgramShouldContinue() override
    {
        return ProgramShouldContinue;
    }
};

}   // namespace NCloud::NFileStore::NStorage
