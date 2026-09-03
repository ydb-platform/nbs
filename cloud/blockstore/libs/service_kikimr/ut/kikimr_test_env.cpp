#include "kikimr_test_env.h"

#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/api/authorizer.h>
#include <cloud/storage/core/libs/auth/authorizer.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/library/actors/core/scheduler_cookie.h>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;

using namespace NCloud::NBlockStore;
using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

TTestActorSystem::TTestActorSystem()
{
    Runtime = std::make_unique<NKikimr::TTestBasicRuntime>(2, false);
    Runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    TAppPrepare app;
    SetupTabletServices(*Runtime, &app);

    Sender = Runtime->AllocateEdgeActor(0);
}

void TTestActorSystem::Start()
{
    // nothing to do
}

void TTestActorSystem::Stop()
{
    Runtime.reset();
}

TLog TTestActorSystem::CreateLog(const TString& component)
{
    Y_UNUSED(component);
    return {};
}

IMonPagePtr TTestActorSystem::RegisterIndexPage(
    const TString& path,
    const TString& title)
{
    Y_UNUSED(path);
    Y_UNUSED(title);
    return {};
}

void TTestActorSystem::RegisterMonPage(IMonPagePtr page)
{
    Y_UNUSED(page);
}

IMonPagePtr TTestActorSystem::GetMonPage(const TString& path)
{
    Y_UNUSED(path);
    return {};
}

TDynamicCountersPtr TTestActorSystem::GetCounters()
{
    return {};
}

TActorId TTestActorSystem::Register(
    IActorPtr actor,
    TStringBuf executorName)
{
    Y_UNUSED(executorName);

    RegistrationCount.fetch_add(1, std::memory_order_relaxed);

    auto actorId = Runtime->Register(actor.release());
    Runtime->EnableScheduleForActor(actorId);

    return actorId;
};

bool TTestActorSystem::Send(const TActorId& recipient, IEventBasePtr event)
{
    Runtime->Send(new IEventHandle(recipient, Sender, event.release()));
    return true;
}

bool TTestActorSystem::Send(IEventHandlePtr event)
{
    Runtime->Send(event.release());
    return true;
}

void TTestActorSystem::Schedule(
    TDuration delta,
    IEventHandlePtr event,
    ISchedulerCookie* cookie)
{
    if (cookie) {
        cookie->Detach();
    }
    Runtime->Schedule(event.release(), delta);
}

TProgramShouldContinue& TTestActorSystem::GetProgramShouldContinue()
{
    return ProgramShouldContinue;
}

void TTestActorSystem::DispatchEvents(TDuration timeout)
{
    Runtime->DispatchEvents(TDispatchOptions(), timeout);
}

void TTestActorSystem::RegisterTestService(IActorPtr serviceActor)
{
    Runtime->RegisterService(
        TActorId(0, "blk-service"),
        Register(std::move(serviceActor)));
}

void TTestActorSystem::RegisterTestAuthorizer(IActorPtr authorizer)
{
    Runtime->RegisterService(
        MakeAuthorizerServiceId(),
        Register(std::move(authorizer)));
}

ui64 TTestActorSystem::GetRegistrationCount() const
{
    return RegistrationCount.load(std::memory_order_relaxed);
}

}   // namespace NCloud::NBlockStore::NServer
