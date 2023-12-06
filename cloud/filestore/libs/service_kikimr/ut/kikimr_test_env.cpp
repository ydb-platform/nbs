#include "kikimr_test_env.h"

#include <cloud/filestore/libs/storage/api/components.h>

#include <cloud/storage/core/libs/api/authorizer.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NCloud::NFileStore {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;

using namespace NCloud::NFileStore::NStorage;

////////////////////////////////////////////////////////////////////////////////

TTestActorSystem::TTestActorSystem()
{
    Runtime = std::make_unique<NKikimr::TTestBasicRuntime>(2, false);
    Runtime->AppendToLogSettings(
        TFileStoreComponents::START,
        TFileStoreComponents::END,
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

    auto actorId = Runtime->Register(actor.release());
    Runtime->EnableScheduleForActor(actorId);

    return actorId;
};

bool TTestActorSystem::Send(const TActorId& recipient, IEventBasePtr event)
{
    Runtime->Send(new IEventHandle(recipient, Sender, event.release()));
    return true;
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
        TActorId(0, "nfs-service"),
        Register(std::move(serviceActor)));
}

void TTestActorSystem::RegisterTestAuthorizer(IActorPtr authorizer)
{
    Runtime->RegisterService(
        NCloud::NStorage::MakeAuthorizerServiceId(),
        Register(std::move(authorizer)));
}


}   // namespace NCloud::NFileStore
