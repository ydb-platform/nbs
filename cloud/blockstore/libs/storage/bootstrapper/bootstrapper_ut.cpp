#include "bootstrapper.h"

#include <cloud/blockstore/libs/storage/api/bootstrapper.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/test_tablet.h>

#include <ydb/core/base/tablet.h>
#include <ydb/core/tablet/tablet_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime()
{
    auto runtime = std::make_unique<TTestBasicRuntime>(1, true);

    runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    // for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
    //     runtime->SetLogPriority(i, NLog::PRI_DEBUG);
    // }
    // runtime->SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    runtime->SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_ERROR);

    SetupTabletServices(*runtime);

    return runtime;
}

TTabletSetupInfoPtr PrepareTabletSetupInfo(TTestActorRuntime& runtime)
{
    const auto& appData = runtime.GetAppData();

    auto factory = [=] (const TActorId& owner, TTabletStorageInfo* storage) {
        auto actor = CreateTestTablet(owner, storage);
        return actor.release();
    };

    return MakeIntrusive<TTabletSetupInfo>(
        factory,
        TMailboxType::ReadAsFilled,
        appData.UserPoolId,
        TMailboxType::ReadAsFilled,
        appData.SystemPoolId);
}

void StartTablet(TTestActorRuntime& runtime, const TActorId& bootstrapper, const TActorId& sender)
{
    Send(runtime, bootstrapper, sender, std::make_unique<TEvBootstrapper::TEvStart>());

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvBootstrapper::TEvStatus>(handle);
    UNIT_ASSERT(response);
    UNIT_ASSERT_C(response->Status == TEvBootstrapper::STARTED, response->Message);
}

void StopTablet(TTestActorRuntime& runtime, const TActorId& bootstrapper, const TActorId& sender)
{
    Send(runtime, bootstrapper, sender, std::make_unique<TEvBootstrapper::TEvStop>());

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvBootstrapper::TEvStatus>(handle);
    UNIT_ASSERT(response);
    UNIT_ASSERT_C(response->Status == TEvBootstrapper::STOPPED, response->Message);
}

void KillTablet(TTestActorRuntime& runtime, ui64 tablet, const TActorId& sender)
{
    SendToPipe(runtime, tablet, sender, std::make_unique<TEvents::TEvPoisonPill>());

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvBootstrapper::TEvStatus>(handle);
    UNIT_ASSERT(response);
    UNIT_ASSERT_C(response->Status == TEvBootstrapper::STARTED, response->Message);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBootstrapperTest)
{
    Y_UNIT_TEST(ShouldStartStopTablet)
    {
        auto runtime = PrepareTestActorRuntime();
        auto sender = runtime->AllocateEdgeActor();

        TTabletStorageInfoPtr storageInfo = CreateTestTabletInfo(
            TestTabletId,
            TTabletTypes::BlockStorePartition);

        auto setupInfo = PrepareTabletSetupInfo(*runtime);

        auto bootstrapper = Register(*runtime, CreateBootstrapper(
            TBootstrapperConfig(),
            sender,
            std::move(storageInfo),
            std::move(setupInfo)));

        StartTablet(*runtime, bootstrapper, sender);
        StopTablet(*runtime, bootstrapper, sender);
    }

    Y_UNIT_TEST(ShouldRestartTablet)
    {
        auto runtime = PrepareTestActorRuntime();
        auto sender = runtime->AllocateEdgeActor();

        TTabletStorageInfoPtr storageInfo = CreateTestTabletInfo(
            TestTabletId,
            TTabletTypes::BlockStorePartition);

        auto setupInfo = PrepareTabletSetupInfo(*runtime);

        TBootstrapperConfig config;
        config.CoolDownTimeout = TDuration::Zero();
        config.RestartAlways = true;

        auto bootstrapper = Register(*runtime, CreateBootstrapper(
            config,
            sender,
            std::move(storageInfo),
            std::move(setupInfo)));

        StartTablet(*runtime, bootstrapper, sender);
        KillTablet(*runtime, TestTabletId, sender);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
