#include "dynconfig.h"

#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/configs_manager/events.h>

#include <contrib/ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NCloud::NBlockStore::NTestDynconfig {

////////////////////////////////////////////////////////////////////////////////

// Publish the initial draft with supplied or newly created ICB controls.
TTestBlockstoreConfigProvider::TTestBlockstoreConfigProvider(
    NProto::TBlockstoreConfig config,
    NStorage::TStorageConfigControlsPtr controls)
    : Config(std::move(config))
    , Controls(
          controls ? std::move(controls)
                   : std::make_shared<NStorage::TStorageConfigControls>(
                         Config.GetStorageService()))
    , Holder(MakeBlockstoreConfig(Config, Controls))
{}

IBlockstoreConfigConstPtr TTestBlockstoreConfigProvider::Get() const
{
    return Holder.Get();
}

void TTestBlockstoreConfigProvider::Publish()
{
    Holder.Set(MakeBlockstoreConfig(Config, Controls));
}

void SendDynconfigTo(
    NActors::TTestActorRuntime& runtime,
    NActors::TActorId actorId,
    IBlockstoreConfigGetterPtr provider)
{
    // Validate the notification inputs before allocating its sender.
    UNIT_ASSERT_C(actorId, "Dynamic configuration recipient is empty");
    UNIT_ASSERT_C(provider, "Dynamic configuration provider is null");

    // Send the current snapshot without waiting for a consumer reply.
    runtime.Send(new NActors::IEventHandle(
        actorId,
        runtime.AllocateEdgeActor(),
        new TEvConfigsManager::TEvConfigChanged(provider->Get())));
}

}   // namespace NCloud::NBlockStore::NTestDynconfig
