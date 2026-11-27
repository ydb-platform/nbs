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
    , StorageConfig(std::make_shared<NStorage::TStorageConfig>(
          Config.GetStorageService(), nullptr, std::move(controls)))
    , Holder(BuildPublication())
{}

IBlockstoreConfigPtr TTestBlockstoreConfigProvider::Get() const
{
    return Holder.Get();
}

void TTestBlockstoreConfigProvider::Publish()
{
    Holder.Set(BuildPublication());
}

IBlockstoreConfigMutablePtr TTestBlockstoreConfigProvider::BuildPublication()
{
    auto publication = MakeBlockstoreConfig(
        Config,
        StorageConfig,
        /*extraParameters=*/{});
    StorageConfig->SetFeaturesConfig(publication->GetFeaturesConfig());
    StorageConfig->ApplyRwConfig(publication->GetStorageServiceConfig());
    return publication;
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
