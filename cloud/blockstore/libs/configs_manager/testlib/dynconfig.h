/*******************************************************************************

Test configuration publications and notifications for direct ConfigsManager
subscribers. Edit the public TTestBlockstoreConfigProvider::Config protobuf
draft, call TTestBlockstoreConfigProvider::Publish(), and use SendDynconfigTo()
to send the current snapshot to a consumer. Published protobuf values remain
independent of draft edits. Each publication refers to the same working Storage
object, including its live feature rules and ICB controls.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/libs/config/blockstore_config_holder.h>

#include <cloud/blockstore/config/blockstore.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NActors {
    class TTestActorRuntime;
}

namespace NCloud::NBlockStore::NTestDynconfig {

////////////////////////////////////////////////////////////////////////////////

// A test provider owning an editable protobuf draft and an atomic publication
// holder.
// Construct with an optional initial draft and shared ICB controls.
// Edit Config and call Publish() to replace the published configuration.
// The provider creates controls when none are supplied.
class TTestBlockstoreConfigProvider final: public IBlockstoreConfigGetter
{
public:
    // Configuration draft owned by the test; edits take effect after Publish().
    // Edit and publish from one test thread, separately from consumer reads.
    NProto::TBlockstoreConfig Config;

    explicit TTestBlockstoreConfigProvider(
        NProto::TBlockstoreConfig config = {},
        NStorage::TStorageConfigControlsPtr controls = {});

    // Get the last published configuration.
    [[nodiscard]] IBlockstoreConfigPtr Get() const override;

    // Publish an independent copy of Config without changing ICB defaults.
    void Publish();

private:
    // Working server configuration shared by all published aggregates.
    const NStorage::TStorageConfigMutablePtr StorageConfig;

    // Latest publication retained by the provider; older configurations stay valid
    // while their readers retain them.
    TBlockstoreConfigHolder Holder;

    // Capture the current draft and apply its values to the working object.
    IBlockstoreConfigMutablePtr BuildPublication();
};

// Send a TEvConfigsManager::TEvConfigChanged configuration notice
// without waiting for an acknowledgement or downstream updates.
// Require a non-empty actorId and a non-null provider.
void SendDynconfigTo(
    NActors::TTestActorRuntime& runtime,
    NActors::TActorId actorId,
    IBlockstoreConfigGetterPtr provider);

}   // namespace NCloud::NBlockStore::NTestDynconfig
