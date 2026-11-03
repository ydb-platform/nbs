/*******************************************************************************

Test configuration publications and notifications for direct ConfigsManager
subscribers. Edit the public TTestBlockstoreConfigProvider::Config protobuf
draft, call TTestBlockstoreConfigProvider::Publish(), and send the provider
to a consumer. Published protobuf values remain independent of draft edits.
Storage snapshots retain the same live ICB controls.

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
// Edit Config and call Publish() to replace the last published snapshot.
// The provider creates controls when none are supplied.
class TTestBlockstoreConfigProvider final: public IBlockstoreConfigProvider
{
public:
    // Configuration draft owned by the test; edits take effect after Publish().
    // Edit and publish from one test thread, separately from consumer reads.
    NProto::TBlockstoreConfig Config;

    explicit TTestBlockstoreConfigProvider(
        NProto::TBlockstoreConfig config = {},
        NStorage::TStorageConfigControlsPtr controls = {});

    // Get the last published snapshot
    [[nodiscard]] IBlockstoreConfigConstPtr Get() const override;

    // Publish an independent snapshot of Config without changing ICB defaults.
    void Publish();

private:
    // Live ICB controls shared by all publications; non-null for this lifetime.
    const NStorage::TStorageConfigControlsPtr Controls;

    // Latest publication retained by the provider; older snapshots stay valid
    // while their readers retain them.
    TBlockstoreConfigHolder Holder;
};

// Send a TEvConfigsManager::TEvConfigChanged configuration notice
// without waiting for an acknowledgement or downstream updates.
// Require a non-empty actorId and a non-null provider.
void SendDynconfigTo(
    NActors::TTestActorRuntime& runtime,
    NActors::TActorId actorId,
    IBlockstoreConfigProviderPtr provider);

}   // namespace NCloud::NBlockStore::NTestDynconfig
