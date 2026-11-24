#pragma once

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NActors {
class TActorSystem;
}   // namespace NActors

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Owns the TCP server of the journalled devices, the devices and their
// executor. The daemon creates the provider and stops it before the actor
// system and the file IO service. The disk agent actor starts the server when
// the devices are ready. Not thread-safe: StartServer is called once, and
// Stop is called only after it returns.
struct IJournalledDeviceServerProvider
    : public IStartable
{
    // Creates and starts the server for the devices that have a journal
    // config. Waits for the journals to restore. Does nothing when there are
    // no such devices.
    virtual NProto::TError StartServer(
        TVector<NProto::TDeviceConfig> devices,
        THashMap<TString, NProto::TJournalConfig> journalConfigs,
        TDeviceClientPtr deviceClient,
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& diskAgentActorId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IJournalledDeviceServerProviderPtr CreateJournalledDeviceServerProvider(
    TDiskAgentConfigConstPtr agentConfig,
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NStorage
