#pragma once

#include <cloud/filestore/libs/storage/api/rdma.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Tablets register and unregister on their own threads, the RDMA request
// handler looks them up on the RDMA poller threads.
class TRdmaTabletRegistry final
    : public IRdmaTabletRegistry
{
private:
    mutable TAdaptiveLock Lock;
    THashMap<NActors::TActorId, NActors::TActorId> Pipes;
    NActors::TActorId ForwarderId;

public:
    // The forwarder gets TEvTabletGone for every tablet that unregisters.
    void AttachForwarder(const NActors::TActorId& forwarderId);

    // Empty if the pipe server is not registered.
    NActors::TActorId FindTablet(const NActors::TActorId& pipeServerId) const;

    void RegisterPipe(
        const NActors::TActorId& pipeServerId,
        const NActors::TActorId& tabletActorId) override;

    void UnregisterPipe(const NActors::TActorId& pipeServerId) override;

    void UnregisterTablet(
        const NActors::TActorContext& ctx,
        const NActors::TActorId& tabletActorId) override;
};

using TRdmaTabletRegistryPtr = std::shared_ptr<TRdmaTabletRegistry>;

}   // namespace NCloud::NFileStore::NStorage
