#pragma once

#include <cloud/filestore/libs/storage/api/rdma.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
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
    THashMap<NActors::TActorId, THashSet<NActors::TActorId>> TabletPipes;

public:
    // Empty if the pipe server is not registered.
    NActors::TActorId FindTablet(
        const NActors::TActorId& pipeServerId) const noexcept;

    void RegisterPipe(
        const NActors::TActorId& pipeServerId,
        const NActors::TActorId& tabletActorId) override;

    void UnregisterPipe(
        const NActors::TActorId& pipeServerId) noexcept override;

    void UnregisterTablet(
        const NActors::TActorId& tabletActorId) noexcept override;

private:
    void EraseTabletPipe(
        const NActors::TActorId& tabletActorId,
        const NActors::TActorId& pipeServerId) noexcept;
};

using TRdmaTabletRegistryPtr = std::shared_ptr<TRdmaTabletRegistry>;

}   // namespace NCloud::NFileStore::NStorage
