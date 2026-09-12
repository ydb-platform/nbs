#pragma once

#include <cloud/blockstore/public/api/protos/mount.pb.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <functional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Parameters of a single MountVolume request, kept for monitoring purposes.
struct TMountInfo
{
    TString DiskId;
    TString ClientId;

    NProto::EVolumeAccessMode VolumeAccessMode =
        NProto::VOLUME_ACCESS_READ_WRITE;
    NProto::EVolumeMountMode VolumeMountMode = NProto::VOLUME_MOUNT_REMOTE;

    ui64 MountSeqNumber = 0;
};

////////////////////////////////////////////////////////////////////////////////

// A client connection along with the volumes mounted over it.
struct TConnectionInfo
{
    ui64 SessionId = 0;
    TString Peer;
    TInstant StartTs;

    TVector<TMountInfo> Mounts;
};

////////////////////////////////////////////////////////////////////////////////

// Keeps track of the RDMA client connections and the volumes mounted over
// each of them.
//
// Every update is applied on a thread of its own, so all of them return
// before the change becomes visible. That single thread is what orders the
// opening of a connection against its closing: the two are reported from
// different transport threads and would otherwise race. Updates never block
// and never throw, which is what the transport threads need from them.
class TMountRegistry final:
    public IStartable
{
private:
    TLog Log;
    ITaskQueuePtr Queue;

    THashMap<ui64, TConnectionInfo> Connections;
    mutable TAdaptiveLock Lock;

public:
    explicit TMountRegistry(TLog log);
    ~TMountRegistry() override;

    void Start() override;
    void Stop() override;

    void AddConnection(
        ui64 sessionId,
        TString peer,
        TInstant startTs) noexcept;

    void RemoveConnection(ui64 sessionId) noexcept;

    void AddMount(ui64 sessionId, TMountInfo info) noexcept;

    void RemoveMount(
        ui64 sessionId,
        TString diskId,
        TString clientId) noexcept;

    // Consistent snapshot of the connections, oldest first.
    TVector<TConnectionInfo> GetConnections() const;

private:
    void Enqueue(std::function<void()> update) noexcept;

    // applied on the registry thread. Announcing a connection is the only
    // thing that creates an entry, and closing it is the only thing that
    // drops one - together with everything mounted over it.
    void DoAddConnection(ui64 sessionId, TString peer, TInstant startTs);
    void DoRemoveConnection(ui64 sessionId);
    void DoAddMount(ui64 sessionId, TMountInfo info);
    void DoRemoveMount(
        ui64 sessionId,
        const TString& diskId,
        const TString& clientId);
};

using TMountRegistryPtr = std::shared_ptr<TMountRegistry>;

////////////////////////////////////////////////////////////////////////////////

TMountRegistryPtr CreateMountRegistry(ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NStorage
