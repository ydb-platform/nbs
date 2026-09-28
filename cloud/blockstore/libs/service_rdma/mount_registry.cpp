#include "mount_registry.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TMountRegistry::TMountRegistry(TLog log)
    : Log(std::move(log))
      // a single thread on purpose, see the comment on the class
    , Queue(CreateThreadPool("RDMA_REG", 1))
{}

TMountRegistry::~TMountRegistry() = default;

void TMountRegistry::Start()
{
    Queue->Start();
}

void TMountRegistry::Stop()
{
    Queue->Stop();
}

////////////////////////////////////////////////////////////////////////////////

void TMountRegistry::Enqueue(std::function<void()> update) noexcept
{
    auto task = [Log = Log, update = std::move(update)]
    {
        // the thread pool runs the task in a noexcept context
        auto error = SafeExecute<NProto::TError>(
            [&]
            {
                update();
                return NProto::TError{};
            });

        if (HasError(error)) {
            STORAGE_WARN(
                "Can't update the mount registry: %s",
                FormatError(error).c_str());
        }
    };

    // called from the transport threads, nothing may escape into them
    auto error = SafeExecute<NProto::TError>(
        [&]
        {
            Queue->ExecuteSimple(std::move(task));
            return NProto::TError{};
        });

    if (HasError(error)) {
        STORAGE_WARN(
            "Can't enqueue a mount registry update: %s",
            FormatError(error).c_str());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMountRegistry::AddConnection(
    ui64 sessionId,
    TString peer,
    TInstant startTs) noexcept
{
    Enqueue(
        [this, sessionId, peer = std::move(peer), startTs]
        { DoAddConnection(sessionId, peer, startTs); });
}

void TMountRegistry::RemoveConnection(ui64 sessionId) noexcept
{
    Enqueue([this, sessionId] { DoRemoveConnection(sessionId); });
}

void TMountRegistry::AddMount(ui64 sessionId, TMountInfo info) noexcept
{
    Enqueue(
        [this, sessionId, info = std::move(info)]() mutable
        { DoAddMount(sessionId, std::move(info)); });
}

void TMountRegistry::RemoveMount(
    ui64 sessionId,
    TString diskId,
    TString clientId) noexcept
{
    Enqueue(
        [this,
         sessionId,
         diskId = std::move(diskId),
         clientId = std::move(clientId)]
        { DoRemoveMount(sessionId, diskId, clientId); });
}

////////////////////////////////////////////////////////////////////////////////

void TMountRegistry::DoAddConnection(
    ui64 sessionId,
    TString peer,
    TInstant startTs)
{
    with_lock (Lock) {
        auto& connection = Connections[sessionId];
        connection.SessionId = sessionId;
        connection.Peer = std::move(peer);
        connection.StartTs = startTs;
    }
}

void TMountRegistry::DoRemoveConnection(ui64 sessionId)
{
    with_lock (Lock) {
        Connections.erase(sessionId);
    }
}

void TMountRegistry::DoAddMount(ui64 sessionId, TMountInfo info)
{
    with_lock (Lock) {
        auto* connection = Connections.FindPtr(sessionId);
        if (!connection) {
            // the connection is announced before it can serve anything and
            // forgotten only after everything it delivered has been answered,
            // so there is no mount to record here - and recording one would
            // bring a closed connection back for good
            return;
        }

        auto it = FindIf(
            connection->Mounts,
            [&](const auto& mount)
            {
                return mount.DiskId == info.DiskId &&
                       mount.ClientId == info.ClientId;
            });

        if (it != connection->Mounts.end()) {
            *it = std::move(info);
        } else {
            connection->Mounts.push_back(std::move(info));
        }
    }
}

void TMountRegistry::DoRemoveMount(
    ui64 sessionId,
    const TString& diskId,
    const TString& clientId)
{
    with_lock (Lock) {
        auto* connection = Connections.FindPtr(sessionId);
        if (!connection) {
            return;
        }

        EraseIf(
            connection->Mounts,
            [&](const auto& mount)
            {
                return mount.DiskId == diskId && mount.ClientId == clientId;
            });
    }
}

TVector<TConnectionInfo> TMountRegistry::GetConnections() const
{
    TVector<TConnectionInfo> result;

    with_lock (Lock) {
        result.reserve(Connections.size());

        for (const auto& [sessionId, connection]: Connections) {
            result.push_back(connection);
        }
    }

    SortBy(result, [](const auto& connection) { return connection.StartTs; });

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TMountRegistryPtr CreateMountRegistry(ILoggingServicePtr logging)
{
    return std::make_shared<TMountRegistry>(
        logging->CreateLog("BLOCKSTORE_SERVER"));
}

}   // namespace NCloud::NBlockStore::NStorage
