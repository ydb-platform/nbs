#include "filesystem_event_handler.h"

#include "fs.h"

#include <cloud/filestore/libs/service/filesystem_event.h>
#include <cloud/filestore/libs/service/mask.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/logger/log.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileSystemEventHandler final
    : public IFileSystemEventHandler
{
private:
    TLog Log;
    const TString FileSystemId;
    const std::weak_ptr<IFileSystem> FileSystem;

public:
    TFileSystemEventHandler(
            TLog log,
            TString fileSystemId,
            std::weak_ptr<IFileSystem> fileSystem)
        : Log(std::move(log))
        , FileSystemId(std::move(fileSystemId))
        , FileSystem(std::move(fileSystem))
    {}

    void OnEvent(const NProto::TFileSystemEvent& event) override
    {
        //
        // The response and the event reach the caches via different paths
        // (the request future vs this handler), so an invalidation may be
        // applied before the response it invalidates is put into the caches.
        // Each invalidation gets a new cache version and the responses to the
        // requests started before it are not cached (see TNodeCache and
        // TNodeRefCache).
        //

        if (event.HasInvalidateNode()) {
            InvalidateNode(event.GetInvalidateNode().GetNodeId());
        }

        if (event.HasInvalidateNodeRef()) {
            const auto& invalidate = event.GetInvalidateNodeRef();
            InvalidateNodeRef(invalidate.GetNodeId(), invalidate.GetName());
        }

        if (event.HasInvalidateNodeXAttr()) {
            const auto& invalidate = event.GetInvalidateNodeXAttr();
            InvalidateNodeXAttr(invalidate.GetNodeId(), invalidate.GetName());
        }
    }

    void OnDisconnect(ui64 tabletId) override
    {
        STORAGE_INFO(
            "[f:%s] FileSystemEvent transport disconnected: %lu",
            FileSystemId.Quote().c_str(),
            tabletId);

        //
        // The tablet drops the subscriptions of a disconnected client - the
        // invalidations may be lost.
        //

        if (auto fileSystem = FileSystem.lock()) {
            fileSystem->InvalidateAllCachedMetadata();
        }
    }

private:
    void InvalidateNode(ui64 nodeId)
    {
        STORAGE_INFO(
            "[f:%s] InvalidateNode: %lu",
            FileSystemId.Quote().c_str(),
            nodeId);

        if (auto fileSystem = FileSystem.lock()) {
            fileSystem->InvalidateCachedNode(nodeId);
        }
    }

    void InvalidateNodeRef(ui64 nodeId, const TString& name)
    {
        //
        // Logging the name in debug mode only - in order not to suddenly start
        // writing file names to some distributed log storage with too broad
        // read privileges.
        //

        STORAGE_DEBUG(
            "[f:%s] InvalidateNodeRef: %lu, %s",
            FileSystemId.Quote().c_str(),
            nodeId,
            name.Quote().c_str());

        STORAGE_INFO(
            "[f:%s] InvalidateNodeRef: %lu, %s",
            FileSystemId.Quote().c_str(),
            nodeId,
            MaskFileName(name).Quote().c_str());

        if (auto fileSystem = FileSystem.lock()) {
            fileSystem->InvalidateCachedNodeRef(nodeId, name);
        }
    }

    void InvalidateNodeXAttr(ui64 nodeId, const TString& name)
    {
        //
        // Same as for the NodeRefs - the name is logged in debug mode only.
        //

        STORAGE_DEBUG(
            "[f:%s] InvalidateNodeXAttr: %lu, %s",
            FileSystemId.Quote().c_str(),
            nodeId,
            name.Quote().c_str());

        STORAGE_INFO(
            "[f:%s] InvalidateNodeXAttr: %lu",
            FileSystemId.Quote().c_str(),
            nodeId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileSystemEventHandlerPtr CreateFileSystemEventHandler(
    TLog log,
    TString fileSystemId,
    std::weak_ptr<IFileSystem> fileSystem)
{
    return std::make_shared<TFileSystemEventHandler>(
        std::move(log),
        std::move(fileSystemId),
        std::move(fileSystem));
}

}   // namespace NCloud::NFileStore::NFuse
