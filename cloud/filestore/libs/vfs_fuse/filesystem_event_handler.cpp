#include "filesystem_event_handler.h"

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

public:
    TFileSystemEventHandler(TLog log, TString fileSystemId)
        : Log(std::move(log))
        , FileSystemId(std::move(fileSystemId))
    {}

    void OnEvent(const NProto::TFileSystemEvent& event) override
    {
        //
        // TODO(#6874): add a per-key version (e.g. the tablet's CommitId) to
        // the responses and to the events before using the events for cache
        // invalidation. The tablet sends the response before the event, but
        // they reach the cache via different paths (the request future vs this
        // handler), so an invalidation may be applied before the response it
        // invalidates is put into the cache - the stale data would then stay
        // cached for the whole cache TTL. A response older than the last
        // applied invalidation of the same key should not be cached.
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
    }

private:
    void InvalidateNode(ui64 nodeId)
    {
        STORAGE_INFO(
            "[f:%s] InvalidateNode: %lu",
            FileSystemId.Quote().c_str(),
            nodeId);
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
    TString fileSystemId)
{
    return std::make_shared<TFileSystemEventHandler>(
        std::move(log),
        std::move(fileSystemId));
}

}   // namespace NCloud::NFileStore::NFuse
