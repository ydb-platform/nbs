#include "filesystem_event_handler.h"

#include <cloud/filestore/libs/service/filesystem_event.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileSystemEventHandler final
    : public IFileSystemEventHandler
{
private:
    const TString FileSystemId;
    TLog Log;

public:
    TFileSystemEventHandler(ILoggingServicePtr logging, TString fileSystemId)
        : FileSystemId(std::move(fileSystemId))
        , Log(logging->CreateLog("NFS_FUSE"))
    {}

    void OnEvent(const NProto::TFileSystemEvent& event) override
    {
        for (const auto& invalidate: event.GetInvalidateNode()) {
            InvalidateNode(invalidate.GetNodeId());
        }

        for (const auto& invalidate: event.GetInvalidateNodeRef()) {
            InvalidateNodeRef(invalidate.GetNodeId(), invalidate.GetName());
        }
    }

    void OnDisconnect() override
    {
        STORAGE_INFO(
            "[f:%s] FileSystemEvent transport disconnected",
            FileSystemId.Quote().c_str());
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
        STORAGE_INFO(
            "[f:%s] InvalidateNodeRef: %lu, %s",
            FileSystemId.Quote().c_str(),
            nodeId,
            name.Quote().c_str());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileSystemEventHandlerPtr CreateFileSystemEventHandler(
    ILoggingServicePtr logging,
    TString fileSystemId)
{
    return std::make_shared<TFileSystemEventHandler>(
        std::move(logging),
        std::move(fileSystemId));
}

}   // namespace NCloud::NFileStore::NFuse
