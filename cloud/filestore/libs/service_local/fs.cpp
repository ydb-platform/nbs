#include "fs.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TLocalFileSystem::TLocalFileSystem(
        TLocalFileStoreConfigPtr config,
        NProto::TFileStore store,
        TFsPath root,
        TFsPath statePath,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IFileIOServicePtr fileIOService)
    : Config(std::move(config))
    , RootPath(std::move(root))
    , StatePath(std::move(statePath))
    , Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
    , Logging(std::move(logging))
    , FileIOService(std::move(fileIOService))
    , Store(std::move(store))
{
    Log = Logging->CreateLog(Store.GetFileSystemId());

    STORAGE_INFO(
        "LocalFileSystemId=" << Store.GetFileSystemId() <<
        ", Config=[" << Config->DumpStr() <<
        "]");

    ScheduleCleanupSessions();
}

////////////////////////////////////////////////////////////////////////////////

NProto::TGetFileStoreInfoResponse TLocalFileSystem::GetFileStoreInfo(
    const NProto::TGetFileStoreInfoRequest& request)
{
    STORAGE_TRACE("GetFileStoreInfo " << DumpMessage(request));

    NProto::TGetFileStoreInfoResponse response;
    response.MutableFileStore()->CopyFrom(Store);

    return response;
}

NProto::TStatFileStoreResponse TLocalFileSystem::StatFileStore(
    const NProto::TStatFileStoreRequest& request)
{
    STORAGE_TRACE("StatFileStore " << DumpMessage(request));

    auto session = GetSession(request);
    auto node = session->LookupNode(RootNodeId);
    if (!node) {
        return TErrorResponse(ErrorInvalidTarget(RootNodeId));
    }

    auto statfs = node->StatFs();

    NProto::TStatFileStoreResponse response;

    auto* fileStore = response.MutableFileStore();
    fileStore->CopyFrom(Store);
    fileStore->SetBlockSize(statfs.BlockSize);
    fileStore->SetBlocksCount(statfs.TotalBlocks);
    fileStore->SetNodesCount(statfs.TotalFiles);

    auto* stats = response.MutableStats();
    stats->SetUsedNodesCount(statfs.TotalFiles - statfs.FreeFiles);
    stats->SetUsedBlocksCount(statfs.TotalBlocks - statfs.FreeBlocks);

    return response;
}

NProto::TCreateCheckpointResponse TLocalFileSystem::CreateCheckpoint(
    const NProto::TCreateCheckpointRequest& request)
{
    STORAGE_TRACE("CreateCheckpoint " << DumpMessage(request));

    // TODO
    return {};
}

NProto::TDestroyCheckpointResponse TLocalFileSystem::DestroyCheckpoint(
    const NProto::TDestroyCheckpointRequest& request)
{
    STORAGE_TRACE("DestroyCheckpoint " << DumpMessage(request));

    // TODO
    return {};
}

////////////////////////////////////////////////////////////////////////////////

void ConvertStats(const TFileStat& stat, NProto::TNodeAttr& node)
{
    if (S_ISREG(stat.Mode)) {
        node.SetType(NProto::E_REGULAR_NODE);
    } else if (S_ISDIR(stat.Mode)) {
        node.SetType(NProto::E_DIRECTORY_NODE);
    } else if (S_ISLNK(stat.Mode)) {
        node.SetType(NProto::E_LINK_NODE);
    } else if (S_ISSOCK(stat.Mode)) {
        node.SetType(NProto::E_SOCK_NODE);
    } else if (S_ISFIFO(stat.Mode)) {
        node.SetType(NProto::E_FIFO_NODE);
    } else {
        ythrow TServiceError(E_IO) << "invalid stats";
    }

    node.SetId(stat.INode);
    node.SetMode(stat.Mode & ~(S_IFMT));
    node.SetUid(stat.Uid);
    node.SetGid(stat.Gid);
    node.SetSize(stat.Size);
    node.SetATime(TInstant::Seconds(stat.ATime).MicroSeconds());
    node.SetMTime(TInstant::Seconds(stat.MTime).MicroSeconds());
    node.SetCTime(TInstant::Seconds(stat.CTime).MicroSeconds());
    node.SetLinks(stat.NLinks);
}

}   // namespace NCloud::NFileStore
