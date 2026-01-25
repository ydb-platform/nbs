#include "index.h"

#include <util/stream/format.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TIndexNodePtr TIndexNode::CreateRoot(const TFsPath& path)
{
    auto node = NLowLevel::Open(path, O_PATH, 0);
    return std::make_shared<TIndexNode>(RootNodeId, std::move(node));
}

TIndexNodePtr TIndexNode::Create(const TIndexNode& parent, const TString& name)
{
    auto node = NLowLevel::OpenAt(parent.NodeFd, name, O_PATH | O_NOFOLLOW, 0);
    auto stat = NLowLevel::Stat(node);

    return std::make_shared<TIndexNode>(stat.INode, std::move(node));
}

TIndexNodePtr TIndexNode::CreateDirectory(const TString& name, int mode)
{
    NLowLevel::MkDirAt(NodeFd, name, mode);

    auto node = NLowLevel::OpenAt(NodeFd, name, O_PATH, 0);
    auto stat = NLowLevel::Stat(node);

    return std::make_shared<TIndexNode>(stat.INode, std::move(node));
}

TIndexNodePtr TIndexNode::CreateFile(const TString& name, int mode)
{
    auto node =
        NLowLevel::OpenAt(NodeFd, name, O_CREAT | O_EXCL | O_WRONLY, mode);
    auto resolved = NLowLevel::Open(node, O_PATH, 0);
    auto stat = NLowLevel::Stat(resolved);

    return std::make_shared<TIndexNode>(stat.INode, std::move(resolved));
}

TIndexNodePtr TIndexNode::CreateSocket(const TString& name, int mode)
{
    NLowLevel::MkSockAt(NodeFd, name, mode);

    auto node = NLowLevel::OpenAt(NodeFd, name, O_PATH, 0);
    auto stat = NLowLevel::Stat(node);

    return std::make_shared<TIndexNode>(stat.INode, std::move(node));
}

TIndexNodePtr TIndexNode::CreateFifo(const TString& name, int mode)
{
    NLowLevel::MkFifoAt(NodeFd, name, mode);

    auto node = NLowLevel::OpenAt(NodeFd, name, O_PATH, 0);
    auto stat = NLowLevel::Stat(node);

    return std::make_shared<TIndexNode>(stat.INode, std::move(node));
}

TIndexNodePtr TIndexNode::CreateCharDevice(const TString& name, int mode, dev_t dev)
{
    NLowLevel::MkCharDeviceAt(NodeFd, name, mode, dev);

    auto node = NLowLevel::OpenAt(NodeFd, name, O_PATH, 0);
    auto stat = NLowLevel::Stat(node);

    return std::make_shared<TIndexNode>(stat.INode, std::move(node));
}

TIndexNodePtr TIndexNode::CreateBlockDevice(const TString& name, int mode, dev_t dev)
{
    NLowLevel::MkBlockDeviceAt(NodeFd, name, mode, dev);

    auto node = NLowLevel::OpenAt(NodeFd, name, O_PATH, 0);
    auto stat = NLowLevel::Stat(node);

    return std::make_shared<TIndexNode>(stat.INode, std::move(node));
}

TIndexNodePtr TIndexNode::CreateLink(const TIndexNode& parent, const TString& name)
{
    NLowLevel::LinkAt(NodeFd, parent.NodeFd, name);
    return TIndexNode::Create(parent, name);
}

TIndexNodePtr TIndexNode::CreateSymlink(const TString& target, const TString& name)
{
    NLowLevel::SymLinkAt(target, NodeFd, name);

    auto node = NLowLevel::OpenAt(NodeFd, name, O_PATH | O_NOFOLLOW, 0);
    auto stat = NLowLevel::Stat(node);

    return std::make_shared<TIndexNode>(stat.INode, std::move(node));
}

void TIndexNode::Rename(
    const TString& name,
    const TIndexNodePtr& newparent,
    const TString& newname,
    unsigned int flags)
{
    return NLowLevel::RenameAt(
        NodeFd,
        name.data(),
        newparent->NodeFd,
        newname.data(),
        flags);
}

void TIndexNode::Unlink(const TString& name, bool directory)
{
    return NLowLevel::UnlinkAt(NodeFd, name, directory);
}

TString TIndexNode::ReadLink() const
{
    return NLowLevel::ReadLink(NodeFd);
}

TVector<NLowLevel::TDirEntry> TIndexNode::List(bool ignoreErrors)
{
    auto res = NLowLevel::ListDirAt(
        NodeFd,
        0,   // start at beginning of dir
        0,   // don't limit number of entries
        ignoreErrors);

    return std::move(res.DirEntries);
}

NLowLevel::TListDirResult
TIndexNode::List(uint64_t offset, size_t entriesLimit, bool ignoreErrors)
{
    return NLowLevel::ListDirAt(NodeFd, offset, entriesLimit, ignoreErrors);
}

NLowLevel::TFileStatEx TIndexNode::Stat()
{
    return NLowLevel::Stat(NodeFd);
}

NLowLevel::TFileStatEx TIndexNode::Stat(const TString& name)
{
    return NLowLevel::StatAt(NodeFd, name);
}

NLowLevel::TFileSystemStat TIndexNode::StatFs() const
{
    return NLowLevel::StatFs(NodeFd);
}

TFileHandle TIndexNode::OpenHandle(int flags)
{
    return NLowLevel::Open(NodeFd, flags, 0);
}

TFileHandle TIndexNode::OpenHandle(const TString& name, int flags, int mode)
{
    return NLowLevel::OpenAt(NodeFd, name.data(), flags, mode);
}

void TIndexNode::Access(int mode)
{
    return NLowLevel::Access(NodeFd, mode);
}

void TIndexNode::Chmod(int mode)
{
    return NLowLevel::Chmod(NodeFd, mode);

}
void TIndexNode::Chown(unsigned int uid, unsigned int gid)
{
    return NLowLevel::Chown(NodeFd, uid, gid);
}

void TIndexNode::Utimes(TInstant atime, TInstant mtime)
{
    return NLowLevel::Utimes(NodeFd, atime, mtime);
}

void TIndexNode::Truncate(size_t size)
{
    return NLowLevel::Truncate(NodeFd, size);
}

TVector<TString> TIndexNode::ListXAttrs()
{
    return NLowLevel::ListXAttrs(NodeFd);
}

void TIndexNode::SetXAttr(const TString& name, const TString& value)
{
    return NLowLevel::SetXAttr(NodeFd, name, value);
}

TString TIndexNode::GetXAttr(const TString& name)
{
    return NLowLevel::GetXAttr(NodeFd, name);
}

void TIndexNode::RemoveXAttr(const TString& name)
{
    return NLowLevel::RemoveXAttr(NodeFd, name);
}

////////////////////////////////////////////////////////////////////////////////

class TNodeLoader: public INodeLoader
{
protected:
    using EFileIdType = NLowLevel::TFileId::EFileIdType;
    using TFileId = NLowLevel::TFileId;
    TFileHandle RootHandle;
    TFileId RootFileId;

public:
    TNodeLoader(TFileHandle&& rootHandle, TFileId&& rootFileId)
        : RootHandle(std::move(rootHandle))
        , RootFileId(std::move(rootFileId))
    {}

    TString ToString() const override
    {
        return TStringBuilder()
               << "NodeLoader(" << RootFileId.ToString() << ")";
    }

    TIndexNodePtr LoadSnapshotsDirNode() override
    {
        return nullptr;
    }

    TIndexNodePtr LoadSnapshotDirNode(ui64 snapshotDirNodeId) override
    {
        Y_UNUSED(snapshotDirNodeId);
        return nullptr;
    }
};

class TLustreNodeLoader: public TNodeLoader
{
public:
    TLustreNodeLoader(TFileHandle&& rootHandle, TFileId&& rootFileId)
        : TNodeLoader(std::move(rootHandle), std::move(rootFileId))
    {}

    TIndexNodePtr LoadNode(ui64 nodeId) const override
    {
        NLowLevel::TFileId fileId(RootFileId);
        fileId.LustreFid.Oid = nodeId & 0xffffff;
        fileId.LustreFid.Seq = (nodeId >> 24) & 0xffffffffff;

        try {
            auto handle = fileId.Open(RootHandle, O_PATH);
            return std::make_shared<TIndexNode>(nodeId, std::move(handle));
        } catch (...) {
        }

        return nullptr;
    }
};

class TVastNfsNodeLoader: public TNodeLoader
{
public:
    TVastNfsNodeLoader(TFileHandle&& rootHandle, TFileId&& rootFileId)
        : TNodeLoader(std::move(rootHandle), std::move(rootFileId))
    {}

    TIndexNodePtr LoadNode(ui64 nodeId) const override
    {
        NLowLevel::TFileId fileId(RootFileId);
        fileId.VastNfsInodeId.IdHigh32 = (nodeId >> 32) & 0xffffffff;
        fileId.VastNfsInodeId.IdLow32 = nodeId & 0xffffffff;
        fileId.VastNfsInodeId.ServerId = nodeId;
        // nfs client will try to resolve inode from cache
        // https://github.com/torvalds/linux/blob/dd83757f6e686a2188997cb58b5975f744bb7786/fs/nfs/export.c#L93
        // by comparing FileType field as well
        // https://github.com/torvalds/linux/blob/dd83757f6e686a2188997cb58b5975f744bb7786/fs/nfs/inode.c#L327
        // Since we can't deduce file type from inode number, we set it to
        // regular file as optimization if the inode is not a regular file the
        // kernel code will try to communicate with nfs server and resolve it
        // there
        // https://github.com/torvalds/linux/blob/dd83757f6e686a2188997cb58b5975f744bb7786/fs/nfs/export.c#L98
        fileId.VastNfsInodeId.FileType = S_IFREG;

        try {
            auto handle = fileId.Open(RootHandle, O_PATH);
            return std::make_shared<TIndexNode>(nodeId, std::move(handle));
        } catch (...) {
        }

        return nullptr;
    }
};

class TWekaNodeLoader: public TNodeLoader
{
private:
    TIndexNodePtr SnapshotsDirNode;
    const TDuration SnapshotsDirRefreshInterval;
    ui64 LastSnapshotsDirRefreshCycles = 0;
    TMap<ui16, ui32> AntiCollisionToSnapViewMap;
    std::function<void()> OnSnapshotsChanged;

public:
    TWekaNodeLoader(
        TFileHandle&& rootHandle,
        TFileId&& rootFileId,
        const TDuration& snapshotsDirRefreshInterval,
        std::function<void()> onSnapshotsChanged)
        : TNodeLoader(std::move(rootHandle), std::move(rootFileId))
        , SnapshotsDirRefreshInterval(snapshotsDirRefreshInterval)
        , OnSnapshotsChanged(std::move(onSnapshotsChanged))
    {}

    TIndexNodePtr LoadNode(ui64 nodeId) const override
    {
        NLowLevel::TFileId fileId(RootFileId);
        fileId.WekaInodeId.InodeId = nodeId >> 16;
        fileId.WekaInodeId.AntiCollisionId = nodeId & 0xffff;
        auto snapViewId = GetSnapViewId(fileId.WekaInodeId.AntiCollisionId);
        if (!snapViewId) {
            return nullptr;
        }
        fileId.WekaInodeId.SnapViewId = *snapViewId;

        try {
            auto handle = fileId.Open(RootHandle, O_PATH);
            return std::make_shared<TIndexNode>(nodeId, std::move(handle));
        } catch (...) {
        }

        return nullptr;
    }

    TIndexNodePtr LoadSnapshotsDirNode() override
    {
        // We need a handle to the snapshots dir which resides at the root
        // of weka file system. The mount root (fsdir) also resides at this
        // level.
        // Example:
        //   root:                  /mnt/weka/wekadir/fsdir
        //   snapshots dir:         /mnt/weka/wekadir/.snapshots
        //
        // This is a special directory with InodeId == 0x2
        if (!SnapshotsDirNode) {
            NLowLevel::TFileId snapshotFileId(RootFileId);
            snapshotFileId.WekaInodeId.InodeId = 0x2;
            SnapshotsDirNode =
                std::move(LoadNode(snapshotFileId.WekaInodeId.InodeContext));

            RefreshAntiCollisionToSnapViewMap();
        }

        return SnapshotsDirNode;
    }

    TIndexNodePtr LoadSnapshotDirNode(ui64 snapshotDirNodeId) override
    {
        RefreshAntiCollisionToSnapViewMap();

        // We need a handle to the snapshot view of the mount root.
        // Example:
        //   root:                  /mnt/weka/wekadir/fsdir
        //   snapshots dir:         /mnt/weka/wekadir/.snapshots
        //   snapshot dir:          /mnt/weka/wekadir/.snapshots/snap1
        //   snapshot view of root: /mnt/weka/wekadir/.snapshots/snap1/fsdir
        // We are given the inode (snapshotRootNodeId) for the snapshot dir:
        //   /mnt/weka/wekadir/.snapshots/snap1
        // We can build a handle to the snapshot view of the mount root by
        // taking a handle for the mount root inode, but replace its
        // AntiCollisionId and SnapViewId with those from the snapshot inode
        // so the handle resolves to the snapshot view of fsdir.

        // Copy InodeId and FsId from RootFileId
        NLowLevel::TFileId snapshotFileId(RootFileId);

        // snapshotDirNodeId is a combination of InodeId and AntiCollisionId
        //   union {
        //       ui64 InodeContext;
        //       struct {
        //           ui64 AntiCollisionId :16;
        //           ui64 InodeId :48;
        //       };
        //   };
        // Use AntiCollisionId from the snapshot dir inode to build a handle to
        // the snapshot view of the mount root as described above
        snapshotFileId.WekaInodeId.AntiCollisionId = snapshotDirNodeId & 0xffff;

        auto snapViewId =
            GetSnapViewId(snapshotFileId.WekaInodeId.AntiCollisionId);
        if (!snapViewId) {
            return nullptr;
        }
        snapshotFileId.WekaInodeId.SnapViewId = *snapViewId;

        return LoadNode(snapshotFileId.WekaInodeId.InodeContext);
    }

private:
    std::optional<ui32> GetSnapViewId(ui16 antiCollisionId) const
    {
        if (antiCollisionId == RootFileId.WekaInodeId.AntiCollisionId) {
            return {RootFileId.WekaInodeId.SnapViewId};
        }

        auto it = AntiCollisionToSnapViewMap.find(antiCollisionId);
        if (it != AntiCollisionToSnapViewMap.end()) {
            return {it->second};
        }

        return {};
    }

    void RefreshAntiCollisionToSnapViewMap()
    {
        if (!SnapshotsDirNode) {
            return;
        }

        if (LastSnapshotsDirRefreshCycles &&
            CyclesToDurationSafe(
                GetCycleCount() - LastSnapshotsDirRefreshCycles) <
                SnapshotsDirRefreshInterval)
        {
            return;
        }

        TMap<ui16, ui32> newMap;
        TVector<NLowLevel::TDirEntry> entries;

        try {
            entries = SnapshotsDirNode->List(true /* ignore errors */);
        } catch (...) {
            ReportLocalFsFailedToRefreshSnapshotsDir(
                TStringBuilder()
                << "List failed, exception=" << CurrentExceptionMessage());
        }

        for (auto& entry: entries) {
            try {
                if (!entry.second.IsDir()) {
                    continue;
                }

                auto handle =
                    SnapshotsDirNode->OpenHandle(entry.first, O_RDONLY, 0);
                TFileId fileId(handle);
                newMap[fileId.WekaInodeId.AntiCollisionId] =
                    fileId.WekaInodeId.SnapViewId;
            } catch (...) {
                ReportLocalFsFailedToRefreshSnapshotsDir(
                    TStringBuilder()
                    << "Open failed, name=" << entry.first
                    << ", exception=" << CurrentExceptionMessage());
            }
        }

        LastSnapshotsDirRefreshCycles = GetCycleCount();

        if (newMap != AntiCollisionToSnapViewMap) {
            AntiCollisionToSnapViewMap = std::move(newMap);
            OnSnapshotsChanged();
        }
    }
};

INodeLoaderPtr CreateNodeLoader(
    const TIndexNodePtr& rootNode,
    const TDuration& snapshotsDirRefreshInterval,
    std::function<void()> onSnapshotsChanged)
{
    using TFileId = NLowLevel::TFileId;
    using EFileIdType = NLowLevel::TFileId::EFileIdType;

    auto rootHandle = rootNode->OpenHandle(O_RDONLY);
    TFileId rootFileId(rootHandle);
    EFileIdType fsType(EFileIdType(rootFileId.FileHandle.handle_type));

    switch (fsType) {
        case EFileIdType::Lustre:
            return std::make_shared<TLustreNodeLoader>(
                std::move(rootHandle),
                std::move(rootFileId));
        case EFileIdType::Weka:
            return std::make_shared<TWekaNodeLoader>(
                std::move(rootHandle),
                std::move(rootFileId),
                snapshotsDirRefreshInterval,
                std::move(onSnapshotsChanged));
        case EFileIdType::VastNfs:
            return std::make_shared<TVastNfsNodeLoader>(
                std::move(rootHandle),
                std::move(rootFileId));
    }

    STORAGE_THROW_SERVICE_ERROR(E_FS_NOTSUPP)
        << "Not supported hande type, RootFileId=" << rootFileId.ToString();

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TNodeMapper::TNodeMapper(INodeLoader& nodeLoader, TRWMutex& nodeLoaderLock)
    : NodeLoader(nodeLoader)
    , NodeLoaderLock(nodeLoaderLock)
{
    TWriteGuard guard(NodeLoaderLock);

    SnapshotsNode = std::move(NodeLoader.LoadSnapshotsDirNode());
    if (!SnapshotsNode) {
        STORAGE_THROW_SERVICE_ERROR(E_FS_NOTSUPP)
            << "Node loader doesn't support snapshots directory";
    }
}

TString TNodeMapper::ToString() const
{
    return TStringBuilder() << "NodeMapper(SnapshotsNodeId=0x"
                            << Hex(SnapshotsNode->GetNodeId()) << ")";
}

TNodeMapper::TRemapResult TNodeMapper::RemapNode(
    ui64 parentNodeId,
    const TString& name)
{
    if (parentNodeId == RootNodeId && name == ".snapshots") {
        return TRemapResult::Remapped(SnapshotsNode);
    }

    if (parentNodeId != SnapshotsNode->GetNodeId()) {
        return TRemapResult::NotNeeded();
    }

    NLowLevel::TFileStatEx stat;

    try {
        stat = SnapshotsNode->Stat(name);
    } catch (...) {
        // name doesn't exist in snapshots directory
        return TRemapResult::NotFound();
    }

    if (!stat.IsDir()) {
        // ignore non-directory nodes in snapshots directory
        return TRemapResult::NotFound();
    }

    try {
        TWriteGuard guard(NodeLoaderLock);
        return TRemapResult::Remapped(
            NodeLoader.LoadSnapshotDirNode(stat.INode));
    } catch (...) {
        ReportLocalFsFailedToLoadSnapshotDir(
            TStringBuilder()
            << "name=" << name << ", exception=" << CurrentExceptionMessage());
    }

    return TRemapResult::NotFound();
}

TNodeMapper::TRemapResult TNodeMapper::RemapNode(
    ui64 parentNodeId,
    ui64 nodeId)
{
    if (parentNodeId != SnapshotsNode->GetNodeId()) {
        return TRemapResult::NotNeeded();
    }

    try {
        TWriteGuard guard(NodeLoaderLock);
        return TRemapResult::Remapped(
            NodeLoader.LoadSnapshotDirNode(nodeId));
    } catch (...) {
        ReportLocalFsFailedToLoadSnapshotDir(
            TStringBuilder() << "nodeId=0x" << Hex(nodeId)
                             << ", exception=" << CurrentExceptionMessage());
    }

    return TRemapResult::NotFound();
}
}   // namespace NCloud::NFileStore
