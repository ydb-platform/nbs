#include "index.h"

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

TFileStat TIndexNode::Stat()
{
    return NLowLevel::Stat(NodeFd);
}

TFileStat TIndexNode::Stat(const TString& name)
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

NLowLevel::TOpenOrCreateResult
TIndexNode::OpenOrCreateHandle(const TString& name, int flags, int mode)
{
    return NLowLevel::OpenOrCreateAt(NodeFd, name.data(), flags, mode);
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

TNodeLoader::TNodeLoader(const TIndexNodePtr& rootNode)
    : RootHandle(rootNode->OpenHandle(O_RDONLY))
    , RootFileId(RootHandle)
{
    switch (NLowLevel::TFileId::EFileIdType(RootFileId.FileHandle.handle_type)) {
    case NLowLevel::TFileId::EFileIdType::Lustre:
    case NLowLevel::TFileId::EFileIdType::Weka:
    case NLowLevel::TFileId::EFileIdType::VastNfs:
        break;
    default:
        ythrow TServiceError(E_FS_NOTSUPP)
            << "Not supported hande type, RootFileId=" << RootFileId.ToString();
    }
}

TIndexNodePtr TNodeLoader::LoadNode(ui64 nodeId) const
{
    NLowLevel::TFileId fileId(RootFileId);

    switch (NLowLevel::TFileId::EFileIdType(fileId.FileHandle.handle_type)) {
    case NLowLevel::TFileId::EFileIdType::Lustre:
        fileId.LustreFid.Oid = nodeId & 0xffffff;
        fileId.LustreFid.Seq = (nodeId >> 24) & 0xffffffffff;
        break;
    case NLowLevel::TFileId::EFileIdType::Weka:
        fileId.WekaInodeId.Id = nodeId;
        break;
    case NLowLevel::TFileId::EFileIdType::VastNfs:
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
        break;
    default:
        ythrow TServiceError(E_FS_NOTSUPP);
    }

    auto handle =  fileId.Open(RootHandle, O_PATH);
    return std::make_shared<TIndexNode>(nodeId, std::move(handle));
}

TString TNodeLoader::ToString() const
{
    return TStringBuilder() << "NodeLoader(" << RootFileId.ToString() << ")";
}

}   // namespace NCloud::NFileStore
