#include "index.h"

#include "lowlevel.h"

#include <cloud/filestore/libs/service/filestore.h>

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

TVector<std::pair<TString, TFileStat>> TIndexNode::List(bool ignoreErrors)
{
    return NLowLevel::ListDirAt(NodeFd, ignoreErrors);
}

TFileStat TIndexNode::Stat()
{
    return NLowLevel::Stat(NodeFd);
}

TFileStat TIndexNode::Stat(const TString& name)
{
    return NLowLevel::StatAt(NodeFd, name);
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

}   // namespace NCloud::NFileStore
