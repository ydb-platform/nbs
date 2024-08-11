#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/system/fstat.h>
#include <util/system/rwlock.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TIndexNode
    : private TNonCopyable
{
private:
    const ui64 NodeId;
    const TFileHandle NodeFd;

public:
    TIndexNode(ui64 nodeId, TFileHandle node)
        : NodeId(nodeId)
        , NodeFd(std::move(node))
    {}

    static TIndexNodePtr CreateRoot(const TFsPath& path);
    static TIndexNodePtr Create(const TIndexNode& parent, const TString& name);

    ui64 GetNodeId() const
    {
        return NodeId;
    }

    TIndexNodePtr CreateFile(const TString& name, int flags);
    TIndexNodePtr CreateDirectory(const TString& name, int flags);
    TIndexNodePtr CreateLink(const TIndexNode& parent, const TString& name);
    TIndexNodePtr CreateSymlink(const TString& name, const TString& target);
    TIndexNodePtr CreateSocket(const TString& name, int flags);

    TVector<std::pair<TString, TFileStat>> List(bool ignoreErrors = false);

    void Rename(
        const TString& name,
        const TIndexNodePtr& newparent,
        const TString& newname,
        unsigned int flags);
    void Unlink(const TString& name, bool directory);

    TString ReadLink() const;

    TFileStat Stat();
    TFileStat Stat(const TString& name);

    TFileHandle OpenHandle(int flags);
    TFileHandle OpenHandle(const TString& name, int flags, int mode);

    //
    // Attrs
    //

    void Access(int mode);
    void Chmod(int mode);
    void Chown(unsigned int uid, unsigned int gid);
    void Utimes(TInstant atime, TInstant mtime);
    void Truncate(size_t size);

    //
    // X Attrs
    //

    TVector<TString> ListXAttrs();
    void SetXAttr(const TString& name, const TString& value);
    TString GetXAttr(const TString& name);
    void RemoveXAttr(const TString& name);
};

////////////////////////////////////////////////////////////////////////////////

class TLocalIndex
{
private:
    struct THash
    {
        template <typename T>
        size_t operator ()(const T& value) const
        {
            return IntHash(GetNodeId(value));
        }
    };

    struct TEqual
    {
        template <typename T1, typename T2>
        bool operator ()(const T1& l, const T2& r) const
        {
            return GetNodeId(l) == GetNodeId(r);
        }
    };

public:
    using TNodeMap = THashSet<TIndexNodePtr, THash, TEqual>;

private:
    TNodeMap Nodes;
    TRWMutex NodesLock;

public:
    TLocalIndex(TNodeMap nodes)
        : Nodes(std::move(nodes))
    {}

    TIndexNodePtr LookupNode(ui64 nodeId)
    {
        TReadGuard guard(NodesLock);

        auto it = Nodes.find(nodeId);
        if (it == Nodes.end()) {
            return nullptr;
        }

        return *it;
    }

    bool TryInsertNode(TIndexNodePtr node)
    {
        TWriteGuard guard(NodesLock);

        auto it = Nodes.find(node->GetNodeId());
        if (it != Nodes.end()) {
            return false;
        }

        Nodes.emplace(std::move(node));
        return true;
    }

    void ForgetNode(ui64 nodeId)
    {
        TWriteGuard guard(NodesLock);

        auto it = Nodes.find(nodeId);
        if (it != Nodes.end()) {
            Nodes.erase(it);
        }
    }

private:
    static auto GetNodeId(const TIndexNodePtr& node)
    {
        return node->GetNodeId();
    }

    template <typename T>
    static auto GetNodeId(const T& value)
    {
        return value;
    }
};

}   // namespace NCloud::NFileStore
