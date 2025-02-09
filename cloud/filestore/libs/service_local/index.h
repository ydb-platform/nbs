#pragma once

#include "public.h"

#include "lowlevel.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/persistent_table.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/stack.h>
#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/system/fstat.h>
#include <util/system/rwlock.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TIndexNode
    : private TNonCopyable
    , public TIntrusiveListItem<TIndexNode>
{
private:
    const ui64 NodeId;
    const TFileHandle NodeFd;
    ui64 RecordIndex = -1;

public:
    TIndexNode(ui64 nodeId, TFileHandle node)
        : NodeId(nodeId)
        , NodeFd(std::move(node))
    {}

    [[nodiscard]] ui64 GetRecordIndex() const
    {
        return RecordIndex;
    }

    void SetRecordIndex(ui64 index)
    {
        RecordIndex = index;
    }

    static TIndexNodePtr CreateRoot(const TFsPath& path);
    static TIndexNodePtr Create(const TIndexNode& parent, const TString& name);

    [[nodiscard]] ui64 GetNodeId() const
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

    [[nodiscard]] TString ReadLink() const;

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

struct INodeLoader
{
    virtual ~INodeLoader() = default;
    virtual TIndexNodePtr LoadNode(ui64 nodeId) const = 0;
    virtual TString ToString() const = 0;
};

class TNodeLoader
    : public INodeLoader
{
private:
    TFileHandle RootHandle;
    NLowLevel::TFileId RootFileId;

public:
    TNodeLoader(const TIndexNodePtr& rootNode);

    [[nodiscard]] TIndexNodePtr LoadNode(ui64 nodeId) const;

    TString ToString() const;
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

    struct TNodeTableHeader
    {
    };

    struct TNodeTableRecord
    {
        ui64 NodeId = 0;
        ui64 ParentNodeId = 0;
        char Name[NAME_MAX + 1] = {};
    };

    using TNodeMap = THashSet<TIndexNodePtr, THash, TEqual>;
    using TNodeTable = TPersistentTable<TNodeTableHeader, TNodeTableRecord>;

    const TFsPath RootPath;
    const TFsPath StatePath;
    ui32 MaxNodeCount;
    bool OpenNodeByHandleEnabled;
    ui32 NodeCleanupBatchSize;
    TNodeMap Nodes;
    std::unique_ptr<TNodeTable> NodeTable;
    TRWMutex NodesLock;
    TLog Log;
    std::shared_ptr<INodeLoader> NodeLoader;
    TIntrusiveList<TIndexNode> NodeAccessList;
    bool DoCleanupNodes = false;

public:
    TLocalIndex(
            TFsPath root,
            TFsPath statePath,
            ui32 maxNodeCount,
            bool openNodeByHandleEnabled,
            ui32 nodeCleanupBatchSize,
            TLog log,
            std::shared_ptr<INodeLoader> nodeLoader = nullptr)
        : RootPath(std::move(root))
        , StatePath(std::move(statePath))
        , MaxNodeCount(maxNodeCount)
        , OpenNodeByHandleEnabled(openNodeByHandleEnabled)
        , NodeCleanupBatchSize(nodeCleanupBatchSize)
        , Log(std::move(log))
    {
        Init(std::move(nodeLoader));
    }

    TIndexNodePtr LookupNode(ui64 nodeId)
    {
        CleanupNodesIfNeeded();

        TReadGuard guard(NodesLock);

        TIndexNodePtr node;

        auto it = Nodes.find(nodeId);
        if (it != Nodes.end()) {
            node = *it;
        } else {
            node = LoadNodeById(nodeId);
            if (node) {
                Nodes.insert(node);
            }
        }

        if (nodeId != RootNodeId && node) {
            NodeAccessList.PushBack(node.get());
        }

        return node;
    }

    [[nodiscard]] bool
    TryInsertNode(TIndexNodePtr node, ui64 parentNodeId, const TString& name)
    {
        CleanupNodesIfNeeded();

        TWriteGuard guard(NodesLock);

        auto it = Nodes.find(node->GetNodeId());
        if (it != Nodes.end()) {
            // TODO: we can find existing node id for hard link since it has the
            // same node id
            return true;
        }

        if (NodeTable) {
            auto recordIndex = NodeTable->AllocRecord();
            if (recordIndex == TNodeTable::InvalidIndex) {
                return false;
            }

            auto* record = NodeTable->RecordData(recordIndex);

            record->NodeId = node->GetNodeId();
            record->ParentNodeId = parentNodeId;

            std::strncpy(record->Name, name.c_str(), NAME_MAX);
            record->Name[NAME_MAX] = 0;

            NodeTable->CommitRecord(recordIndex);

            node->SetRecordIndex(recordIndex);
        }

        NodeAccessList.PushBack(node.get());
        Nodes.emplace(std::move(node));
        return true;
    }

    TIndexNodePtr ForgetNode(ui64 nodeId)
    {
        TWriteGuard guard(NodesLock);

        return ForgetNodeNoLock(nodeId);
    }

    void Clear()
    {
        TWriteGuard guard(NodesLock);

        if (NodeTable) {
            NodeTable->Clear();
        }

        auto it = Nodes.find(RootNodeId);
        Y_ABORT_UNLESS(it != Nodes.end());

        auto root = *it;
        Nodes.clear();
        Nodes.insert(root);
    }

private:
    void Init(std::shared_ptr<INodeLoader> nodeLoader)
    {
        auto root = TIndexNode::CreateRoot(RootPath);
        STORAGE_INFO(
            "Init index, Root=" << RootPath <<
            ", StatePath=" << StatePath <<
            ", MaxNodeCount=" << MaxNodeCount);

        if (OpenNodeByHandleEnabled) {
            try {
                if (nodeLoader) {
                    NodeLoader = std::move(nodeLoader);
                } else {
                    NodeLoader = std::make_unique<TNodeLoader>(root);
                }

                STORAGE_INFO(
                    "UseNodeFileHandles, Root=" << RootPath <<
                    ", Inode=" << root->Stat().INode <<
                    ", NodeLoader=" << NodeLoader->ToString());
            } catch (...) {
                STORAGE_ERROR(
                    "Failed to initialize NodeLoader" <<
                    ", Exception=" << CurrentExceptionMessage());
            }
        }

        Nodes.insert(root);

        if (!NodeLoader) {
            NodeTable = std::make_unique<TNodeTable>(
                (StatePath / "nodes").GetPath(),
                MaxNodeCount);

            RecoverNodesFromPersistentTable();
        }

    }

    void RecoverNodesFromPersistentTable()
    {
        // enties are ordered by NodeId in TMap but this doesn't mean that
        // a/b/c/d has order a, b, c, d usually inode number increased but
        // directories can move so directory a which was created later can
        // contain directory b which was created before so  and NodeId(a) >
        // NodeId(b) for a/b
        TMap<ui64, ui64> unresolvedRecords;
        for (auto it = NodeTable->begin(); it != NodeTable->end(); it++) {
            unresolvedRecords[it->NodeId] = it.GetIndex();
            STORAGE_TRACE(
                "Unresolved record, NodeId=" << it->NodeId << ", ParentNodeId="
                                             << it->ParentNodeId
                                             << ", Name=" << it->Name);
        }

        while (!unresolvedRecords.empty()) {
            TStack<ui64> unresolvedPath;
            unresolvedPath.push(unresolvedRecords.begin()->second);

            // For entry /a we can resolve immediately and create TIndexNode
            // but for entry d in /a/b/c/d path we must resolve the whole path
            // recursively
            while (!unresolvedPath.empty()) {
                auto pathElemIndex = unresolvedPath.top();
                auto* pathElemRecord = NodeTable->RecordData(pathElemIndex);

                STORAGE_TRACE(
                    "Resolve node start, NodeId=" << pathElemRecord->NodeId);

                auto parentNodeIt = Nodes.find(pathElemRecord->ParentNodeId);
                if (parentNodeIt == Nodes.end()) {
                    // parent is not resolved

                    STORAGE_TRACE(
                        "Need to resolve parent NodeId="
                        << pathElemRecord->ParentNodeId);
                    auto parentRecordIt =
                        unresolvedRecords.find(pathElemRecord->ParentNodeId);
                    if (parentRecordIt == unresolvedRecords.end()) {
                        // parent was not saved in persistent table so we can't
                        // resolve it in case of d in path /a/b/c/d if we
                        // discover that b can't be resolved we need to discard
                        // b, c, d inodes
                        STORAGE_ERROR(
                            "Parent node is missing in table, NodeId="
                            << pathElemRecord->ParentNodeId);
                        while (!unresolvedPath.empty()) {
                            auto discardedIndex = unresolvedPath.top();
                            auto* discardedRecord =
                                NodeTable->RecordData(discardedIndex);
                            STORAGE_WARN(
                                "Discarding NodeId="
                                << discardedRecord->NodeId);
                            unresolvedRecords.erase(discardedRecord->NodeId);
                            NodeTable->DeleteRecord(discardedIndex);
                            unresolvedPath.pop();
                        }
                        continue;
                    }

                    // add to unresolvedPath and solve recursively
                    unresolvedPath.push(parentRecordIt->second);
                    continue;
                }

                // parent already resolved so we can create node and resolve
                // this entry
                try {
                    auto node =
                        TIndexNode::Create(**parentNodeIt, pathElemRecord->Name);
                    node->SetRecordIndex(pathElemIndex);
                    NodeAccessList.PushBack(node.get());
                    Nodes.insert(node);

                    STORAGE_TRACE(
                        "Resolve node end, NodeId=" << pathElemRecord->NodeId);
                } catch (const TServiceError& e) {
                    STORAGE_ERROR(
                        "Resolve node failed, NodeId="
                        << pathElemRecord->NodeId
                        << ", Exception=" << FormatResultCode(e.GetCode())
                        << " " << e.GetMessage());
                    NodeTable->DeleteRecord(pathElemIndex);
                }

                unresolvedPath.pop();
                unresolvedRecords.erase(pathElemRecord->NodeId);
            }
        }
    }

    static auto GetNodeId(const TIndexNodePtr& node)
    {
        return node->GetNodeId();
    }

    template <typename T>
    static auto GetNodeId(const T& value)
    {
        return value;
    }

    TIndexNodePtr LoadNodeById(ui64 nodeId)
    {
        try {
            if (NodeLoader) {
                return NodeLoader->LoadNode(nodeId);
            }
        } catch (...) {
        }
        return nullptr;
    }

    TIndexNodePtr ForgetNodeNoLock(ui64 nodeId)
    {
        TIndexNodePtr node = nullptr;
        auto it = Nodes.find(nodeId);
        if (it != Nodes.end()) {
            node = *it;
            if (NodeTable) {
                NodeTable->DeleteRecord(node->GetRecordIndex());
            }
            node->TIntrusiveListItem<TIndexNode>::Unlink();
            Nodes.erase(it);
        }

        return node;
    }

    void CleanupNodesIfNeeded()
    {
        // Clean nodes only if we can safely load them
        if (!NodeLoader) {
            return;
        }

        // Clean one node per lookup node, or `NodeCleanupBatchSize` nodes if
        // `MaxNodeCount` is reached
        ui32 maxNodesToClean = 1;
        if (Nodes.size() >= MaxNodeCount) {
            maxNodesToClean = NodeCleanupBatchSize;
            DoCleanupNodes = true;
        }

        if (!DoCleanupNodes) {
            return;
        }

        // Stop cleanup when the number of nodes in use drops to half the limit.
        if (DoCleanupNodes && (Nodes.size() <= (MaxNodeCount / 2))) {
            DoCleanupNodes = false;
            return;
        }

        TWriteGuard guard(NodesLock);

        auto it = NodeAccessList.begin();
        while (maxNodesToClean && it != NodeAccessList.end()) {
            auto nodeId = it->GetNodeId();
            ++it;
            --maxNodesToClean;
            ForgetNodeNoLock(nodeId);
        }
    }
};

}   // namespace NCloud::NFileStore
