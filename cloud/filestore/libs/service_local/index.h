#pragma once

#include "public.h"

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
{
private:
    const ui64 NodeId;
    const TFileHandle NodeFd;
    ui64 RecordIndex;
    ui64 TableIndex;

public:
    TIndexNode(ui64 nodeId, TFileHandle node)
        : NodeId(nodeId)
        , NodeFd(std::move(node))
    {}

    ui64 GetRecordIndex() const
    {
        return RecordIndex;
    }

    void SetRecordIndex(ui64 index)
    {
        RecordIndex = index;
    }

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

    struct TNodeTableHeader
    {
    };

    struct TNodeTableRecord
    {
        ui64 NodeId;
        ui64 ParentNodeId;
        char Name[NAME_MAX + 1];
    };


private:
    using TNodeMap = THashSet<TIndexNodePtr, THash, TEqual>;
    using TNodeTable = TPersistentTable<TNodeTableHeader, TNodeTableRecord>;

    TNodeMap Nodes;
    std::unique_ptr<TNodeTable> NodesTable;
    TRWMutex NodesLock;
    const TLog& Log;

public:
    TLocalIndex(
            const TFsPath& root,
            const TFsPath& statePath,
            ui32 maxInodeCount,
            const TLog& log)
        : Log(log)
    {
        Init(root, statePath, maxInodeCount);
    }

    TIndexNodePtr LookupNode(ui64 nodeId)
    {
        TReadGuard guard(NodesLock);

        auto it = Nodes.find(nodeId);
        if (it == Nodes.end()) {
            return nullptr;
        }

        return *it;
    }

    [[nodiscard]] bool
    TryInsertNode(TIndexNodePtr node, ui64 parentNodeId, const TString& name)
    {
        TWriteGuard guard(NodesLock);

        auto it = Nodes.find(node->GetNodeId());
        if (it != Nodes.end()) {
            return true;
        }

        auto recordIndex = NodesTable->AllocRecord();
        if (recordIndex == TNodeTable::InvalidIndex) {
            return false;
        }

        auto* record = NodesTable->RecordData(recordIndex);

        record->NodeId = node->GetNodeId();
        record->ParentNodeId = parentNodeId;

        std::strncpy(record->Name, name.c_str(), NAME_MAX);
        record->Name[NAME_MAX] = 0;

        NodesTable->CommitRecord(recordIndex);

        node->SetRecordIndex(recordIndex);
        Nodes.emplace(std::move(node));

        return true;
    }

    TIndexNodePtr ForgetNode(ui64 nodeId)
    {
        TWriteGuard guard(NodesLock);

        TIndexNodePtr node = nullptr;
        auto it = Nodes.find(nodeId);
        if (it != Nodes.end()) {
            node = *it;
            NodesTable->DeleteRecord(node->GetRecordIndex());
            Nodes.erase(it);
        }

        return node;
    }

private:
    void Init(const TFsPath& root, const TFsPath& statePath, ui32 maxInodeCount)
    {
        STORAGE_INFO(
            "Init index, Root=" << root <<
            ", StatePath=" << statePath
            << ", MaxInodeCount=" << maxInodeCount);

        Nodes.insert(TIndexNode::CreateRoot(root));

        NodesTable = std::make_unique<TNodeTable>(
            (statePath / "nodes").GetPath(),
            maxInodeCount);

        TMap<ui64, ui64> unresolvedRecords;
        for (auto it = NodesTable->begin(); it != NodesTable->end(); it++) {
            unresolvedRecords[it->NodeId] = it.GetIndex();
            STORAGE_TRACE(
                "Unresolved record, NodeId=" << it->NodeId <<
                ", ParentNodeId=" << it->ParentNodeId <<
                ", Name=" << it->Name);
        }

        while (!unresolvedRecords.empty()) {
            TStack<ui64> unresolvedPath;
            unresolvedPath.push(unresolvedRecords.begin()->second);

            while (!unresolvedPath.empty()) {
                auto pathElemIndex = unresolvedPath.top();
                auto pathElemRecord = NodesTable->RecordData(pathElemIndex);

                STORAGE_TRACE(
                    "Resolve node start, NodeId=" << pathElemRecord->NodeId);

                auto parentNodeIt = Nodes.find(pathElemRecord->ParentNodeId);
                if (parentNodeIt == Nodes.end()) {
                    STORAGE_TRACE(
                        "Need to resolve parent NodeId="
                        << pathElemRecord->ParentNodeId);
                    auto parentRecordIt =
                        unresolvedRecords.find(pathElemRecord->ParentNodeId);
                    if (parentRecordIt == unresolvedRecords.end()) {
                        STORAGE_ERROR(
                            "Parent node is missing in table, NodeId="
                            << pathElemRecord->ParentNodeId);
                        while (!unresolvedPath.empty()) {
                            auto discardedIndex = unresolvedPath.top();
                            auto* discardedRecord =
                                NodesTable->RecordData(discardedIndex);
                            STORAGE_WARN(
                                "Discarding NodeId="
                                << discardedRecord->NodeId);
                            unresolvedRecords.erase(discardedRecord->NodeId);
                            NodesTable->DeleteRecord(discardedIndex);
                            unresolvedPath.pop();
                        }
                        continue;
                    }

                    unresolvedPath.push(parentRecordIt->second);
                    continue;
                }

                auto node =
                    TIndexNode::Create(**parentNodeIt, pathElemRecord->Name);
                node->SetRecordIndex(pathElemIndex);

                Nodes.insert(std::move(node));

                unresolvedPath.pop();
                unresolvedRecords.erase(pathElemRecord->NodeId);

                STORAGE_TRACE(
                    "Resolve node end, NodeId=" << pathElemRecord->NodeId);
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

};

}   // namespace NCloud::NFileStore
