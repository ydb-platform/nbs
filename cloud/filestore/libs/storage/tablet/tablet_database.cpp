#include "tablet_database.h"

#include "tablet_schema.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletDatabase::InitSchema(bool useNoneCompactionPolicy)
{
    Materialize<TIndexTabletSchema>();

    TSchemaInitializer<TIndexTabletSchema::TTables>::InitStorage(
        useNoneCompactionPolicy,
        Database.Alter());
}

////////////////////////////////////////////////////////////////////////////////
// FileSystem

static constexpr ui32 FileSystemMetaId = 0;

void TIndexTabletDatabase::WriteFileSystem(
    const NProto::TFileSystem& fileSystem)
{
    using TTable = TIndexTabletSchema::FileSystem;

    Table<TTable>()
        .Key(FileSystemMetaId)
        .Update(NIceDb::TUpdate<TTable::Proto>(fileSystem));
}

bool TIndexTabletDatabase::ReadFileSystem(NProto::TFileSystem& fileSystem)
{
    using TTable = TIndexTabletSchema::FileSystem;

    auto it = Table<TTable>()
        .Key(FileSystemMetaId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        fileSystem = it.GetValue<TTable::Proto>();
    }

    return true;
}

bool TIndexTabletDatabase::ReadFileSystemStats(NProto::TFileSystemStats& stats)
{
    using TTable = TIndexTabletSchema::FileSystem;

    auto it = Table<TTable>()
        .Key(FileSystemMetaId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
#define FILESTORE_IMPLEMENT_STATS(name, ...)                                   \
        stats.Set##name(it.GetValue<TTable::name>());                          \
// FILESTORE_IMPLEMENT_STATS

FILESTORE_FILESYSTEM_STATS(FILESTORE_IMPLEMENT_STATS)

#undef FILESTORE_IMPLEMENT_STATS
    }

    return true;
}

#define FILESTORE_IMPLEMENT_STATS(name, ...)                                   \
    void TIndexTabletDatabase::Write##name(ui64 value)                         \
    {                                                                          \
        using TTable = TIndexTabletSchema::FileSystem;                         \
        Table<TTable>()                                                        \
            .Key(FileSystemMetaId)                                             \
            .Update(NIceDb::TUpdate<TTable::name>(value));                     \
    }                                                                          \
// FILESTORE_IMPLEMENT_STATS

FILESTORE_FILESYSTEM_STATS(FILESTORE_IMPLEMENT_STATS)

#undef FILESTORE_IMPLEMENT_STATS


void TIndexTabletDatabase::WriteStorageConfig(
    const NProto::TStorageConfig& storageConfig)
{
    using TTable = TIndexTabletSchema::FileSystem;

    Table<TTable>()
        .Key(FileSystemMetaId)
        .Update(NIceDb::TUpdate<TTable::StorageConfig>(storageConfig));
}

bool TIndexTabletDatabase::ReadStorageConfig(
    TMaybe<NProto::TStorageConfig>& storageConfig)
{
    using TTable = TIndexTabletSchema::FileSystem;

    auto it = Table<TTable>()
        .Key(FileSystemMetaId)
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        storageConfig = it.GetValue<TTable::StorageConfig>();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// Nodes

void TIndexTabletDatabase::WriteNode(
    ui64 nodeId,
    ui64 commitId,
    const NProto::TNode& attrs)
{
    using TTable = TIndexTabletSchema::Nodes;

    Table<TTable>()
        .Key(nodeId)
        .Update(
            NIceDb::TUpdate<TTable::CommitId>(commitId),
            NIceDb::TUpdate<TTable::Proto>(attrs)
        );
}

void TIndexTabletDatabase::DeleteNode(ui64 nodeId)
{
    using TTable = TIndexTabletSchema::Nodes;

    Table<TTable>()
        .Key(nodeId)
        .Delete();
}

bool TIndexTabletDatabase::ReadNode(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    using TTable = TIndexTabletSchema::Nodes;

    auto it = Table<TTable>()
        .Key(nodeId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        ui64 minCommitId = it.GetValue<TTable::CommitId>();
        ui64 maxCommitId = InvalidCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            node = TNode {
                nodeId,
                it.GetValue<TTable::Proto>(),
                minCommitId,
                maxCommitId
            };
        }
    }

    return true;
}

bool TIndexTabletDatabase::ReadNodes(
    ui64 startNodeId,
    ui64 maxNodes,
    ui64& nextNodeId,
    TVector<TNode>& nodes)
{
    using TTable = TIndexTabletSchema::Nodes;

    auto it = Table<TTable>().GreaterOrEqual(startNodeId).Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid() && maxNodes > 0) {
        nodes.emplace_back(TNode{
            it.GetValue<TTable::NodeId>(),
            it.GetValue<TTable::Proto>(),
            it.GetValue<TTable::CommitId>(),
            InvalidCommitId});

        --maxNodes;

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    if (it.IsValid()) {
        nextNodeId = it.GetValue<TTable::NodeId>();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// Nodes_Ver

void TIndexTabletDatabase::WriteNodeVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const NProto::TNode& attrs)
{
    using TTable = TIndexTabletSchema::Nodes_Ver;

    Table<TTable>()
        .Key(nodeId, ReverseCommitId(minCommitId))
        .Update(
            NIceDb::TUpdate<TTable::MaxCommitId>(maxCommitId),
            NIceDb::TUpdate<TTable::Proto>(attrs)
        );
}

void TIndexTabletDatabase::DeleteNodeVer(ui64 nodeId, ui64 commitId)
{
    using TTable = TIndexTabletSchema::Nodes_Ver;

    Table<TTable>()
        .Key(nodeId, ReverseCommitId(commitId))
        .Delete();
}

bool TIndexTabletDatabase::ReadNodeVer(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    using TTable = TIndexTabletSchema::Nodes_Ver;

    auto it = Table<TTable>()
        .GreaterOrEqual(nodeId, ReverseCommitId(commitId))
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        if (nodeId != it.GetValue<TTable::NodeId>()) {
            // no more entries
            break;
        }

        ui64 minCommitId = ReverseCommitId(it.GetValue<TTable::MinCommitId>());
        ui64 maxCommitId = it.GetValueOrDefault<TTable::MaxCommitId>(InvalidCommitId);

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            node = TNode {
                nodeId,
                it.GetValue<TTable::Proto>(),
                minCommitId,
                maxCommitId
            };
            break;
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// NodeAttrs

void TIndexTabletDatabase::WriteNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    using TTable = TIndexTabletSchema::NodeAttrs;

    Table<TTable>()
        .Key(nodeId, name)
        .Update(
            NIceDb::TUpdate<TTable::CommitId>(commitId),
            NIceDb::TUpdate<TTable::Value>(value),
            NIceDb::TUpdate<TTable::Version>(version)
        );
}

void TIndexTabletDatabase::DeleteNodeAttr(ui64 nodeId, const TString& name)
{
    using TTable = TIndexTabletSchema::NodeAttrs;

    Table<TTable>()
        .Key(nodeId, name)
        .Delete();
}

bool TIndexTabletDatabase::ReadNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    using TTable = TIndexTabletSchema::NodeAttrs;

    auto it = Table<TTable>()
        .Key(nodeId, name)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        ui64 minCommitId = it.GetValue<TTable::CommitId>();
        ui64 maxCommitId = InvalidCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            attr = TNodeAttr {
                nodeId,
                name,
                it.GetValue<TTable::Value>(),
                minCommitId,
                maxCommitId,
                it.GetValueOrDefault<TTable::Version>(0)
            };
        }
    }

    return true;
}

bool TIndexTabletDatabase::ReadNodeAttrs(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    using TTable = TIndexTabletSchema::NodeAttrs;

    auto it = Table<TTable>()
        .Prefix(nodeId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        ui64 minCommitId = it.GetValue<TTable::CommitId>();
        ui64 maxCommitId = InvalidCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            attrs.emplace_back(TNodeAttr {
                nodeId,
                it.GetValue<TTable::Name>(),
                it.GetValue<TTable::Value>(),
                minCommitId,
                maxCommitId,
                it.GetValueOrDefault<TTable::Version>(0)
            });
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// NodeAttrs_Ver

void TIndexTabletDatabase::WriteNodeAttrVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    using TTable = TIndexTabletSchema::NodeAttrs_Ver;

    Table<TTable>()
        .Key(nodeId, name, ReverseCommitId(minCommitId))
        .Update(
            NIceDb::TUpdate<TTable::MaxCommitId>(maxCommitId),
            NIceDb::TUpdate<TTable::Value>(value),
            NIceDb::TUpdate<TTable::Version>(version));
}

void TIndexTabletDatabase::DeleteNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name)
{
    using TTable = TIndexTabletSchema::NodeAttrs_Ver;

    Table<TTable>()
        .Key(nodeId, name, ReverseCommitId(commitId))
        .Delete();
}

bool TIndexTabletDatabase::ReadNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    using TTable = TIndexTabletSchema::NodeAttrs_Ver;

    auto it = Table<TTable>()
        .GreaterOrEqual(nodeId, name, ReverseCommitId(commitId))
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        if (nodeId != it.GetValue<TTable::NodeId>() ||
            name != it.GetValue<TTable::Name>())
        {
            // no more entries
            break;
        }

        ui64 minCommitId = ReverseCommitId(it.GetValue<TTable::MinCommitId>());
        ui64 maxCommitId = it.GetValueOrDefault<TTable::MaxCommitId>(InvalidCommitId);

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            attr = TNodeAttr {
                nodeId,
                name,
                it.GetValue<TTable::Value>(),
                minCommitId,
                maxCommitId,
                it.GetValueOrDefault<TTable::Version>(0)
            };
            break;
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TIndexTabletDatabase::ReadNodeAttrVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    using TTable = TIndexTabletSchema::NodeAttrs_Ver;

    auto it = Table<TTable>()
        .Prefix(nodeId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        ui64 minCommitId = ReverseCommitId(it.GetValue<TTable::MinCommitId>());
        ui64 maxCommitId = it.GetValueOrDefault<TTable::MaxCommitId>(InvalidCommitId);

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            attrs.emplace_back(TNodeAttr {
                nodeId,
                it.GetValue<TTable::Name>(),
                it.GetValue<TTable::Value>(),
                minCommitId,
                maxCommitId,
                it.GetValueOrDefault<TTable::Version>(0)
            });
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// NodeRefs

void TIndexTabletDatabase::WriteNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    ui64 childNodeId,
    const TString& shardId,
    const TString& shardNodeName)
{
    using TTable = TIndexTabletSchema::NodeRefs;

    Table<TTable>()
        .Key(nodeId, name)
        .Update(
            NIceDb::TUpdate<TTable::CommitId>(commitId),
            NIceDb::TUpdate<TTable::ChildId>(childNodeId),
            NIceDb::TUpdate<TTable::ShardId>(shardId),
            NIceDb::TUpdate<TTable::ShardNodeName>(shardNodeName)
        );
}

void TIndexTabletDatabase::DeleteNodeRef(ui64 nodeId, const TString& name)
{
    using TTable = TIndexTabletSchema::NodeRefs;

    Table<TTable>()
        .Key(nodeId, name)
        .Delete();
}

bool TIndexTabletDatabase::ReadNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    using TTable = TIndexTabletSchema::NodeRefs;

    auto it = Table<TTable>()
        .Key(nodeId, name)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        ui64 minCommitId = it.GetValue<TTable::CommitId>();
        ui64 maxCommitId = InvalidCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            ref = TNodeRef {
                nodeId,
                name,
                it.GetValue<TTable::ChildId>(),
                it.GetValue<TTable::ShardId>(),
                it.GetValue<TTable::ShardNodeName>(),
                minCommitId,
                maxCommitId
            };
        }
    }

    return true;
}

bool TIndexTabletDatabase::ReadNodeRefs(
    ui64 nodeId,
    ui64 commitId,
    const TString& cookie,
    TVector<TNodeRef>& refs,
    ui32 maxBytes,
    TString* next,
    ui32* skippedRefs)
{
    using TTable = TIndexTabletSchema::NodeRefs;

    auto it = Table<TTable>()
        .GreaterOrEqual(nodeId, cookie)
        .LessOrEqual(nodeId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    ui32 bytes = 0;
    ui32 skipped = 0;
    while (it.IsValid()) {
        ui64 minCommitId = it.GetValue<TTable::CommitId>();
        ui64 maxCommitId = InvalidCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            refs.emplace_back(TNodeRef {
                nodeId,
                it.GetValue<TTable::Name>(),
                it.GetValue<TTable::ChildId>(),
                it.GetValue<TTable::ShardId>(),
                it.GetValue<TTable::ShardNodeName>(),
                minCommitId,
                maxCommitId
            });

            // FIXME
            bytes += refs.back().Name.size();
        } else {
            ++skipped;
        }

        if (!it.Next()) {
            return false;   // not ready
        }

        if (maxBytes && bytes >= maxBytes) {
            break;
        }
    }

    if (next && it.IsValid()) {
        *next = it.GetValue<TTable::Name>();
    }

    if (skippedRefs) {
        *skippedRefs = skipped;
    }

    return true;
}

bool TIndexTabletDatabase::ReadNodeRefs(
    ui64 startNodeId,
    const TString& startCookie,
    ui64 maxCount,
    TVector<TNodeRef>& refs,
    ui64& nextNodeId,
    TString& nextCookie)
{
    using TTable = TIndexTabletSchema::NodeRefs;

    if (!startNodeId && startCookie.empty()) {
        Table<TTable>().Precharge();
    }

    auto it = Table<TTable>().GreaterOrEqual(startNodeId, startCookie).Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid() && maxCount > 0) {
        refs.emplace_back(TNodeRef{
            it.GetValue<TTable::NodeId>(),
            it.GetValue<TTable::Name>(),
            it.GetValue<TTable::ChildId>(),
            it.GetValue<TTable::ShardId>(),
            it.GetValue<TTable::ShardNodeName>(),
            it.GetValue<TTable::CommitId>(),
            InvalidCommitId});
        --maxCount;

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    if (it.IsValid()) {
        nextNodeId = it.GetValue<TTable::NodeId>();
        nextCookie = it.GetValue<TTable::Name>();
    }

    return true;
}

bool TIndexTabletDatabase::PrechargeNodeRefs(
    ui64 nodeId,
    const TString& cookie,
    ui32 bytesToPrecharge)
{
    using TTable = TIndexTabletSchema::NodeRefs;

    return Table<TTable>()
        .GreaterOrEqual(nodeId, cookie)
        .LessOrEqual(nodeId)
         // NOTE: rows count, bytes count
        .Precharge(Max<ui64>(), bytesToPrecharge);
}

////////////////////////////////////////////////////////////////////////////////
// NodeRefs_Ver

void TIndexTabletDatabase::WriteNodeRefVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TString& name,
    ui64 childNodeId,
    const TString& shardId,
    const TString& shardNodeName)
{
    using TTable = TIndexTabletSchema::NodeRefs_Ver;

    Table<TTable>()
        .Key(nodeId, name, ReverseCommitId(minCommitId))
        .Update(
            NIceDb::TUpdate<TTable::MaxCommitId>(maxCommitId),
            NIceDb::TUpdate<TTable::ChildId>(childNodeId),
            NIceDb::TUpdate<TTable::ShardId>(shardId),
            NIceDb::TUpdate<TTable::ShardNodeName>(shardNodeName)
        );
}

void TIndexTabletDatabase::DeleteNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name)
{
    using TTable = TIndexTabletSchema::NodeRefs_Ver;

    Table<TTable>()
        .Key(nodeId, name, ReverseCommitId(commitId))
        .Delete();
}

bool TIndexTabletDatabase::ReadNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    using TTable = TIndexTabletSchema::NodeRefs_Ver;

    auto it = Table<TTable>()
        .GreaterOrEqual(nodeId, name, ReverseCommitId(commitId))
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        if (nodeId != it.GetValue<TTable::NodeId>() ||
            name != it.GetValue<TTable::Name>())
        {
            // no more entries
            break;
        }

        ui64 minCommitId = ReverseCommitId(it.GetValue<TTable::MinCommitId>());
        ui64 maxCommitId = it.GetValueOrDefault<TTable::MaxCommitId>(InvalidCommitId);

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            ref = TNodeRef {
                nodeId,
                name,
                it.GetValue<TTable::ChildId>(),
                it.GetValue<TTable::ShardId>(),
                it.GetValue<TTable::ShardNodeName>(),
                minCommitId,
                maxCommitId
            };
            break;
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TIndexTabletDatabase::ReadNodeRefVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeRef>& refs)
{
    using TTable = TIndexTabletSchema::NodeRefs_Ver;

    auto it = Table<TTable>()
        .Prefix(nodeId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        ui64 minCommitId = ReverseCommitId(it.GetValue<TTable::MinCommitId>());
        ui64 maxCommitId = it.GetValueOrDefault<TTable::MaxCommitId>(InvalidCommitId);

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            refs.emplace_back(TNodeRef {
                nodeId,
                it.GetValue<TTable::Name>(),
                it.GetValue<TTable::ChildId>(),
                it.GetValue<TTable::ShardId>(),
                it.GetValue<TTable::ShardNodeName>(),
                minCommitId,
                maxCommitId
            });
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TIndexTabletDatabase::WriteTruncateQueueEntry(ui64 nodeId, TByteRange range)
{
    using TTable = TIndexTabletSchema::TruncateQueue;

    NProto::TTruncateEntry entry;
    entry.SetNodeId(nodeId);
    entry.SetOffset(range.Offset);
    entry.SetLength(range.Length);

    Table<TTable>()
        .Key(nodeId)
        .Update(NIceDb::TUpdate<TTable::Proto>(entry));
}

void TIndexTabletDatabase::DeleteTruncateQueueEntry(ui64 nodeId)
{
    using TTable = TIndexTabletSchema::TruncateQueue;

    Table<TTable>()
        .Key(nodeId)
        .Delete();
}

bool TIndexTabletDatabase::ReadTruncateQueue(TVector<NProto::TTruncateEntry>& entries)
{
    using TTable = TIndexTabletSchema::TruncateQueue;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        entries.emplace_back(it.GetValue<TTable::Proto>());

        if (!it.Next()) {
            return false; // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// Sessions

void TIndexTabletDatabase::WriteSession(const NProto::TSession& session)
{
    using TTable = TIndexTabletSchema::Sessions;

    Table<TTable>()
        .Key(session.GetSessionId())
        .Update(NIceDb::TUpdate<TTable::Proto>(session));
}

void TIndexTabletDatabase::DeleteSession(const TString& sessionId)
{
    using TTable = TIndexTabletSchema::Sessions;

    Table<TTable>()
        .Key(sessionId)
        .Delete();
}

bool TIndexTabletDatabase::ReadSessions(TVector<NProto::TSession>& sessions)
{
    using TTable = TIndexTabletSchema::Sessions;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        sessions.emplace_back(it.GetValue<TTable::Proto>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// SessionHandles

void TIndexTabletDatabase::WriteSessionHandle(
    const NProto::TSessionHandle& handle)
{
    using TTable = TIndexTabletSchema::SessionHandles;

    Table<TTable>()
        .Key(handle.GetSessionId(), handle.GetHandle())
        .Update(NIceDb::TUpdate<TTable::Proto>(handle));
}

void TIndexTabletDatabase::DeleteSessionHandle(
    const TString& sessionId,
    ui64 handle)
{
    using TTable = TIndexTabletSchema::SessionHandles;

    Table<TTable>()
        .Key(sessionId, handle)
        .Delete();
}

template <typename T>
static bool DoReadSessionHandles(T& it, TVector<NProto::TSessionHandle>& handles)
{
    using TTable = TIndexTabletSchema::SessionHandles;

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        handles.emplace_back(it.template GetValue<TTable::Proto>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TIndexTabletDatabase::ReadSessionHandles(
    TVector<NProto::TSessionHandle>& handles)
{
    using TTable = TIndexTabletSchema::SessionHandles;

    auto it = Table<TTable>()
        .Select();

    return DoReadSessionHandles(it, handles);
}

bool TIndexTabletDatabase::ReadSessionHandles(
    const TString& sessionId,
    TVector<NProto::TSessionHandle>& handles)
{
    using TTable = TIndexTabletSchema::SessionHandles;

    auto it = Table<TTable>()
        .Prefix(sessionId)
        .Select();

    return DoReadSessionHandles(it, handles);
}

////////////////////////////////////////////////////////////////////////////////
// SessionLocks

void TIndexTabletDatabase::WriteSessionLock(const NProto::TSessionLock& lock)
{
    using TTable = TIndexTabletSchema::SessionLocks;

    Table<TTable>()
        .Key(lock.GetSessionId(), lock.GetLockId())
        .Update(NIceDb::TUpdate<TTable::Proto>(lock));
}

void TIndexTabletDatabase::DeleteSessionLock(
    const TString& sessionId,
    ui64 lockId)
{
    using TTable = TIndexTabletSchema::SessionLocks;

    Table<TTable>()
        .Key(sessionId, lockId)
        .Delete();
}

template <typename T>
static bool DoReadSessionLocks(T& it, TVector<NProto::TSessionLock>& locks)
{
    using TTable = TIndexTabletSchema::SessionLocks;

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        locks.emplace_back(it.template GetValue<TTable::Proto>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TIndexTabletDatabase::ReadSessionLocks(
    TVector<NProto::TSessionLock>& locks)
{
    using TTable = TIndexTabletSchema::SessionLocks;

    auto it = Table<TTable>()
        .Select();

    return DoReadSessionLocks(it, locks);
}

bool TIndexTabletDatabase::ReadSessionLocks(
    const TString& sessionId,
    TVector<NProto::TSessionLock>& locks)
{
    using TTable = TIndexTabletSchema::SessionLocks;

    auto it = Table<TTable>()
        .Prefix(sessionId)
        .Select();

    return DoReadSessionLocks(it, locks);
}

////////////////////////////////////////////////////////////////////////////////
// SessionDuplicateCache

void TIndexTabletDatabase::WriteSessionDupCacheEntry(
    const NProto::TDupCacheEntry& entry)
{
    using TTable = TIndexTabletSchema::SessionDupCache;

    Table<TTable>()
        .Key(entry.GetSessionId(), entry.GetEntryId())
        .Update(NIceDb::TUpdate<TTable::Proto>(entry));
}

void TIndexTabletDatabase::DeleteSessionDupCacheEntry(
    const TString& sessionId, ui64 entryId)
{
    using TTable = TIndexTabletSchema::SessionDupCache;

    Table<TTable>()
        .Key(sessionId, entryId)
        .Delete();
}

bool TIndexTabletDatabase::ReadSessionDupCacheEntries(
    TVector<NProto::TDupCacheEntry>& entries)
{
    using TTable = TIndexTabletSchema::SessionDupCache;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        entries.emplace_back(it.template GetValue<TTable::Proto>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// FreshBytes

void TIndexTabletDatabase::WriteFreshBytes(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    TStringBuf data)
{
    using TTable = TIndexTabletSchema::FreshBytes;

    Table<TTable>()
        .Key(commitId, nodeId, offset)
        .Update(
            NIceDb::TUpdate<TTable::Data>(TString(data)),
            NIceDb::TUpdate<TTable::Len>(data.size()));
}

void TIndexTabletDatabase::WriteFreshBytesDeletionMarker(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    ui64 len)
{
    using TTable = TIndexTabletSchema::FreshBytes;

    Table<TTable>()
        .Key(commitId, nodeId, offset)
        .Update(
            NIceDb::TUpdate<TTable::Data>(TString()),
            NIceDb::TUpdate<TTable::Len>(len));
}

void TIndexTabletDatabase::DeleteFreshBytes(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset)
{
    using TTable = TIndexTabletSchema::FreshBytes;

    Table<TTable>()
        .Key(commitId, nodeId, offset)
        .Delete();
}

bool TIndexTabletDatabase::ReadFreshBytes(TVector<TFreshBytesEntry>& bytes)
{
    using TTable = TIndexTabletSchema::FreshBytes;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        ui64 minCommitId = it.GetValue<TTable::MinCommitId>();
        ui64 nodeId = it.GetValue<TTable::NodeId>();
        ui64 offset = it.GetValue<TTable::Offset>();
        ui64 len = it.GetValue<TTable::Len>();
        TString data = it.GetValue<TTable::Data>();
        if (data && !len) {
            // backwards-compat
            len = data.size();
        }

        bytes.emplace_back(TFreshBytesEntry {
            nodeId,
            minCommitId,
            offset,
            std::move(data),
            len
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// FreshBlocks

void TIndexTabletDatabase::WriteFreshBlock(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    TStringBuf blockData)
{
    using TTable = TIndexTabletSchema::FreshBlocks;

    Table<TTable>()
        .Key(nodeId, blockIndex, ReverseCommitId(commitId))
        .Update(NIceDb::TUpdate<TTable::BlockData>(TString(blockData)));
}

void TIndexTabletDatabase::MarkFreshBlockDeleted(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    ui32 blockIndex)
{
    using TTable = TIndexTabletSchema::FreshBlocks;

    Table<TTable>()
        .Key(nodeId, blockIndex, ReverseCommitId(minCommitId))
        .Update(NIceDb::TUpdate<TTable::MaxCommitId>(maxCommitId));
}

void TIndexTabletDatabase::DeleteFreshBlock(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    using TTable = TIndexTabletSchema::FreshBlocks;

    Table<TTable>()
        .Key(nodeId, blockIndex, ReverseCommitId(commitId))
        .Delete();
}

bool TIndexTabletDatabase::ReadFreshBlocks(TVector<TFreshBlock>& blocks)
{
    using TTable = TIndexTabletSchema::FreshBlocks;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        ui64 nodeId = it.GetValue<TTable::NodeId>();
        ui32 blockIndex = it.GetValue<TTable::BlockIndex>();

        ui64 minCommitId = ReverseCommitId(it.GetValue<TTable::MinCommitId>());
        ui64 maxCommitId = it.GetValueOrDefault<TTable::MaxCommitId>(InvalidCommitId);

        blocks.emplace_back(TFreshBlock {
            nodeId,
            blockIndex,
            minCommitId,
            maxCommitId,
            it.GetValue<TTable::BlockData>()
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// MixedBlocks

void TIndexTabletDatabase::WriteMixedBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId,
    const TBlockList& blockList,
    ui32 garbageBlocks,
    ui32 checkpointBlocks)
{
    using TTable = TIndexTabletSchema::MixedBlocks;

    TStringBuf encodedBlocks{
        blockList.GetEncodedBlocks().begin(),
        blockList.GetEncodedBlocks().end()};

    TStringBuf encodedDeletionMarkers{
        blockList.GetEncodedDeletionMarkers().begin(),
        blockList.GetEncodedDeletionMarkers().end()};

    Table<TTable>()
        .Key(rangeId, blobId.CommitId(), blobId.UniqueId())
        .Update(
            NIceDb::TUpdate<TTable::Blocks>(encodedBlocks),
            NIceDb::TUpdate<TTable::DeletionMarkers>(encodedDeletionMarkers),
            NIceDb::TUpdate<TTable::GarbageBlocksCount>(garbageBlocks),
            NIceDb::TUpdate<TTable::CheckpointBlocksCount>(checkpointBlocks)
        );
}

void TIndexTabletDatabase::DeleteMixedBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    using TTable = TIndexTabletSchema::MixedBlocks;

    Table<TTable>()
        .Key(rangeId, blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TIndexTabletDatabase::ReadMixedBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId,
    TMaybe<TMixedBlob>& blob,
    IAllocator* alloc)
{
    using TTable = TIndexTabletSchema::MixedBlocks;

    auto it = Table<TTable>()
        .Key(rangeId, blobId.CommitId(), blobId.UniqueId())
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        TPartialBlobId blobId {
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobUniqueId>()
        };

        TByteVector blocks = FromStringBuf(
            it.GetValue<TTable::Blocks>(),
            alloc);
        TByteVector deletionMarkers = FromStringBuf(
            it.GetValue<TTable::DeletionMarkers>(),
            alloc);

        TBlockList blockList { std::move(blocks), std::move(deletionMarkers) };

        blob = TMixedBlob{
            blobId,
            std::move(blockList),
            it.GetValue<TTable::GarbageBlocksCount>(),
            it.GetValue<TTable::CheckpointBlocksCount>()};
    }

    return true;
}

bool TIndexTabletDatabase::ReadMixedBlocks(
    ui32 rangeId,
    TVector<TMixedBlob>& blobs,
    IAllocator* alloc)
{
    using TTable = TIndexTabletSchema::MixedBlocks;

    auto it = Table<TTable>()
        .Prefix(rangeId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        TPartialBlobId blobId {
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobUniqueId>()
        };

        TByteVector blocks = FromStringBuf(
            it.GetValue<TTable::Blocks>(),
            alloc);
        TByteVector deletionMarkers = FromStringBuf(
            it.GetValue<TTable::DeletionMarkers>(),
            alloc);

        TBlockList blockList { std::move(blocks), std::move(deletionMarkers) };

        blobs.emplace_back(
            TMixedBlob{
                blobId,
                std::move(blockList),
                it.GetValue<TTable::GarbageBlocksCount>(),
                it.GetValue<TTable::CheckpointBlocksCount>()});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// DeletionMarkers

void TIndexTabletDatabase::WriteDeletionMarkers(
    ui32 rangeId,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount)
{
    using TTable = TIndexTabletSchema::DeletionMarkers;

    Table<TTable>()
        .Key(rangeId, nodeId, commitId, blockIndex)
        .Update(NIceDb::TUpdate<TTable::BlocksCount>(blocksCount));
}

void TIndexTabletDatabase::DeleteDeletionMarker(
    ui32 rangeId,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    using TTable = TIndexTabletSchema::DeletionMarkers;

    Table<TTable>()
        .Key(rangeId, nodeId, commitId, blockIndex)
        .Delete();
}

bool TIndexTabletDatabase::ReadDeletionMarkers(
    ui32 rangeId,
    TVector<TDeletionMarker>& deletionMarkers)
{
    using TTable = TIndexTabletSchema::DeletionMarkers;

    auto it = Table<TTable>()
        .Prefix(rangeId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        deletionMarkers.emplace_back(
            it.GetValue<TTable::NodeId>(),
            it.GetValue<TTable::CommitId>(),
            it.GetValue<TTable::BlockIndex>(),
            it.GetValue<TTable::BlocksCount>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// LargeDeletionMarkers

void TIndexTabletDatabase::WriteLargeDeletionMarkers(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount)
{
    using TTable = TIndexTabletSchema::LargeDeletionMarkers;

    Table<TTable>()
        .Key(nodeId, commitId, blockIndex)
        .Update(NIceDb::TUpdate<TTable::BlocksCount>(blocksCount));
}

void TIndexTabletDatabase::DeleteLargeDeletionMarker(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    using TTable = TIndexTabletSchema::LargeDeletionMarkers;

    Table<TTable>()
        .Key(nodeId, commitId, blockIndex)
        .Delete();
}

bool TIndexTabletDatabase::ReadLargeDeletionMarkers(
    TVector<TDeletionMarker>& deletionMarkers)
{
    using TTable = TIndexTabletSchema::LargeDeletionMarkers;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        deletionMarkers.emplace_back(
            it.GetValue<TTable::NodeId>(),
            it.GetValue<TTable::CommitId>(),
            it.GetValue<TTable::BlockIndex>(),
            it.GetValue<TTable::BlocksCount>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// OrphanNodes

void TIndexTabletDatabase::WriteOrphanNode(ui64 nodeId)
{
    using TTable = TIndexTabletSchema::OrphanNodes;

    Table<TTable>()
        .Key(nodeId)
        .Update();
}

void TIndexTabletDatabase::DeleteOrphanNode(ui64 nodeId)
{
    using TTable = TIndexTabletSchema::OrphanNodes;

    Table<TTable>()
        .Key(nodeId)
        .Delete();
}

bool TIndexTabletDatabase::ReadOrphanNodes(TVector<ui64>& nodeIds)
{
    using TTable = TIndexTabletSchema::OrphanNodes;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        nodeIds.emplace_back(it.GetValue<TTable::NodeId>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// NewBlobs

void TIndexTabletDatabase::WriteNewBlob(const TPartialBlobId& blobId)
{
    using TTable = TIndexTabletSchema::NewBlobs;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Update();
}

void TIndexTabletDatabase::DeleteNewBlob(const TPartialBlobId& blobId)
{
    using TTable = TIndexTabletSchema::NewBlobs;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TIndexTabletDatabase::ReadNewBlobs(TVector<TPartialBlobId>& blobIds)
{
    using TTable = TIndexTabletSchema::NewBlobs;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        blobIds.emplace_back(
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobUniqueId>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// GarbageBlobs

void TIndexTabletDatabase::WriteGarbageBlob(const TPartialBlobId& blobId)
{
    using TTable = TIndexTabletSchema::GarbageBlobs;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Update();
}

void TIndexTabletDatabase::DeleteGarbageBlob(const TPartialBlobId& blobId)
{
    using TTable = TIndexTabletSchema::GarbageBlobs;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TIndexTabletDatabase::ReadGarbageBlobs(TVector<TPartialBlobId>& blobIds)
{
    using TTable = TIndexTabletSchema::GarbageBlobs;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        blobIds.emplace_back(
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobUniqueId>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// Checkpoints

void TIndexTabletDatabase::WriteCheckpoint(
    const NProto::TCheckpoint& checkpoint)
{
    using TTable = TIndexTabletSchema::Checkpoints;

    Table<TTable>()
        .Key(checkpoint.GetCheckpointId())
        .Update(NIceDb::TUpdate<TTable::Proto>(checkpoint));
}

void TIndexTabletDatabase::DeleteCheckpoint(const TString& checkpointId)
{
    using TTable = TIndexTabletSchema::Checkpoints;

    Table<TTable>()
        .Key(checkpointId)
        .Delete();
}

bool TIndexTabletDatabase::ReadCheckpoints(
    TVector<NProto::TCheckpoint>& checkpoints)
{
    using TTable = TIndexTabletSchema::Checkpoints;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        checkpoints.emplace_back(it.GetValue<TTable::Proto>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// CheckpointNodes

void TIndexTabletDatabase::WriteCheckpointNode(ui64 checkpointId, ui64 nodeId)
{
    using TTable = TIndexTabletSchema::CheckpointNodes;

    Table<TTable>()
        .Key(checkpointId, nodeId)
        .Update();
}

void TIndexTabletDatabase::DeleteCheckpointNode(ui64 checkpointId, ui64 nodeId)
{
    using TTable = TIndexTabletSchema::CheckpointNodes;

    Table<TTable>()
        .Key(checkpointId, nodeId)
        .Delete();
}

bool TIndexTabletDatabase::ReadCheckpointNodes(
    ui64 checkpointId,
    TVector<ui64>& nodes,
    size_t maxCount)
{
    using TTable = TIndexTabletSchema::CheckpointNodes;

    auto it = Table<TTable>()
        .Prefix(checkpointId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid() && maxCount) {
        nodes.emplace_back(it.GetValue<TTable::NodeId>());
        --maxCount;

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// CheckpointBlobs

void TIndexTabletDatabase::WriteCheckpointBlob(
    ui64 checkpointId,
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    using TTable = TIndexTabletSchema::CheckpointBlobs;

    Table<TTable>()
        .Key(checkpointId, rangeId, blobId.CommitId(), blobId.UniqueId())
        .Update();
}

void TIndexTabletDatabase::DeleteCheckpointBlob(
    ui64 checkpointId,
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    using TTable = TIndexTabletSchema::CheckpointBlobs;

    Table<TTable>()
        .Key(checkpointId, rangeId, blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TIndexTabletDatabase::ReadCheckpointBlobs(
    ui64 checkpointId,
    TVector<TCheckpointBlob>& blobs,
    size_t maxCount)
{
    using TTable = TIndexTabletSchema::CheckpointBlobs;

    auto it = Table<TTable>()
        .Prefix(checkpointId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid() && maxCount) {
        TPartialBlobId blobId {
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobUniqueId>()
        };

        blobs.emplace_back(TCheckpointBlob {
            it.GetValue<TTable::RangeId>(),
            blobId
        });

        --maxCount;

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// CompactionMap

void TIndexTabletDatabase::ForceWriteCompactionMap(
    ui32 rangeId,
    ui32 blobsCount,
    ui32 deletionsCount,
    ui32 garbageBlocksCount)
{
    using TTable = TIndexTabletSchema::CompactionMap;

    Table<TTable>()
        .Key(rangeId)
        .Update(NIceDb::TUpdate<TTable::BlobsCount>(blobsCount))
        .Update(NIceDb::TUpdate<TTable::DeletionsCount>(deletionsCount))
        .Update(NIceDb::TUpdate<TTable::GarbageBlocksCount>(garbageBlocksCount));
}

void TIndexTabletDatabase::WriteCompactionMap(
    ui32 rangeId,
    ui32 blobsCount,
    ui32 deletionsCount,
    ui32 garbageBlocksCount)
{
    using TTable = TIndexTabletSchema::CompactionMap;

    if (blobsCount || deletionsCount || garbageBlocksCount) {
        ForceWriteCompactionMap(
            rangeId,
            blobsCount,
            deletionsCount,
            garbageBlocksCount);
    } else {
        Table<TTable>().Key(rangeId).Delete();
    }
}

bool TIndexTabletDatabase::ReadCompactionMap(
    TVector<TCompactionRangeInfo>& compactionMap)
{
    return ReadCompactionMap(compactionMap, 0, Max<ui32>(), true);
}

bool TIndexTabletDatabase::ReadCompactionMap(
    TVector<TCompactionRangeInfo>& compactionMap,
    ui32 firstRangeId,
    ui32 rangeCount,
    bool prechargeAll)
{
    using TTable = TIndexTabletSchema::CompactionMap;

    if (!firstRangeId && prechargeAll) {
        Table<TTable>().Precharge();
    }

    auto it = Table<TTable>()
        .GreaterOrEqual(firstRangeId)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        compactionMap.push_back({
            it.GetValue<TTable::RangeId>(),
            {
                it.GetValue<TTable::BlobsCount>(),
                it.GetValue<TTable::DeletionsCount>(),
                it.GetValue<TTable::GarbageBlocksCount>(),
            }
        });

        if (compactionMap.size() == rangeCount) {
            break;
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// TabletStorageInfo

bool TIndexTabletDatabase::ReadTabletStorageInfo(
    NCloud::NProto::TTabletStorageInfo& tabletStorageInfo)
{
    using TTable = TIndexTabletSchema::TabletStorageInfo;

    auto it = Table<TTable>()
        .Key(FileSystemMetaId)
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid()) {
        tabletStorageInfo = it.GetValue<TTable::Proto>();
    }

    return true;
}

void TIndexTabletDatabase::WriteTabletStorageInfo(
    const NCloud::NProto::TTabletStorageInfo& tabletStorageInfo)
{
    using TTable = TIndexTabletSchema::TabletStorageInfo;

    Table<TTable>()
        .Key(FileSystemMetaId)
        .Update(NIceDb::TUpdate<TTable::Proto>(tabletStorageInfo));
}

////////////////////////////////////////////////////////////////////////////////
// SessionHistory

void TIndexTabletDatabase::WriteSessionHistoryEntry(
    const NProto::TSessionHistoryEntry& entry)
{
    using TTable = TIndexTabletSchema::SessionHistory;

    Table<TTable>()
        .Key(entry.GetEntryId())
        .Update(NIceDb::TUpdate<TTable::Proto>(entry));
}

void TIndexTabletDatabase::DeleteSessionHistoryEntry(ui64 entryId) {
    using TTable = TIndexTabletSchema::SessionHistory;

    Table<TTable>()
        .Key(entryId)
        .Delete();
}


bool TIndexTabletDatabase::ReadSessionHistoryEntries(
    TVector<NProto::TSessionHistoryEntry>& entries)
{
    using TTable = TIndexTabletSchema::SessionHistory;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        entries.emplace_back(it.template GetValue<TTable::Proto>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// OpLog

void TIndexTabletDatabase::WriteOpLogEntry(const NProto::TOpLogEntry& entry)
{
    using TTable = TIndexTabletSchema::OpLog;

    Table<TTable>()
        .Key(entry.GetEntryId())
        .Update(NIceDb::TUpdate<TTable::Proto>(entry));
}

void TIndexTabletDatabase::DeleteOpLogEntry(ui64 entryId) {
    using TTable = TIndexTabletSchema::OpLog;

    Table<TTable>()
        .Key(entryId)
        .Delete();
}


bool TIndexTabletDatabase::ReadOpLog(TVector<NProto::TOpLogEntry>& opLog)
{
    using TTable = TIndexTabletSchema::OpLog;

    auto it = Table<TTable>()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        opLog.emplace_back(it.template GetValue<TTable::Proto>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TIndexTabletDatabaseProxy::TIndexTabletDatabaseProxy(
        NKikimr::NTable::TDatabase& database,
        TVector<TInMemoryIndexState::TIndexStateRequest>& nodeUpdates)
    : TIndexTabletDatabase(database)
    , NodeUpdates(nodeUpdates)
{}

bool TIndexTabletDatabaseProxy::ReadNode(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    auto result = TIndexTabletDatabase::ReadNode(nodeId, commitId, node);
    if (result && node) {
        // If ReadNode was successful, it is reasonable to update the cache with
        // the value that has just been read.
        NodeUpdates.emplace_back(TInMemoryIndexState::TWriteNodeRequest{
            .NodeId = nodeId,
            .Row = {.CommitId = node->MinCommitId, .Node = node->Attrs}});
    }
    return result;
}

bool TIndexTabletDatabaseProxy::ReadNodes(
    ui64 startNodeId,
    ui64 maxNodes,
    ui64& nextNodeId,
    TVector<TNode>& nodes)
{
    auto result = TIndexTabletDatabase::ReadNodes(
        startNodeId,
        maxNodes,
        nextNodeId,
        nodes);
    if (result) {
        // If ReadNodes was successful, it is reasonable to update the cache
        // with the values that have just been read.
        for (const auto& node: nodes) {
            NodeUpdates.emplace_back(TInMemoryIndexState::TWriteNodeRequest{
                .NodeId = node.NodeId,
                .Row = {.CommitId = node.MinCommitId, .Node = node.Attrs}});
        }
    }
    return result;
}

void TIndexTabletDatabaseProxy::WriteNode(
    ui64 nodeId,
    ui64 commitId,
    const NProto::TNode& attrs)
{
    TIndexTabletDatabase::WriteNode(nodeId, commitId, attrs);
    NodeUpdates.emplace_back(TInMemoryIndexState::TWriteNodeRequest{
        .NodeId = nodeId,
        .Row = {.CommitId = commitId, .Node = attrs}});
}

void TIndexTabletDatabaseProxy::DeleteNode(ui64 nodeId)
{
    TIndexTabletDatabase::DeleteNode(nodeId);
    NodeUpdates.emplace_back(
        TInMemoryIndexState::TDeleteNodeRequest{.NodeId = nodeId});
}

void TIndexTabletDatabaseProxy::WriteNodeVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const NProto::TNode& attrs)
{
    TIndexTabletDatabase::WriteNodeVer(nodeId, minCommitId, maxCommitId, attrs);
    // TODO(#1146): _Ver tables not yet supported
}

void TIndexTabletDatabaseProxy::DeleteNodeVer(ui64 nodeId, ui64 commitId)
{
    TIndexTabletDatabase::DeleteNodeVer(nodeId, commitId);
    // TODO(#1146): _Ver tables not yet supported
}

bool TIndexTabletDatabaseProxy::ReadNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    auto result =
        TIndexTabletDatabase::ReadNodeAttr(nodeId, commitId, name, attr);
    if (result && attr) {
        // If ReadNodeAttr  was successful, it is reasonable to update the cache
        // with the value that has just been read.
        NodeUpdates.emplace_back(TInMemoryIndexState::TWriteNodeAttrsRequest{
            .NodeAttrsKey = {nodeId, name},
            .NodeAttrsRow = {
                .CommitId = attr->MinCommitId,
                .Value = attr->Value,
                .Version = attr->Version}});
    }
    return result;
}

void TIndexTabletDatabaseProxy::WriteNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    TIndexTabletDatabase::WriteNodeAttr(nodeId, commitId, name, value, version);
    NodeUpdates.emplace_back(TInMemoryIndexState::TWriteNodeAttrsRequest{
        .NodeAttrsKey = {nodeId, name},
        .NodeAttrsRow =
            {.CommitId = commitId, .Value = value, .Version = version}});
}

void TIndexTabletDatabaseProxy::DeleteNodeAttr(ui64 nodeId, const TString& name)
{
    TIndexTabletDatabase::DeleteNodeAttr(nodeId, name);
    NodeUpdates.emplace_back(
        TInMemoryIndexState::TDeleteNodeAttrsRequest{nodeId, name});
}

void TIndexTabletDatabaseProxy::WriteNodeAttrVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    TIndexTabletDatabase::WriteNodeAttrVer(
        nodeId,
        minCommitId,
        maxCommitId,
        name,
        value,
        version);
    // TODO(#1146): _Ver tables not yet supported
}

void TIndexTabletDatabaseProxy::DeleteNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name)
{
    TIndexTabletDatabase::DeleteNodeAttrVer(nodeId, commitId, name);
    // TODO(#1146): _Ver tables not yet supported
}

bool TIndexTabletDatabaseProxy::ReadNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    auto result =
        TIndexTabletDatabase::ReadNodeRef(nodeId, commitId, name, ref);
    if (result && ref) {
        // If ReadNodeRef was successful, it is reasonable to update the cache
        // with the value that has just been read.
        NodeUpdates.emplace_back(ExtractWriteNodeRefsFromNodeRef(*ref));
    }
    return result;
}

bool TIndexTabletDatabaseProxy::ReadNodeRefs(
    ui64 nodeId,
    ui64 commitId,
    const TString& cookie,
    TVector<TNodeRef>& refs,
    ui32 maxBytes,
    TString* next,
    ui32* skippedRefs)
{
    ui32 skipped = 0;
    if (!skippedRefs) {
        skippedRefs = &skipped;
    }
    auto result = TIndexTabletDatabase::ReadNodeRefs(
        nodeId,
        commitId,
        cookie,
        refs,
        maxBytes,
        next,
        skippedRefs);
    if (result) {
        // If ReadNodeRefs was successful, it is reasonable to update the cache
        // with the values that have just been read.
        for (const auto& ref: refs) {
            NodeUpdates.emplace_back(ExtractWriteNodeRefsFromNodeRef(ref));
        }

        // If the next cookie is empty and cookie is also empty, that means that
        // we have read all the nodes associated with the given nodeId. This
        // means that we can mark this nodeId's refs as fully cached after the
        // WriteNodeRef operations are applied and if and only if following two
        // conditions are met:
        //
        // 1. No nodeRefs were skipped due to commitId filtering
        // 2. All refs.size() fit into the underlying cache
        if (next && next->empty() && cookie.empty() && *skippedRefs == 0) {
            NodeUpdates.emplace_back(
                TInMemoryIndexState::TMarkNodeRefsAsCachedRequest{
                    .NodeId = nodeId,
                    .RefsSize = refs.size()});
        }
    }
    return result;
}

bool TIndexTabletDatabaseProxy::ReadNodeRefs(
    ui64 startNodeId,
    const TString& startCookie,
    ui64 maxCount,
    TVector<TNodeRef>& refs,
    ui64& nextNodeId,
    TString& nextCookie)
{
    auto result = TIndexTabletDatabase::ReadNodeRefs(
        startNodeId,
        startCookie,
        maxCount,
        refs,
        nextNodeId,
        nextCookie);
    if (result) {
        // If ReadNodeRefs was successful, it is reasonable to update the cache
        // with the values that have just been read.
        for (const auto& ref: refs) {
            NodeUpdates.emplace_back(ExtractWriteNodeRefsFromNodeRef(ref));
        }
    }
    return result;
}

void TIndexTabletDatabaseProxy::WriteNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    ui64 childNode,
    const TString& shardId,
    const TString& shardNodeName)
{
    TIndexTabletDatabase::WriteNodeRef(
        nodeId,
        commitId,
        name,
        childNode,
        shardId,
        shardNodeName);
    NodeUpdates.emplace_back(TInMemoryIndexState::TWriteNodeRefsRequest{
        .NodeRefsKey = {nodeId, name},
        .NodeRefsRow = {
            .CommitId = commitId,
            .ChildId = childNode,
            .ShardId = shardId,
            .ShardNodeName = shardNodeName}});
}

void TIndexTabletDatabaseProxy::DeleteNodeRef(ui64 nodeId, const TString& name)
{
    TIndexTabletDatabase::DeleteNodeRef(nodeId, name);
    NodeUpdates.emplace_back(
        TInMemoryIndexState::TDeleteNodeRefsRequest{nodeId, name});
}

void TIndexTabletDatabaseProxy::WriteNodeRefVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TString& name,
    ui64 childNode,
    const TString& shardId,
    const TString& shardNodeName)
{
    TIndexTabletDatabase::WriteNodeRefVer(
        nodeId,
        minCommitId,
        maxCommitId,
        name,
        childNode,
        shardId,
        shardNodeName);
    // TODO(#1146): _Ver tables not yet supported
}

void TIndexTabletDatabaseProxy::DeleteNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name)
{
    TIndexTabletDatabase::DeleteNodeRefVer(nodeId, commitId, name);
    // TODO(#1146): _Ver tables not yet supported
}

TInMemoryIndexState::TWriteNodeRefsRequest
TIndexTabletDatabaseProxy::ExtractWriteNodeRefsFromNodeRef(const TNodeRef& ref)
{
    return TInMemoryIndexState::TWriteNodeRefsRequest{
        .NodeRefsKey = {ref.NodeId, ref.Name},
        .NodeRefsRow = {
            .CommitId = ref.MinCommitId,
            .ChildId = ref.ChildNodeId,
            .ShardId = ref.ShardId,
            .ShardNodeName = ref.ShardNodeName}};
}

}   // namespace NCloud::NFileStore::NStorage
