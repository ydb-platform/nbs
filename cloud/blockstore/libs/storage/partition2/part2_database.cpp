#include "part2_database.h"

#include "part2_schema.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui16 ProcessCompactionCounter(ui32 value)
{
    return value > Max<ui16>() ? Max<ui16>() : value;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::InitSchema()
{
    Materialize<TPartitionSchema>();

    TSchemaInitializer<TPartitionSchema::TTables>::InitStorage(
        Database.Alter());
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteMeta(const NProto::TPartitionMeta& meta)
{
    using TTable = TPartitionSchema::Meta;

    Table<TTable>().Key(1).Update(NIceDb::TUpdate<TTable::PartitionMeta>(meta));
}

bool TPartitionDatabase::ReadMeta(TMaybe<NProto::TPartitionMeta>& meta)
{
    using TTable = TPartitionSchema::Meta;

    auto it = Table<TTable>().Key(1).Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        meta = it.GetValue<TTable::PartitionMeta>();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionDatabase::ReadFreshBlockUpdates(TFreshBlockUpdates& updates)
{
    using TTable = TPartitionSchema::FreshBlockUpdates;

    auto it = Table<TTable>().Range().Select();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        auto commitId = it.template GetValue<typename TTable::CommitId>();
        auto blockRange = TBlockRange32::MakeClosedInterval(
            it.template GetValue<typename TTable::StartIndex>(),
            it.template GetValue<typename TTable::EndIndex>());

        updates.emplace_back(commitId, blockRange);

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TPartitionDatabase::AddFreshBlockUpdate(TFreshBlockUpdate update)
{
    using TTable = TPartitionSchema::FreshBlockUpdates;

    Table<TTable>()
        .Key(update.CommitId, update.BlockRange.Start)
        .Update(NIceDb::TUpdate<TTable::EndIndex>(update.BlockRange.End));
}

void TPartitionDatabase::TrimFreshBlockUpdates(
    TFreshBlockUpdates::const_iterator first,
    TFreshBlockUpdates::const_iterator last)
{
    using TTable = TPartitionSchema::FreshBlockUpdates;

    for (; first != last; ++first) {
        Table<TTable>().Key(first->CommitId, first->BlockRange.Start).Delete();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteGlobalBlob(
    const TPartialBlobId& blobId,
    const NProto::TBlobMeta2& meta)
{
    using TTable = TPartitionSchema::GlobalBlobs;

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Update<TTable::BlobMeta>(meta);
}

void TPartitionDatabase::DeleteGlobalBlob(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::GlobalBlobs;

    Table<TTable>().Key(blobId.CommitId(), blobId.UniqueId()).Delete();
}

void TPartitionDatabase::WriteZoneBlob(
    ui32 zoneId,
    const TPartialBlobId& blobId,
    const NProto::TBlobMeta2& meta)
{
    using TTable = TPartitionSchema::ZoneBlobs;

    Table<TTable>()
        .Key(zoneId, blobId.CommitId(), blobId.UniqueId())
        .Update<TTable::BlobMeta>(meta);
}

void TPartitionDatabase::DeleteZoneBlob(
    ui32 zoneId,
    const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::ZoneBlobs;

    Table<TTable>().Key(zoneId, blobId.CommitId(), blobId.UniqueId()).Delete();
}

bool TPartitionDatabase::ReadGlobalBlobs(TVector<TBlobMeta>& blobs)
{
    using TTable = TPartitionSchema::GlobalBlobs;

    auto it = Table<TTable>().Range().Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobId>());

        blobs.push_back({blobId, it.GetValueOrDefault<TTable::BlobMeta>()});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::ReadZoneBlobs(ui32 zoneId, TVector<TBlobMeta>& blobs)
{
    using TTable = TPartitionSchema::ZoneBlobs;

    auto it = Table<TTable>().Prefix(zoneId).Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobId>());

        blobs.push_back({blobId, it.GetValueOrDefault<TTable::BlobMeta>()});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

template <class TSchema, class TTable>
bool ReadKnownBlobIdsImpl(
    TTable& table,
    TVector<TPartialBlobId>& blobIds,
    ui64 commitId)
{
    auto it = table.Range().Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.template GetValue<typename TSchema::BlobCommitId>(),
            it.template GetValue<typename TSchema::BlobId>());

        if (!IsDeletionMarker(blobId) && blobId.CommitId() >= commitId) {
            blobIds.push_back(blobId);
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::ReadKnownBlobIds(TVector<TPartialBlobId>& blobIds)
{
    auto global = Table<TPartitionSchema::GlobalBlobs>();
    auto ready =
        ReadKnownBlobIdsImpl<TPartitionSchema::GlobalBlobs>(global, blobIds, 0);
    auto local = Table<TPartitionSchema::ZoneBlobs>();
    ready &=
        ReadKnownBlobIdsImpl<TPartitionSchema::ZoneBlobs>(local, blobIds, 0);
    return ready;
}

bool TPartitionDatabase::ReadAllZoneBlobIds(
    TVector<TPartialBlobId>& blobIds,
    ui64 commitId)
{
    using TTable = TPartitionSchema::ZoneBlobs;
    auto local = Table<TTable>();
    return ReadKnownBlobIdsImpl<TTable>(local, blobIds, commitId);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteBlockList(
    const TPartialBlobId& blobId,
    const TBlockList& blockList)
{
    using TTable = TPartitionSchema::BlockLists;

    TStringBuf encodedBlocks{
        blockList.EncodedBlocks().begin(),
        blockList.EncodedBlocks().end()};

    TStringBuf encodedDeletedBlocks{
        blockList.EncodedDeletedBlocks().begin(),
        blockList.EncodedDeletedBlocks().end()};

    Table<TTable>()
        .Key(blobId.CommitId(), blobId.UniqueId())
        .Update(
            NIceDb::TUpdate<TTable::Blocks>(encodedBlocks),
            NIceDb::TUpdate<TTable::DeletedBlocks>(encodedDeletedBlocks));
}

void TPartitionDatabase::DeleteBlockList(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::BlockLists;

    Table<TTable>().Key(blobId.CommitId(), blobId.UniqueId()).Delete();
}

bool TPartitionDatabase::ReadBlockList(
    const TPartialBlobId& blobId,
    TMaybe<TBlockList>& blockList)
{
    using TTable = TPartitionSchema::BlockLists;

    auto it =
        Table<TTable>().Key(blobId.CommitId(), blobId.UniqueId()).Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        TByteVector blocks = FromStringBuf(
            it.GetValueOrDefault<TTable::Blocks>(),
            GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList));
        TByteVector deletedBlocks = FromStringBuf(
            it.GetValueOrDefault<TTable::DeletedBlocks>(),
            GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList));

        blockList = {std::move(blocks), std::move(deletedBlocks)};
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteGlobalBlobUpdate(
    ui64 deletionId,
    ui64 commitId,
    const TBlockRange32& blockRange)
{
    using TTable = TPartitionSchema::GlobalBlobUpdates;

    Table<TTable>()
        .Key(deletionId)
        .Update(
            NIceDb::TUpdate<TTable::CommitId>(commitId),
            NIceDb::TUpdate<TTable::StartIndex>(blockRange.Start),
            NIceDb::TUpdate<TTable::EndIndex>(blockRange.End));
}

void TPartitionDatabase::DeleteGlobalBlobUpdate(ui64 deletionId)
{
    using TTable = TPartitionSchema::GlobalBlobUpdates;

    Table<TTable>().Key(deletionId).Delete();
}

void TPartitionDatabase::WriteZoneBlobUpdate(
    ui32 zoneId,
    ui64 deletionId,
    ui64 commitId,
    const TBlockRange32& blockRange)
{
    using TTable = TPartitionSchema::ZoneBlobUpdates;

    Table<TTable>()
        .Key(zoneId, deletionId)
        .Update(
            NIceDb::TUpdate<TTable::CommitId>(commitId),
            NIceDb::TUpdate<TTable::StartIndex>(blockRange.Start),
            NIceDb::TUpdate<TTable::EndIndex>(blockRange.End));
}

void TPartitionDatabase::DeleteZoneBlobUpdate(ui32 zoneId, ui64 deletionId)
{
    using TTable = TPartitionSchema::ZoneBlobUpdates;

    Table<TTable>().Key(zoneId, deletionId).Delete();
}

template <class TTable, class TIterator>
bool ReadBlobUpdatesImpl(TIterator& it, TVector<TBlobUpdate>& updates)
{
    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto deletionId = it.template GetValue<typename TTable::DeletionId>();
        auto commitId = it.template GetValue<typename TTable::CommitId>();
        auto blockRange = TBlockRange32::MakeClosedInterval(
            it.template GetValue<typename TTable::StartIndex>(),
            it.template GetValue<typename TTable::EndIndex>());

        updates.emplace_back(blockRange, commitId, deletionId);

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::ReadGlobalBlobUpdates(TVector<TBlobUpdate>& updates)
{
    using TTable = TPartitionSchema::GlobalBlobUpdates;

    auto it = Table<TTable>().Range().Select();

    return ReadBlobUpdatesImpl<TTable>(it, updates);
}

bool TPartitionDatabase::ReadZoneBlobUpdates(
    ui32 zoneId,
    TVector<TBlobUpdate>& updates)
{
    using TTable = TPartitionSchema::ZoneBlobUpdates;

    auto it = Table<TTable>().Prefix(zoneId).Select();

    return ReadBlobUpdatesImpl<TTable>(it, updates);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteGlobalBlobGarbage(const TBlobGarbage& garbage)
{
    using TTable = TPartitionSchema::GlobalBlobGarbage;

    Table<TTable>()
        .Key(garbage.BlobId.CommitId(), garbage.BlobId.UniqueId())
        .Update<TTable::BlockCount>(garbage.BlockCount);
}

void TPartitionDatabase::DeleteGlobalBlobGarbage(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::GlobalBlobGarbage;

    Table<TTable>().Key(blobId.CommitId(), blobId.UniqueId()).Delete();
}

void TPartitionDatabase::WriteZoneBlobGarbage(
    ui32 zoneId,
    const TBlobGarbage& garbage)
{
    using TTable = TPartitionSchema::ZoneBlobGarbage;

    Table<TTable>()
        .Key(zoneId, garbage.BlobId.CommitId(), garbage.BlobId.UniqueId())
        .Update<TTable::BlockCount>(garbage.BlockCount);
}

void TPartitionDatabase::DeleteZoneBlobGarbage(
    ui32 zoneId,
    const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::ZoneBlobGarbage;

    Table<TTable>().Key(zoneId, blobId.CommitId(), blobId.UniqueId()).Delete();
}

template <class TTable, class TIterator>
bool ReadGlobalBlobGarbageImpl(
    TIterator& it,
    TVector<TPartitionDatabase::TBlobGarbage>& garbage)
{
    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.template GetValue<typename TTable::BlobCommitId>(),
            it.template GetValue<typename TTable::BlobId>());

        ui16 blockCount = it.template GetValue<typename TTable::BlockCount>();

        garbage.push_back({blobId, blockCount});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::ReadGlobalBlobGarbage(
    TVector<TPartitionDatabase::TBlobGarbage>& garbage)
{
    using TTable = TPartitionSchema::GlobalBlobGarbage;

    auto it = Table<TTable>().Range().Select();

    return ReadGlobalBlobGarbageImpl<TTable>(it, garbage);
}

bool TPartitionDatabase::ReadZoneBlobGarbage(
    ui32 zoneId,
    TVector<TPartitionDatabase::TBlobGarbage>& garbage)
{
    using TTable = TPartitionSchema::ZoneBlobGarbage;

    auto it = Table<TTable>().Prefix(zoneId).Select();

    return ReadGlobalBlobGarbageImpl<TTable>(it, garbage);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteCheckpoint(const NProto::TCheckpointMeta& meta)
{
    using TTable = TPartitionSchema::Checkpoints;

    Table<TTable>()
        .Key(meta.GetCommitId())
        .Update<TTable::CheckpointMeta>(meta);
}

void TPartitionDatabase::DeleteCheckpoint(ui64 commitId)
{
    using TTable = TPartitionSchema::Checkpoints;

    Table<TTable>().Key(commitId).Delete();
}

bool TPartitionDatabase::ReadCheckpoint(
    ui64 commitId,
    TMaybe<NProto::TCheckpointMeta>& meta)
{
    using TTable = TPartitionSchema::Checkpoints;

    auto it = Table<TTable>().Key(commitId).Select<TTable::CheckpointMeta>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        meta = it.GetValueOrDefault<TTable::CheckpointMeta>();
    }

    return true;
}

bool TPartitionDatabase::ReadCheckpoints(
    TVector<NProto::TCheckpointMeta>& checkpoints)
{
    using TTable = TPartitionSchema::Checkpoints;

    auto it = Table<TTable>().Range().Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        checkpoints.push_back(it.GetValueOrDefault<TTable::CheckpointMeta>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteCheckpointBlob(
    ui64 commitId,
    const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::CheckpointBlobs;

    Table<TTable>()
        .Key(commitId, blobId.CommitId(), blobId.UniqueId())
        .Update();
}

void TPartitionDatabase::DeleteCheckpointBlob(
    ui64 commitId,
    const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::CheckpointBlobs;

    Table<TTable>()
        .Key(commitId, blobId.CommitId(), blobId.UniqueId())
        .Delete();
}

bool TPartitionDatabase::ReadCheckpointBlobs(
    TVector<TPartitionDatabase::TCheckpointBlob>& blobs)
{
    using TTable = TPartitionSchema::CheckpointBlobs;

    auto it = Table<TTable>().Range().Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        ui64 commitId = it.GetValue<TTable::CommitId>();

        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobId>());

        blobs.push_back({commitId, blobId});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TPartitionDatabase::ReadCheckpointBlobs(
    ui64 commitId,
    TVector<TPartialBlobId>& blobIds)
{
    using TTable = TPartitionSchema::CheckpointBlobs;

    auto it =
        Table<TTable>().GreaterOrEqual(commitId).LessOrEqual(commitId).Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobId>());

        blobIds.push_back(blobId);

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteCompactionMap(
    ui32 blockIndex,
    ui32 blobCount,
    ui32 blockCount)
{
    using TTable = TPartitionSchema::CompactionMap;

    Table<TTable>()
        .Key(blockIndex)
        .Update(NIceDb::TUpdate<TTable::BlobCount>(blobCount))
        .Update(NIceDb::TUpdate<TTable::BlockCount>(blockCount));
}

void TPartitionDatabase::DeleteCompactionMap(ui32 blockIndex)
{
    using TTable = TPartitionSchema::CompactionMap;

    Table<TTable>().Key(blockIndex).Delete();
}

bool TPartitionDatabase::ReadCompactionMap(
    TVector<TCompactionCounter>& compactionMap)
{
    using TTable = TPartitionSchema::CompactionMap;

    auto it = Table<TTable>().Range().Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        compactionMap.emplace_back(
            it.GetValue<TTable::BlockIndex>(),
            TRangeStat{
                ProcessCompactionCounter(it.GetValue<TTable::BlobCount>()),
                ProcessCompactionCounter(it.GetValue<TTable::BlockCount>()),
                0,
                0,
                0,
                0,
                false,
                0});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteGarbageBlob(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::GarbageBlobs;

    Table<TTable>().Key(blobId.CommitId(), blobId.UniqueId()).Update();
}

void TPartitionDatabase::DeleteGarbageBlob(const TPartialBlobId& blobId)
{
    using TTable = TPartitionSchema::GarbageBlobs;

    Table<TTable>().Key(blobId.CommitId(), blobId.UniqueId()).Delete();
}

bool TPartitionDatabase::ReadGarbageBlobs(TVector<TPartialBlobId>& blobIds)
{
    using TTable = TPartitionSchema::GarbageBlobs;

    auto it = Table<TTable>().Range().Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto blobId = MakePartialBlobId(
            it.GetValue<TTable::BlobCommitId>(),
            it.GetValue<TTable::BlobId>());

        blobIds.push_back(blobId);

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
