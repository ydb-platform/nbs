#include "volume_database.h"

#include "volume_schema.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 META_KEY = 1;
constexpr ui32 THROTTLER_STATE_KEY = 1;

THistoryLogKey CreateHistoryLogKey(ui64 ts, ui64 seqNo)
{
    return THistoryLogKey{
        TInstant::MicroSeconds(Max<ui64>() - ts),
        Max<ui64>() - seqNo};
}

THistoryLogKey CreateReversedHistoryLogKey(const THistoryLogKey& key)
{
    return CreateHistoryLogKey(key.Timestamp.MicroSeconds(), key.SeqNo);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::InitSchema()
{
    Materialize<TVolumeSchema>();

    TSchemaInitializer<TVolumeSchema::TTables>::InitStorage(Database.Alter());
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WriteMeta(const NProto::TVolumeMeta& meta)
{
    using TTable = TVolumeSchema::Meta;

    Table<TTable>()
        .Key(META_KEY)
        .Update(NIceDb::TUpdate<TTable::VolumeMeta>(meta));
}

bool TVolumeDatabase::ReadMeta(TMaybe<NProto::TVolumeMeta>& meta)
{
    using TTable = TVolumeSchema::Meta;

    auto it = Table<TTable>()
        .Key(META_KEY)
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        meta = it.GetValue<TTable::VolumeMeta>();
    }

    return true;
}

void TVolumeDatabase::WriteStartPartitionsNeeded(const bool startPartitionsNeeded)
{
    using TTable = TVolumeSchema::Meta;

    Table<TTable>()
        .Key(META_KEY)
        .Update(NIceDb::TUpdate<TTable::StartPartitionsNeeded>(startPartitionsNeeded));
}

bool TVolumeDatabase::ReadStartPartitionsNeeded(TMaybe<bool>& startPartitionsNeeded)
{
    using TTable = TVolumeSchema::Meta;

    auto it = Table<TTable>()
        .Key(META_KEY)
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        startPartitionsNeeded = it.GetValue<TTable::StartPartitionsNeeded>();
    }

    return true;
}

void TVolumeDatabase::WriteStorageConfig(const NProto::TStorageServiceConfig& storageConfig)
{
    using TTable = TVolumeSchema::Meta;

    Table<TTable>()
        .Key(META_KEY)
        .Update(NIceDb::TUpdate<TTable::StorageConfig>(storageConfig));
}

bool TVolumeDatabase::ReadStorageConfig(
    TMaybe<NProto::TStorageServiceConfig>& storageConfig)
{
    using TTable = TVolumeSchema::Meta;

    auto it = Table<TTable>()
        .Key(META_KEY)
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid() && it.HaveValue<TTable::StorageConfig>()) {
        storageConfig = it.GetValue<TTable::StorageConfig>();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WriteMetaHistory(
    ui32 version,
    const TVolumeMetaHistoryItem& meta)
{
    using TTable = TVolumeSchema::MetaHistory;

    Table<TTable>()
        .Key(version)
        .Update(
            NIceDb::TUpdate<TTable::Timestamp>(meta.Timestamp.MicroSeconds()),
            NIceDb::TUpdate<TTable::VolumeMeta>(meta.Meta));

    // simple size limit
    Table<TTable>()
        .Key(version - 1000)
        .Delete();
}

bool TVolumeDatabase::ReadMetaHistory(TVector<TVolumeMetaHistoryItem>& metas)
{
    using TTable = TVolumeSchema::MetaHistory;

    auto it = Table<TTable>()
        .Range()
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto timestamp = TInstant::MicroSeconds(it.GetValue<TTable::Timestamp>());
        auto meta = it.GetValue<TTable::VolumeMeta>();
        metas.push_back({timestamp, std::move(meta)});

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WriteClients(const THashMap<TString, TVolumeClientState>& infos)
{
    using TTable = TVolumeSchema::Clients;

    for (const auto& pair: infos) {
        Table<TTable>()
            .Key(pair.first)
            .Update(NIceDb::TUpdate<TTable::ClientInfo>(pair.second.GetVolumeClientInfo()));
    }
}

bool TVolumeDatabase::ReadClients(THashMap<TString, TVolumeClientState>& infos)
{
    using TTable = TVolumeSchema::Clients;

    auto it = Table<TTable>()
        .Range()
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto info = it.GetValue<TTable::ClientInfo>();
        infos.emplace(info.GetClientId(), TVolumeClientState(info));

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WriteClient(const NProto::TVolumeClientInfo& info)
{
    using TTable = TVolumeSchema::Clients;

    Table<TTable>()
        .Key(info.GetClientId())
        .Update(NIceDb::TUpdate<TTable::ClientInfo>(info));
}

bool TVolumeDatabase::ReadClient(
    const TString& clientId,
    TMaybe<NProto::TVolumeClientInfo>& info)
{
    using TTable = TVolumeSchema::Clients;

    auto it = Table<TTable>()
        .Key(clientId)
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid()) {
        info = it.GetValue<TTable::ClientInfo>();
    }

    return true;
}

void TVolumeDatabase::RemoveClient(const TString& clientId)
{
    using TTable = TVolumeSchema::Clients;

    Table<TTable>()
        .Key(clientId)
        .Delete();
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeDatabase::ReadOutdatedHistory(
    TInstant oldestTimestamp,
    ui32 itemCount,
    TVector<THistoryLogKey>& records)
{
    if (!itemCount) {
        return true;
    }

    using TTable = TVolumeSchema::History;

    auto dbKey =
        CreateReversedHistoryLogKey(THistoryLogKey(oldestTimestamp));

    auto it = Table<TTable>()
        .GreaterOrEqual(
            dbKey.Timestamp.MicroSeconds(),
            dbKey.SeqNo)
        .Select<TTable::TKeyColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto key = CreateHistoryLogKey(
            it.GetValue<TTable::Timestamp>(),
            it.GetValue<TTable::SeqNo>());
        records.push_back(key);

        if (records.size() == itemCount) {
            return true;
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TVolumeDatabase::ReadHistory(
    THistoryLogKey startTs,
    TInstant endTs,
    ui64 numRecords,
    TVolumeMountHistorySlice& records)
{
    using TTable = TVolumeSchema::History;

    auto dbKey = CreateReversedHistoryLogKey(startTs);

    auto it = Table<TTable>()
        .GreaterOrEqual(
            dbKey.Timestamp.MicroSeconds(),
            dbKey.SeqNo)
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto itemKey = CreateHistoryLogKey(
            it.template GetValue<TTable::Timestamp>(),
            it.template GetValue<TTable::SeqNo>());

        if (itemKey.Timestamp < endTs) {
            return true;
        }

        if (records.Items.size() == numRecords) {
            records.NextOlderRecord.emplace(itemKey);
            return true;
        }

        THistoryLogItem item {itemKey, it.template GetValue<TTable::OperationInfo>()};

        records.Items.push_back(std::move(item));

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TVolumeDatabase::DeleteHistoryEntry(THistoryLogKey entry)
{
    using TTable = TVolumeSchema::History;

    auto dbKey = CreateReversedHistoryLogKey(entry);

    Table<TTable>()
        .Key(
            dbKey.Timestamp.MicroSeconds(),
            dbKey.SeqNo)
        .Delete();
}

void TVolumeDatabase::WriteHistory(THistoryLogItem item)
{
    using TTable = TVolumeSchema::History;

    auto dbKey = CreateReversedHistoryLogKey(item.Key);

    Table<TTable>()
        .Key(dbKey.Timestamp.MicroSeconds(), dbKey.SeqNo)
        .Update(NIceDb::TUpdate<TTable::OperationInfo>(item.Operation));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WritePartStats(
    ui64 partTabletId,
    const NProto::TCachedPartStats& stats)
{
    using TTable = TVolumeSchema::PartStats;

    Table<TTable>()
        .Key(partTabletId)
        .Update(NIceDb::TUpdate<TTable::Stats>(stats));
}

bool TVolumeDatabase::ReadPartStats(TVector<TPartStats>& stats)
{
    using TTable = TVolumeSchema::PartStats;

    auto it = Table<TTable>().Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        stats.push_back({
            it.GetValue<TTable::PartTabletId>(),
            it.GetValue<TTable::Stats>()
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TVolumeDatabase::WriteNonReplPartStats(
    ui64 id,
    const NProto::TCachedPartStats& stats)
{
    using TTable = TVolumeSchema::NonReplPartStats;

    Table<TTable>()
        .Key(id)
        .Update(NIceDb::TUpdate<TTable::Stats>(stats));
}

bool TVolumeDatabase::ReadNonReplPartStats(TVector<TPartStats>& stats)
{
    using TTable = TVolumeSchema::NonReplPartStats;

    auto it = Table<TTable>().Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        stats.push_back({
            it.GetValue<TTable::Id>(),
            it.GetValue<TTable::Stats>()
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WriteCheckpointRequest(
    const TCheckpointRequest& request)
{
    using TTable = TVolumeSchema::CheckpointRequests;

    Table<TTable>()
        .Key(request.RequestId)
        .Update(
            NIceDb::TUpdate<TTable::CheckpointId>(request.CheckpointId),
            NIceDb::TUpdate<TTable::Timestamp>(request.Timestamp.MicroSeconds()),
            NIceDb::TUpdate<TTable::State>(static_cast<ui32>(request.State)),
            NIceDb::TUpdate<TTable::ReqType>(static_cast<ui32>(request.ReqType)),
            NIceDb::TUpdate<TTable::CheckpointType>(static_cast<ui32>(request.Type)));
}

void TVolumeDatabase::UpdateCheckpointRequest(
    ui64 requestId,
    bool completed,
    const TString& shadowDiskId,
    EShadowDiskState shadowDiskState,
    const TString& error)
{
    using TTable = TVolumeSchema::CheckpointRequests;

    auto state = completed ? ECheckpointRequestState::Completed
                           : ECheckpointRequestState::Rejected;

    Table<TTable>().Key(requestId).Update(
        NIceDb::TUpdate<TTable::State>(static_cast<ui32>(state)),
        NIceDb::TUpdate<TTable::ShadowDiskId>(shadowDiskId),
        NIceDb::TUpdate<TTable::ShadowDiskState>(
            static_cast<ui32>(shadowDiskState)),
        NIceDb::TUpdate<TTable::CheckpointError>(error));
}

void TVolumeDatabase::UpdateShadowDiskState(
    ui64 requestId,
    ui64 processedBlockCount,
    EShadowDiskState shadowDiskState)
{
    using TTable = TVolumeSchema::CheckpointRequests;

    Table<TTable>().Key(requestId).Update(
        NIceDb::TUpdate<TTable::ShadowDiskProcessedBlockCount>(
            processedBlockCount),
        NIceDb::TUpdate<TTable::ShadowDiskState>(
            static_cast<ui32>(shadowDiskState)));
}

bool TVolumeDatabase::CollectCheckpointsToDelete(
    TDuration deletedCheckpointHistoryLifetime,
    TInstant now,
    THashMap<TString, TInstant>& deletedCheckpoints)
{
    using TTable = TVolumeSchema::CheckpointRequests;

    auto it = Table<TTable>().Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        auto requestState = static_cast<ECheckpointRequestState>(it.GetValue<TTable::State>());
        auto timestamp = TInstant::MicroSeconds(it.GetValue<TTable::Timestamp>());
        auto reqType = static_cast<ECheckpointRequestType>(it.GetValue<TTable::ReqType>());

        if (reqType == ECheckpointRequestType::Delete &&
            requestState == ECheckpointRequestState::Completed &&
            timestamp + deletedCheckpointHistoryLifetime <= now)
        {
            deletedCheckpoints[it.GetValue<TTable::CheckpointId>()] = timestamp;
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}


bool TVolumeDatabase::ReadCheckpointRequests(
    const THashMap<TString, TInstant>& deletedCheckpoints,
    TVector<TCheckpointRequest>& requests,
    TVector<ui64>& outdatedCheckpointRequestIds)
{
    using TTable = TVolumeSchema::CheckpointRequests;

    auto it = Table<TTable>().Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        const auto& id = it.GetValue<TTable::CheckpointId>();
        auto checkpointIt = deletedCheckpoints.find(id);
        if (checkpointIt != deletedCheckpoints.end() &&
            TInstant::MicroSeconds(it.GetValue<TTable::Timestamp>()) <= checkpointIt->second)
        {
            outdatedCheckpointRequestIds.push_back(it.GetValue<TTable::RequestId>());
        } else {
            requests.emplace_back(
                it.GetValue<TTable::RequestId>(),
                it.GetValue<TTable::CheckpointId>(),
                TInstant::MicroSeconds(it.GetValue<TTable::Timestamp>()),
                static_cast<ECheckpointRequestType>(
                    it.GetValue<TTable::ReqType>()),
                static_cast<ECheckpointRequestState>(
                    it.GetValue<TTable::State>()),
                static_cast<ECheckpointType>(
                    it.GetValue<TTable::CheckpointType>()),
                it.GetValue<TTable::ShadowDiskId>(),
                static_cast<EShadowDiskState>(
                    it.GetValue<TTable::ShadowDiskState>()),
                it.GetValue<TTable::ShadowDiskProcessedBlockCount>(),
                it.GetValue<TTable::CheckpointError>());
        }

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TVolumeDatabase::DeleteCheckpointEntry(ui64 requestId)
{
    using TTable = TVolumeSchema::CheckpointRequests;

    Table<TTable>()
        .Key(requestId)
        .Delete();
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WriteUsedBlocks(
    const TCompressedBitmap::TSerializedChunk& chunk)
{
    using TTable = TVolumeSchema::UsedBlocks;

    Table<TTable>()
        .Key(chunk.ChunkIdx)
        .Update(NIceDb::TUpdate<TTable::Bitmap>(chunk.Data));
}

bool TVolumeDatabase::ReadUsedBlocks(TCompressedBitmap& usedBlocks)
{
    using TTable = TVolumeSchema::UsedBlocks;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        usedBlocks.Update({
            it.GetValue<TTable::RangeIndex>(),
            it.GetValue<TTable::Bitmap>()
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WriteThrottlerState(const TThrottlerStateInfo& stateInfo)
{
    using TTable = TVolumeSchema::ThrottlerState;

    Table<TTable>()
        .Key(THROTTLER_STATE_KEY)
        .Update(NIceDb::TUpdate<TTable::Budget>(stateInfo.Budget));
}

bool TVolumeDatabase::ReadThrottlerState(TMaybe<TThrottlerStateInfo>& stateInfo)
{
    using TTable = TVolumeSchema::ThrottlerState;

    auto it = Table<TTable>()
        .Key(THROTTLER_STATE_KEY)
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        stateInfo = {
            it.GetValue<TTable::Budget>()
        };
    }

    return true;
}


////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::WriteVolumeParams(
    const TVector<TRuntimeVolumeParamsValue>& volumeParams)
{
    using TTable = TVolumeSchema::VolumeParams;

    for (const auto& param: volumeParams) {
        Table<TTable>()
            .Key(param.Key)
            .Update(
                NIceDb::TUpdate<TTable::Value>(param.Value),
                NIceDb::TUpdate<TTable::ValidUntil>(
                    param.ValidUntil.MicroSeconds()));
    }
}

void TVolumeDatabase::DeleteVolumeParams(const TVector<TString>& keys)
{
    using TTable = TVolumeSchema::VolumeParams;

    for (const auto& key: keys) {
        Table<TTable>()
            .Key(key)
            .Delete();
    }
}

bool TVolumeDatabase::ReadVolumeParams(
    TVector<TRuntimeVolumeParamsValue>& volumeParams)
{
    using TTable = TVolumeSchema::VolumeParams;

    volumeParams.clear();

    auto it = Table<TTable>()
        .Range()
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        volumeParams.push_back({
            it.GetValue<TTable::Key>(),
            it.GetValue<TTable::Value>(),
            TInstant::MicroSeconds(
                it.GetValue<TTable::ValidUntil>()
            )
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TVolumeDatabase::WriteFollower(const TFollowerDiskInfo& follower)
{
    using TTable = TVolumeSchema::FollowerDisks;

    Y_ABORT_UNLESS(follower.Link.LinkUUID);

    Table<TTable>()
        .Key(follower.Link.LinkUUID)
        .Update(
            NIceDb::TUpdate<TTable::CreatedAt>(
                follower.CreatedAt.MicroSeconds()),
            NIceDb::TUpdate<TTable::LeaderDiskId>(follower.Link.LeaderDiskId),
            NIceDb::TUpdate<TTable::LeaderShardId>(follower.Link.LeaderShardId),
            NIceDb::TUpdate<TTable::FollowerDiskId>(
                follower.Link.FollowerDiskId),
            NIceDb::TUpdate<TTable::FollowerShardId>(
                follower.Link.FollowerShardId),
            NIceDb::TUpdate<TTable::FollowerMediaKind>(follower.MediaKind),
            NIceDb::TUpdate<TTable::State>(static_cast<ui32>(follower.State)),
            NIceDb::TUpdate<TTable::ErrorMessage>(follower.ErrorMessage));

    if (follower.MigratedBytes) {
        Table<TTable>()
            .Key(follower.Link.LinkUUID)
            .Update(NIceDb::TUpdate<TTable::MigratedBytes>(
                *follower.MigratedBytes));
    } else {
        Table<TTable>()
            .Key(follower.Link.LinkUUID)
            .UpdateToNull<TTable::MigratedBytes>();
    }
}

void TVolumeDatabase::DeleteFollower(const TLeaderFollowerLink& link)
{
    using TTable = TVolumeSchema::FollowerDisks;

    Table<TTable>().Key(link.LinkUUID).Delete();
}

bool TVolumeDatabase::ReadFollowers(TFollowerDisks& followers)
{
    using TTable = TVolumeSchema::FollowerDisks;

    followers.clear();

    auto it = Table<TTable>().Range().Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        followers.push_back(TFollowerDiskInfo{
            .Link{
                .LinkUUID = it.GetValue<TTable::Uuid>(),
                .LeaderDiskId = it.GetValue<TTable::LeaderDiskId>(),
                .LeaderShardId = it.GetValue<TTable::LeaderShardId>(),
                .FollowerDiskId = it.GetValue<TTable::FollowerDiskId>(),
                .FollowerShardId = it.GetValue<TTable::FollowerShardId>()},
            .CreatedAt =
                TInstant::MicroSeconds(it.GetValue<TTable::CreatedAt>()),
            .State = static_cast<TFollowerDiskInfo::EState>(
                it.GetValue<TTable::State>()),
            .MediaKind = static_cast<NProto::EStorageMediaKind>(
                it.GetValue<TTable::FollowerMediaKind>()),
            .MigratedBytes = it.HaveValue<TTable::MigratedBytes>()
                                 ? it.GetValue<TTable::MigratedBytes>()
                                 : std::optional<ui64>(),
            .ErrorMessage = it.GetValue<TTable::ErrorMessage>()});
        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TVolumeDatabase::WriteLeader(const TLeaderDiskInfo& leader)
{
    using TTable = TVolumeSchema::LeaderDisks;

    Table<TTable>()
        .Key(leader.Link.LinkUUID)
        .Update(
            NIceDb::TUpdate<TTable::CreatedAt>(leader.CreatedAt.MicroSeconds()),
            NIceDb::TUpdate<TTable::LeaderDiskId>(leader.Link.LeaderDiskId),
            NIceDb::TUpdate<TTable::LeaderShardId>(leader.Link.LeaderShardId),
            NIceDb::TUpdate<TTable::FollowerDiskId>(leader.Link.FollowerDiskId),
            NIceDb::TUpdate<TTable::FollowerShardId>(
                leader.Link.FollowerShardId),
            NIceDb::TUpdate<TTable::State>(static_cast<ui32>(leader.State)),
            NIceDb::TUpdate<TTable::ErrorMessage>(leader.ErrorMessage));
}

void TVolumeDatabase::DeleteLeader(const TLeaderFollowerLink& link)
{
    using TTable = TVolumeSchema::LeaderDisks;

    Table<TTable>().Key(link.LinkUUID).Delete();
}

bool TVolumeDatabase::ReadLeaders(TLeaderDisks& leaders)
{
    using TTable = TVolumeSchema::LeaderDisks;

    leaders.clear();

    auto it = Table<TTable>().Range().Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        leaders.push_back(TLeaderDiskInfo{
            .Link{
                .LinkUUID = it.GetValue<TTable::Uuid>(),
                .LeaderDiskId = it.GetValue<TTable::LeaderDiskId>(),
                .LeaderShardId = it.GetValue<TTable::LeaderShardId>(),
                .FollowerDiskId = it.GetValue<TTable::FollowerDiskId>(),
                .FollowerShardId = it.GetValue<TTable::FollowerShardId>()},
            .CreatedAt =
                TInstant::MicroSeconds(it.GetValue<TTable::CreatedAt>()),
            .State = static_cast<TLeaderDiskInfo::EState>(
                it.GetValue<TTable::State>()),
            .ErrorMessage = it.GetValue<TTable::ErrorMessage>()});
        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
