#pragma once

#include "volume_state.h"

#include <cloud/blockstore/libs/storage/volume/model/meta.h>

#include <cloud/blockstore/libs/storage/protos/volume.pb.h>
#include <cloud/blockstore/config/storage.pb.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <contrib/ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TVolumeDatabase
    : public NKikimr::NIceDb::TNiceDb
{
public:
    TVolumeDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema();

    //
    // Meta
    //

    void WriteMeta(const NProto::TVolumeMeta& meta);
    bool ReadMeta(TMaybe<NProto::TVolumeMeta>& meta);
    void WriteStartPartitionsNeeded(const bool startPartitionsNeeded);
    bool ReadStartPartitionsNeeded(TMaybe<bool>& startPartitionsNeeded);
    void WriteStorageConfig(const NProto::TStorageServiceConfig& storageConfig);
    bool ReadStorageConfig(TMaybe<NProto::TStorageServiceConfig>& storageConfig);

    //
    // MetaHistory
    //

    void WriteMetaHistory(ui32 version, const TVolumeMetaHistoryItem& meta);
    bool ReadMetaHistory(TVector<TVolumeMetaHistoryItem>& metas);

    //
    // Clients
    //

    void WriteClients(const THashMap<TString, TVolumeClientState>& infos);
    bool ReadClients(THashMap<TString, TVolumeClientState>& infos);

    void WriteClient(const NProto::TVolumeClientInfo& info);
    bool ReadClient(const TString& clientId, TMaybe<NProto::TVolumeClientInfo>& info);

    void RemoveClient(const TString& clientId);

    //
    // History
    //

    void WriteHistory(THistoryLogItem item);

    bool ReadOutdatedHistory(
        TInstant oldestTimestamp,
        ui32 itemCount,
        TVector<THistoryLogKey>& records);

    bool ReadHistory(
        THistoryLogKey startTs,
        TInstant endTs,
        ui64 numRecords,
        TVolumeMountHistorySlice& records);

    void DeleteHistoryEntry(THistoryLogKey entry);

    //
    // PartStats
    //

    struct TPartStats
    {
        // For DiskRegistry-based always 0.
        // For BlobStorage-based TabletId used.
        ui64 TabletId = 0;
        NProto::TCachedPartStats Stats;
    };

    void WritePartStats(
        ui64 partTabletId,
        const NProto::TCachedPartStats& stats);
    bool ReadPartStats(TVector<TPartStats>& stats);

    void WriteNonReplPartStats(ui64 id, const NProto::TCachedPartStats& stats);
    bool ReadNonReplPartStats(TVector<TPartStats>& stats);

    //
    // CheckpointRequests
    //

    void WriteCheckpointRequest(const TCheckpointRequest& state);
    bool CollectCheckpointsToDelete(
        TDuration deletedCheckpointHistoryLifetime,
        TInstant now,
        THashMap<TString, TInstant>& deletedCheckpoints);
    void UpdateCheckpointRequest(
        ui64 requestId,
        bool completed,
        const TString& shadowDiskId,
        EShadowDiskState shadowDiskState,
        const TString& error);
    void UpdateShadowDiskState(
        ui64 requestId,
        ui64 processedBlockCount,
        EShadowDiskState shadowDiskState);
    bool ReadCheckpointRequests(
        const THashMap<TString, TInstant>& deletedCheckpoints,
        TVector<TCheckpointRequest>& requests,
        TVector<ui64>& outdatedCheckpointRequestIds);
    void DeleteCheckpointEntry(ui64 requestId);

    //
    // UsedBlocks
    //

    void WriteUsedBlocks(const TCompressedBitmap::TSerializedChunk& chunk);
    bool ReadUsedBlocks(TCompressedBitmap& usedBlocks);

    //
    // ThrottlerState
    //

    struct TThrottlerStateInfo
    {
        ui64 Budget = 0;
    };

    void WriteThrottlerState(const TThrottlerStateInfo& stateInfo);
    bool ReadThrottlerState(TMaybe<TThrottlerStateInfo>& stateInfo);

    //
    // VolumeParams
    //

    void WriteVolumeParams(
        const TVector<TRuntimeVolumeParamsValue>& volumeParams);
    void DeleteVolumeParams(const TVector<TString>& keys);
    bool ReadVolumeParams(TVector<TRuntimeVolumeParamsValue>& volumeParams);
};

}   // namespace NCloud::NBlockStore::NStorage
