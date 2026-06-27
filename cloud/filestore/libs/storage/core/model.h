#pragma once

#include "config.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TMultiShardFileStoreConfig
{
    NKikimrFileStore::TConfig MainFileSystemConfig;
    TVector<NKikimrFileStore::TConfig> ShardConfigs;
};

ui32 ComputeShardCount(
    const ui64 blocksCount,
    const ui32 blockSize,
    const ui64 shardAllocationUnit,
    const ui32 minShardCount,
    const ui32 maxShardCount);

TMultiShardFileStoreConfig SetupMultiShardFileStorePerformanceAndChannels(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore,
    const NProto::TFileStorePerformanceProfile& clientProfile,
    const ui32 explicitShardCount);

void SetupFileStorePerformanceAndChannels(
    bool allocateMixed0Channel,
    const TStorageConfig& config,
    NKikimrFileStore::TConfig& fileStore,
    const NProto::TFileStorePerformanceProfile& clientProfile);

// This prefix precedes a shard number in a shard ID.
constexpr TStringBuf ShardNumPrefix = "_s";

// It is not possible to have more than MaxShardCount shards,
// as we reserve two bytes to store it in handles and node IDs.
constexpr ui64 MaxShardCount = Max<ui16>();

// Range of possible ShardId encoding versions.
constexpr char MinShardIdEncodingVersion = 1;
constexpr char MaxShardIdEncodingVersion = 31;

// A filesystem ID can be stored encoded in the tablet database.
// In this case, it starts with a character denoting an encoding
// version.
inline bool IsFilesystemIdEncoded(const TString& fsId)
{
    return !fsId.empty() && MinShardIdEncodingVersion <= fsId[0] &&
           fsId[0] <= MaxShardIdEncodingVersion;
}

NCloud::NProto::TError ValidateFilesystemId(const TString& fsId);

}   // namespace NCloud::NFileStore::NStorage
