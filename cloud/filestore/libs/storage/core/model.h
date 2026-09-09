#pragma once

#include "config.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <util/generic/guid.h>
#include <util/generic/vector.h>

#include <charconv>

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
//
// Max<ui16>() is reserved for the synthetic filestore controls namespace
constexpr ui64 MaxShardCount = Max<ui16>() - 1;

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

constexpr size_t MaxHexDigitsInGuidWord = sizeof(TGUID::dw[0]) * 2;

// At most two hexadecimal characters per GUID byte, plus three dashes.
// Example: 57d8913c-c009f3cd-f059ad8a-cabde340
constexpr size_t MaxGuidAsStringChars = sizeof(TGUID::dw) * 2 + 3;

NCloud::NProto::TError ValidateFilesystemId(const TString& fsId);

inline void CreateGuidString(const TGUID& guid, TString& str)
{
    str.ReserveAndResize(MaxGuidAsStringChars);
    char* ptr = str.Detach();
    const char* buffStart = ptr;

    for (ui32 i = 0; i < sizeof(TGUID::dw) / sizeof(TGUID::dw[0]); ++i) {
        auto result = std::to_chars(
            ptr,
            ptr + MaxHexDigitsInGuidWord,
            guid.dw[i],
            16);
        ptr = result.ptr;
        *(ptr++) = '-';
    }

    *(--ptr) = 0;
    str.ReserveAndResize(ptr - buffStart);
}

}   // namespace NCloud::NFileStore::NStorage
