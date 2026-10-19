#pragma once

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MergedBlobCompressionVersion = 1;
constexpr ui32 MergedBlobCompressionLz4 = 1;
constexpr ui32 MergedBlobCompressionChunkSize = 32 * 1024;
constexpr ui32 MaxMergedBlobLogicalBytes = 128 * 1024 * 1024;

struct TCompressedMergedBlob
{
    TString Payload;
    NProto::TBlobCompression Compression;
    ui64 MetadataBytes = 0;
};

struct TCompressedBlobChunk
{
    ui32 Index = 0;
    ui32 Offset = 0;
    ui32 Size = 0;
};

// A successful attempt can return an empty result: the caller must retain the
// original raw blob, including its original id and absence of format metadata.
// blobMeta is the raw TBlobMeta which would otherwise be published.
NProto::TError CompressMergedBlob(
    TStringBuf raw,
    ui32 blockSize,
    ui32 minSavingsPercentage,
    const NProto::TBlobMeta& blobMeta,
    TCompressedMergedBlob& result);

// Checks the complete descriptor before any allocation, offset arithmetic or
// codec invocation. A present but empty descriptor is never a legacy blob.
NProto::TError ValidateMergedBlobCompression(
    const NProto::TBlobCompression& compression,
    ui32 physicalBytes,
    ui32 blockSize);

// Deduplicates chunks for possibly noncontiguous logical block offsets.
NProto::TError PlanCompressedBlobRead(
    const NProto::TBlobCompression& compression,
    ui32 physicalBytes,
    ui32 blockSize,
    const TVector<ui16>& blockOffsets,
    TVector<TCompressedBlobChunk>& chunks);

// payload must contain exactly one complete encoded chunk. Integrity is checked
// independently of optional logical block checksums.
NProto::TError DecodeMergedBlobChunk(
    const NProto::TBlobCompression& compression,
    ui32 chunkIndex,
    TStringBuf payload,
    TString& decoded);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
