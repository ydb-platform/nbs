#pragma once

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/util/rope.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * Compresses blob payloads in independently decodable chunks and restores
 * selected blocks from the compressed data.
 *
 * Each chunk contains whole blocks; only the final chunk may be shorter than
 * the configured chunk size. Compressed chunks are concatenated in blob order,
 * with their sizes, codec and uncompressed chunk size stored in metadata.
 * PlanReadBlob selects the byte ranges needed for a read, and DecompressBlob
 * restores the requested blocks from those ranges.
 *
 * The constructor's chunk size and codec configure writes. Reads use the blob's
 * metadata and require the same block size that was used for compression.
 * Invalid inputs, compressed data and codec failures are returned as errors.
 */
class TBlobCompressor
{
public:
    struct TCompressedBlob
    {
        // Compressed chunks concatenated in their original order.
        TString Data;

        // Codec, uncompressed chunk size and compressed size of each chunk.
        NProto::TBlobCompression Compression;
    };

    struct TReadPlan
    {
        // Query storage; only the first QueriesCount entries are used.
        TArrayHolder<NKikimr::TEvBlobStorage::TEvGet::TQuery> Queries;

        // Number of initialized queries, after deduplication and coalescing.
        size_t QueriesCount = 0;

        // Block offsets for each request.
        // ith element contains list of block offsets which depend on the ith
        // query.
        TVector<TVector<ui32>> BlockOffsetsForEachRequest;
    };

private:
    // Used to identify the affected tablet in validation errors.
    const ui64 TabletId;

    // Size of one uncompressed block in bytes, used for both reads and writes.
    ui32 BlockSize;

    // Uncompressed chunk size in bytes, used for writes.
    const std::optional<ui32> ChunkSize;

    // Codec recorded in the metadata of newly compressed blobs.
    const std::optional<NProto::TBlobCompression::ECodec> CodecId;

public:
    /**
     * Stores the block size and compression settings. CompressBlob validates
     * the settings and returns an error if they are invalid.
     *
     * @param tabletId - Tablet ID used in validation errors.
     * @param blockSize - Nonzero size of one uncompressed block in bytes.
     * @param chunkSize - Uncompressed chunk size in bytes; must be a multiple
     *  of and at least blockSize.
     * @param codec - Compression codec: LZ4 or ZSTD.
     */
    TBlobCompressor(
        ui64 tabletId,
        ui32 blockSize,
        ui32 chunkSize,
        NProto::TBlobCompression::ECodec codec);

    /**
     * Constructs a blob compressor without write settings. It can only plan
     * reads and decompress blobs with known compression settings.
     *
     * @param tabletId - Tablet ID used in validation errors.
     * @param blockSize - Nonzero size of one uncompressed block in bytes.
     */
    explicit TBlobCompressor(ui64 tabletId, ui32 blockSize);

    /**
     * Compresses a blob using the configured chunk size and codec. The final
     * chunk may be shorter than ChunkSize. Returns an error if the settings
     * or payload are invalid, or compression fails.
     *
     * @param blobPayload - Uncompressed blob data containing whole blocks.
     *
     * @return - Concatenated compressed chunks and their metadata, or an error.
     *  Invalid arguments return E_ARGUMENT; codec failures return E_FAIL.
     *  An empty payload produces empty data and no chunks, with codec and chunk
     *  size still recorded in the metadata.
     */
    [[nodiscard]] TResultOrError<TCompressedBlob> CompressBlob(
        TStringBuf blobPayload) const;

    /**
     * Restores requested blocks using the blob's codec and chunk size. Each
     * requested chunk is decoded once, even if several blocks refer to it.
     * Returns an error for invalid inputs or decoding failures.
     * Output buffers may already have been written when a failure occurs.
     *
     * @param compressedBuffers - Data from the ranges returned by PlanReadBlob
     *  for these block offsets, concatenated in query order. Each rope must
     *  contain only complete compressed chunks, which may span rope segments.
     *  Empty ropes are allowed. Each compressed chunk is extracted into a
     *  reusable buffer before decoding.
     * @param outputBuffers - One buffer of exactly BlockSize bytes
     *  per requested block, in blockOffsets order, including duplicates.
     *  Buffers must be writable and must not overlap input.
     * @param compression - Metadata describing the compressed blob.
     * @param blockOffsets - Block positions within the original blob, sorted
     *  in nondecreasing order. Duplicate offsets produce a block in each
     *  corresponding output buffer. Every offset must refer to an existing
     *  block.
     *
     * @return - Success, E_ARGUMENT for invalid settings, metadata, offsets or
     *  output buffers, or E_REJECTED for invalid compressed data or decoding
     *  failures. Codec lookup failures return E_FAIL.
     */
    [[nodiscard]] NProto::TError DecompressBlob(
        const TVector<TRope>& compressedBuffers,
        const TSgList& outputBuffers,
        const NProto::TBlobCompression& compression,
        const TVector<ui16>& blockOffsets,
        TVector<ui32>* blockChecksums) const;

    /**
     * Plans reads of the complete compressed chunks needed for the requested
     * blocks, using the chunk size from the blob's metadata. Duplicate chunks
     * are read once and adjacent chunks are coalesced into a single range.
     * Returns E_ARGUMENT for invalid chunk metadata, offsets outside the chunk
     * list or read ranges extending beyond the compressed blob.
     *
     * The caller must supply valid block offsets: the metadata does not store
     * the original blob size, so this method cannot detect offsets beyond the
     * end of a partial final chunk.
     *
     * @param blobId - ID of the compressed blob to read.
     * @param compression - Metadata describing the compressed blob.
     * @param blockOffsets - Block positions within the original blob, sorted
     *  in nondecreasing order; duplicates are allowed.
     *
     * @return - A read plan or an error. The plan contains an owning query array
     *  and the number of initialized entries. Queries are for blobId, sorted by
     *  byte offset (Shift), and cover disjoint ranges of complete compressed
     *  chunks. Storage is allocated for at most blockOffsets.size() queries;
     *  only the first QueriesCount entries are used. An empty block list returns
     *  a null array and zero count without validating the blob ID or metadata.
     */
    [[nodiscard]] TResultOrError<TReadPlan> PlanReadBlob(
        const NKikimr::TLogoBlobID& blobId,
        const NProto::TBlobCompression& compression,
        const TVector<ui16>& blockOffsets) const;
};

using TPartialBlobId2CompressionInfoMap = THashMap<
    TPartialBlobId,
    std::optional<NProto::TBlobCompression>,
    TPartialBlobIdHash>;

using TBlobId2CompressionInfoMap =
    THashMap<NKikimr::TLogoBlobID, std::optional<NProto::TBlobCompression>>;

}   // namespace NCloud::NBlockStore::NStorage
