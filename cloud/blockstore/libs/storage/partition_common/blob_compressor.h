#pragma once

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/util/rope.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NBlockCodecs {
struct ICodec;
}

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
 * Caller invariant violations abort with the tablet ID in the diagnostic.
 * Invalid compressed data and decoding failures are returned as errors.
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
    // Used to identify the affected tablet in invariant violations.
    const ui64 TabletId;

    // Size of one uncompressed block in bytes, used for both reads and writes.
    const ui32 BlockSize;

    // Uncompressed chunk size in bytes, used for writes.
    const ui32 ChunkSize;

    // Codec recorded in the metadata of newly compressed blobs.
    const NProto::TBlobCompression::ECodec CodecId;

    // Codec implementation used to compress new blobs.
    const NBlockCodecs::ICodec* const Codec;

public:
    /**
     * Configures the block size and compression settings. Aborts if the sizes
     * are invalid, the codec is unsupported or codec lookup fails.
     *
     * @param tabletId - Tablet ID used in invariant violations.
     * @param blockSize - Nonzero size of one uncompressed block in bytes.
     * @param chunkSize - Uncompressed chunk size in bytes; must be a multiple
     *  of and greater than blockSize.
     * @param codec - Compression codec: LZ4 or ZSTD.
     */
    explicit TBlobCompressor(
        ui64 tabletId,
        ui32 blockSize,
        ui32 chunkSize,
        NProto::TBlobCompression::ECodec codec);

    /**
     * Compresses a blob using the configured chunk size and codec. The final
     * chunk may be shorter than ChunkSize. Aborts if the payload does not
     * contain whole blocks or compression fails.
     *
     * @param blobPayload - Uncompressed blob data containing whole blocks.
     *
     * @return - Concatenated compressed chunks and their metadata. An empty
     *  payload produces empty data and no chunks, with codec and chunk size
     *  still recorded in the metadata.
     */
    [[nodiscard]] TCompressedBlob CompressBlob(TStringBuf blobPayload) const;

    /**
     * Restores requested blocks using the blob's codec and chunk size. Each
     * requested chunk is decoded once, even if several blocks refer to it.
     * Returns an error for invalid compressed buffers or decoding failures.
     * Aborts on caller invariant violations.
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
     * @return - Success, or E_REJECTED if the compressed data is invalid or
     *  cannot be decoded.
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
     * Aborts on invalid chunk metadata, offsets outside the chunk list or
     * read ranges extending beyond the compressed blob.
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
     * @return - An owning query array and the number of initialized entries.
     *  Queries are for blobId, sorted by byte offset (Shift), and cover
     *  disjoint ranges of complete compressed chunks. Storage is allocated for
     *  at most blockOffsets.size() queries; only the first QueriesCount entries
     *  are used. An empty block list returns a null array and zero count without
     *  validating the blob ID or metadata.
     */
    [[nodiscard]] TReadPlan PlanReadBlob(
        const NKikimr::TLogoBlobID& blobId,
        const NProto::TBlobCompression& compression,
        const TVector<ui16>& blockOffsets) const;
};

}   // namespace NCloud::NBlockStore::NStorage
