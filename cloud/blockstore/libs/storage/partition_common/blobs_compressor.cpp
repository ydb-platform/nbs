#include "blobs_compressor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/format.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> GetCompressionChunkSizes(TVector<ui64> sizes)
{
    SortUnique(sizes);
    return sizes;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlobsCompressor::TBlobsCompressor(
    const NBlockCodecs::ICodec* blobCodec,
    ui32 blobCompressionRate,
    TVector<ui64> blobCompressionChunkSizesForEstimation,
    TThreadSafePartCountersPtr partCounters)
    : BlobCodec(blobCodec)
    , BlobCompressionRate(blobCompressionRate)
    , BlobCompressionChunkSizesForEstimation(
          GetCompressionChunkSizes(
              std::move(blobCompressionChunkSizesForEstimation)))
    , PartCounters(std::move(partCounters))
{}

void TBlobsCompressor::CalculateCompressionMetrics(const TPartialBlobId& blobId,
                                                   TStringBuf blobContent) const
{
    if (!BlobCodec || !BlobCompressionRate ||
        blobId.GetHash() % BlobCompressionRate != 0)
    {
        return;
    }

    const auto size = EstimateCompressedSize(blobContent, blobContent.size());
    PartCounters->Access(
        [&](auto& counters)
        {
            counters->Cumulative.UncompressedBytesWritten.Increment(
                blobContent.size());
            counters->Cumulative.CompressedBytesWritten.Increment(size);
        });
    for (const ui64 chunkSize: BlobCompressionChunkSizesForEstimation) {
        const auto size = EstimateCompressedSize(
            blobContent,
            chunkSize ? chunkSize : blobContent.size());
        PartCounters->Access(
            [&](auto& counters)
            {
                auto* compression =
                    counters->BlobCompression.FindPtr(chunkSize);
                if (!compression) {
                    ReportCompressionChunkNotFound(
                        TStringBuilder() << "Compression counters not "
                                            "found for chunk size "
                                         << FormatByteSize(chunkSize));
                    return;
                }
                compression->UncompressedBytesWritten.Increment(
                    blobContent.size());
                compression->CompressedBytesWritten.Increment(size);
            });
    }
}

ui64 TBlobsCompressor::EstimateCompressedSize(TStringBuf blobContent,
                                              ui64 chunkSize) const
{
    Y_ABORT_UNLESS(chunkSize > 0);
    const ui64 uncompressedSize = blobContent.size();

    TString out;
    ui64 compressedSize = 0;
    while (blobContent) {
        const auto chunk =
            blobContent.SubStr(0, Min<ui64>(chunkSize, blobContent.size()));
        out.ReserveAndResize(BlobCodec->MaxCompressedLength(chunk));
        compressedSize += BlobCodec->Compress(chunk, out.begin());
        blobContent.Skip(chunk.size());
    }
    return Min(compressedSize, uncompressedSize);
}

}   // namespace NCloud::NBlockStore::NStorage
