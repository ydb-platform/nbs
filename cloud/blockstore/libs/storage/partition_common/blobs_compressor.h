#pragma once

#include "part_counters_wrapper.h"

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <library/cpp/blockcodecs/codecs.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBlobsCompressor
{
private:
    const NBlockCodecs::ICodec* BlobCodec;
    const ui32 BlobCompressionRate;
    const TVector<ui64> BlobCompressionChunkSizesForEstimation;
    const TThreadSafePartCountersPtr PartCounters;

public:
    TBlobsCompressor(const NBlockCodecs::ICodec* blobCodec,
                     ui32 blobCompressionRate,
                     TVector<ui64> blobCompressionChunkSizesForEstimation,
                     TThreadSafePartCountersPtr partCounters);

    void CalculateCompressionMetrics(const TPartialBlobId& blobId,
                                     TStringBuf blobContent) const;

private:
    ui64 EstimateCompressedSize(TStringBuf blobContent, ui64 chunkSize) const;
};

}   // namespace NCloud::NBlockStore::NStorage
