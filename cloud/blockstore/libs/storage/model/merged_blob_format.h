#pragma once

#include <util/system/types.h>

#include <memory>

namespace NCloud::NBlockStore::NProto {
class TBlobCompression;
}

namespace NCloud::NBlockStore::NStorage {

struct TMergedBlobCompressionStats
{
    bool Background = false;
    ui64 FormatErrors = 0;
    ui64 RawMergedReadLogicalBytes = 0;
    ui64 Attempts = 0;
    ui64 Accepted = 0;
    ui64 RawFallback = 0;
    ui64 AdmissionRejected = 0;
    ui64 LogicalBytes = 0;
    ui64 PhysicalBytes = 0;
    ui64 MetadataBytes = 0;
    ui64 EncodeCpuMicros = 0;
    ui64 ReadPhysicalBytes = 0;
    ui64 ReadLogicalBytes = 0;
    ui64 DecodedChunks = 0;
    ui64 DecodeCpuMicros = 0;
    ui64 DecodeErrors = 0;
    ui64 ReadAdmissionRejected = 0;

    template <typename TCounters>
    void Publish(TCounters& counters) const
    {
        counters.MergedBlobCompressionAttempts.Increment(Attempts);
        counters.MergedBlobCompressionAccepted.Increment(Accepted);
        counters.MergedBlobCompressionRawFallback.Increment(RawFallback);
        counters.MergedBlobCompressionAdmissionRejected.Increment(AdmissionRejected);
        counters.MergedBlobCompressionLogicalBytes.Increment(LogicalBytes);
        counters.MergedBlobCompressionPhysicalBytes.Increment(PhysicalBytes);
        counters.MergedBlobCompressionMetadataBytes.Increment(MetadataBytes);
        counters.MergedBlobCompressionEncodeCpuMicros.Increment(EncodeCpuMicros);
        counters.MergedBlobCompressionReadPhysicalBytes.Increment(ReadPhysicalBytes);
        counters.MergedBlobCompressionReadLogicalBytes.Increment(ReadLogicalBytes);
        counters.MergedBlobCompressionDecodedChunks.Increment(DecodedChunks);
        counters.MergedBlobCompressionDecodeCpuMicros.Increment(DecodeCpuMicros);
        counters.MergedBlobCompressionDecodeErrors.Increment(DecodeErrors);
        counters.MergedBlobCompressionReadAdmissionRejected.Increment(ReadAdmissionRejected);
        counters.MergedBlobCompressionRawFallbackBytes.Increment(RawFallback ? LogicalBytes : 0);
        counters.MergedBlobCompressionAcceptedLogicalBytes.Increment(Accepted ? LogicalBytes : 0);
        counters.MergedBlobCompressionFormatErrors.Increment(FormatErrors);
        counters.MergedBlobCompressionForegroundReadLogicalBytes.Increment(!Background ? ReadLogicalBytes : 0);
        counters.MergedBlobCompressionForegroundReadPhysicalBytes.Increment(!Background ? ReadPhysicalBytes : 0);
        counters.MergedBlobCompressionForegroundDecodedChunks.Increment(!Background ? DecodedChunks : 0);
        counters.MergedBlobCompressionForegroundDecodeCpuMicros.Increment(!Background ? DecodeCpuMicros : 0);
        counters.MergedBlobCompressionForegroundRawMergedReadLogicalBytes.Increment(!Background ? RawMergedReadLogicalBytes : 0);
        counters.MergedBlobCompressionBackgroundReadLogicalBytes.Increment(Background ? ReadLogicalBytes : 0);
        counters.MergedBlobCompressionBackgroundReadPhysicalBytes.Increment(Background ? ReadPhysicalBytes : 0);
        counters.MergedBlobCompressionBackgroundDecodedChunks.Increment(Background ? DecodedChunks : 0);
        counters.MergedBlobCompressionBackgroundDecodeCpuMicros.Increment(Background ? DecodeCpuMicros : 0);
        counters.MergedBlobCompressionBackgroundRawMergedReadLogicalBytes.Increment(Background ? RawMergedReadLogicalBytes : 0);
        counters.MergedBlobCompressionBackgroundEncodeCpuMicros.Increment(Background ? EncodeCpuMicros : 0);
    }
};

// Kept once per index row/blob and shared by its logical block marks.
// A present but invalid descriptor must never be interpreted as legacy raw.
struct TMergedBlobFormat
{
    ui32 LogicalBlocks = 0;
    std::shared_ptr<const NProto::TBlobCompression> Compression;
    bool Invalid = false;
};

}   // namespace NCloud::NBlockStore::NStorage
