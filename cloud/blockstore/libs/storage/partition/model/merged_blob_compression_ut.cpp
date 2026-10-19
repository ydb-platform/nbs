#include "merged_blob_compression.h"
#include "merged_blob_compression_policy.h"

#include <cloud/blockstore/libs/storage/core/config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

NProto::TBlobMeta MakeMeta(ui32 blocks)
{
    NProto::TBlobMeta meta;
    meta.MutableMergedBlocks()->SetEnd(blocks - 1);
    return meta;
}

TCompressedMergedBlob Encode(const TString& raw, ui32 blockSize)
{
    TCompressedMergedBlob encoded;
    auto error = CompressMergedBlob(
        raw, blockSize, 10, MakeMeta(raw.size() / blockSize), encoded);
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    UNIT_ASSERT(!encoded.Payload.empty());
    return encoded;
}

}   // namespace

Y_UNIT_TEST_SUITE(TMergedBlobCompressionTest)
{

    Y_UNIT_TEST(ShouldApplyAllowlistBeforeStableWriterPercentages)
    {
        NProto::TStorageServiceConfig proto;
        NCloud::NProto::TFeaturesConfig features;
        auto* feature = features.AddFeatures();
        feature->SetName("MergedBlobCompression");
        feature->MutableWhitelist()->AddEntityIds("allowed");
        const auto selected = [&](TString disk, ui64 commit, bool background) {
            TStorageConfig config(proto,
                std::make_shared<NFeatures::TFeaturesConfig>(features));
            return SelectMergedBlobCompression(
                config, "cloud", "folder", disk, commit, 3, background);
        };
        proto.SetDirectMergedBlobCompressionPercentage(100);
        UNIT_ASSERT(selected("allowed", 1, false));
        UNIT_ASSERT(!selected("other", 1, false));
        feature->SetCloudProbability(1);
        feature->SetFolderProbability(1);
        UNIT_ASSERT(!selected("other", 1, false));
        feature->MutableBlacklist()->AddEntityIds("allowed");
        UNIT_ASSERT(!selected("allowed", 1, false));
        feature->ClearBlacklist();

        UNIT_ASSERT(!selected("allowed", 1, true));
        proto.SetDirectMergedBlobCompressionPercentage(37);
        ui32 chosen = 0;
        for (ui64 i = 0; i < 1000; ++i) {
            const bool value = selected("allowed", i, false);
            UNIT_ASSERT_VALUES_EQUAL(value, selected("allowed", i, false));
            chosen += value;
        }
        UNIT_ASSERT(chosen > 250 && chosen < 500);
        proto.SetMergedBlobCompressionCodec("unknown");
        UNIT_ASSERT(!selected("allowed", 1, false));
        proto.SetMergedBlobCompressionCodec("lz4");
        proto.SetMergedBlobCompressionChunkSize(16384);
        UNIT_ASSERT(!selected("allowed", 1, false));
        proto.SetMergedBlobCompressionChunkSize(32768);
        proto.SetDirectMergedBlobCompressionPercentage(101);
        UNIT_ASSERT(!selected("allowed", 1, false));
        proto.SetDirectMergedBlobCompressionPercentage(100);
        proto.SetMergedBlobCompressionMinSavingsPercentage(101);
        UNIT_ASSERT(!selected("allowed", 1, false));
    }

    Y_UNIT_TEST(ShouldBoundAndSeparateCompressionAdmissionPools)
    {
        TVector<std::shared_ptr<void>> foreground;
        for (ui32 i = 0; i < 4; ++i) {
            auto token = TryAcquireMergedBlobBudget(false, false, 1024);
            UNIT_ASSERT(token);
            foreground.push_back(std::move(token));
        }
        UNIT_ASSERT(!TryAcquireMergedBlobBudget(false, false, 1024));
        auto background = TryAcquireMergedBlobBudget(true, false, 1024);
        auto reader = TryAcquireMergedBlobBudget(false, true, 1024);
        UNIT_ASSERT(background && reader);
        foreground.pop_back();
        UNIT_ASSERT(TryAcquireMergedBlobBudget(false, false, 1024));
        UNIT_ASSERT(!TryAcquireMergedBlobBudget(true, true, 257ull * 1024 * 1024));
        UNIT_ASSERT(!TryAcquireMergedBlobBudget(false, true, 0));
    }

    Y_UNIT_TEST(ShouldPublishAdmissionReservationsAndReleaseOnLastOwner)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        RegisterMergedBlobCompressionCounters(counters);
        auto foreground = counters->GetSubgroup("scope", "foreground")
            ->GetSubgroup("operation", "encode");
        auto background = counters->GetSubgroup("scope", "background")
            ->GetSubgroup("operation", "decode");
        const auto value = [&](TStringBuf name) {
            return foreground->GetCounter(TString(name))->Val();
        };
        UNIT_ASSERT_VALUES_EQUAL(value("ReservedBytes"), 0);
        UNIT_ASSERT_VALUES_EQUAL(value("ActiveOperations"), 0);
        UNIT_ASSERT_VALUES_EQUAL(value("QueuedOperations"), 0);
        UNIT_ASSERT_VALUES_EQUAL(value("ReservationLimitBytes"), 512ull * 1024 * 1024);
        const auto attempts = foreground->GetCounter("AdmissionAttempts", true)->Val();
        const auto rejected = foreground->GetCounter("AdmissionRejected", true)->Val();
        auto token = TryAcquireMergedBlobBudget(false, false, 300ull * 1024 * 1024);
        UNIT_ASSERT(token);
        UNIT_ASSERT_VALUES_EQUAL(value("ReservedBytes"), 300ull * 1024 * 1024);
        UNIT_ASSERT_VALUES_EQUAL(value("ActiveOperations"), 1);
        UNIT_ASSERT(!TryAcquireMergedBlobBudget(false, false, 213ull * 1024 * 1024));
        UNIT_ASSERT_VALUES_EQUAL(foreground->GetCounter("AdmissionAttempts", true)->Val(), attempts + 2);
        UNIT_ASSERT_VALUES_EQUAL(foreground->GetCounter("AdmissionRejected", true)->Val(), rejected + 1);
        UNIT_ASSERT_VALUES_EQUAL(background->GetCounter("ReservedBytes")->Val(), 0);
        auto backgroundToken = TryAcquireMergedBlobBudget(true, true, 12345);
        UNIT_ASSERT(backgroundToken);
        UNIT_ASSERT_VALUES_EQUAL(background->GetCounter("ReservedBytes")->Val(), 12345);
        auto secondOwner = token;
        token.reset();
        UNIT_ASSERT_VALUES_EQUAL(value("ActiveOperations"), 1);
        secondOwner.reset();
        backgroundToken.reset();
        UNIT_ASSERT_VALUES_EQUAL(value("ActiveOperations"), 0);
        UNIT_ASSERT_VALUES_EQUAL(value("ReservedBytes"), 0);
        UNIT_ASSERT(ui64(value("PeakReservedBytes")) >= 300ull * 1024 * 1024);
        UNIT_ASSERT(value("PeakActiveOperations") >= 1);
        UNIT_ASSERT_VALUES_EQUAL(background->GetCounter("ReservedBytes")->Val(), 0);
    }

    Y_UNIT_TEST(ShouldReadIndependentChunksAndPartialTail)
    {
        for (ui32 blockSize: {4096, 65536, 131072}) {
            TString raw;
            for (ui32 i = 0; i < 17; ++i) {
                raw.append(TString(blockSize, 'a' + i));
            }
            auto encoded = Encode(raw, blockSize);
            UNIT_ASSERT(!HasError(ValidateMergedBlobCompression(
                encoded.Compression, encoded.Payload.size(), blockSize)));
            const auto& c = encoded.Compression;
            TString decoded, complete;
            ui32 offset = 0;
            for (ui32 i = 0; i < ui32(c.ChunkSizesSize()); ++i) {
                auto error = DecodeMergedBlobChunk(
                    c,
                    i,
                    TStringBuf(encoded.Payload).SubStr(
                        offset, c.GetChunkSizes(i)),
                    decoded);
                UNIT_ASSERT_C(!HasError(error), FormatError(error));
                complete += decoded;
                offset += c.GetChunkSizes(i);
            }
            UNIT_ASSERT_VALUES_EQUAL(raw, complete);

            TVector<TCompressedBlobChunk> chunks;
            UNIT_ASSERT(!HasError(PlanCompressedBlobRead(
                c, encoded.Payload.size(), blockSize, {16, 0, 16}, chunks)));
            const ui32 chunksPerBlock =
                (blockSize + MergedBlobCompressionChunkSize - 1) /
                MergedBlobCompressionChunkSize;
            UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 2 * chunksPerBlock);
            UNIT_ASSERT_VALUES_EQUAL(chunks.front().Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(
                chunks.back().Index,
                (raw.size() - 1) / MergedBlobCompressionChunkSize);
        }
    }

    Y_UNIT_TEST(ShouldCoalesceCrossingAndNoncontiguousReads)
    {
        // Blocks need not divide the chunk size. This also checks a logical
        // block crossing a chunk boundary instead of merely adjacent reads.
        const ui32 blockSize = 3 * 4096;
        const auto encoded = Encode(TString(12 * blockSize, 'x'), blockSize);
        TVector<TCompressedBlobChunk> chunks;
        UNIT_ASSERT(!HasError(PlanCompressedBlobRead(
            encoded.Compression,
            encoded.Payload.size(),
            blockSize,
            {2, 3, 11},
            chunks)));
        UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(chunks[0].Index, 0);
        UNIT_ASSERT_VALUES_EQUAL(chunks[1].Index, 1);
        UNIT_ASSERT_VALUES_EQUAL(chunks[2].Index, 4);
    }

    Y_UNIT_TEST(ShouldCountBothDurableMetadataCopies)
    {
        const TString raw(65536, 'a');
        auto meta = MakeMeta(raw.size() / 4096);
        for (ui32 i = 0; i < 16; ++i) {
            meta.AddBlockChecksums(i);
        }
        TCompressedMergedBlob encoded;
        UNIT_ASSERT(!HasError(CompressMergedBlob(
            raw, 4096, 10, meta, encoded)));
        UNIT_ASSERT(!encoded.Payload.empty());
        auto published = meta;
        *published.MutableCompression() = encoded.Compression;
        UNIT_ASSERT_VALUES_EQUAL(
            encoded.MetadataBytes,
            encoded.Compression.ByteSizeLong() +
                published.ByteSizeLong() - meta.ByteSizeLong());
        UNIT_ASSERT(
            (encoded.Payload.size() + encoded.MetadataBytes) * 100 <=
            raw.size() * 90);
        UNIT_ASSERT(!meta.HasCompression());

        UNIT_ASSERT(!HasError(CompressMergedBlob(
            raw, 4096, 100, meta, encoded)));
        UNIT_ASSERT(encoded.Payload.empty());
        UNIT_ASSERT_VALUES_EQUAL(encoded.Compression.ByteSizeLong(), 0);
    }

    Y_UNIT_TEST(ShouldFallBackForIncompressibleData)
    {
        TFastRng64 rng(42);
        auto raw = TString::Uninitialized(128 * 1024);
        for (char& byte: raw) {
            byte = rng.GenRand();
        }
        TCompressedMergedBlob encoded;
        UNIT_ASSERT(!HasError(CompressMergedBlob(
            raw, 4096, 10, MakeMeta(raw.size() / 4096), encoded)));
        UNIT_ASSERT(encoded.Payload.empty());
        UNIT_ASSERT_VALUES_EQUAL(encoded.MetadataBytes, 0);
    }

    Y_UNIT_TEST(ShouldRejectMalformedMetadata)
    {
        const auto encoded = Encode(TString(65536, 'a'), 4096);
        auto reject = [&](const NProto::TBlobCompression& c)
        {
            UNIT_ASSERT(HasError(ValidateMergedBlobCompression(
                c, encoded.Payload.size(), 4096)));
            TVector<TCompressedBlobChunk> chunks;
            UNIT_ASSERT(HasError(PlanCompressedBlobRead(
                c, encoded.Payload.size(), 4096, {0}, chunks)));
            UNIT_ASSERT(chunks.empty());
        };
        reject({});
        auto c = encoded.Compression;
        c.SetVersion(2);
        reject(c);
        c = encoded.Compression;
        c.SetCodec(100);
        reject(c);
        c = encoded.Compression;
        c.SetChunkSize(65536);
        reject(c);
        c = encoded.Compression;
        c.SetLogicalSize(Max<ui32>());
        reject(c);
        c = encoded.Compression;
        c.SetBlockSize(65536);
        reject(c);
        c = encoded.Compression;
        c.ClearChunkSizes();
        reject(c);
        c = encoded.Compression;
        c.SetChunkSizes(0, 0);
        reject(c);
        c = encoded.Compression;
        c.SetChunkSizes(0, Max<ui32>());
        reject(c);
        c = encoded.Compression;
        c.ClearChunkChecksums();
        reject(c);

        TVector<TCompressedBlobChunk> chunks;
        UNIT_ASSERT(HasError(PlanCompressedBlobRead(
            encoded.Compression,
            encoded.Payload.size(),
            4096,
            {16},
            chunks)));
    }

    Y_UNIT_TEST(ShouldRejectTruncatedAndCorruptedPayload)
    {
        const auto encoded = Encode(TString(65536, 'a'), 4096);
        const auto& c = encoded.Compression;
        TString decoded = "previous result";
        auto payload = encoded.Payload.substr(0, c.GetChunkSizes(0));
        UNIT_ASSERT(HasError(DecodeMergedBlobChunk(
            c, 0, TStringBuf(payload).SubStr(0, payload.size() - 1), decoded)));
        UNIT_ASSERT(decoded.empty());

        // Check every single byte corruption, including changes which remain
        // a syntactically valid LZ4 stream. Logical checksums need not be enabled.
        for (size_t i = 0; i < payload.size(); ++i) {
            auto corrupted = payload;
            corrupted[i] = static_cast<char>(corrupted[i]) ^ 1;
            UNIT_ASSERT_C(
                HasError(DecodeMergedBlobChunk(c, 0, corrupted, decoded)),
                i);
            UNIT_ASSERT(decoded.empty());
        }
        UNIT_ASSERT(HasError(DecodeMergedBlobChunk(c, 2, payload, decoded)));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
