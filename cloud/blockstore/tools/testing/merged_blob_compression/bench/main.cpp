#include <cloud/blockstore/libs/storage/partition/model/merged_blob_compression.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <contrib/libs/lz4/lz4.h>
#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/zstd/include/zstd.h>
#include <contrib/libs/fastlz/fastlz.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>
#include <contrib/ydb/core/base/logoblob.h>
#include <google/protobuf/util/json_util.h>
#include <ctime>
#include <map>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace NCloud::NBlockStore;
using namespace NCloud::NBlockStore::NStorage::NPartition;
namespace {
constexpr size_t BlobSize = 4 * 1024 * 1024;
constexpr size_t BlockSize = 4096;
using TClock = std::chrono::steady_clock;
struct TBlob {
    TString Raw;
    TCompressedMergedBlob Encoded;
};
void Require(bool ok, const char* text) {
    if (!ok) throw std::runtime_error(text);
}
ui64 Nanos(TClock::time_point start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        TClock::now() - start).count();
}
ui64 CpuNanos() {
    timespec ts{};
    Require(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0, "thread CPU clock");
    return ui64(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}
NCloud::NBlockStore::NProto::TBlobMeta Meta(size_t bytes) {
    NCloud::NBlockStore::NProto::TBlobMeta m;
    m.MutableMergedBlocks()->SetStart(0);
    m.MutableMergedBlocks()->SetEnd(bytes / BlockSize - 1);
    return m;
}
// Experimental codec/chunk combinations never pass the v1 format validator
// and must never be written to NBS. Protobuf framing and both-copy cost are
// identical; the deployed candidate calls the production helper below.
TCompressedMergedBlob Encode(TStringBuf raw, const std::string& codec, size_t chunk) {
    TCompressedMergedBlob out;
    auto meta = Meta(raw.size());
    if (codec == "lz4" && chunk == MergedBlobCompressionChunkSize) {
        Require(!NCloud::HasError(CompressMergedBlob(raw, BlockSize, 10, meta, out)),
                "production compression failed");
        return out;
    }
    auto& d = out.Compression;
    d.SetVersion(1); d.SetCodec(1); d.SetLogicalSize(raw.size());
    d.SetBlockSize(BlockSize); d.SetChunkSize(chunk);
    for (size_t offset = 0; offset < raw.size(); offset += chunk) {
        size_t size = std::min(chunk, raw.size() - offset);
        TString encoded = TString::Uninitialized(size * 2 + 1024);
        size_t n = 0;
        if (codec == "lz4") {
            int r = LZ4_compress_default(raw.data() + offset, encoded.begin(),
                                        size, encoded.size());
            Require(r > 0, "lz4 encode"); n = r;
        } else if (codec == "snappy") {
            snappy::RawCompress(raw.data() + offset, size, encoded.begin(), &n);
        } else if (codec == "fastlz") {
            int r = fastlz_compress(raw.data() + offset, size, encoded.begin());
            Require(r > 0, "fastlz encode"); n = r;
        } else {
            int level = codec == "zstd-fast" ? -1 : codec == "zstd-1" ? 1 : 3;
            n = ZSTD_compress(encoded.begin(), encoded.size(),
                              raw.data() + offset, size, level);
            Require(!ZSTD_isError(n), "zstd encode");
        }
        encoded.resize(n);
        d.AddChunkSizes(n);
        d.AddChunkChecksums(ComputeDefaultDigest({encoded.data(), n}));
        out.Payload.append(encoded);
    }
    auto encodedMeta = meta;
    *encodedMeta.MutableCompression() = d;
    out.MetadataBytes = d.ByteSizeLong() + encodedMeta.ByteSizeLong() - meta.ByteSizeLong();
    if ((out.Payload.size() + out.MetadataBytes) * 100 > raw.size() * 90 ||
        out.Payload.size() >= raw.size()) return {};
    return out;
}
void Decode(const TCompressedMergedBlob& blob, size_t index, TStringBuf payload,
            const std::string& codec, TString& out) {
    const auto& d = blob.Compression;
    if (codec == "lz4" && d.GetChunkSize() == MergedBlobCompressionChunkSize) {
        Require(!NCloud::HasError(DecodeMergedBlobChunk(d, index, payload, out)),
                "production decode failed");
        return;
    }
    Require(ComputeDefaultDigest({payload.data(), payload.size()}) ==
                d.GetChunkChecksums(index), "chunk checksum");
    size_t size = std::min<size_t>(d.GetChunkSize(),
                         d.GetLogicalSize() - index * d.GetChunkSize());
    out = TString::Uninitialized(size);
    if (codec == "lz4") {
        Require(LZ4_decompress_safe(payload.data(), out.begin(), payload.size(), size) ==
                    int(size), "lz4 decode");
    } else if (codec == "snappy") {
        size_t n = 0;
        Require(snappy::GetUncompressedLength(payload.data(), payload.size(), &n) &&
                    n == size &&
                    snappy::RawUncompress(payload.data(), payload.size(), out.begin()),
                "snappy decode");
    } else if (codec == "fastlz") {
        Require(fastlz_decompress(payload.data(), payload.size(), out.begin(), size) ==
                    int(size), "fastlz decode");
    } else {
        Require(ZSTD_decompress(out.begin(), size, payload.data(), payload.size()) == size,
                "zstd decode");
    }
}
struct TRead { size_t Offset; size_t Size; };
void Read(const std::vector<TBlob>& blobs, const TRead& r, const std::string& codec,
          ui64& physical, ui64& chunks) {
    TString output = TString::Uninitialized(r.Size);
    size_t copied = 0;
    while (copied < r.Size) {
        size_t global = r.Offset + copied;
        const auto& b = blobs.at(global / BlobSize);
        size_t start = global % BlobSize;
        size_t bytes = std::min(r.Size - copied, b.Raw.size() - start);
        Require(bytes != 0, "empty blob segment");
        if (b.Encoded.Payload.empty()) {
            std::memcpy(output.begin() + copied, b.Raw.data() + start, bytes);
            physical += bytes;
        } else {
            const auto& d = b.Encoded.Compression;
            TVector<TCompressedBlobChunk> plan;
            if (codec == "lz4" && d.GetChunkSize() == MergedBlobCompressionChunkSize) {
                TVector<ui16> offsets;
                for (size_t i = start / BlockSize; i < (start + bytes) / BlockSize; ++i)
                    offsets.push_back(i);
                Require(!NCloud::HasError(PlanCompressedBlobRead(
                    d, b.Encoded.Payload.size(), BlockSize, offsets, plan)), "read plan");
            } else {
                ui32 offset = 0;
                for (ui32 i = 0; i < d.ChunkSizesSize(); ++i) {
                    if (size_t(i) * d.GetChunkSize() < start + bytes &&
                        size_t(i + 1) * d.GetChunkSize() > start)
                        plan.push_back({i, offset, d.GetChunkSizes(i)});
                    offset += d.GetChunkSizes(i);
                }
            }
            for (const auto& c: plan) {
                TString decoded;
                // Copy simulates receipt of exactly the requested encoded fragment.
                TString encoded(b.Encoded.Payload.data() + c.Offset, c.Size);
                Decode(b.Encoded, c.Index, encoded, codec, decoded);
                size_t begin = std::max(start, size_t(c.Index) * d.GetChunkSize());
                size_t end = std::min(start + bytes,
                                     size_t(c.Index) * d.GetChunkSize() + decoded.size());
                std::memcpy(output.begin() + copied + begin - start,
                            decoded.data() + begin - size_t(c.Index) * d.GetChunkSize(),
                            end - begin);
                physical += c.Size; ++chunks;
            }
        }
        Require(std::memcmp(output.data() + copied, b.Raw.data() + start, bytes) == 0,
                "read data mismatch");
        copied += bytes;
    }
}
struct TInventory {
    ui64 LogicalBytes = 0;
    ui64 PayloadBytes = 0;
    ui64 MetadataBytes = 0;
    ui64 CompressedLogicalBytes = 0;
    ui64 CompressedBlobs = 0;
    ui64 RawBlobs = 0;
    ui64 DuplicatePieces = 0;
};
TInventory Inventory(const NProto::TDescribeBlocksResponse& response, ui32 blockSize) {
    Require(!NCloud::HasError(response.GetError()), "DescribeBlocks failed");
    Require(response.HasBlobFormatVersion() && response.GetBlobFormatVersion() == 1,
            "DescribeBlocks capability acknowledgement required");
    Require(blockSize != 0, "block size required");
    Require(response.FreshBlockRangesSize() == 0, "drain Fresh blocks before inventory");
    std::map<std::string, std::string> seen;
    TInventory result;
    for (const auto& piece: response.GetBlobPieces()) {
        const auto& proto = piece.GetBlobId();
        Require(proto.HasRawX1() && proto.HasRawX2() && proto.HasRawX3(), "incomplete BlobId");
        const auto id = NKikimr::LogoBlobIDFromLogoBlobID(proto);
        const ui64 physical = id.BlobSize();
        Require(physical != 0 && piece.RangesSize() != 0, "empty blob or ranges");
        ui64 logical = physical;
        ui64 metadata = 0;
        if (piece.HasCompression()) {
            Require(!NCloud::HasError(ValidateMergedBlobCompression(
                piece.GetCompression(), physical, blockSize)), "invalid descriptor");
            logical = piece.GetCompression().GetLogicalSize();
            Require(ui64(piece.GetLogicalBlocks()) * blockSize == logical, "logical size mismatch");
            NProto::TBlobMeta meta;
            *meta.MutableCompression() = piece.GetCompression();
            metadata = piece.GetCompression().ByteSizeLong() + meta.ByteSizeLong();
        } else {
            Require(physical % blockSize == 0, "raw blob alignment");
            Require(!piece.GetLogicalBlocks() ||
                    ui64(piece.GetLogicalBlocks()) * blockSize == physical,
                    "missing descriptor or raw size mismatch");
        }
        for (const auto& range: piece.GetRanges()) {
            Require(range.GetBlocksCount() &&
                    (ui64(range.GetBlobOffset()) + range.GetBlocksCount()) * blockSize <= logical,
                    "range outside logical blob");
        }
        auto identity = piece;
        identity.ClearRanges();
        const TString text = id.ToString();
        const std::string key(text.data(), text.size());
        const std::string value = identity.SerializeAsString();
        auto [it, inserted] = seen.emplace(key, value);
        if (!inserted) {
            Require(it->second == value, "conflicting duplicate blob descriptors");
            ++result.DuplicatePieces;
            continue;
        }
        result.LogicalBytes += logical;
        result.PayloadBytes += physical;
        result.MetadataBytes += metadata;
        if (piece.HasCompression()) {
            ++result.CompressedBlobs;
            result.CompressedLogicalBytes += logical;
        } else {
            ++result.RawBlobs;
        }
    }
    return result;
}
void PrintInventory(const TInventory& x) {
    std::cout << "{\"scope\":\"unique live full blobs referenced by DescribeBlocks; excludes unreferenced checkpoint and garbage blobs\","
        << "\"logical_bytes\":" << x.LogicalBytes
        << ",\"payload_bytes\":" << x.PayloadBytes
        << ",\"metadata_bytes\":" << x.MetadataBytes
        << ",\"compressed_logical_bytes\":" << x.CompressedLogicalBytes
        << ",\"compressed_blobs\":" << x.CompressedBlobs
        << ",\"raw_blobs\":" << x.RawBlobs
        << ",\"duplicate_pieces\":" << x.DuplicatePieces << "}\n";
}
void CheckInventory() {
    TString raw(4 * 1024 * 1024, 'x');
    auto encoded = Encode(raw, "lz4", 32768);
    NProto::TDescribeBlocksResponse response;
    response.SetBlobFormatVersion(1);
    auto* piece = response.AddBlobPieces();
    NKikimr::LogoBlobIDFromLogoBlobID(
        NKikimr::TLogoBlobID(42, 1, 1, 3, encoded.Payload.size(), 0), piece->MutableBlobId());
    *piece->MutableCompression() = encoded.Compression;
    piece->SetLogicalBlocks(raw.size() / BlockSize);
    piece->AddRanges()->SetBlocksCount(raw.size() / BlockSize);
    auto good = Inventory(response, BlockSize);
    Require(good.LogicalBytes == raw.size() && good.PayloadBytes == encoded.Payload.size() &&
            good.MetadataBytes == encoded.MetadataBytes, "inventory accounting mismatch");
    *response.AddBlobPieces() = response.GetBlobPieces(0);
    Require(Inventory(response, BlockSize).DuplicatePieces == 1 &&
            Inventory(response, BlockSize).PayloadBytes == good.PayloadBytes,
            "inventory must deduplicate blobs");
    for (ui32 failure = 0; failure != 5; ++failure) {
        auto bad = response;
        if (failure == 0) bad.ClearBlobFormatVersion();
        if (failure == 1) bad.MutableBlobPieces(0)->ClearCompression();
        if (failure == 2) bad.MutableBlobPieces(0)->MutableCompression()->SetVersion(2);
        if (failure == 3) bad.MutableBlobPieces(1)->SetBSGroupId(123);
        if (failure == 4) bad.MutableBlobPieces(0)->MutableRanges(0)->SetBlocksCount(1025);
        bool rejected = false;
        try { Inventory(bad, BlockSize); } catch (const std::exception&) { rejected = true; }
        Require(rejected, "malformed inventory was accepted");
    }
    std::cout << "inventory self-test: accounting, deduplication and five rejection cases passed\n";
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "--self-test-inventory") {
            CheckInventory();
            return 0;
        }
        if (argc == 4 && std::string(argv[1]) == "--inventory") {
            std::ifstream input(argv[2], std::ios::binary);
            Require(bool(input), "open DescribeBlocks JSON");
            const std::string json((std::istreambuf_iterator<char>(input)), {});
            NProto::TDescribeBlocksResponse response;
            Require(google::protobuf::util::JsonStringToMessage(json, &response).ok(),
                    "invalid DescribeBlocks JSON");
            PrintInventory(Inventory(response, std::stoul(argv[3])));
            return 0;
        }
        Require(argc == 6, "usage: merged-blob-bench CORPUS READS.tsv CODEC CHUNK_BYTES REPEATS");
        const std::string codec = argv[3];
        Require(codec == "lz4" || codec == "snappy" || codec == "fastlz" ||
                codec == "zstd-fast" || codec == "zstd-1" || codec == "zstd-3", "codec");
        size_t chunk = std::stoull(argv[4]), repeats = std::stoull(argv[5]);
        Require(chunk == 16384 || chunk == 32768 || chunk == 65536 ||
                chunk == 131072 || chunk == 262144 || chunk == BlobSize, "chunk");
        Require(repeats > 0, "repeats");
        std::ifstream input(argv[1], std::ios::binary);
        Require(bool(input), "open corpus");
        std::vector<TBlob> blobs;
        for (;;) {
            TString raw = TString::Uninitialized(BlobSize);
            input.read(raw.begin(), raw.size());
            size_t n = input.gcount();
            if (!n) break;
            Require(n % BlockSize == 0, "corpus must contain whole logical blocks");
            raw.resize(n); blobs.push_back({std::move(raw), {}});
        }
        Require(!blobs.empty(), "empty corpus");
        size_t total = (blobs.size() - 1) * BlobSize + blobs.back().Raw.size();
        std::ifstream trace(argv[2]);
        Require(bool(trace), "open reads");
        std::vector<TRead> reads;
        TRead r;
        while (trace >> r.Offset >> r.Size) {
            Require(r.Size && r.Size <= BlobSize && r.Offset % BlockSize == 0 &&
                    r.Size % BlockSize == 0 && r.Offset <= total &&
                    r.Size <= total - r.Offset, "read bounds/alignment");
            reads.push_back(r);
        }
        Require(trace.eof() && !reads.empty(), "invalid/empty read trace");
        std::cout << "kind\trepeat\tindex\tlogical_bytes\tpayload_bytes\tmetadata_bytes"
                     "\taccepted\tchunks\tcpu_ns\twall_ns\n";
        for (size_t pass = 0; pass < repeats; ++pass) {
            for (size_t i = 0; i < blobs.size(); ++i) {
                auto& b = blobs[i];
                auto start = TClock::now(); ui64 cpu = CpuNanos();
                b.Encoded = Encode(b.Raw, codec, chunk);
                cpu = CpuNanos() - cpu; ui64 wall = Nanos(start);
                bool accepted = !b.Encoded.Payload.empty();
                std::cout << "encode\t" << pass << '\t' << i << '\t' << b.Raw.size()
                    << '\t' << (accepted ? b.Encoded.Payload.size() : b.Raw.size())
                    << '\t' << b.Encoded.MetadataBytes << '\t' << accepted
                    << "\t0\t" << cpu << '\t' << wall << '\n';
            }
            for (size_t i = 0; i < reads.size(); ++i) {
                ui64 physical = 0, chunks = 0;
                auto start = TClock::now(); ui64 cpu = CpuNanos();
                Read(blobs, reads[i], codec, physical, chunks);
                cpu = CpuNanos() - cpu; ui64 wall = Nanos(start);
                std::cout << "read\t" << pass << '\t' << i << '\t' << reads[i].Size
                    << '\t' << physical << "\t0\t0\t" << chunks << '\t'
                    << cpu << '\t' << wall << '\n';
            }
        }
        Require(bool(std::cout), "write results");
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n'; return 1;
    }
}
