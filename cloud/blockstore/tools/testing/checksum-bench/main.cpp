#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>

#include <sys/resource.h>
#include <time.h>

#include <algorithm>
#include <cstring>
#include <iostream>

// Component benchmark, not a substitute for a BlobStorage-backed disk test.
// Each invocation measures one mode in a fresh process.
namespace {
volatile ui64 Sink = 0;

double CpuSeconds()
{
    timespec ts{};
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

double WallSeconds()
{
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}
}   // namespace

int main(int argc, char** argv)
{
    if (argc != 6) {
        std::cerr << "usage: checksum-bench MODE read|write REQUEST_BYTES "
                     "WORKING_SET_BYTES SECONDS\n"
                     "MODE: off, prefix, original, scratch, optimized\n";
        return 2;
    }
    const TString mode = argv[1];
    const TString operation = argv[2];
    const size_t requestBytes = FromString<size_t>(argv[3]);
    const size_t workingSetBytes = FromString<size_t>(argv[4]);
    const double seconds = FromString<double>(argv[5]);
    constexpr size_t blockSize = 4096;
    constexpr size_t blocksPerBlob = 1024;
    if ((mode != "off" && mode != "prefix" && mode != "original" &&
         mode != "scratch" && mode != "optimized") ||
        (operation != "read" && operation != "write") || !requestBytes ||
        requestBytes % blockSize || !workingSetBytes ||
        workingSetBytes % requestBytes ||
        requestBytes > blocksPerBlob * blockSize || seconds <= 0)
    {
        return 2;
    }
    const bool enabled = mode != "off";
    const bool reuseMetadata = mode == "optimized";
    const bool reserveChecksums = mode == "optimized" || mode == "scratch";
    TString source = TString::Uninitialized(workingSetBytes);
    ui64 random = 0x4e425336373131ULL;
    for (size_t i = 0; i < source.size(); i += sizeof(random)) {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        memcpy(source.begin() + i, &random, sizeof(random));
    }
    TString destination = TString::Uninitialized(requestBytes);
    const size_t storedBlocks =
        enabled
            ? std::min(workingSetBytes,
                       mode == "prefix" ? size_t{1} << 30 : workingSetBytes) /
                  blockSize
            : 0;
    TVector<ui32> stored(storedBlocks);
    NCloud::NBlockStore::NProto::TBlobMeta meta;
    meta.MutableMergedBlocks()->SetEnd(blocksPerBlob - 1);
    NCloud::NBlockStore::NProto::TBlobMeta noChecksums = meta;
    if (enabled) {
        for (size_t i = 0; i < blocksPerBlob; ++i) {
            meta.AddBlockChecksums(Crc32c(
                source.data() + (i * blockSize) % workingSetBytes, blockSize));
        }
    }
    const TString serialized = meta.SerializeAsString();
    const auto metadataBytes = meta.ByteSizeLong() - noChecksums.ByteSizeLong();
    const auto metadataMemory =
        meta.SpaceUsedLong() - noChecksums.SpaceUsedLong();
    NCloud::NBlockStore::NProto::TBlobMeta parsedMetadata;
    if (!parsedMetadata.ParseFromString(serialized)) {
        return 3;
    }
    NCloud::NBlockStore::NProto::TBlobMeta reservedMetadata = noChecksums;
    reservedMetadata.MutableBlockChecksums()->Reserve(
        meta.BlockChecksumsSize());
    for (ui32 checksum: meta.GetBlockChecksums()) {
        reservedMetadata.AddBlockChecksums(checksum);
    }
    const auto parsedMetadataMemory =
        parsedMetadata.SpaceUsedLong() - noChecksums.SpaceUsedLong();
    const auto reservedMetadataMemory =
        reservedMetadata.SpaceUsedLong() - noChecksums.SpaceUsedLong();
    const double initStart = CpuSeconds();
    for (size_t i = 0; i < stored.size(); ++i) {
        stored[i] = Crc32c(source.data() + i * blockSize, blockSize);
    }
    const double initializationCpu = CpuSeconds() - initStart;
    ui64 processed = 0;
    ui64 checksumCount = 0;
    ui64 metadataParses = 0;
    ui64 digest = 0;
    const double startCpu = CpuSeconds();
    const double startWall = WallSeconds();
    size_t offset = 0;
    do {
        const size_t requestBlocks = requestBytes / blockSize;
        TVector<ui32> checksums;
        if (enabled && reserveChecksums) {
            checksums.reserve(requestBlocks);
        }
        TString scratch;
        if (enabled && mode == "scratch" && operation == "read") {
            scratch = TString::Uninitialized(blockSize);
        }
        for (size_t i = 0; i < requestBlocks; ++i) {
            const size_t block = (offset / blockSize) + i;
            const char* from = source.data() + offset + i * blockSize;
            char* to = destination.begin() + i * blockSize;
            const bool check = block < stored.size();
            if (!check) {
                memcpy(to, from, blockSize);
                continue;
            }
            if (operation == "read") {
                if (!reuseMetadata || i == 0) {
                    NCloud::NBlockStore::NProto::TBlobMeta parsed;
                    if (!parsed.ParseFromString(serialized)) {
                        return 3;
                    }
                    digest += parsed.GetBlockChecksums(block % blocksPerBlob);
                    ++metadataParses;
                }
                if (mode == "optimized") {
                    checksums.push_back(Crc32c(from, blockSize));
                    memcpy(to, from, blockSize);
                } else {
                    if (mode != "scratch") {
                        scratch = TString::Uninitialized(blockSize);
                    }
                    memcpy(scratch.begin(), from, blockSize);
                    checksums.push_back(Crc32c(scratch.data(), blockSize));
                    memcpy(to, scratch.data(), blockSize);
                }
                if (checksums.back() != stored[block]) {
                    return 4;
                }
            } else {
                // Match WriteBlob: first copy caller data into owned storage.
                memcpy(to, from, blockSize);
                checksums.push_back(Crc32c(to, blockSize));
                stored[block] = checksums.back();
            }
            digest += checksums.back();
            ++checksumCount;
        }
        digest += static_cast<unsigned char>(destination[requestBytes - 1]);
        processed += requestBytes;
        // An odd stride visits every block of the power-of-two working sets.
        const size_t stride = requestBytes == blockSize ? 104729 : 1;
        offset = (offset + stride * requestBytes) % workingSetBytes;
        Sink = digest;
    } while (WallSeconds() - startWall < seconds);
    const double cpu = CpuSeconds() - startCpu;
    const double wall = WallSeconds() - startWall;
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cout
        << "mode,operation,request_bytes,working_set_bytes,processed_bytes,"
           "cpu_seconds,wall_seconds,cpu_seconds_per_GB,cores,"
           "checksums,metadata_parses,checksum_array_bytes,"
           "blob_metadata_serialized_bytes,blob_metadata_constructed_bytes,"
           "blob_metadata_parsed_bytes,blob_metadata_reserved_bytes,"
           "initialization_cpu_seconds,maxrss_bytes,fast_crc32c\n";
    std::cout << mode << ',' << operation << ',' << requestBytes << ','
              << workingSetBytes << ',' << processed << ',' << cpu << ','
              << wall << ',' << cpu * 1e9 / processed << ',' << cpu / wall
              << ',' << checksumCount << ',' << metadataParses << ','
              << stored.capacity() * sizeof(ui32) << ',' << metadataBytes << ','
              << metadataMemory << ',' << parsedMetadataMemory << ','
              << reservedMetadataMemory << ',' << initializationCpu << ','
              << usage.ru_maxrss * 1024 << ',' << HaveFastCrc32c() << '\n';
}
