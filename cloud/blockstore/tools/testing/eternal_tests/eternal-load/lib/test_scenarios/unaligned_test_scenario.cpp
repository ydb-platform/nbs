#include "unaligned_test_scenario.h"

#include "test_scenario_base.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/random/random.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TTestScenarioBaseConfig BaseConfig = {
    .DefaultMinReadByteCount = 4_KB,
    .DefaultMaxReadByteCount = 1_MB,
    .DefaultMinWriteByteCount = 4_KB,
    .DefaultMaxWriteByteCount = 1_MB,
    .DefaultMinRegionByteCount = 3_MB,
    .DefaultMaxRegionByteCount = 10_MB,
};

constexpr size_t RegionBlockByteCount = 1_KB;

////////////////////////////////////////////////////////////////////////////////

/**
* Test scenario:
*
* File is split into N non-overlapping regions of various size, each region
* is at least |MinRegionByteCount| bytes and at most |MaxRegionByteCount| bytes.
*
* |IoDepth| workers run concurrently. Each worker performs read and write
* operations in an infinite cycle. The ratio between read and write operations
* is determined by |WriteOperationPercentage| parameter.
*
* Write operation:
* - A worker randomly selects a region that is not currently being written;
* - The region is locked by the worker for writing;
* - The worker updates the region metadata: new sequence number, write time;
* - The worker generates new data for the region and writes it using a series
*   of consecutive write requests of various size (from |MinWriteByteCount| to
*   |MaxWriteByteCount|);
* - The worker updates the region metadata: current state = new state;
* - The region is unlocked.
*
* Read operation:
* - A worker selects a random offset and length (from |MinReadByteCount| to
*   |MaxReadByteCount|) within the data area of the file (may overlap several
*   regions);
* - The worker reads the data using a single read request;
* - The worker checks the data integrity:
*   - For each region that was fully or partially read, the worker checks the
*     region state before and after the read;
*   - If the region state did not change during the read, the data is verified
*     against the expected data pattern;
*   - If the region state changed during the read, but the worker knows all
*     intermediate states (i.e. the region was not written by another worker
*     during the read), the data is verified against all known patterns;
*   - Otherwise, the data is not verified.
*
* Data:
* - Each region is split into blocks of size |RegionBlockByteCount|, each filled
*   with a generated pattern,
* - The values of TRegionState and block offset are used as a seed for the
*   pattern generation.
*/

struct Y_PACKED TFileHeader
{
    ui64 Magic = 0;
    ui64 RegionCount = 0;
    ui32 Crc32 = 0;

    // This is used as a marker that the header is properly initialized
    static constexpr ui64 ExpectedMagic = 0x733c1dfaa5d2a452;
};

// Defines the expected content in the region
struct Y_PACKED TRegionState
{
    // Sequence number, atomically incremented on each write
    // The value is reset on test start - writes from different test runs are
    // distinguished by TestStartTime
    ui64 SeqNum = 0;
    ui64 TestStartTime = 0;
    ui64 LastWriteTime = 0;

    bool operator==(const TRegionState& rhs) const
    {
        return SeqNum == rhs.SeqNum && TestStartTime == rhs.TestStartTime &&
               LastWriteTime == rhs.LastWriteTime;
    }
};

struct Y_PACKED TRegionMetadata
{
    ui64 Offset = 0;
    ui64 ByteCount = 0;
    // CurrentState != NewStats means that the region is being written
    // Since write is not performed atomically, the reader may observe
    // a combination of two states
    TRegionState CurrentState = {};
    TRegionState NewState = {};

    ui64 End() const
    {
        return Offset + ByteCount;
    }
};

// A region is split in blocks of size |RegionBlockByteCount|, each filled with
// a generated pattern
struct Y_PACKED TRegionDataBlock
{
    ui64 SeqNum = 0;
    ui64 Offset = 0;
    ui64 TestStartTime = 0;
    ui64 WriteTime = 0;
    // Data is filled with pseudo-random values generated from the above fields
    // The array size is chosen to make the struct size = |RegionBlockByteCount|
    // (there are 5 ui64 fields besides Data - need to subtract them)
    std::array<ui64, (RegionBlockByteCount / sizeof(ui64)) - 5> Data = {};
    ui64 Crc32 = 0;

    TStringBuf AsStringBuf(size_t len) const
    {
        Y_ABORT_UNLESS(len <= sizeof(TRegionDataBlock));
        return TStringBuf(reinterpret_cast<const char*>(this), len);
    }
};

static_assert(sizeof(TRegionDataBlock) == RegionBlockByteCount);

// Read operation may overlap several regions
struct TReadRegionInfo
{
    size_t Index = 0;
    ui64 OffsetInRegion = 0;
    ui64 OffsetInReadBuffer = 0;
    ui64 Length = 0;
    // Read and write operations are not mutually exclusive.
    // The state of the region may change during the read.
    TRegionMetadata BeforeRead;
    TRegionMetadata AfterRead;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
T RandomNumberFromRange(T min, T max)
{
    Y_ABORT_UNLESS(min <= max);
    return RandomNumber(max - min + 1) + min;
}

ui64 UpdateSeed(ui64 seed, ui64 nonce)
{
    return (seed * 397) + (nonce * 31) + 13;
}

ui64 NextValue(ui64* seed)
{
    *seed = UpdateSeed(*seed, 0);
    return *seed;
}

void GenerateRegionData(
    const TRegionState& state,
    ui64 offset,
    TRegionDataBlock* regionData)
{
    regionData->SeqNum = state.SeqNum;
    regionData->Offset = offset;
    regionData->TestStartTime = state.TestStartTime;
    regionData->WriteTime = state.LastWriteTime;

    ui64 seed = UpdateSeed(0, regionData->SeqNum);
    seed = UpdateSeed(seed, regionData->Offset);
    seed = UpdateSeed(seed, regionData->TestStartTime);
    seed = UpdateSeed(seed, regionData->WriteTime);

    for (auto& v: regionData->Data) {
        v = NextValue(&seed);
    }

    regionData->Crc32 = Crc32c(regionData, offsetof(TRegionDataBlock, Crc32));
}

// Alignment can be non-power-of-two in this test scenario
ui64 AlignUp(ui64 value, ui64 alignment)
{
    return (value + alignment - 1) / alignment * alignment;
}

////////////////////////////////////////////////////////////////////////////////

class TUnalignedTestScenario: public TTestScenarioBase
{
private:
    class TTestWorker;

    using IService = NTesting::ITestExecutorIOService;

    TMutex Lock;
    TVector<TRegionMetadata> RegionMetadata;
    // Workers are not allowed to concurrently write to the same region.
    // When a worker starts writing to a region, it sets the corresponding flag
    // to true. When the write is complete, the flag is reset to false.
    TVector<bool> RegionLockedForWriteFlags;
    std::atomic<ui64> NextSeqNum = 0;
    ui64 FileSize = 0;

    std::atomic<ui64> ValidationOffset = 0;
    std::atomic<ui64> ValidatedByteCount = 0;
    bool ShouldValidate = false;

public:
    TUnalignedTestScenario(IConfigHolderPtr configHolder, const TLog& log);

private:
    ui64 GetNextRegionByteCount(ui64 remainingFileSize) const;
    bool GenerateRegionMetadata();
    bool ValidateRegionMetadata() const;
    bool Init(TFileHandle& file) override;
    bool InitialValidationInProgress() const;

    void Read(IService& service, TVector<char>& readBuffer);
    void Read(
        IService& service,
        ui64 offset,
        ui64 length,
        bool isInitialValidation,
        TVector<char>& readBuffer);
    TVector<TReadRegionInfo> GetReadRegions(ui64 offset, ui64 length) const;
    void UpdateReadRegions(TVector<TReadRegionInfo>& regions) const;
    void ValidateReadData(
        IService& service,
        TStringBuf readBuffer,
        const TVector<TReadRegionInfo>& regions) const;
    void ValidateReadDataRegion(
        IService& service,
        TStringBuf readBuffer,
        const TVector<TRegionState>& expectedStates,
        size_t regionIndex,
        ui64 offsetInRegion) const;
    bool ValidateReadDataFragment(
        TStringBuf readBuffer,
        const TVector<TRegionDataBlock>& expectedData) const;

    size_t WriteBegin(IService& service);
    void WriteRegionData(
        IService& service,
        size_t index,
        TVector<char>& writeBuffer);
    void WriteEnd(IService& service, size_t index);
    size_t AcquireRandomRegion();
    void ReleaseRegion(size_t index);
};

////////////////////////////////////////////////////////////////////////////////

enum class EOperation
{
    Idle,
    Read,
    // Find a region to write and lock it; write updated region metadata
    WriteBegin,
    // Generate and write region data (multiple write requests)
    WriteRegionData,
    // Write updated region metadata; unlock the region
    WriteEnd
};

class TUnalignedTestScenario::TTestWorker: public ITestScenarioWorker
{
private:
    TUnalignedTestScenario* TestScenario = nullptr;
    TVector<char> ReadBuffer;
    TVector<char> WriteBuffer;
    EOperation Operation = EOperation::Idle;
    size_t WriteRegionIndex = 0;
    TLog Log;

public:
    TTestWorker(TUnalignedTestScenario* testScenario, const TLog& log)
        : TestScenario(testScenario)
        , ReadBuffer(testScenario->MaxReadByteCount)
        , WriteBuffer(testScenario->MaxRegionByteCount)
        , Log(log)
    {}

    void Run(
        double secondsSinceTestStart,
        ITestExecutorIOService& service) override
    {
        if (Operation == EOperation::Idle) {
            if (TestScenario->InitialValidationInProgress()) {
                Operation = EOperation::Read;
            } else {
                const auto writeRate = TestScenario->GetWriteProbabilityPercent(
                    secondsSinceTestStart);
                if (RandomNumber(100u) >= writeRate) {
                    Operation = EOperation::Read;
                } else {
                    Operation = EOperation::WriteBegin;
                }
            }
        }

        switch (Operation)
        {
            case EOperation::Read:
                TestScenario->Read(service, ReadBuffer);
                Operation = EOperation::Idle;
                break;

            case EOperation::WriteBegin:
                WriteRegionIndex = TestScenario->WriteBegin(service);
                Operation = EOperation::WriteRegionData;
                break;

            case EOperation::WriteRegionData:
                TestScenario->WriteRegionData(
                    service,
                    WriteRegionIndex,
                    WriteBuffer);
                Operation = EOperation::WriteEnd;
                break;

            case EOperation::WriteEnd:
                TestScenario->WriteEnd(service, WriteRegionIndex);
                Operation = EOperation::Idle;
                break;

            default:
                Y_ABORT("Invalid operation %d", Operation);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

#define INIT_CONFIG_PARAMS_HELPER(config, x) \
    if ((config).GetMin##x() > 0) { \
        Min##x = (config).GetMin##x(); \
    } \
    if ((config).GetMax##x() > 0) { \
        Max##x = (config).GetMax##x(); \
    } \
    Y_ENSURE(Min##x <= Max##x, \
        "Invalid configuration: Min" #x " (" << Min##x << ") > Max" #x \
        " (" << Max##x << ")"); \
    (config).SetMin##x(Min##x); \
    (config).SetMax##x(Max##x); \

TUnalignedTestScenario::TUnalignedTestScenario(
        IConfigHolderPtr configHolder,
        const TLog& log)
    : TTestScenarioBase(BaseConfig, std::move(configHolder), log)
{
    auto& config = ConfigHolder->GetConfig();
    for (ui32 i = 0; i < config.GetIoDepth(); ++i) {
        AddWorker(std::make_unique<TTestWorker>(this, log));
    }
}

#undef INIT_CONFIG_PARAMS_HELPER

ui64 TUnalignedTestScenario::GetNextRegionByteCount(
    ui64 remainingFileSize) const
{
    if (remainingFileSize <= MaxRegionByteCount) {
        return remainingFileSize;
    }

    // We want to ensure that the remaining size will be enough to fit at
    // least one region
    ui64 maxRegionByteCount = Min(
        MaxRegionByteCount,
        remainingFileSize - MinRegionByteCount - sizeof(TRegionMetadata)
    );

    Y_ABORT_UNLESS(MinRegionByteCount <= maxRegionByteCount);

    return RandomNumberFromRange(MinRegionByteCount, maxRegionByteCount);
}

bool TUnalignedTestScenario::GenerateRegionMetadata()
{
    // File structure:
    // - TFileHeader
    // - TRegionMetadata (xN)
    // - TRegion (xN) - variable size

    auto size = FileSize;
    auto minSize =
        sizeof(TFileHeader) + sizeof(TRegionMetadata) + MinRegionByteCount;

    if (size < minSize) {
        STORAGE_ERROR(
            "File size " << size << " is less than the minimal allowed size "
                         << minSize);
        return false;
    }

    size -= sizeof(TFileHeader);

    while (size > 0) {
        size -= sizeof(TRegionMetadata);

        TRegionMetadata metadata;
        metadata.ByteCount = GetNextRegionByteCount(size);
        Y_ABORT_UNLESS(metadata.ByteCount <= size);
        size -= metadata.ByteCount;

        RegionMetadata.push_back(metadata);
    }

    ui64 offset =
        sizeof(TFileHeader) + (sizeof(TRegionMetadata) * RegionMetadata.size());

    for (auto& metadata: RegionMetadata) {
        metadata.Offset = offset;
        offset += metadata.ByteCount;
    }

    return true;
}

bool TUnalignedTestScenario::ValidateRegionMetadata() const
{
    ui64 offset =
        sizeof(TFileHeader) + RegionMetadata.size() * sizeof(TRegionMetadata);

    for (size_t i = 0; i < RegionMetadata.size(); i++) {
        const auto& metadata = RegionMetadata[i];
        if (metadata.Offset != offset) {
            STORAGE_ERROR(
                "File format error: region #"
                << i << " is not contiguous (expected offset " << offset
                << ", found " << metadata.Offset << ")");
            return false;
        }
        if (metadata.Offset > FileSize ||
            metadata.ByteCount > FileSize - metadata.Offset)
        {
            STORAGE_ERROR(
                "File format error: region #"
                << i << " points outside the file (offset " << metadata.Offset
                << ", size " << metadata.ByteCount << ", file size " << FileSize
                << ")");
            return false;
        }
        offset += metadata.ByteCount;
    }

    return true;
}

bool TUnalignedTestScenario::Init(TFileHandle& file)
{
    FileSize = static_cast<ui64>(file.GetLength());

    TFileHeader header;
    if (file.Read(&header, sizeof(TFileHeader)) != sizeof(TFileHeader)) {
        STORAGE_ERROR("Cannot read file header");
        return false;
    }

    if (header.Magic == TFileHeader::ExpectedMagic) {
        auto actualCrc32 = Crc32c(&header, offsetof(TFileHeader, Crc32));
        if (header.Crc32 != actualCrc32) {
            STORAGE_ERROR("Header CRC mismatch");
            return false;
        }

        RegionMetadata.resize(static_cast<size_t>(header.RegionCount));
        for (auto& metadata: RegionMetadata) {
            if (file.Read(&metadata, sizeof(TRegionMetadata)) !=
                sizeof(TRegionMetadata))
            {
                STORAGE_ERROR("Cannot read test metadata");
                return false;
            }
        }
    } else {
        if (!GenerateRegionMetadata()) {
            return false;
        }

        file.Seek(0, SeekDir::sSet);

        header.Magic = TFileHeader::ExpectedMagic;
        header.RegionCount = RegionMetadata.size();
        header.Crc32 = Crc32c(&header, offsetof(TFileHeader, Crc32));
        if (file.Write(&header, sizeof(TFileHeader)) != sizeof(TFileHeader)) {
            STORAGE_ERROR("Cannot write file header");
            return false;
        }

        for (const auto& metadata: RegionMetadata) {
            if (file.Write(&metadata, sizeof(TRegionMetadata)) !=
                sizeof(TRegionMetadata))
            {
                STORAGE_ERROR("Cannot write test metadata");
                return false;
            }
        }
        file.Flush();
    }

    if (GetWorkerCount() > RegionMetadata.size()) {
        STORAGE_ERROR(
            "The number of workers "
            << GetWorkerCount()
            << " is greater than the number of regions in the file "
            << RegionMetadata.size());
        return false;
    }

    RegionLockedForWriteFlags = TVector<bool>(RegionMetadata.size(), false);

    STORAGE_INFO("File format: " << RegionMetadata.size() << " regions");

    for (const auto& metadata: RegionMetadata) {
        if (metadata.NewState.SeqNum != 0) {
            STORAGE_INFO(
                "Test file contains written data and will be fully validated "
                "before writing new data");
            ShouldValidate = true;
            break;
        }
    }

    return ValidateRegionMetadata();
}

bool TUnalignedTestScenario::InitialValidationInProgress() const
{
    return ShouldValidate && ValidatedByteCount.load() < FileSize;
}

void TUnalignedTestScenario::Read(
    ITestExecutorIOService& service,
    TVector<char>& readBuffer)
{
    auto len =
        Min(RandomNumberFromRange(MinReadByteCount, MaxReadByteCount),
            readBuffer.size(),
            FileSize);

    if (ShouldValidate) {
        auto offset = ValidationOffset.fetch_add(len);
        if (offset == 0) {
            STORAGE_INFO("Starting sequential read validation");
        }
        if (offset < FileSize) {
            len = Min(len, FileSize - offset);
            Read(service, offset, len, true, readBuffer);
            return;
        }
    }

    auto randomOffset = RandomNumber(FileSize - len + 1);

    Read(service, randomOffset, len, false, readBuffer);
}

void TUnalignedTestScenario::Read(
    IService& service,
    ui64 offset,
    ui64 length,
    bool isInitialValidation,
    TVector<char>& readBuffer)
{
    auto regions = GetReadRegions(offset, length);
    auto buffer = TStringBuf(readBuffer.data(), length);

    service.Read(
        readBuffer.begin(),
        length,
        offset,
        [this,
         &service,
         regions = std::move(regions),
         buffer,
         isInitialValidation]() mutable
        {
            UpdateReadRegions(regions);
            ValidateReadData(service, buffer, regions);
            if (isInitialValidation) {
                auto prev = ValidatedByteCount.fetch_add(buffer.size());
                Y_ABORT_UNLESS(prev < FileSize);
                Y_ABORT_UNLESS(prev + buffer.size() <= FileSize);
                if (prev + buffer.size() == FileSize) {
                    ShouldValidate = false;
                    STORAGE_INFO("Finished sequential read validation");
                }
            }
        });
}

TVector<TReadRegionInfo> TUnalignedTestScenario::GetReadRegions(
    ui64 offset,
    ui64 length) const
{
    auto guard = Guard(Lock);

    TVector<TReadRegionInfo> res;

    for (size_t i = 0; i < RegionMetadata.size(); i++) {
        const auto& regionMetadata = RegionMetadata[i];
        const auto begin = Max(offset, regionMetadata.Offset);
        const auto end = Min(offset + length, regionMetadata.End());
        if (begin < end) {
            res.push_back(
                {.Index = i,
                 .OffsetInRegion = begin - regionMetadata.Offset,
                 .OffsetInReadBuffer = begin - offset,
                 .Length = end - begin,
                 .BeforeRead = regionMetadata,
                 .AfterRead = {}});
        }
    }

    return res;
}

void TUnalignedTestScenario::UpdateReadRegions(
    TVector<TReadRegionInfo>& regions) const
{
    auto guard = Guard(Lock);

    for (auto& region: regions) {
        region.AfterRead = RegionMetadata[region.Index];
    }
}

void TUnalignedTestScenario::ValidateReadData(
    IService& service,
    TStringBuf readBuffer,
    const TVector<TReadRegionInfo>& regions) const
{
    for (const auto& region: regions) {
        auto readBufferFragment =
            readBuffer.SubStr(region.OffsetInReadBuffer, region.Length);

        if (region.BeforeRead.CurrentState.SeqNum == 0) {
            // The block has not been written yet - skip
            continue;
        }

        if (region.AfterRead.CurrentState != region.BeforeRead.CurrentState &&
            region.AfterRead.CurrentState != region.BeforeRead.NewState)
        {
            // We can validate the data only if we know about all writes that
            // happened during the read
            continue;
        }

        TVector<TRegionState> expectedStates;
        expectedStates.push_back(region.BeforeRead.CurrentState);

        if (region.BeforeRead.NewState != region.BeforeRead.CurrentState) {
            expectedStates.push_back(region.BeforeRead.NewState);
        }

        if (region.AfterRead.NewState != region.BeforeRead.NewState &&
            region.AfterRead.NewState != region.BeforeRead.CurrentState)
        {
            expectedStates.push_back(region.AfterRead.NewState);
        }

        ValidateReadDataRegion(
            service,
            readBufferFragment,
            expectedStates,
            region.Index,
            region.OffsetInRegion);
    }
}

void TUnalignedTestScenario::ValidateReadDataRegion(
    IService& service,
    TStringBuf readBuffer,
    const TVector<TRegionState>& expectedStates,
    size_t regionIndex,
    ui64 offsetInRegion) const
{
    if (expectedStates.empty()) {
        return;
    }

    TVector<TRegionDataBlock> expectedBuffers(expectedStates.size());

    // Region data is split into blocks of size |RegionBlockByteCount|
    // Skip partial block at the beginning
    ui64 offset =
        AlignUp(offsetInRegion, sizeof(TRegionDataBlock)) - offsetInRegion;

    while (offset < readBuffer.size()) {
        for (size_t i = 0; i < expectedStates.size(); i++) {
            GenerateRegionData(
                expectedStates[i],
                offsetInRegion + offset,
                &expectedBuffers[i]);
        }

        auto fragment = readBuffer.SubStr(offset, sizeof(TRegionDataBlock));

        if (ValidateReadDataFragment(fragment, expectedBuffers)) {
            offset += sizeof(TRegionDataBlock);
            continue;
        }

        const auto offsetInFile =
            RegionMetadata[regionIndex].Offset + offsetInRegion + offset;

        TStringBuilder sb;
        sb << "Read validation failed";
        sb << "\nWrong data at file range [" << offsetInFile << ", "
           << offsetInFile + fragment.size() << "], Region: " << regionIndex
           << ", OffsetInRegion: " << offsetInRegion + offset;

        for (size_t i = 0; i < expectedStates.size(); i++) {
            sb << "\nExpected pattern #" << (i + 1) << ": ("
               << expectedStates[i].SeqNum << ", "
               << expectedStates[i].TestStartTime << ", "
               << expectedStates[i].LastWriteTime << ")\n  "
               << HexText(expectedBuffers[i].AsStringBuf(fragment.size()));
        }

        sb << "\nActual read data:";
        if (fragment.size() >= offsetof(TRegionDataBlock, Data)) {
            TRegionDataBlock regionData;
            MemCopy(
                reinterpret_cast<char*>(&regionData),
                fragment.data(),
                fragment.size());
            sb << " ("
               << regionData.SeqNum << ", "
               << regionData.TestStartTime << ", "
               << regionData.WriteTime << ")";
        }
        sb << "\n  " << HexText(fragment);

        service.Fail(sb);
        break;
    }
}

bool TUnalignedTestScenario::ValidateReadDataFragment(
    TStringBuf readBuffer,
    const TVector<TRegionDataBlock>& expectedData) const
{
    // Optimization: compare as a whole first
    for (const auto& expected: expectedData) {
        if (readBuffer == expected.AsStringBuf(readBuffer.size())) {
            return true;
        }
    }

    if (expectedData.size() == 1) {
        return false;
    }

    // Compare against each buffer per byte
    TVector<TStringBuf> expectedBuffers;
    for (const auto& expected: expectedData) {
        expectedBuffers.push_back(expected.AsStringBuf(readBuffer.size()));
    }

    for (size_t i = 0; i < readBuffer.size(); i++) {
        bool match = false;
        for (const auto& expected: expectedBuffers) {
            if (readBuffer[i] == expected[i]) {
                match = true;
                break;
            }
        }
        if (!match) {
            return false;
        }
    }

    return true;
}

size_t TUnalignedTestScenario::WriteBegin(IService& service)
{
    auto guard = Guard(Lock);
    size_t index = 0;

    while (true) {
        // The number of workers is guaranteed to be not greater than the
        // number of regions, so this loop will eventually terminate
        index = RandomNumber(RegionMetadata.size());
        if (!RegionLockedForWriteFlags[index]) {
            RegionLockedForWriteFlags[index] = true;
            break;
        }
    }

    auto& metadata = RegionMetadata[index];
    if (metadata.NewState == metadata.CurrentState) {
        metadata.NewState = {
            .SeqNum = NextSeqNum++,
            .TestStartTime = TestStartTime.GetValue(),
            .LastWriteTime = Now().GetValue()
        };
    } else {
        STORAGE_DEBUG(
            "Writing to region #"
            << index << " was interrupted in the previous test run, restoring");
    }

    guard.Release();

    auto metadataOffset = sizeof(TFileHeader) + index * sizeof(TRegionMetadata);

    service.Write(
        &metadata.NewState,
        sizeof(metadata.NewState),
        metadataOffset + offsetof(TRegionMetadata, NewState),
        []() {});

    return index;
}

void TUnalignedTestScenario::WriteRegionData(
    IService& service,
    size_t index,
    TVector<char>& writeBuffer)
{
    const auto& metadata = RegionMetadata[index];

    Y_ABORT_UNLESS(writeBuffer.size() >= metadata.ByteCount);

    TRegionDataBlock regionData;
    ui64 offset = 0;

    while (offset < metadata.ByteCount) {
        GenerateRegionData(metadata.NewState, offset, &regionData);

        auto len = Min(metadata.ByteCount - offset, sizeof(TRegionDataBlock));

        MemCopy(
            writeBuffer.begin() + offset,
            reinterpret_cast<const char*>(&regionData),
            len);

        offset += len;
    }

    ui64 ofs = 0;
    while (ofs < metadata.ByteCount) {
        auto len =
            Min(RandomNumberFromRange(MinWriteByteCount, MaxWriteByteCount),
                metadata.ByteCount - ofs);

        service.Write(
            writeBuffer.begin() + ofs,
            len,
            metadata.Offset + ofs,
            []() {});
        ofs += len;
    }
}

void TUnalignedTestScenario::WriteEnd(IService& service, size_t index)
{
    const auto& metadata = RegionMetadata[index];
    auto metadataOffset = sizeof(TFileHeader) + index * sizeof(TRegionMetadata);

    service.Write(
        &metadata.NewState,
        sizeof(metadata.NewState),
        metadataOffset + offsetof(TRegionMetadata, CurrentState),
        [this, index]()
        {
            auto guard = Guard(Lock);
            Y_ABORT_UNLESS(RegionLockedForWriteFlags[index]);
            RegionLockedForWriteFlags[index] = false;
            RegionMetadata[index].CurrentState = RegionMetadata[index].NewState;
        });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestScenarioPtr CreateUnalignedTestScenario(
    IConfigHolderPtr configHolder,
    const TLog& log)
{
    return ITestScenarioPtr(
        new TUnalignedTestScenario(std::move(configHolder), log));
}

}   // namespace NCloud::NBlockStore::NTesting
