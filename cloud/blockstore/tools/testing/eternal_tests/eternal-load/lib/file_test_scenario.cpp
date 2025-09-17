#include "file_test_scenario.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultMinReadSize = 4_KB;
constexpr ui64 DefaultMaxReadSize = 1_MB;

constexpr ui64 DefaultMinWriteSize = 4_KB;
constexpr ui64 DefaultMaxWriteSize = 1_MB;

constexpr ui64 DefaultMinRegionSize = 3_MB;
constexpr ui64 DefaultMaxRegionSize = 10_MB;

constexpr size_t RegionBlockSize = 1_KB;

////////////////////////////////////////////////////////////////////////////////

/**
* Test scenario:
* - File is split into N non-overlapping regions of various size (about 1-10MiB)
* - There are K threads that randomly overwrite the regions;
* - The same threads perform random read (read may overlap several regions);
* - The beginning of a file contains the metadata table that is updated on each
*   write and is periodically checked.
*/

struct Y_PACKED TFileHeader
{
    ui64 Signature = 0;
    ui64 RegionCount = 0;
    ui32 Crc32 = 0;

    // This is used as a marker that a header is properly initialized
    static constexpr ui64 ExpectedSignature = 0x733c1dfaa5d2a452;
};

struct Y_PACKED TRegionState
{
    ui64 SeqNum = 0;
    ui64 TestStartTime = 0;
    ui64 WriteTime = 0;

    bool operator==(const TRegionState& rhs) const
    {
        return SeqNum == rhs.SeqNum && TestStartTime == rhs.TestStartTime &&
               WriteTime == rhs.WriteTime;
    }
};

struct Y_PACKED TRegionMetadata
{
    ui64 Offset = 0;
    ui64 Size = 0;
    TRegionState CurrentState = {};
    TRegionState NewState = {};

    ui64 End() const
    {
        return Offset + Size;
    }
};

struct Y_PACKED TRegionData
{
    ui64 SeqNum = 0;
    ui64 Offset = 0;
    ui64 TestStartTime = 0;
    ui64 WriteTime = 0;
    std::array<ui64, (RegionBlockSize / sizeof(ui64)) - 5> Data = {};
    ui64 Crc32 = 0;

    TStringBuf AsStringBuf(size_t len) const
    {
        Y_ABORT_UNLESS(len <= sizeof(TRegionData));
        return TStringBuf(reinterpret_cast<const char*>(this), len);
    }
};

struct TReadRegionInfo
{
    size_t Index = 0;
    ui64 OffsetInRegion = 0;
    ui64 OffsetInReadBuffer = 0;
    ui64 Length = 0;
    TRegionMetadata BeforeRead;
    TRegionMetadata AfterRead;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
T RandomNumberFromRange(T min, T max)
{
    Y_ABORT_UNLESS(min <= max);
    return RandomNumber(max - min + 1);
}

ui64 UpdateSeed(ui64 seed, ui64 value)
{
    return (seed * 397) + (value * 31) + 13;
}

void GenerateRegionData(
    const TRegionState& state,
    ui64 offset,
    TRegionData* regionData)
{
    regionData->SeqNum = state.SeqNum;
    regionData->Offset = offset;
    regionData->TestStartTime = state.TestStartTime;
    regionData->WriteTime = state.WriteTime;

    ui64 seed = UpdateSeed(0, regionData->SeqNum);
    seed = UpdateSeed(seed, regionData->Offset);
    seed = UpdateSeed(seed, regionData->TestStartTime);
    seed = UpdateSeed(seed, regionData->WriteTime);

    for (auto& v: regionData->Data) {
        seed = UpdateSeed(seed, 0);
        v = seed;
    }

    regionData->Crc32 = Crc32c(regionData, offsetof(TRegionData, Crc32));
}

////////////////////////////////////////////////////////////////////////////////

class TFileTestScenario: public ITestScenario
{
private:
    class TTestThread;

    using IContext = NTesting::ITestThreadContext;

    IConfigHolderPtr ConfigHolder;
    TLog Log;
    TMutex Lock;
    TVector<std::unique_ptr<ITestThread>> Threads;
    TVector<TRegionMetadata> RegionMetadata;
    TVector<bool> RegionUseFlags;
    std::atomic<ui64> NextSeqNum = 0;
    std::optional<double> PhaseDuration;
    ui32 WriteRate = 0;
    ui64 FileSize = 0;
    const TInstant TestStartTime;

    ui64 MinReadSize = DefaultMinReadSize;
    ui64 MaxReadSize = DefaultMaxReadSize;
    ui64 MinWriteSize = DefaultMinWriteSize;
    ui64 MaxWriteSize = DefaultMaxWriteSize;
    ui64 MinRegionSize = DefaultMinRegionSize;
    ui64 MaxRegionSize = DefaultMaxRegionSize;

public:
    TFileTestScenario(IConfigHolderPtr configHolder, const TLog& log);

    ui32 GetThreadCount() const override
    {
        return static_cast<ui32>(Threads.size());
    }

    ITestThread* GetThread(ui32 index) const override
    {
        return Threads[index].get();
    }

private:
    ui64 GetNextRegionSize(ui64 size) const;
    bool Format();
    bool ValidateFormat() const;
    bool Init(TFileHandle& file) override;
    ui32 GetWriteRate(double secondsSinceTestStart) const;

    void Read(IContext* context, TVector<char>& readBuffer);
    TVector<TReadRegionInfo> GetReadRegions(ui64 offset, ui64 length) const;
    void UpdateReadRegions(TVector<TReadRegionInfo>& regions) const;
    void ValidateReadData(
        IContext* context,
        TStringBuf readBuffer,
        const TVector<TReadRegionInfo>& regions) const;
    void ValidateReadDataRegion(
        IContext* context,
        TStringBuf readBuffer,
        const TVector<TRegionState>& expectedStates,
        ui64 offsetInRegion) const;
    bool ValidateReadDataFragment(
        TStringBuf readBuffer,
        const TVector<TRegionData>& expectedData) const;

    size_t WriteBegin(IContext* context);
    void WriteBody(IContext* context, size_t index, TVector<char>& writeBuffer);
    void WriteEnd(IContext* context, size_t index);
    size_t AcquireRandomRegion();
    void ReleaseRegion(size_t index);
};

////////////////////////////////////////////////////////////////////////////////

enum class EOperation
{
    Idle,
    Read,
    WriteBegin,
    WriteBody,
    WriteEnd
};

class TFileTestScenario::TTestThread: public ITestThread
{
private:
    TFileTestScenario* TestScenario = nullptr;
    TVector<char> ReadBuffer;
    TVector<char> WriteBuffer;
    EOperation Operation = EOperation::Idle;
    size_t WriteRegionIndex = 0;
    TLog Log;

public:
    TTestThread(TFileTestScenario* testScenario, const TLog& log)
        : TestScenario(testScenario)
        , ReadBuffer(testScenario->MaxReadSize)
        , WriteBuffer(testScenario->MaxRegionSize)
        , Log(log)
    {}

    void Run(double secondsSinceTestStart, ITestThreadContext* context) override
    {
        if (Operation == EOperation::Idle) {
            auto writeRate = TestScenario->GetWriteRate(secondsSinceTestStart);
            if (RandomNumber(100u) >= writeRate) {
                Operation = EOperation::Read;
            } else {
                Operation = EOperation::WriteBegin;
            }
        }

        switch (Operation)
        {
            case EOperation::Read:
                TestScenario->Read(context, ReadBuffer);
                Operation = EOperation::Idle;
                break;

            case EOperation::WriteBegin:
                WriteRegionIndex = TestScenario->WriteBegin(context);
                Operation = EOperation::WriteBody;
                break;

            case EOperation::WriteBody:
                TestScenario->WriteBody(
                    context,
                    WriteRegionIndex,
                    WriteBuffer);
                Operation = EOperation::WriteEnd;
                break;

            case EOperation::WriteEnd:
                TestScenario->WriteEnd(context, WriteRegionIndex);
                Operation = EOperation::Idle;
                break;

            default:
                Y_ABORT("Invalid operation %d", Operation);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

#define HELPER(config, x) \
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

TFileTestScenario::TFileTestScenario(
        IConfigHolderPtr configHolder,
        const TLog& log)
    : ConfigHolder(std::move(configHolder))
    , Log(log)
    , TestStartTime(Now())
{
    auto& config = ConfigHolder->GetConfig();
    for (ui32 i = 0; i < config.GetIoDepth(); ++i) {
        Threads.push_back(std::make_unique<TTestThread>(this, log));
    }

    if (config.HasAlternatingPhase()) {
        PhaseDuration =
            TDuration::Parse(config.GetAlternatingPhase()).SecondsFloat();
    }

    WriteRate = config.GetWriteRate();

    auto& fileTestConfig = *config.MutableFileTest();
    HELPER(fileTestConfig, ReadSize);
    HELPER(fileTestConfig, WriteSize);
    HELPER(fileTestConfig, RegionSize);
}

#undef HELPER

ui64 TFileTestScenario::GetNextRegionSize(ui64 size) const
{
    if (size <= MaxRegionSize) {
        return size;
    }

    // We want to ensure that the remaining size will be enough to fit at
    // least one region
    ui64 maxRegionSize = Min(
        MaxRegionSize,
        size - MinRegionSize - sizeof(TRegionMetadata)
    );

    Y_ABORT_UNLESS(MinRegionSize <= maxRegionSize);

    return RandomNumber(maxRegionSize - MinRegionSize + 1) + MinRegionSize;
}

bool TFileTestScenario::Format()
{
    // File structure:
    // - TFileHeader
    // - TRegionMetadata (xN)
    // - TRegion (xN) - variable size

    auto size = FileSize;
    auto minSize =
        sizeof(TFileHeader) + sizeof(TRegionMetadata) + MinRegionSize;

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
        metadata.Size = GetNextRegionSize(size);
        Y_ABORT_UNLESS(metadata.Size <= size);
        size -= metadata.Size;

        RegionMetadata.push_back(metadata);
    }

    ui64 offset =
        sizeof(TFileHeader) + (sizeof(TRegionMetadata) * RegionMetadata.size());

    for (auto& metadata: RegionMetadata) {
        metadata.Offset = offset;
        offset += metadata.Size;
    }

    return true;
}

bool TFileTestScenario::ValidateFormat() const
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
            metadata.Size > FileSize - metadata.Offset)
        {
            STORAGE_ERROR(
                "File format error: region #"
                << i << " points outside the file (offset " << metadata.Offset
                << ", size " << metadata.Size << ", file size " << FileSize
                << ")");
            return false;
        }
        offset += metadata.Size;
    }

    return true;
}

bool TFileTestScenario::Init(TFileHandle& file)
{
    FileSize = static_cast<ui64>(file.GetLength());

    TFileHeader header;
    if (file.Read(&header, sizeof(TFileHeader)) != sizeof(TFileHeader)) {
        STORAGE_ERROR("Cannot read file header");
        return false;
    }

    if (header.Signature == TFileHeader::ExpectedSignature) {
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
        if (!Format()) {
            return false;
        }

        file.Seek(0, SeekDir::sSet);

        header.Signature = TFileHeader::ExpectedSignature;
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
    }

    file.Flush();

    if (GetThreadCount() > RegionMetadata.size()) {
        STORAGE_ERROR(
            "The number of threads "
            << GetThreadCount()
            << " is greater than the number of regions in the file "
            << RegionMetadata.size());
        return false;
    }

    RegionUseFlags = TVector<bool>(RegionMetadata.size(), false);

    STORAGE_INFO("File format: " << RegionMetadata.size() << " regions");

    return ValidateFormat();
}

ui32 TFileTestScenario::GetWriteRate(double secondsSinceTestStart) const
{
    if (PhaseDuration) {
        auto iter = secondsSinceTestStart / PhaseDuration.value();
        return static_cast<ui64>(iter) % 2 == 1 ? 100 - WriteRate : WriteRate;
    }
    return WriteRate;
}

void TFileTestScenario::Read(
    ITestThreadContext* context,
    TVector<char>& readBuffer)
{
    auto dataOffset = RegionMetadata.front().Offset;
    auto dataSize = RegionMetadata.back().End() - dataOffset;

    auto len =
        Min(RandomNumberFromRange(MinReadSize, MaxReadSize),
            readBuffer.size(),
            dataSize);

    auto offset = RandomNumber(dataSize - len + 1) + dataOffset;

    auto regions = GetReadRegions(offset, len);
    auto buffer = TStringBuf(readBuffer.data(), len);

    context->Read(
        readBuffer.begin(),
        len,
        offset,
        [this, context, regions = std::move(regions), buffer]() mutable
        {
            UpdateReadRegions(regions);
            ValidateReadData(context, buffer, regions);
        });
}

TVector<TReadRegionInfo> TFileTestScenario::GetReadRegions(
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

void TFileTestScenario::UpdateReadRegions(
    TVector<TReadRegionInfo>& regions) const
{
    auto guard = Guard(Lock);

    for (auto& region: regions) {
        region.AfterRead = RegionMetadata[region.Index];
    }
}

void TFileTestScenario::ValidateReadData(
    IContext* context,
    TStringBuf readBuffer,
    const TVector<TReadRegionInfo>& regions) const
{
    for (const auto& region: regions) {
        auto readBufferFragment =
            readBuffer.SubStr(region.OffsetInReadBuffer, region.Length);

        if (region.AfterRead.CurrentState != region.BeforeRead.CurrentState &&
            region.AfterRead.CurrentState != region.BeforeRead.NewState)
        {
            // We can validate the data only if we know about all writes that
            // happened during the read
            continue;
        }

        TVector<TRegionState> expectedStates;
        if (region.BeforeRead.CurrentState.SeqNum != 0) {
            expectedStates.push_back(region.BeforeRead.CurrentState);
        }
        if (region.BeforeRead.NewState != region.BeforeRead.CurrentState) {
            expectedStates.push_back(region.BeforeRead.NewState);
        }
        if (region.AfterRead.NewState != region.BeforeRead.NewState &&
            region.AfterRead.NewState != region.BeforeRead.CurrentState)
        {
            expectedStates.push_back(region.AfterRead.NewState);
        }

        ValidateReadDataRegion(
            context,
            readBufferFragment,
            expectedStates,
            region.OffsetInRegion);
    }
}

void TFileTestScenario::ValidateReadDataRegion(
    IContext* context,
    TStringBuf readBuffer,
    const TVector<TRegionState>& expectedStates,
    ui64 offsetInRegion) const
{
    /* STORAGE_INFO(
        "Validating read data at offset "
        << offsetInRegion
        << " (length " << readBuffer.size() << ", expected states "
        << expectedStates.size() << ")"
    ); */

    if (expectedStates.empty()) {
        return;
    }

    TVector<TRegionData> expectedBuffers(
        expectedStates.size());

    auto offset = ((offsetInRegion + sizeof(TRegionData) - 1) /
                   sizeof(TRegionData) * sizeof(TRegionData)) - offsetInRegion;

    while (offset < readBuffer.size()) {
        for (size_t i = 0; i < expectedStates.size(); i++) {
            GenerateRegionData(
                expectedStates[i],
                offsetInRegion + offset,
                &expectedBuffers[i]);
        }

        auto fragment = readBuffer.SubStr(offset, sizeof(TRegionData));

        if (ValidateReadDataFragment(fragment, expectedBuffers)) {
            offset += sizeof(TRegionData);
            continue;
        }

        TStringBuilder sb;
        sb << "Read validation failed at offset " << offsetInRegion + offset;
        sb << ", expected one of " << expectedStates.size() << " patterns:";

        for (size_t i = 0; i < expectedStates.size(); i++) {
            sb << "\nPattern #" << (i + 1) << ": "
               << expectedStates[i].SeqNum << ", "
               << expectedStates[i].TestStartTime << ", "
               << expectedStates[i].WriteTime;
        }

        if (fragment.size() >= offsetof(TRegionData, Data)) {
            TRegionData regionData;
            MemCopy(
                reinterpret_cast<char*>(&regionData),
                fragment.data(),
                fragment.size());
            sb << "\nRead data: "
               << regionData.SeqNum << ", "
               << regionData.TestStartTime << ", "
               << regionData.WriteTime;
        }

        context->Fail(sb);
        break;
    }
}

bool TFileTestScenario::ValidateReadDataFragment(
    TStringBuf readBuffer,
    const TVector<TRegionData>& expectedData) const
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

size_t TFileTestScenario::WriteBegin(IContext* context)
{
    auto guard = Guard(Lock);
    size_t index = 0;

    while (true) {
        // The number of threads is guaranteed to be not greater than the
        // number of regions, so this loop will eventually terminate
        index = RandomNumber(RegionMetadata.size());
        if (!RegionUseFlags[index]) {
            RegionUseFlags[index] = true;
            break;
        }
    }

    auto& metadata = RegionMetadata[index];
    if (metadata.NewState == metadata.CurrentState) {
        metadata.NewState = {
            .SeqNum = NextSeqNum++,
            .TestStartTime = TestStartTime.GetValue(),
            .WriteTime = Now().GetValue()
        };
    } else {
        STORAGE_DEBUG(
            "Writing to region #"
            << index << " was interrupted in the previous test run, restoring");
    }

    guard.Release();

    auto metadataOffset = sizeof(TFileHeader) + index * sizeof(TRegionMetadata);

    context->Write(
        &metadata.NewState,
        sizeof(metadata.NewState),
        metadataOffset + offsetof(TRegionMetadata, NewState),
        []() {});

    return index;
}

void TFileTestScenario::WriteBody(
    IContext* context,
    size_t index,
    TVector<char>& writeBuffer)
{
    const auto& metadata = RegionMetadata[index];

    Y_ABORT_UNLESS(writeBuffer.size() >= metadata.Size);

    TRegionData regionData;
    ui64 offset = 0;

    while (offset < metadata.Size) {
        GenerateRegionData(metadata.NewState, offset, &regionData);

        auto len = Min(metadata.Size - offset, sizeof(TRegionData));

        MemCopy(
            writeBuffer.begin() + offset,
            reinterpret_cast<const char*>(&regionData),
            len);

        offset += len;
    }

    ui64 ofs = 0;
    while (ofs < metadata.Size) {
        auto len =
            Min(RandomNumberFromRange(MinWriteSize, MaxWriteSize),
                metadata.Size - ofs);

        context->Write(
            writeBuffer.begin() + ofs,
            len,
            metadata.Offset + ofs,
            []() {});
        ofs += len;
    }
}

void TFileTestScenario::WriteEnd(IContext* context, size_t index)
{
    const auto& metadata = RegionMetadata[index];
    auto metadataOffset = sizeof(TFileHeader) + index * sizeof(TRegionMetadata);

    context->Write(
        &metadata.NewState,
        sizeof(metadata.NewState),
        metadataOffset + offsetof(TRegionMetadata, CurrentState),
        [this, index]()
        {
            auto guard = Guard(Lock);
            Y_ABORT_UNLESS(RegionUseFlags[index]);
            RegionUseFlags[index] = false;
            RegionMetadata[index].CurrentState = RegionMetadata[index].NewState;
        });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestScenarioPtr CreateFileTestScenario(
    IConfigHolderPtr configHolder,
    const TLog& log)
{
    return ITestScenarioPtr(
        new TFileTestScenario(std::move(configHolder), log));
}

}   // namespace NCloud::NBlockStore::NTesting
