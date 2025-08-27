#include "file_test_scenario.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/random/random.h>

#include <atomic>

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MaxReadSize = 1_MB;
constexpr ui64 MaxWriteSize = 1_MB;

constexpr ui64 MinRegionSize = 3_MB;
constexpr ui64 MaxRegionSize = 10_MB;

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

struct Y_PACKED TRegionMetadata
{
    ui64 Offset = 0;
    ui64 Size = 0;
    ui64 MinWriteSeqNum = 0;
    ui64 MaxWriteSeqNum = 0;
};

struct Y_PACKED TRegionMarker
{
    ui64 SeqNum = 0;
    ui64 TestStartTime = 0;
    ui64 WriteTime = 0;
    ui32 RandomNumber = 0;
    ui32 Crc32 = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFileTestScenario: public ITestScenario
{
private:
    class TTestThread;

    using IContext = NTesting::ITestThreadContext;

    IConfigHolderPtr ConfigHolder;
    TLog Log;
    TVector<std::unique_ptr<ITestThread>> Threads;
    TVector<TRegionMetadata> RegionMetadata;
    TVector<std::unique_ptr<std::atomic_bool>> RegionUseFlags;
    std::atomic<ui64> NextSeqNum = 0;
    std::optional<double> PhaseDuration;
    ui32 WriteRate = 0;
    ui64 FileSize = 0;
    const TInstant TestStartTime;

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
    bool Format();
    bool ValidateFormat() const;
    bool Init(TFileHandle& file) override;
    ui32 GetWriteRate(double secondsSinceTestStart) const;
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
    ui64 MetadataOffset = 0;
    size_t MetadataIndex = 0;
    TLog Log;

public:
    TTestThread(TFileTestScenario* testScenario, const TLog& log)
        : TestScenario(testScenario)
        , ReadBuffer(MaxReadSize)
        , WriteBuffer(MaxRegionSize)
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
                PerformRead(context);
                break;

            case EOperation::WriteBegin:
                PerformWriteBegin(context);
                break;

            case EOperation::WriteBody:
                PerformWriteBody(context);
                break;

            case EOperation::WriteEnd:
                PerformWriteEnd(context);
                break;

            default:
                Y_ABORT("Invalid operation %d", Operation);
        }
    }

    void PerformRead(ITestThreadContext* context)
    {
        auto len =
            Min(RandomNumber(ReadBuffer.size()) + 1, TestScenario->FileSize);
        auto offset = RandomNumber(TestScenario->FileSize - len + 1);

        context->Read(
            ReadBuffer.begin(),
            len,
            offset,
            []()
            {
                // TODO(nasonov): read data validation
            });

        Operation = EOperation::Idle;
    }

    void PerformWriteBegin(ITestThreadContext* context)
    {
        while (true) {
            MetadataIndex = RandomNumber(TestScenario->RegionMetadata.size());
            if (!TestScenario->RegionUseFlags[MetadataIndex]->exchange(true)) {
                break;
            }
        }

        auto& metadata = TestScenario->RegionMetadata[MetadataIndex];
        metadata.MaxWriteSeqNum = TestScenario->NextSeqNum++;

        MetadataOffset =
            sizeof(TFileHeader) + MetadataIndex * sizeof(TRegionMetadata);

        context->Write(
            &metadata.MaxWriteSeqNum,
            sizeof(metadata.MaxWriteSeqNum),
            MetadataOffset + offsetof(TRegionMetadata, MaxWriteSeqNum),
            []() {});

        Operation = EOperation::WriteBody;
    }

    void PerformWriteBody(ITestThreadContext* context)
    {
        const auto& metadata = TestScenario->RegionMetadata[MetadataIndex];

        TRegionMarker marker;
        marker.SeqNum = metadata.MaxWriteSeqNum;
        marker.TestStartTime = TestScenario->TestStartTime.GetValue();
        marker.WriteTime = Now().GetValue();
        marker.RandomNumber = RandomNumber<ui32>();
        marker.Crc32 = Crc32c(&marker, offsetof(TRegionMarker, Crc32));

        const char* markerBuf = reinterpret_cast<const char*>(&marker);

        Y_ABORT_UNLESS(WriteBuffer.size() >= metadata.Size);

        ui64 ofs = 0;
        while (ofs < metadata.Size) {
            ui64 len = Min(sizeof(TRegionMarker), metadata.Size - ofs);
            MemCopy(WriteBuffer.begin() + ofs, markerBuf, len);
            ofs += len;
        }

        ofs = 0;
        while (ofs < metadata.Size) {
            auto len = Min(RandomNumber(MaxWriteSize) + 1, metadata.Size - ofs);
            context->Write(
                WriteBuffer.begin() + ofs,
                len,
                metadata.Offset + ofs,
                []() {});
            ofs += len;
        }

        Operation = EOperation::WriteEnd;
    }

    void PerformWriteEnd(ITestThreadContext* context)
    {
        auto& metadata = TestScenario->RegionMetadata[MetadataIndex];
        metadata.MinWriteSeqNum = metadata.MaxWriteSeqNum;

        context->Write(
            &metadata.MinWriteSeqNum,
            sizeof(metadata.MinWriteSeqNum),
            MetadataOffset + offsetof(TRegionMetadata, MinWriteSeqNum),
            [flag = TestScenario->RegionUseFlags[MetadataIndex].get()]() {
                flag->store(false);
            });

        Operation = EOperation::Idle;
    }
};

////////////////////////////////////////////////////////////////////////////////

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
}

ui64 GetNextRegionSize(ui64 size)
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

    for (const auto& metadata: RegionMetadata) {
        NextSeqNum = Max(NextSeqNum.load(), metadata.MaxWriteSeqNum + 1);
        RegionUseFlags.push_back(std::make_unique<std::atomic_bool>(false));
    }

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
