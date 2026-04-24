#pragma once

#include "numeric.h"

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>
#include <util/system/align.h>
#include <util/system/file.h>
#include <util/system/filemap.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <cstring>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// When the deallocation condition is met, AnyOp will attempt to shrink the
// data area for all operations, while AllocOnlyOp will attempt to shrink it
// only during allocations.
enum class EDynamicPersistentTableShrinkMode
{
    AnyOp = 0,
    AllocOnlyOp,
};

struct TDynamicPersistentTableConfig
{
    ui64 MaxRecords = 0;
    ui64 InitialDataAreaSize = 0;
    ui64 MaxDataAreaStepSize = 0;
    ui64 InitialDataCompactionBufferSize = 0;
    ui64 GapSpacePercentageCompactionThreshold = 30;
    ui64 ShrinkLowMemoryOpThreshold = 100;
    ui64 ShrinkTriggerPercent = 25;
    ui64 ShrinkReservePercent = 25;
    EDynamicPersistentTableShrinkMode ShrinkMode =
        EDynamicPersistentTableShrinkMode::AllocOnlyOp;
};

////////////////////////////////////////////////////////////////////////////////

/**
 * TDynamicPersistentTable - A crash-resilient, file-based table for storing
 * variable-sized data records with automatic memory management.
 *
 * Key Features/Guarantees:
 * - Variable-sized records with automatic data area expansion
 * - Crash-safe operations with operation journaling and recovery
 * - Data integrity protection using CRC32 checksums
 * - Automatic space reclamation through record and data area compaction
 * - Automatic data area expansion if required
 * - Doubly-linked list organization for efficient traversal during compaction
 * - Thread Safety: NOT thread-safe - external synchronization required for
 *   concurrent access.
 *
 * Usage Pattern:
 * 1. AllocRecord() - Reserve space for a new record
 * 2. WriteRecordData() - Write data to the allocated record
 * 3. CommitRecord() - Make the record visible to readers
 * 4. DeleteRecord() - Mark record for deletion and space reclamation
 *
 * Template Parameter H: Header data type stored in the file header for
 * application-specific metadata.
 */

template <typename H>
class TDynamicPersistentTable
{
public:
    enum class EListOperationState
    {
        None = 0,
        Add,
        Remove,
    };

    struct THeader
    {
        ui32 Version = 0;
        ui64 HeaderSize = 0;
        ui64 RecordDescriptorSize = 0;
        ui64 DataAreaOffset = 0;
        ui64 DataAreaSize = 0;
        ui64 DataCompactionBufferSize = 0;
        ui64 NextFreeRecordIndex = 0;
        ui64 MaxRecords = 0;

        // indexes used during compaction
        ui64 CompactedRecordSrcIndex = InvalidIndex;
        ui64 CompactedRecordDstIndex = InvalidIndex;

        // journal of list operations in case of crash
        // for correctness check we only need state and ListOperationIndex
        EListOperationState ListOperationState = EListOperationState::None;
        ui64 ListOperationIndex = InvalidIndex;
        ui64 ListOperationPrevIndex = InvalidIndex;
        ui64 ListOperationNextIndex = InvalidIndex;

        // index of data record that is being compacted in case of crash
        ui64 DataCompactionRecordIndex = InvalidIndex;

        H Data;
    };

    enum class ERecordState
    {
        Free = 0,
        Allocated,
        Stored,
    };

    struct TRecordDescriptor
    {
        ui64 DataOffset = 0;
        ui64 DataSize = 0;
        ui64 PrevDataIndex = InvalidIndex;
        ui64 NextDataIndex = InvalidIndex;
        ERecordState State = ERecordState::Free;
        ui32 Crc32 = 0;
    };

private:
    enum class EDataRetrievalMode
    {
        NoValidation = 0,
        WithValidation,
    };

    TString FileName;
    const TDynamicPersistentTableConfig Config;
    ui64 MaxRecords = 0;
    ui64 LowMemoryOps = 0;

    ui64 NextFreeRecordIndex = 0;
    ui64 GapSpaceSize = 0;
    ui64 DataAreaOffset = 0;
    ui64 DataAreaSize = 0;
    ui64 NextDataOffset = 0;
    ui64 HeadDataIndex = InvalidIndex;
    ui64 TailDataIndex = InvalidIndex;

    std::unique_ptr<TFileMap> FileMap;
    TDeque<ui64> FreeRecordIndexes;

    THeader* HeaderPtr = nullptr;
    TRecordDescriptor* DescriptorsPtr = nullptr;
    char* FilePtr = nullptr;
    char* DataPtr = nullptr;
    char* DataCompactionBufferPtr = nullptr;

public:
    static constexpr ui64 InvalidIndex = -1;
    static constexpr ui32 Version = 2;

    class TIterator
    {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = TStringBuf;
        using difference_type = std::ptrdiff_t;
        using pointer = TStringBuf*;
        using reference = TStringBuf&;

        TIterator(const TDynamicPersistentTable& table, ui64 index)
            : Table(table)
            , Index(index)
        {
            SkipEmptyRecords();
        }

        bool operator==(const TIterator& other) const
        {
            return &Table == &other.Table && Index == other.Index;
        }

        bool operator!=(const TIterator& other) const
        {
            return !(*this == other);
        }

        TIterator& operator++()
        {
            ++Index;
            SkipEmptyRecords();
            return *this;
        }

        TIterator operator++(int)
        {
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        TStringBuf operator*()
        {
            return Table.GetRecordWithValidation(Index);
        }

        TStringBuf operator->()
        {
            return Table.GetRecordWithValidation(Index);
        }

        ui64 GetIndex() const
        {
            return Index;
        }

        ui64 GetDataSize() const
        {
            if (Index >= Table.MaxRecords) {
                return 0;
            }
            return Table.DescriptorsPtr[Index].DataSize;
        }

    private:
        void SkipEmptyRecords()
        {
            while (Index < Table.MaxRecords &&
                   Table.DescriptorsPtr[Index].State != ERecordState::Stored)
            {
                ++Index;
            }
        }

    private:
        const TDynamicPersistentTable& Table;
        ui64 Index;
    };

    TIterator begin() const
    {
        return TIterator(*this, 0);
    }

    TIterator end() const
    {
        return TIterator(*this, MaxRecords);
    }

public:
    TDynamicPersistentTable(
        const TString& fileName,
        const TDynamicPersistentTableConfig& config)
        : FileName(fileName)
        , Config(config)
        , MaxRecords(config.MaxRecords)
        , DataAreaSize(config.InitialDataAreaSize)
    {
        Y_ABORT_UNLESS(Config.MaxRecords != 0, "MaxRecords must not be zero");
        Y_ABORT_UNLESS(
            Config.InitialDataAreaSize != 0,
            "InitialDataAreaSize must not be zero");
        Y_ABORT_UNLESS(
            IsPowerOf2(Config.InitialDataAreaSize),
            "InitialDataAreaSize must be power of 2: %lu",
            Config.InitialDataAreaSize);
        Y_ABORT_UNLESS(
            Config.MaxDataAreaStepSize != 0,
            "MaxDataAreaStepSize must not be zero");
        Init();
    }

    H* HeaderData()
    {
        return &HeaderPtr->Data;
    }

    TStringBuf GetRecord(ui64 index) const
    {
        return GetRecord(index, EDataRetrievalMode::NoValidation);
    }

    TStringBuf GetRecordWithValidation(ui64 index) const
    {
        return GetRecord(index, EDataRetrievalMode::WithValidation);
    }

    ui64 AllocRecord(ui64 dataSize)
    {
        ui64 index = InvalidIndex;
        if (dataSize == 0) {
            return InvalidIndex;
        }

        if (!FreeRecordIndexes.empty()) {
            index = FreeRecordIndexes.front();
            FreeRecordIndexes.pop_front();
        } else if (NextFreeRecordIndex < MaxRecords) {
            index = NextFreeRecordIndex++;
        }

        if (index == InvalidIndex) {
            return InvalidIndex;
        }

        HeaderPtr->NextFreeRecordIndex = NextFreeRecordIndex;

        if (NextDataOffset + dataSize > DataAreaSize) {
            TryCompactDataArea(dataSize);
            if (NextDataOffset + dataSize > DataAreaSize) {
                ExpandDataArea(dataSize);
            }
        }

        DescriptorsPtr[index].DataOffset = DataAreaOffset + NextDataOffset;
        DescriptorsPtr[index].DataSize = dataSize;

        PrepareAddRecord(index);
        FinishAddRecord();

        NextDataOffset += dataSize;
        UpdateShrinkCounters();
        TryShrinkDataArea();

        return index;
    }

    bool CommitRecord(ui64 index)
    {
        if (index >= MaxRecords ||
            DescriptorsPtr[index].State != ERecordState::Allocated)
        {
            return false;
        }

        DescriptorsPtr[index].State = ERecordState::Stored;

        return true;
    }

    bool DeleteRecord(ui64 index)
    {
        if (index >= MaxRecords ||
            DescriptorsPtr[index].State != ERecordState::Stored)
        {
            return false;
        }

        PrepareRemoveRecord(index);
        FinishRemoveRecord();

        GapSpaceSize += DescriptorsPtr[index].DataSize;

        FreeRecordIndexes.push_back(index);
        UpdateShrinkCounters();
        if (Config.ShrinkMode == EDynamicPersistentTableShrinkMode::AnyOp) {
            TryShrinkDataArea();
        }

        return true;
    }

    ui64 CountRecords() const
    {
        return NextFreeRecordIndex - FreeRecordIndexes.size();
    }

    void TryDeallocateMemory()
    {
        if (!IsLowMemoryUsage()) {
            return;
        }

        ShrinkDataArea();
    }

    void Clear()
    {
        NextFreeRecordIndex = 0;
        NextDataOffset = 0;
        GapSpaceSize = 0;
        HeadDataIndex = InvalidIndex;
        TailDataIndex = InvalidIndex;
        FreeRecordIndexes.clear();

        FileMap->ResizeAndRemap(0, 0);
        FileMap.reset();
        Init();
    }

    TMemoryOutput GetAllocatedRecordDataWriter(ui64 index)
    {
        if (index >= MaxRecords ||
            DescriptorsPtr[index].State != ERecordState::Allocated)
        {
            return TMemoryOutput(nullptr, 0);
        }

        return TMemoryOutput(
            FilePtr + DescriptorsPtr[index].DataOffset,
            DescriptorsPtr[index].DataSize);
    }

    bool WriteRecordData(ui64 index, const void* data, ui64 size)
    {
        if (index >= MaxRecords || size > DescriptorsPtr[index].DataSize ||
            data == nullptr ||
            DescriptorsPtr[index].State == ERecordState::Free)
        {
            return false;
        }

        char* recordData = FilePtr + DescriptorsPtr[index].DataOffset;
        std::memcpy(recordData, data, size);
        if (size < DescriptorsPtr[index].DataSize) {
            GapSpaceSize += DescriptorsPtr[index].DataSize - size;
            DescriptorsPtr[index].DataSize = size;
        }

        DescriptorsPtr[index].Crc32 =
            Crc32c(recordData, DescriptorsPtr[index].DataSize);

        return true;
    }

private:
    // TODO remove after migration is done
    void MigrateDataOffsetsToAbsolute()
    {
        if (HeaderPtr->Version != 1) {
            return;
        }

        for (ui64 index = 0; index < NextFreeRecordIndex; ++index) {
            if (DescriptorsPtr[index].State == ERecordState::Free) {
                continue;
            }
            DescriptorsPtr[index].DataOffset += DataAreaOffset;
        }

        HeaderPtr->Version = Version;
    }

    void PrepareAddRecord(ui64 index)
    {
        HeaderPtr->ListOperationState = EListOperationState::Add;
        // currently we add records only to the end of the list
        HeaderPtr->ListOperationPrevIndex = TailDataIndex;
        HeaderPtr->ListOperationIndex = index;
    }

    void FinishAddRecord()
    {
        if (HeaderPtr->ListOperationState != EListOperationState::Add &&
            HeaderPtr->ListOperationState != EListOperationState::None)
        {
            return;
        }

        Y_DEFER
        {
            HeaderPtr->ListOperationState = EListOperationState::None;
            HeaderPtr->ListOperationPrevIndex = InvalidIndex;
            HeaderPtr->ListOperationIndex = InvalidIndex;
        };

        if (HeaderPtr->ListOperationState == EListOperationState::None ||
            HeaderPtr->ListOperationIndex == InvalidIndex)
        {
            return;
        }

        ui64 prevIndex = HeaderPtr->ListOperationPrevIndex;
        ui64 index = HeaderPtr->ListOperationIndex;

        DescriptorsPtr[index].PrevDataIndex = InvalidIndex;
        DescriptorsPtr[index].NextDataIndex = InvalidIndex;

        if (prevIndex != InvalidIndex) {
            DescriptorsPtr[prevIndex].NextDataIndex = index;
        }

        DescriptorsPtr[index].PrevDataIndex = prevIndex;

        TailDataIndex = index;

        if (HeadDataIndex == InvalidIndex) {
            HeadDataIndex = index;
        }

        DescriptorsPtr[index].State = ERecordState::Allocated;
    }

    void PrepareRemoveRecord(ui64 index)
    {
        HeaderPtr->ListOperationState = EListOperationState::Remove;
        HeaderPtr->ListOperationPrevIndex = DescriptorsPtr[index].PrevDataIndex;
        HeaderPtr->ListOperationNextIndex = DescriptorsPtr[index].NextDataIndex;
        HeaderPtr->ListOperationIndex = index;
    }

    void FinishRemoveRecord()
    {
        if (HeaderPtr->ListOperationState != EListOperationState::Remove &&
            HeaderPtr->ListOperationState != EListOperationState::None)
        {
            return;
        }

        Y_DEFER
        {
            HeaderPtr->ListOperationState = EListOperationState::None;
            HeaderPtr->ListOperationPrevIndex = InvalidIndex;
            HeaderPtr->ListOperationNextIndex = InvalidIndex;
            HeaderPtr->ListOperationIndex = InvalidIndex;
        };

        if (HeaderPtr->ListOperationState == EListOperationState::None ||
            HeaderPtr->ListOperationIndex == InvalidIndex)
        {
            return;
        }

        ui64 prevIndex = HeaderPtr->ListOperationPrevIndex;
        ui64 nextIndex = HeaderPtr->ListOperationNextIndex;
        ui64 index = HeaderPtr->ListOperationIndex;

        if (prevIndex != InvalidIndex) {
            DescriptorsPtr[prevIndex].NextDataIndex = nextIndex;
        } else {
            HeadDataIndex = nextIndex;
        }

        if (nextIndex != InvalidIndex) {
            DescriptorsPtr[nextIndex].PrevDataIndex = prevIndex;
        } else {
            TailDataIndex = prevIndex;
        }

        DescriptorsPtr[index].State = ERecordState::Free;

        DescriptorsPtr[index].PrevDataIndex = InvalidIndex;
        DescriptorsPtr[index].NextDataIndex = InvalidIndex;
    }

    void Init()
    {
        TFile file(FileName, OpenAlways | WrOnly);
        if (file.GetLength() == 0) {
            file.Resize(sizeof(THeader));
        }
        file.Close();

        FileMap = std::make_unique<TFileMap>(FileName, TMemoryMapCommon::oRdWr);
        FileMap->Map(0, sizeof(THeader));

        HeaderPtr = reinterpret_cast<THeader*>(FileMap->Ptr());
        if (HeaderPtr->MaxRecords == 0) {
            HeaderPtr->Version = Version;
            HeaderPtr->HeaderSize = sizeof(THeader);
            HeaderPtr->RecordDescriptorSize = sizeof(TRecordDescriptor);
            HeaderPtr->MaxRecords = MaxRecords;
            HeaderPtr->NextFreeRecordIndex = 0;
            HeaderPtr->DataAreaOffset =
                sizeof(THeader) + MaxRecords * sizeof(TRecordDescriptor);
            HeaderPtr->DataAreaSize = DataAreaSize;
            HeaderPtr->DataCompactionBufferSize =
                Config.InitialDataCompactionBufferSize;
            HeaderPtr->CompactedRecordSrcIndex = InvalidIndex;
            HeaderPtr->CompactedRecordDstIndex = InvalidIndex;
        }

        // TODO remove Version==1 after migration is done
        Y_ABORT_UNLESS(
            HeaderPtr->Version == Version || HeaderPtr->Version == 1,
            "Invalid header version %d",
            HeaderPtr->Version);
        Y_ABORT_UNLESS(
            HeaderPtr->HeaderSize == sizeof(THeader),
            "Invalid header size %lu != %lu",
            HeaderPtr->HeaderSize,
            sizeof(THeader));
        Y_ABORT_UNLESS(
            HeaderPtr->RecordDescriptorSize == sizeof(TRecordDescriptor),
            "Invalid record descriptor size %lu != %lu",
            HeaderPtr->RecordDescriptorSize,
            sizeof(TRecordDescriptor));

        MaxRecords = HeaderPtr->MaxRecords;
        NextFreeRecordIndex = HeaderPtr->NextFreeRecordIndex;
        DataAreaOffset = HeaderPtr->DataAreaOffset;
        DataAreaSize = HeaderPtr->DataAreaSize;

        ResizeAndRemap();

        MigrateDataOffsetsToAbsolute();

        ResetShrinkState();

        FinishRemoveRecord();
        FinishAddRecord();

        FinishDataAreaCompaction();

        CompactRecords();
        CompactDataArea();
        ShrinkDataArea();
    }

    ui64 CalcFileSize(ui64 dataAreaSize)
    {
        return sizeof(THeader) + MaxRecords * sizeof(TRecordDescriptor) +
               dataAreaSize + HeaderPtr->DataCompactionBufferSize;
    }

    void CompactRecords()
    {
        ui64 writeRecordIndex = 0;
        ui64 readRecordIndex = 0;

        FinishCompactRecord();

        while (readRecordIndex < NextFreeRecordIndex) {
            if (DescriptorsPtr[readRecordIndex].State != ERecordState::Stored) {
                if (DescriptorsPtr[readRecordIndex].State ==
                    ERecordState::Allocated)
                {
                    PrepareRemoveRecord(readRecordIndex);
                    FinishRemoveRecord();
                }
                ++readRecordIndex;
                continue;
            }

            ui64 calculatedCrc = Crc32c(
                FilePtr + DescriptorsPtr[readRecordIndex].DataOffset,
                DescriptorsPtr[readRecordIndex].DataSize);
            if (calculatedCrc != DescriptorsPtr[readRecordIndex].Crc32) {
                PrepareRemoveRecord(readRecordIndex);
                FinishRemoveRecord();
                ++readRecordIndex;
                continue;
            }

            if (writeRecordIndex != readRecordIndex) {
                PrepareCompactRecord(readRecordIndex, writeRecordIndex);
                FinishCompactRecord();
            }
            if (DescriptorsPtr[writeRecordIndex].PrevDataIndex == InvalidIndex)
            {
                HeadDataIndex = writeRecordIndex;
            }
            if (DescriptorsPtr[writeRecordIndex].NextDataIndex == InvalidIndex)
            {
                TailDataIndex = writeRecordIndex;
            }

            ++writeRecordIndex;
            ++readRecordIndex;
        }

        NextFreeRecordIndex = writeRecordIndex;
        HeaderPtr->NextFreeRecordIndex = NextFreeRecordIndex;
    }

    void PrepareCompactRecord(ui64 srcIndex, ui64 dstIndex)
    {
        // set compacted indexes atomically (64bit memory assignment)
        // if we restart during the copy we can retry the copy until we succeed
        // and set at least one of the indexes to InvalidIndex value
        HeaderPtr->CompactedRecordSrcIndex = srcIndex;
        HeaderPtr->CompactedRecordDstIndex = dstIndex;
    }

    void FinishCompactRecord()
    {
        if (HeaderPtr->CompactedRecordSrcIndex == InvalidIndex ||
            HeaderPtr->CompactedRecordDstIndex == InvalidIndex)
        {
            // Prepare phase didn't finish and none of the records were moved
            HeaderPtr->CompactedRecordSrcIndex = InvalidIndex;
            HeaderPtr->CompactedRecordDstIndex = InvalidIndex;
            return;
        }

        Y_ABORT_UNLESS(
            HeaderPtr->CompactedRecordSrcIndex < MaxRecords,
            "Invalid CompactedRecordSrcIndex %lu",
            HeaderPtr->CompactedRecordSrcIndex);
        Y_ABORT_UNLESS(
            HeaderPtr->CompactedRecordDstIndex < MaxRecords,
            "Invalid CompactedRecordDstIndex %lu",
            HeaderPtr->CompactedRecordDstIndex);

        std::memcpy(
            &DescriptorsPtr[HeaderPtr->CompactedRecordDstIndex],
            &DescriptorsPtr[HeaderPtr->CompactedRecordSrcIndex],
            sizeof(TRecordDescriptor));

        DescriptorsPtr[HeaderPtr->CompactedRecordSrcIndex].State =
            ERecordState::Free;
        DescriptorsPtr[HeaderPtr->CompactedRecordDstIndex].State =
            ERecordState::Stored;

        ui64 prev =
            DescriptorsPtr[HeaderPtr->CompactedRecordDstIndex].PrevDataIndex;
        ui64 next =
            DescriptorsPtr[HeaderPtr->CompactedRecordDstIndex].NextDataIndex;

        if (prev != InvalidIndex) {
            DescriptorsPtr[prev].NextDataIndex =
                HeaderPtr->CompactedRecordDstIndex;
        }
        if (next != InvalidIndex) {
            DescriptorsPtr[next].PrevDataIndex =
                HeaderPtr->CompactedRecordDstIndex;
        }

        HeaderPtr->CompactedRecordSrcIndex = InvalidIndex;
        HeaderPtr->CompactedRecordDstIndex = InvalidIndex;
    }

    void TryCompactDataArea(ui64 requiredRecordDataSize)
    {
        const ui64 threshold = PercentOf(
            DataAreaSize,
            Config.GapSpacePercentageCompactionThreshold);
        const bool shouldCompact = GapSpaceSize > threshold ||
                                   GapSpaceSize > Config.MaxDataAreaStepSize;
        if (shouldCompact && GapSpaceSize >= requiredRecordDataSize) {
            CompactDataArea();
        }
    }

private:
    void UpdateShrinkCounters()
    {
        if (IsLowMemoryUsage()) {
            ++LowMemoryOps;
        } else {
            LowMemoryOps = 0;
        }
    }

    void TryShrinkDataArea()
    {
        if (LowMemoryOps < Config.ShrinkLowMemoryOpThreshold) {
            return;
        }

        ShrinkDataArea();
    }

    ui64 CalcShrinkTargetSize()
    {
        const ui64 live = GetLiveDataSize();
        const ui64 shrinkTriggerThreshold =
            PercentOf(DataAreaSize, Config.ShrinkTriggerPercent);

        ui64 reserve = Config.InitialDataAreaSize;
        const ui64 percentReserve =
            PercentOf(live, Config.ShrinkReservePercent);
        if (reserve < percentReserve) {
            reserve = percentReserve;
        }

        if (live > shrinkTriggerThreshold &&
            DataAreaSize - live >= Config.MaxDataAreaStepSize)
        {
            const ui64 maxStepReserve = Config.MaxDataAreaStepSize / 2;
            if (reserve < maxStepReserve) {
                reserve = maxStepReserve;
            }
        }

        if (reserve > Config.MaxDataAreaStepSize) {
            reserve = Config.MaxDataAreaStepSize;
        }

        ui64 target = live + reserve;
        target = AlignUp(target, Config.InitialDataAreaSize);

        if (target > DataAreaSize) {
            return DataAreaSize;
        }

        return target;
    }

    void ShrinkDataArea()
    {
        if (GapSpaceSize != 0) {
            CompactDataArea();
        }

        const ui64 newDataAreaSize = CalcShrinkTargetSize();
        if (newDataAreaSize == DataAreaSize) {
            ResetShrinkState();
            return;
        }

        DataAreaSize = newDataAreaSize;
        HeaderPtr->DataAreaSize = newDataAreaSize;

        ResizeAndRemap();

        ResetShrinkState();
    }

    void CompactDataArea()
    {
        ui64 newOffset = 0;
        ui64 currentIndex = HeadDataIndex;
        while (currentIndex != InvalidIndex) {
            Y_ABORT_UNLESS(
                currentIndex < NextFreeRecordIndex,
                "Invalid data index %lu in linked list",
                currentIndex);
            Y_ABORT_UNLESS(
                DescriptorsPtr[currentIndex].State != ERecordState::Free,
                "Non-stored record %lu found in data linked list",
                currentIndex);

            ui64 oldOffset =
                DescriptorsPtr[currentIndex].DataOffset - DataAreaOffset;
            ui64 dataSize = DescriptorsPtr[currentIndex].DataSize;
            ui64 nextIndex = DescriptorsPtr[currentIndex].NextDataIndex;

            if (oldOffset != newOffset) {
                if (HeaderPtr->DataCompactionBufferSize < dataSize) {
                    HeaderPtr->DataCompactionBufferSize = dataSize;
                    ResizeAndRemap();
                }

                std::memcpy(
                    DataCompactionBufferPtr,
                    FilePtr + DescriptorsPtr[currentIndex].DataOffset,
                    dataSize);

                // We can omit handling the case where a crash occurs after
                // currentIndex but before setting newOffset, since in that
                // scenario the data will simply be copied back to the old
                // location.
                HeaderPtr->DataCompactionRecordIndex = currentIndex;

                DescriptorsPtr[currentIndex].DataOffset =
                    DataAreaOffset + newOffset;

                std::memcpy(
                    DataPtr + newOffset,
                    DataCompactionBufferPtr,
                    dataSize);

                HeaderPtr->DataCompactionRecordIndex = InvalidIndex;
            }
            newOffset += dataSize;
            currentIndex = nextIndex;
        }

        NextDataOffset = newOffset;
        GapSpaceSize = 0;
    }

    void FinishDataAreaCompaction()
    {
        if (HeaderPtr->DataCompactionRecordIndex == InvalidIndex) {
            return;
        }

        const ui64 currentIndex = HeaderPtr->DataCompactionRecordIndex;

        std::memcpy(
            FilePtr + DescriptorsPtr[currentIndex].DataOffset,
            DataCompactionBufferPtr,
            DescriptorsPtr[currentIndex].DataSize);

        HeaderPtr->DataCompactionRecordIndex = InvalidIndex;

        HeaderPtr->DataCompactionBufferSize =
            Config.InitialDataCompactionBufferSize;
    }

    void ExpandDataArea(ui64 requiredRecordDataSize)
    {
        ui64 newDataAreaSize = DataAreaSize;
        while (requiredRecordDataSize > newDataAreaSize - NextDataOffset) {
            const ui64 dataAreaStepSize =
                newDataAreaSize < Config.MaxDataAreaStepSize
                    ? newDataAreaSize
                    : Config.MaxDataAreaStepSize;

            newDataAreaSize += dataAreaStepSize;
        }

        DataAreaSize = newDataAreaSize;
        HeaderPtr->DataAreaSize = DataAreaSize;

        ResizeAndRemap();

        ResetShrinkState();
    }

    ui64 GetLiveDataSize() const
    {
        return NextDataOffset - GapSpaceSize;
    }

    bool IsLowMemoryUsage() const
    {
        const ui64 live = GetLiveDataSize();
        const ui64 threshold =
            PercentOf(DataAreaSize, Config.ShrinkTriggerPercent);
        return live <= threshold ||
               (DataAreaSize - live >= Config.MaxDataAreaStepSize);
    }

    void ResetShrinkState()
    {
        LowMemoryOps = 0;
    }

    void ResizeAndRemap()
    {
        FileMap->ResizeAndRemap(0, CalcFileSize(DataAreaSize));
        HeaderPtr = reinterpret_cast<THeader*>(FileMap->Ptr());
        FilePtr = reinterpret_cast<char*>(FileMap->Ptr());
        DescriptorsPtr =
            reinterpret_cast<TRecordDescriptor*>(FilePtr + sizeof(THeader));
        DataPtr = FilePtr + DataAreaOffset;
        DataCompactionBufferPtr = DataPtr + DataAreaSize;
    }

    TStringBuf GetRecord(ui64 index, EDataRetrievalMode mode) const
    {
        if (index >= MaxRecords ||
            DescriptorsPtr[index].State != ERecordState::Stored)
        {
            return TStringBuf();
        }

        TStringBuf buf(
            FilePtr + DescriptorsPtr[index].DataOffset,
            DescriptorsPtr[index].DataSize);

        if (mode == EDataRetrievalMode::WithValidation) {
            ui32 calculatedCrc = Crc32c(buf.data(), buf.size());
            if (calculatedCrc != DescriptorsPtr[index].Crc32) {
                return TStringBuf();
            }
        }

        return buf;
    }
};

}   // namespace NCloud
