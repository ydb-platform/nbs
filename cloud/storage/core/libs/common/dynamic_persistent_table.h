#pragma once

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>
#include <util/system/file.h>
#include <util/system/filemap.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <cstring>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

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
        ui32 CRC32 = 0;
    };

private:
    enum class EDataRetrievalMode
    {
        NoValidation = 0,
        WithValidation,
    };

    TString FileName;
    ui64 MaxRecords = 0;
    ui64 InitialDataAreaSize = 0;
    ui64 InitialDataCompactionBufferSize = 0;
    ui64 GapSpacePercentageCompactionThreshold = 30;

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
    char* DataPtr = nullptr;
    char* DataCompactionBufferPtr = nullptr;

public:
    static constexpr ui64 InvalidIndex = -1;
    static constexpr ui32 Version = 1;

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
        ui64 maxRecords,
        ui64 initialDataAreaSize,
        ui64 initialDataCompactionBufferSize,
        ui64 gapSpacePercentageCompactionThreshold)
        : FileName(fileName)
        , MaxRecords(maxRecords)
        , InitialDataAreaSize(initialDataAreaSize)
        , InitialDataCompactionBufferSize(initialDataCompactionBufferSize)
        , GapSpacePercentageCompactionThreshold(
              gapSpacePercentageCompactionThreshold)
        , DataAreaSize(initialDataAreaSize)
    {
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

        DescriptorsPtr[index].DataOffset = NextDataOffset;
        DescriptorsPtr[index].DataSize = dataSize;

        PrepareAddRecord(index);
        FinishAddRecord();

        NextDataOffset += dataSize;

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

        return true;
    }

    ui64 CountRecords() const
    {
        return NextFreeRecordIndex - FreeRecordIndexes.size();
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
            DataPtr + DescriptorsPtr[index].DataOffset,
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

        char* recordData = DataPtr + DescriptorsPtr[index].DataOffset;
        std::memcpy(recordData, data, size);
        if (size < DescriptorsPtr[index].DataSize) {
            GapSpaceSize += DescriptorsPtr[index].DataSize - size;
            DescriptorsPtr[index].DataSize = size;
        }

        DescriptorsPtr[index].CRC32 =
            Crc32c(recordData, DescriptorsPtr[index].DataSize);

        return true;
    }

private:
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
                InitialDataCompactionBufferSize;
            HeaderPtr->CompactedRecordSrcIndex = InvalidIndex;
            HeaderPtr->CompactedRecordDstIndex = InvalidIndex;
        }

        Y_ABORT_UNLESS(
            HeaderPtr->Version == Version,
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

        FinishRemoveRecord();
        FinishAddRecord();

        FinishDataAreaCompaction();

        CompactRecords();
        CompactDataArea();
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

            ui64 calculatedCRC = Crc32c(
                DataPtr + DescriptorsPtr[readRecordIndex].DataOffset,
                DescriptorsPtr[readRecordIndex].DataSize);
            if (calculatedCRC != DescriptorsPtr[readRecordIndex].CRC32) {
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

    void TryCompactDataArea(ui64 requiredSize)
    {
        uint64_t threshold = (InitialDataAreaSize / 100) *
                                 GapSpacePercentageCompactionThreshold +
                             ((InitialDataAreaSize % 100) *
                              GapSpacePercentageCompactionThreshold) /
                                 100;
        if (GapSpaceSize > threshold && GapSpaceSize >= requiredSize) {
            CompactDataArea();
        }
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

            ui64 oldOffset = DescriptorsPtr[currentIndex].DataOffset;
            ui64 dataSize = DescriptorsPtr[currentIndex].DataSize;
            ui64 nextIndex = DescriptorsPtr[currentIndex].NextDataIndex;

            if (oldOffset != newOffset) {
                if (HeaderPtr->DataCompactionBufferSize < dataSize) {
                    HeaderPtr->DataCompactionBufferSize = dataSize;
                    ResizeAndRemap();
                }

                std::memcpy(
                    DataCompactionBufferPtr,
                    DataPtr + oldOffset,
                    dataSize);

                HeaderPtr->DataCompactionRecordIndex = currentIndex;

                DescriptorsPtr[currentIndex].DataOffset = newOffset;

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
            DataPtr + DescriptorsPtr[currentIndex].DataOffset,
            DataCompactionBufferPtr,
            DescriptorsPtr[currentIndex].DataSize);

        HeaderPtr->DataCompactionRecordIndex = InvalidIndex;

        HeaderPtr->DataCompactionBufferSize = InitialDataCompactionBufferSize;
    }

    void ExpandDataArea(ui64 requiredSize)
    {
        ui64 newDataAreaSize = DataAreaSize;
        while (NextDataOffset + requiredSize > newDataAreaSize) {
            newDataAreaSize *= 2;
        }

        DataAreaSize = newDataAreaSize;
        HeaderPtr->DataAreaSize = DataAreaSize;

        ResizeAndRemap();
    }

    void ResizeAndRemap()
    {
        FileMap->ResizeAndRemap(0, CalcFileSize(DataAreaSize));
        HeaderPtr = reinterpret_cast<THeader*>(FileMap->Ptr());
        DescriptorsPtr = reinterpret_cast<TRecordDescriptor*>(
            reinterpret_cast<char*>(HeaderPtr) + sizeof(THeader));
        DataPtr = reinterpret_cast<char*>(HeaderPtr) + DataAreaOffset;
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
            DataPtr + DescriptorsPtr[index].DataOffset,
            DescriptorsPtr[index].DataSize);

        if (mode == EDataRetrievalMode::WithValidation) {
            ui32 calculatedCRC = Crc32c(buf.data(), buf.size());
            if (calculatedCRC != DescriptorsPtr[index].CRC32) {
                return TStringBuf();
            }
        }

        return buf;
    }
};

}   // namespace NCloud
