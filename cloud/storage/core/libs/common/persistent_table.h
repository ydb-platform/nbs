#pragma once

#include <util/generic/deque.h>
#include <util/system/file.h>
#include <util/system/filemap.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <optional>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename H, typename R>
class TPersistentTable
{
public:
    struct THeader;
    struct TRecord;

private:
    TString FileName;
    size_t RecordCount = 0;
    size_t NextFreeRecord = 0;

    std::unique_ptr<TFileMap> FileMap;
    TDeque<ui64> FreeRecords;
    THeader* HeaderPtr = nullptr;
    TRecord* RecordsPtr = nullptr;

public:
    static constexpr ui64 InvalidIndex = -1;
    static constexpr ui32 Version = 1;

    struct THeader
    {
        ui32 Version = 0;
        size_t HeaderSize = 0;
        size_t RecordSize = 0;
        size_t RecordCount = 0;

        // indexes used during compaction
        ui64 CompactedRecordSrcIndex = InvalidIndex;
        ui64 CompactedRecordDstIndex = InvalidIndex;

        H Data;
    };

    enum class ERecordState: ui8
    {
        Free = 0,
        Allocated,
        Stored,
    };

    struct TRecord
    {
        R Data;
        ERecordState State = ERecordState::Free;
    };

    class TIterator
    {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = R;
        using difference_type = std::ptrdiff_t;
        using pointer = R*;
        using reference = R&;

        TIterator(TPersistentTable& table, ui64 index)
            : Table(table)
            , Index(index)
        {
            SkipEmptyRecords();
        }

        bool operator==(const TIterator& other) const
        {
            return Index == other.Index;
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

        R& operator*()
        {
            return Table.RecordsPtr[Index].Data;
        }

        R* operator->()
        {
            return &Table.RecordsPtr[Index].Data;
        }

        ui64 GetIndex() const
        {
            return Index;
        }

    private:
        void SkipEmptyRecords()
        {
            while (Index < Table.RecordCount &&
                   Table.RecordsPtr[Index].State != ERecordState::Stored)
            {
                ++Index;
            }
        }

    private:
        TPersistentTable& Table;
        ui64 Index;
    };

    TIterator begin()
    {
        return TIterator(*this, 0);
    }

    TIterator end()
    {
        return TIterator(*this, RecordCount);
    }

public:
    TPersistentTable(const TString& fileName, size_t initialRecordCount)
        : FileName(fileName)
        , RecordCount(initialRecordCount)
    {
        Init();
    }

    H* HeaderData()
    {
        return &HeaderPtr->Data;
    }

    R* RecordData(ui64 index)
    {
        return &RecordsPtr[index].Data;
    }

    ui64 AllocRecord()
    {
        ui64 index = InvalidIndex;

        if (!FreeRecords.empty()) {
            index = FreeRecords.front();
            FreeRecords.pop_front();
        } else if (NextFreeRecord < RecordCount) {
            index = NextFreeRecord++;
        }

        if (index != InvalidIndex) {
            RecordsPtr[index].State = ERecordState::Allocated;
        }

        return index;
    }

    void CommitRecord(ui64 index)
    {
        Y_ABORT_UNLESS(index < RecordCount);
        RecordsPtr[index].State = ERecordState::Stored;
    }

    void DeleteRecord(ui64 index)
    {
        Y_ABORT_UNLESS(index < RecordCount, "%lu < %lu", index, RecordCount);
        RecordsPtr[index].State = ERecordState::Free;

        if (index + 1 == NextFreeRecord) {
            NextFreeRecord--;
        } else {
            FreeRecords.push_back(index);
        }
    }

    size_t CountRecords()
    {
        return NextFreeRecord - FreeRecords.size();
    }

    void Clear()
    {
        NextFreeRecord = 0;
        FileMap->ResizeAndRemap(0, 0);
        FileMap.reset();
        FreeRecords.clear();
        Init();
    }

private:
    void Init()
    {
        // if file doesn't exist create file with zeroed header
        TFile file(FileName, OpenAlways | WrOnly);
        if (file.GetLength() == 0) {
            file.Resize(CalcFileSize(0));
        }
        file.Close();

        FileMap = std::make_unique<TFileMap>(FileName, TMemoryMapCommon::oRdWr);
        FileMap->Map(0, sizeof(THeader));

        auto* header = reinterpret_cast<THeader*>(FileMap->Ptr());
        if (header->RecordCount == 0) {
            header->Version = Version;
            header->RecordCount = RecordCount;
            header->HeaderSize = sizeof(THeader);
            header->RecordSize = sizeof(TRecord);
            header->CompactedRecordSrcIndex = InvalidIndex;
            header->CompactedRecordDstIndex = InvalidIndex;
        }

        Y_ABORT_UNLESS(
            header->Version == Version,
            "Invalid header version %d",
            header->Version);
        Y_ABORT_UNLESS(
            header->HeaderSize == sizeof(THeader),
            "Invalid header size %lu != %lu",
            header->HeaderSize,
            sizeof(THeader));
        Y_ABORT_UNLESS(
            header->RecordSize == sizeof(TRecord),
            "Invalid record size %lu != %lu",
            header->RecordSize,
            sizeof(TRecord));

        const auto initalRecordCount = RecordCount;

        auto resizeTable = [this](auto newRecordCount) {
            FileMap->ResizeAndRemap(0, CalcFileSize(newRecordCount));
            HeaderPtr = reinterpret_cast<THeader*>(FileMap->Ptr());
            RecordsPtr = reinterpret_cast<TRecord*>(HeaderPtr + 1);

            RecordCount = newRecordCount;
            HeaderPtr->RecordCount = RecordCount;
        };

        // allow table to grow if table was previously allocated with smaller size
        resizeTable(std::max(header->RecordCount, initalRecordCount));

        CompactRecords();

        // shrink table if it fits into requested size after compaction
        if (initalRecordCount < RecordCount && NextFreeRecord <= initalRecordCount) {
            resizeTable(initalRecordCount);
        }
    }

    size_t CalcFileSize(size_t recordCount)
    {
        return sizeof(THeader) + recordCount * sizeof(TRecord);
    }

    void CompactRecords()
    {
        ui64 writeRecordIndex = 0;
        ui64 readRecordIndex = 0;

        // try to complete last record compaction if it was interrupted
        FinishCompactRecord();

        while (readRecordIndex < RecordCount) {
            if (RecordsPtr[readRecordIndex].State != ERecordState::Stored) {
                RecordsPtr[readRecordIndex++].State = ERecordState::Free;
                continue;
            }

            if (writeRecordIndex != readRecordIndex) {
                PrepareCompactRecord(readRecordIndex, writeRecordIndex);
                FinishCompactRecord();
            }
            ++writeRecordIndex;
            ++readRecordIndex;
        }

        NextFreeRecord = writeRecordIndex;
    }

    void PrepareCompactRecord(ui64 srcIndex, ui64 dstIndex)
    {
        // set compacted indexes atomically (64bit memory assigment)
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
            HeaderPtr->CompactedRecordSrcIndex < RecordCount,
            "Invalid CompactedRecordSrcIndex %lu",
            HeaderPtr->CompactedRecordSrcIndex);
        Y_ABORT_UNLESS(
            HeaderPtr->CompactedRecordDstIndex < RecordCount,
            "Invalid CompactedRecordDstIndex %lu",
            HeaderPtr->CompactedRecordDstIndex);

        std::memcpy(
            &RecordsPtr[HeaderPtr->CompactedRecordDstIndex],
            &RecordsPtr[HeaderPtr->CompactedRecordSrcIndex],
            sizeof(TRecord));

        RecordsPtr[HeaderPtr->CompactedRecordSrcIndex].State =
            ERecordState::Free;
        RecordsPtr[HeaderPtr->CompactedRecordDstIndex].State =
            ERecordState::Stored;

        // setting one of the indexes to InvalidIndex marks the completion of
        // record compaction
        HeaderPtr->CompactedRecordSrcIndex = InvalidIndex;
        HeaderPtr->CompactedRecordDstIndex = InvalidIndex;
    }
};

}   // namespace NCloud
