#pragma once

#include <util/generic/list.h>
#include <util/generic/vector.h>
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
private:
    struct THeader
    {
        size_t HeaderSize = 0;
        size_t RecordSize = 0;
        size_t RecordCount = 0;
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

    TString FileName;
    size_t RecordCount = 0;
    size_t NextFreeRecord = 0;

    std::unique_ptr<TFileMap> FileMap;
    TList<ui64> FreeRecords;
    THeader* HeaderPtr = nullptr;
    TRecord* RecordsPtr = nullptr;

public:
    static constexpr ui64 InvalidIndex = -1;

    class TIterator
    {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = R;
        using difference_type = std::ptrdiff_t;
        using pointer = R*;
        using reference = R&;

        TIterator(TPersistentTable& table, ui64 index)
            : Index(index)
            , Table(table)
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
        ui64 Index;
        TPersistentTable& Table;
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
        // if file doesn't exist create file with zeroed header
        TFile file(FileName, OpenAlways | WrOnly);
        if (file.GetLength() == 0) {
            file.Resize(CalcFileSize(0));
        }
        file.Close();

        FileMap = std::make_unique<TFileMap>(FileName, TMemoryMapCommon::oRdWr);
        FileMap->Map(0, sizeof(THeader));

        auto* header = static_cast<THeader*>(FileMap->Ptr());
        if (header->RecordCount == 0) {
            header->RecordCount = RecordCount;
            header->HeaderSize = sizeof(THeader);
            header->RecordSize = sizeof(TRecord);
        }

        Y_ABORT_UNLESS(header->HeaderSize == sizeof(THeader));
        Y_ABORT_UNLESS(header->RecordSize == sizeof(TRecord));

        RecordCount = header->RecordCount;

        FileMap->ResizeAndRemap(0, CalcFileSize(RecordCount));
        HeaderPtr = static_cast<THeader*>(FileMap->Ptr());
        RecordsPtr = static_cast<TRecord*>((void*)(HeaderPtr + 1));

        CompactRecords();
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
        Y_ABORT_UNLESS(index < RecordCount);
        RecordsPtr[index].State = ERecordState::Free;
        if (index + 1 == NextFreeRecord) {
            NextFreeRecord--;
        } else {
            FreeRecords.push_back(index);
        }
    }

    size_t CountRecords()
    {
        return std::distance(begin(), end());
    }

private:
    size_t CalcFileSize(size_t recordCount)
    {
        return sizeof(THeader) + recordCount * sizeof(TRecord);
    }

    void CompactRecords()
    {
        ui64 writeRecordIndex = 0;
        ui64 readRecordIndex = 0;

        while (readRecordIndex < RecordCount) {
            if (RecordsPtr[readRecordIndex].State != ERecordState::Stored) {
                readRecordIndex++;
                continue;
            }

            if (writeRecordIndex != readRecordIndex) {
                std::memcpy(
                    &RecordsPtr[writeRecordIndex],
                    &RecordsPtr[readRecordIndex],
                    sizeof(TRecord));
                RecordsPtr[readRecordIndex].State = ERecordState::Free;
            }
            writeRecordIndex++;
            readRecordIndex++;
        }

        NextFreeRecord = writeRecordIndex;
    }
};

}   // namespace NCloud
