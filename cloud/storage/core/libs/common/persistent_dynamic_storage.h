#pragma once

#include "persistent_dynamic_table.h"

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud {

template <typename H>
class TPersistentDynamicStorage
{
private:
    TPersistentDynamicTable<H> Table;

public:
    static constexpr ui64 InvalidIndex =
        TPersistentDynamicTable<H>::InvalidIndex;

    using TIterator = typename TPersistentDynamicTable<H>::TIterator;

    TPersistentDynamicStorage(
        const TString& fileName,
        ui64 maxRecords,
        ui64 initialDataAreaSize = 1024 * 1024,
        ui8 gapSpacePercentageCompactionThreshold = 30)
        : Table(
              fileName,
              maxRecords,
              initialDataAreaSize,
              gapSpacePercentageCompactionThreshold)
    {}

    H* HeaderData()
    {
        return Table.HeaderData();
    }

    size_t CountRecords()
    {
        return Table.CountRecords();
    }

    void Clear()
    {
        Table.Clear();
    }

    ui64 CreateRecord(const TBuffer& record)
    {
        ui64 index = Table.AllocRecord(record.Size());
        if (index == InvalidIndex) {
            return InvalidIndex;
        }

        if (!Table.WriteRecordData(index, record.Data(), record.Size())) {
            return InvalidIndex;
        }

        Table.CommitRecord(index);

        return index;
    }

    bool DeleteRecord(ui64 index)
    {
        return Table.DeleteRecord(index);
    }

    TStringBuf GetRecord(ui64 index)
    {
        return Table.GetRecordWithValidation(index);
    }

    ui64 UpdateRecord(ui64 index, const TBuffer& record)
    {
        TStringBuf currentRecord = Table.GetRecord(index);
        if (currentRecord.empty()) {
            return InvalidIndex;
        }

        if (record.Size() <= currentRecord.size()) {
            if (!Table.WriteRecordData(index, record.Data(), record.Size())) {
                return InvalidIndex;
            }

            return index;
        }

        Table.DeleteRecord(index);

        return CreateRecord(record);
    }

    TIterator begin()
    {
        return Table.begin();
    }

    TIterator end()
    {
        return Table.end();
    }
};

}   // namespace NCloud
