#include "persistent_table.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/list.h>
#include <util/random/random.h>

#include <iterator>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THeader
{
    ui64 Val;
};

struct TRecord
{
    ui64 Index;
    ui64 Val;
};

struct TReferenceImplementation
{
    ui32 MaxTableSize;
    ui32 NextFreeRecord;
    TList<ui64> FreeRecords;
    TMap<ui64, ui64> Records;

    TReferenceImplementation(ui32 tableSize)
        : MaxTableSize(tableSize)
        , NextFreeRecord(0)
    {}

    size_t CountRecords()
    {
        return Records.size();
    }

    ui64 AllocRecord()
    {
        if (!FreeRecords.empty()) {
            return FreeRecords.front();
        }

        if (NextFreeRecord < MaxTableSize) {
            return NextFreeRecord;
        }

        return TPersistentTable<THeader, TRecord>::InvalidIndex;
    }

    void CommitRecord(ui64 index, ui64 val)
    {
        Records[index] = val;
        if (auto count = FreeRecords.remove(index); count > 0) {
            UNIT_ASSERT_VALUES_EQUAL(1, count);
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL(NextFreeRecord, index);
        ++NextFreeRecord;
    }

    void DeleteRecord(ui64 index)
    {
        auto count = Records.erase(index);
        UNIT_ASSERT_VALUES_EQUAL(1, count);

        if (index + 1 == NextFreeRecord) {
            NextFreeRecord--;
            return;
        }

        FreeRecords.push_back(index);
    }

    ui64 RecordVal(ui64 index)
    {
        auto it = Records.find(index);
        UNIT_ASSERT_C(it != Records.end(), "index " << index << " not found");

        return it->second;
    }

    ui64 SomeRecord()
    {
        auto count = RandomNumber(Records.size());
        for (auto& [index, _]: Records) {
            if (count > 0) {
                count--;
                continue;
            }
            return index;
        }

        return Records.begin()->second;
    }

    void CompactRecords()
    {
        ui64 index = 0;
        TMap<ui64, ui64> newRecords;
        for (auto& [_, val]: Records) {
            newRecords[index++] = val;
        }

        Records = std::move(newRecords);
        NextFreeRecord = index;
        FreeRecords.clear();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPersistentTableTest)
{
    Y_UNIT_TEST(ShouldPersistHeader)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";
        TVector<ui64> headerValues = {123, 567, 99, 4};

        for (auto& val: headerValues) {
            {
                TPersistentTable<THeader, TRecord> table(tablePath, 32);
                auto* header = table.HeaderData();
                header->Val = val;
            }

            {
                TPersistentTable<THeader, TRecord> table(tablePath, 32);
                auto* header = table.HeaderData();
                UNIT_ASSERT_VALUES_EQUAL(val, header->Val);
            }
        }
    }

    Y_UNIT_TEST(ShouldPersistRecords)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";

        TVector<ui64> recordValues = {1, 344, 67, 68, 56};

        {
            TPersistentTable<THeader, TRecord> table(tablePath, 32);
            for (auto& val: recordValues) {
                auto index = table.AllocRecord();
                auto* record = table.RecordData(index);
                record->Val = val;
                record->Index = index;
                table.CommitRecord(index);
            }
        }

        {
            TPersistentTable<THeader, TRecord> table(tablePath, 32);
            UNIT_ASSERT_VALUES_EQUAL(recordValues.size(), table.CountRecords());

            auto it = table.begin();
            for (ui64 index = 0; index < recordValues.size(); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(index, it.GetIndex());
                UNIT_ASSERT_VALUES_EQUAL(index, it->Index);
                UNIT_ASSERT_VALUES_EQUAL(recordValues[index], it->Val);
                ++it;
            }
        }
    }

    Y_UNIT_TEST(ShouldLimitAllocatedRecords)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";

        auto tableSize = 32;

        {
            TPersistentTable<THeader, TRecord> table(tablePath, tableSize);
            for (auto i = 0; i < tableSize; ++i) {
                auto index = table.AllocRecord();
                UNIT_ASSERT_VALUES_UNEQUAL(table.InvalidIndex, index);
            }

            UNIT_ASSERT_VALUES_EQUAL(table.InvalidIndex, table.AllocRecord());
            table.DeleteRecord(0);
            UNIT_ASSERT_VALUES_UNEQUAL(table.InvalidIndex, table.AllocRecord());
        }
    }

    Y_UNIT_TEST(ShouldReuseDeletedRecords)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";

        TVector<ui64> recordValues = {1, 344, 67, 68, 56};
        {
            TPersistentTable<THeader, TRecord> table(tablePath, 32);
            for (auto& data: recordValues) {
                auto index = table.AllocRecord();
                table.RecordData(index)->Val = data;
                table.CommitRecord(index);
            }

            table.DeleteRecord(1);
            table.DeleteRecord(3);

            auto index = table.AllocRecord();
            UNIT_ASSERT_VALUES_EQUAL(1, index);

            index = table.AllocRecord();
            UNIT_ASSERT_VALUES_EQUAL(3, index);

            index = table.AllocRecord();
            UNIT_ASSERT_VALUES_EQUAL(recordValues.size(), index);
        }
    }

    Y_UNIT_TEST(ShouldCompactRecord)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";

        TVector<ui64> recordValues = {1, 344, 67, 68, 56};
        {
            TPersistentTable<THeader, TRecord> table(tablePath, 32);
            for (auto& data: recordValues) {
                auto index = table.AllocRecord();
                table.RecordData(index)->Index = index;
                table.RecordData(index)->Val = data;
                table.CommitRecord(index);
            }

            TVector<ui64> newRecordData;
            for (ui64 index = 0; index < recordValues.size(); ++index) {
                if (index % 2 == 0) {
                    newRecordData.push_back(recordValues[index]);
                } else {
                    table.DeleteRecord(index);
                }
            }
            recordValues = std::move(newRecordData);
        }

        {
            TPersistentTable<THeader, TRecord> table(tablePath, 32);
            ui64 index = 0;
            for (auto it = table.begin(); it != table.end(); ++index, ++it) {
                UNIT_ASSERT_LT(index, recordValues.size());
                UNIT_ASSERT_VALUES_EQUAL(index, it.GetIndex());
                UNIT_ASSERT_VALUES_EQUAL(recordValues[index], it->Val);
            }
            UNIT_ASSERT_VALUES_EQUAL(index, recordValues.size());
        }
    }

    Y_UNIT_TEST(ShouldResumeAbortedCompaction)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";

        using TTable = TPersistentTable<THeader, TRecord>;
        auto getTableHeader = [](auto& table) {
            auto *userHeader = table.HeaderData();
            auto* tableHeader = reinterpret_cast<TTable::THeader*>(
                reinterpret_cast<char*>(userHeader) -
                offsetof(TTable::THeader, Data));
            return tableHeader;
        };

        TVector<ui64> recordValues = {1, 344, 67, 68, 56};
        {
            TTable table(tablePath, 32);
            for (auto& data: recordValues) {
                auto index = table.AllocRecord();
                table.RecordData(index)->Index = index;
                table.RecordData(index)->Val = data;
                table.CommitRecord(index);
            }

            auto* tableHeader = getTableHeader(table);

            UNIT_ASSERT_VALUES_EQUAL(TTable::Version, tableHeader->Version);
            UNIT_ASSERT_VALUES_EQUAL(TTable::InvalidIndex, tableHeader->CompactedRecordSrcIndex);
            UNIT_ASSERT_VALUES_EQUAL(TTable::InvalidIndex, tableHeader->CompactedRecordDstIndex);

            // compaction copied entry 3 to 1 but restarted before setting
            // CompactedRecordSrcIndex/CompactedRecordDstIndex to InvalidIndex
            tableHeader->CompactedRecordSrcIndex = 3;
            tableHeader->CompactedRecordDstIndex = 1;
            table.DeleteRecord(tableHeader->CompactedRecordSrcIndex);

            recordValues[tableHeader->CompactedRecordDstIndex] =
                recordValues[tableHeader->CompactedRecordSrcIndex];
            recordValues.erase(recordValues.begin() + tableHeader->CompactedRecordSrcIndex);
        }

        {
            TTable table(tablePath, 32);
            ui64 index = 0;
            for (auto it = table.begin(); it != table.end(); ++index, ++it) {
                UNIT_ASSERT_LT(index, recordValues.size());
                UNIT_ASSERT_VALUES_EQUAL(index, it.GetIndex());
                UNIT_ASSERT_VALUES_EQUAL(recordValues[index], it->Val);
            }
            UNIT_ASSERT_VALUES_EQUAL(index, recordValues.size());

            auto* tableHeader = getTableHeader(table);

            UNIT_ASSERT_VALUES_EQUAL(TTable::Version, tableHeader->Version);
            UNIT_ASSERT_VALUES_EQUAL(TTable::InvalidIndex, tableHeader->CompactedRecordSrcIndex);
            UNIT_ASSERT_VALUES_EQUAL(TTable::InvalidIndex, tableHeader->CompactedRecordDstIndex);

            // compaction started setting the compacted indexes but restarted before coping the data
            tableHeader->CompactedRecordSrcIndex = 23;
            tableHeader->CompactedRecordDstIndex = TTable::InvalidIndex;
        }

        {
            TTable table(tablePath, 32);
            ui64 index = 0;
            for (auto it = table.begin(); it != table.end(); ++index, ++it) {
                UNIT_ASSERT_LT(index, recordValues.size());
                UNIT_ASSERT_VALUES_EQUAL(index, it.GetIndex());
                UNIT_ASSERT_VALUES_EQUAL(recordValues[index], it->Val);
            }
            UNIT_ASSERT_VALUES_EQUAL(index, recordValues.size());
        }
    }

    Y_UNIT_TEST(ShouldCountRecords)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";

        auto tableSize = 32;

        {
            TPersistentTable<THeader, TRecord> table(tablePath, tableSize);
            UNIT_ASSERT_VALUES_EQUAL(0, table.CountRecords());

            for (auto i = 0; i < tableSize; ++i) {
                auto index = table.AllocRecord();
                UNIT_ASSERT_VALUES_UNEQUAL(table.InvalidIndex, index);
                UNIT_ASSERT_VALUES_EQUAL(table.CountRecords(), index + 1);
            }

            ui32 deletedRecords = 0;
            for (auto i = 0; i < tableSize; ++i) {
                if (i % 2 == 0) {
                    ++deletedRecords;
                    table.DeleteRecord(i);
                    UNIT_ASSERT_VALUES_EQUAL(
                        table.CountRecords(),
                        tableSize - deletedRecords);
                }
            }
        }
    }

    Y_UNIT_TEST(RandomizedAllocDeleteRestore)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";

        const ui32 tableSize = 100;
        const ui32 testRecords = 1000;
        const double restoreProbability = 0.05;

        std::unique_ptr<TPersistentTable<THeader, TRecord>> table;
        TReferenceImplementation ri(tableSize);

        auto restore = [&]()
        {
            table = std::make_unique<TPersistentTable<THeader, TRecord>>(
                tablePath,
                tableSize);
            ri.CompactRecords();
        };

        restore();

        ui32 remainingRecords = testRecords;
        while (remainingRecords || ri.CountRecords()) {
            const bool shouldAlloc = remainingRecords && RandomNumber<bool>();
            // Cerr << "remainingRecords=" << remainingRecords
            //      << ", shouldAlloc=" << shouldAlloc << Endl;
            if (shouldAlloc) {
                auto index = table->AllocRecord();
                UNIT_ASSERT_VALUES_EQUAL(index, ri.AllocRecord());
                if (index != TPersistentTable<THeader, TRecord>::InvalidIndex) {
                    remainingRecords--;
                    auto *record = table->RecordData(index);
                    record->Val = RandomNumber<ui64>();
                    ri.CommitRecord(index, record->Val);
                    table->CommitRecord(index);
                    // Cerr << "commit index=" << index
                    //      << " ,value=" << record->Val << Endl;
                }
            } else {
                if (ri.CountRecords()) {
                    auto index = ri.SomeRecord();
                    auto* record = table->RecordData(index);
                    UNIT_ASSERT_VALUES_EQUAL(record->Val, ri.RecordVal(index));
                    ri.DeleteRecord(index);
                    table->DeleteRecord(index);
                    // Cerr << "delete index=" << index << Endl;
                }
            }

            if (RandomNumber<double>() < restoreProbability) {
                // Cerr << "restore" << Endl;
                restore();
            }

            UNIT_ASSERT_VALUES_EQUAL(ri.CountRecords(), table->CountRecords());
        }
    }

    Y_UNIT_TEST(ShouldClearRecords)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";

        auto tableSize = 32;

        auto validateFillEmptyTable = [&](auto table) {
            UNIT_ASSERT_VALUES_EQUAL(0, table->CountRecords());

            for (auto i = 0; i < tableSize; ++i) {
                auto index = table->AllocRecord();
                UNIT_ASSERT_VALUES_UNEQUAL(table->InvalidIndex, index);
                UNIT_ASSERT_VALUES_EQUAL(table->CountRecords(), index + 1);
                table->CommitRecord(index);
            }
        };

        using TTable = TPersistentTable<THeader, TRecord>;

        // validate non existing table can be filled
        auto table = std::make_shared<TTable>(tablePath, tableSize);
        validateFillEmptyTable(table);

        // validate cleared table can be filled
        table->Clear();
        validateFillEmptyTable(table);

        // validate new instance of cleared table can be filled
        table->Clear();
        table.reset();
        table = std::make_shared<TTable>(tablePath, tableSize);
        validateFillEmptyTable(table);
    }

    Y_UNIT_TEST(ShouldGrowTableIfRecordCountIncreased)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";
        auto initialTableSize = 32;
        auto increasedTableSize = 48;

        using TTable = TPersistentTable<THeader, TRecord>;

        auto indexToVal = [](auto index)
        {
            return 9900 + index;
        };

        // fill table with initialTableSize elements
        auto table = std::make_shared<TTable>(tablePath, initialTableSize);
        for (auto i = 0; i < initialTableSize; i++) {
            auto index = table->AllocRecord();
            UNIT_ASSERT_VALUES_UNEQUAL(table->InvalidIndex, index);
            UNIT_ASSERT_VALUES_EQUAL(table->CountRecords(), index + 1);
            table->RecordData(index)->Index = i;
            table->RecordData(index)->Val = indexToVal(i);
            table->CommitRecord(index);
        }

        // init table with increased increasedTableSize elements
        table = std::make_shared<TTable>(tablePath, increasedTableSize);
        UNIT_ASSERT_VALUES_EQUAL(initialTableSize, table->CountRecords());

        // validate previous elements remain in the table
        auto it = table->begin();
        for (auto i = 0; i < initialTableSize ; i++) {
            UNIT_ASSERT_C(it != table->end(), "index " << i << " not found");
            UNIT_ASSERT_VALUES_EQUAL(i, it.GetIndex());
            UNIT_ASSERT_VALUES_EQUAL(i, it->Index);
            UNIT_ASSERT_VALUES_EQUAL(indexToVal(i), it->Val);
            it++;
        }

        // validate that (increasedTableSize - initialTableSize) elements can be added
        for (auto i = initialTableSize; i < increasedTableSize ; i++) {
            auto index = table->AllocRecord();
            UNIT_ASSERT_VALUES_UNEQUAL(table->InvalidIndex, index);
            UNIT_ASSERT_VALUES_EQUAL(table->CountRecords(), index + 1);
            table->RecordData(index)->Index = i;
            table->RecordData(index)->Val = indexToVal(i);
            table->CommitRecord(index);
        }

        UNIT_ASSERT_VALUES_EQUAL(increasedTableSize, table->CountRecords());

        // validate table is full
        UNIT_ASSERT_VALUES_EQUAL(table->InvalidIndex, table->AllocRecord());
    }

    Y_UNIT_TEST(ShouldNotShrinkTableIfRecordCountDecreasedInOccupiedTable)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";
        auto initialTableSize = 32;
        auto decreasedTableSize = 16;

        using TTable = TPersistentTable<THeader, TRecord>;

        auto indexToVal = [](auto index)
        {
            return 9900 + index;
        };

        // fill table with initialTableSize elements
        auto table = std::make_shared<TTable>(tablePath, initialTableSize);
        for (auto i = 0; i < initialTableSize; i++) {
            auto index = table->AllocRecord();
            UNIT_ASSERT_VALUES_UNEQUAL(table->InvalidIndex, index);
            UNIT_ASSERT_VALUES_EQUAL(table->CountRecords(), index + 1);
            table->RecordData(index)->Index = i;
            table->RecordData(index)->Val = indexToVal(i);
            table->CommitRecord(index);
        }

        // init table with decreased decreasedTableSize elements
        table = std::make_shared<TTable>(tablePath, decreasedTableSize);
        UNIT_ASSERT_VALUES_EQUAL(initialTableSize, table->CountRecords());

        // validate previous elements remain in the table
        auto it = table->begin();
        for (auto i = 0; i < initialTableSize ; i++) {
            UNIT_ASSERT_C(it != table->end(), "index " << i << " not found");
            UNIT_ASSERT_VALUES_EQUAL(i, it.GetIndex());
            UNIT_ASSERT_VALUES_EQUAL(i, it->Index);
            UNIT_ASSERT_VALUES_EQUAL(indexToVal(i), it->Val);
            it++;
        }

        // validate table is full
        UNIT_ASSERT_VALUES_EQUAL(table->InvalidIndex, table->AllocRecord());
    }

    Y_UNIT_TEST(ShouldShrinkTableIfRecordCountDecreasedInPartiallyOccupiedTable)
    {
        TTempDir dir;
        auto tablePath = dir.Path() / "table";
        auto initialTableSize = 32;
        auto decreasedTableSize = initialTableSize / 2;

        using TTable = TPersistentTable<THeader, TRecord>;

        auto indexToVal = [](auto index)
        {
            return 9900 + index;
        };

        // fill table with initialTableSize elements
        auto table = std::make_shared<TTable>(tablePath, initialTableSize);
        for (auto i = 0; i < initialTableSize; i++) {
            auto index = table->AllocRecord();
            UNIT_ASSERT_VALUES_UNEQUAL(table->InvalidIndex, index);
            UNIT_ASSERT_VALUES_EQUAL(table->CountRecords(), index + 1);
            table->RecordData(index)->Index = i;
            table->RecordData(index)->Val = indexToVal(i);
            table->CommitRecord(index);
        }

        // delete odd elements
        for (auto i = 0; i < initialTableSize; i++) {
            if (i % 2 != 0) {
                table->DeleteRecord(i);
            }
        }

        // init table with decreased decreasedTableSize elements
        table = std::make_shared<TTable>(tablePath, decreasedTableSize);
        UNIT_ASSERT_VALUES_EQUAL(decreasedTableSize, table->CountRecords());

        // validate previous elements remain in the table
        auto it = table->begin();
        for (auto i = 0; i < decreasedTableSize ; i++) {
            UNIT_ASSERT_C(it != table->end(), "index " << i << " not found");
            UNIT_ASSERT_VALUES_EQUAL(i, it.GetIndex());
            UNIT_ASSERT_VALUES_EQUAL(i*2, it->Index);
            UNIT_ASSERT_VALUES_EQUAL(indexToVal(i*2), it->Val);
            it++;
        }

        // validate table is full
        UNIT_ASSERT_VALUES_EQUAL(table->InvalidIndex, table->AllocRecord());

        // validate we can allocate after deleting entry
        table->DeleteRecord(decreasedTableSize-1);
        UNIT_ASSERT_VALUES_EQUAL(decreasedTableSize-1, table->AllocRecord());

    }
}

}   // namespace NCloud
