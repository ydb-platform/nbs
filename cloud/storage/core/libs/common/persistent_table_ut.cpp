#include "persistent_table.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>

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
                table.StoreRecord(index);
            }
        }

        {
            TPersistentTable<THeader, TRecord> table(tablePath, 32);
            UNIT_ASSERT_VALUES_EQUAL(recordValues.size(), table.CountRecords());

            auto it = table.begin();
            for (ui64 index = 0; index < recordValues.size(); index++) {
                UNIT_ASSERT_VALUES_EQUAL(index, it.GetIndex());
                UNIT_ASSERT_VALUES_EQUAL(index, it->Index);
                UNIT_ASSERT_VALUES_EQUAL(recordValues[index], it->Val);
                it++;
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
            for (auto i = 0; i < tableSize; i++) {
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
                table.StoreRecord(index);
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
                table.StoreRecord(index);
            }

            TVector<ui64> newRecordData;
            for (ui64 index = 0; index < recordValues.size(); index++) {
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
            for (auto it = table.begin(); it != table.end(); index++, it++) {
                UNIT_ASSERT_LT(index, recordValues.size());
                UNIT_ASSERT_VALUES_EQUAL(index, it.GetIndex());
                UNIT_ASSERT_VALUES_EQUAL(recordValues[index], it->Val);
            }
            UNIT_ASSERT_VALUES_EQUAL(index, recordValues.size());
        }
    }
}

}   // namespace NCloud
