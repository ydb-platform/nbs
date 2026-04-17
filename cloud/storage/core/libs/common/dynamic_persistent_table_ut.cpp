#include "dynamic_persistent_table.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/map.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>

#include <cstring>
#include <memory>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestHeader
{
    ui64 Val = 0;
};

struct TTestData
{
    ui64 Id;
    TString Name;
    TVector<ui32> Values;

    TString Serialize() const
    {
        TString result;
        result.reserve(
            sizeof(Id) + sizeof(ui32) + Name.size() + sizeof(ui32) +
            Values.size() * sizeof(ui32));

        result.append(reinterpret_cast<const char*>(&Id), sizeof(Id));

        ui32 nameLen = Name.size();
        result.append(reinterpret_cast<const char*>(&nameLen), sizeof(nameLen));
        result.append(Name.data(), Name.size());

        ui32 valuesCount = Values.size();
        result.append(
            reinterpret_cast<const char*>(&valuesCount),
            sizeof(valuesCount));
        result.append(
            reinterpret_cast<const char*>(Values.data()),
            Values.size() * sizeof(ui32));

        return result;
    }

    static TTestData Deserialize(const char* data, size_t size)
    {
        TTestData result;
        const char* ptr = data;

        if (size < sizeof(ui64)) {
            return result;
        }
        std::memcpy(&result.Id, ptr, sizeof(ui64));
        ptr += sizeof(ui64);
        size -= sizeof(ui64);

        if (size < sizeof(ui32)) {
            return result;
        }
        ui32 nameLen;
        std::memcpy(&nameLen, ptr, sizeof(ui32));
        ptr += sizeof(ui32);
        size -= sizeof(ui32);

        if (size < nameLen) {
            return result;
        }
        result.Name = TString(ptr, nameLen);
        ptr += nameLen;
        size -= nameLen;

        if (size < sizeof(ui32)) {
            return result;
        }
        ui32 valuesCount;
        std::memcpy(&valuesCount, ptr, sizeof(ui32));
        ptr += sizeof(ui32);
        size -= sizeof(ui32);

        if (size < valuesCount * sizeof(ui32)) {
            return result;
        }
        result.Values.resize(valuesCount);
        if (valuesCount > 0) {
            std::memcpy(result.Values.data(), ptr, valuesCount * sizeof(ui32));
        }

        return result;
    }

    bool operator==(const TTestData& other) const
    {
        return Id == other.Id && Name == other.Name && Values == other.Values;
    }
};

using TTable = TDynamicPersistentTable<TTestHeader>;

TTable CreateTable(
    const TString& fileName,
    ui64 maxRecords,
    ui64 initialDataAreaSize,
    ui64 maxDataAreaStepSize = 1_GB,
    ui64 initialDataCompactionBufferSize = 100,
    ui64 gapSpacePercentageCompactionThreshold =
        TTable::DefaultGapSpacePercentageCompactionThreshold,
    ui64 shrinkLowMemoryOpThreshold = TTable::DefaultShrinkLowMemoryOpThreshold,
    ui64 shrinkTriggerPercent = TTable::DefaultShrinkTriggerPercent,
    ui64 shrinkReservePercent = TTable::DefaultShrinkReservePercent,
    TTable::EShrinkMode shrinkMode = TTable::DefaultShrinkMode)
{
    return TTable(
        fileName,
        maxRecords,
        initialDataAreaSize,
        maxDataAreaStepSize,
        initialDataCompactionBufferSize,
        gapSpacePercentageCompactionThreshold,
        shrinkLowMemoryOpThreshold,
        shrinkTriggerPercent,
        shrinkReservePercent,
        shrinkMode);
}

// Helper functions for accessing internal table structures
TTable::THeader* GetTableHeader(TTable& table)
{
    TTestHeader* userHeader = table.HeaderData();
    TTable::THeader* tableHeader = reinterpret_cast<TTable::THeader*>(
        reinterpret_cast<char*>(userHeader) - offsetof(TTable::THeader, Data));
    return tableHeader;
}

std::tuple<TTable::THeader*, TTable::TRecordDescriptor*, char*>
GetTableInternals(TTable& table)
{
    auto* tableHeader = GetTableHeader(table);

    TTable::TRecordDescriptor* descriptorsPtr =
        reinterpret_cast<TTable::TRecordDescriptor*>(
            reinterpret_cast<char*>(tableHeader) + tableHeader->HeaderSize);

    char* dataPtr =
        reinterpret_cast<char*>(tableHeader) + tableHeader->DataAreaOffset;

    return std::make_tuple(tableHeader, descriptorsPtr, dataPtr);
}

ui64 AllocAndCommitRecord(TTable& table, const TString& data)
{
    ui64 index = table.AllocRecord(data.size());
    UNIT_ASSERT_VALUES_UNEQUAL(TTable::InvalidIndex, index);
    UNIT_ASSERT(table.WriteRecordData(index, data.data(), data.size()));
    UNIT_ASSERT(table.CommitRecord(index));
    return index;
}

TVector<ui64>
FillTable(TTable& table, ui64 recordsCount, const TString& payload)
{
    TVector<ui64> indices;
    indices.reserve(recordsCount);
    for (ui64 i = 0; i < recordsCount; ++i) {
        indices.push_back(AllocAndCommitRecord(table, payload));
    }
    return indices;
}

void DeleteRecords(TTable& table, const TVector<ui64>& indices, ui64 fromIndex)
{
    for (ui64 i = fromIndex; i < indices.size(); ++i) {
        UNIT_ASSERT(table.DeleteRecord(indices[i]));
    }
}

TVector<TTestData> TestDataRecords = {
    {1, "first", {10, 20}},
    {2, "second", {30, 40}},
    {3, "third", {50, 60}},
    {4, "fourth", {70, 80}},
    {5, "fifth", {90, 100}}};

struct TReferenceImplementation
{
    ui32 MaxTableSize;
    ui32 NextFreeRecord = 0;
    TVector<TString> Records;

    TDeque<ui64> FreeRecords;

    TReferenceImplementation(ui32 tableSize)
        : MaxTableSize(tableSize)
        , Records(tableSize)
    {}

    size_t CountRecords()
    {
        return NextFreeRecord - FreeRecords.size();
    }

    ui64 AllocRecord(ui64 dataSize)
    {
        if (dataSize == 0) {
            return TDynamicPersistentTable<TTestHeader>::InvalidIndex;
        }

        if (!FreeRecords.empty()) {
            ui64 index = FreeRecords.front();
            FreeRecords.pop_front();
            return index;
        }

        if (NextFreeRecord < MaxTableSize) {
            return NextFreeRecord++;
        }

        return TDynamicPersistentTable<TTestHeader>::InvalidIndex;
    }

    void CommitRecord(ui64 index, const TString& data)
    {
        Records[index] = data;
    }

    void DeleteRecord(ui64 index)
    {
        UNIT_ASSERT(!Records[index].empty());
        Records[index].clear();

        FreeRecords.push_back(index);
    }

    TString GetRecord(ui64 index)
    {
        if (index < Records.size()) {
            return Records[index];
        }
        return TString();
    }

    ui64 SomeRecord()
    {
        TVector<ui64> validIndices;
        for (ui32 i = 0; i < NextFreeRecord; ++i) {
            if (!Records[i].empty()) {
                validIndices.push_back(i);
            }
        }

        if (validIndices.empty()) {
            return TDynamicPersistentTable<TTestHeader>::InvalidIndex;
        }

        return validIndices[RandomNumber(validIndices.size())];
    }

    void Compact()
    {
        int writeIndex = 0;
        for (ui32 i = 0; i < NextFreeRecord; ++i) {
            if (!Records[i].empty()) {
                Records[writeIndex++] = Records[i];
            }
        }

        NextFreeRecord = writeIndex;
        FreeRecords.clear();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDynamicPersistentTableTest)
{
    Y_UNIT_TEST(ShouldCreateEmptyTable)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        {
            auto table = CreateTable(tablePath, 32, 1024);
            UNIT_ASSERT_VALUES_EQUAL(0, table.CountRecords());

            auto* header = table.HeaderData();
            UNIT_ASSERT(header != nullptr);
            header->Val = 42;
        }

        {
            auto table = CreateTable(tablePath, 32, 1024);
            UNIT_ASSERT_VALUES_EQUAL(0, table.CountRecords());
            UNIT_ASSERT_VALUES_EQUAL(42, table.HeaderData()->Val);
        }
    }

    Y_UNIT_TEST(ShouldAllocAndStoreVariableSizeRecords)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 32, 1024);

        TTestData smallData{1, "small", {1, 2, 3}};
        TTestData largeData{
            2,
            "large_name_with_more_characters",
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}};

        TString smallSerialized = smallData.Serialize();
        TString largeSerialized = largeData.Serialize();

        ui64 smallIndex = table.AllocRecord(smallSerialized.size());
        UNIT_ASSERT(
            smallIndex != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
        UNIT_ASSERT(table.WriteRecordData(
            smallIndex,
            smallSerialized.data(),
            smallSerialized.size()));
        table.CommitRecord(smallIndex);

        ui64 largeIndex = table.AllocRecord(largeSerialized.size());
        UNIT_ASSERT(
            largeIndex != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
        UNIT_ASSERT(table.WriteRecordData(
            largeIndex,
            largeSerialized.data(),
            largeSerialized.size()));
        table.CommitRecord(largeIndex);

        UNIT_ASSERT_VALUES_EQUAL(2, table.CountRecords());

        TStringBuf smallRecord = table.GetRecordWithValidation(smallIndex);
        UNIT_ASSERT(!smallRecord.empty());
        TTestData recoveredSmall =
            TTestData::Deserialize(smallRecord.data(), smallRecord.size());
        UNIT_ASSERT(recoveredSmall == smallData);

        TStringBuf largeRecord = table.GetRecordWithValidation(largeIndex);
        UNIT_ASSERT(!largeRecord.empty());
        TTestData recoveredLarge =
            TTestData::Deserialize(largeRecord.data(), largeRecord.size());
        UNIT_ASSERT(recoveredLarge == largeData);
    }

    Y_UNIT_TEST(ShouldIterateOverStoredRecords)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 32, 1024);

        TVector<TTestData> testRecords = {
            {1, "first", {1, 2}},
            {2, "second", {3, 4, 5}},
            {3, "third", {6, 7, 8, 9}}};

        TVector<ui64> indices;
        for (const auto& testData: testRecords) {
            TString serialized = testData.Serialize();
            ui64 index = table.AllocRecord(serialized.size());
            UNIT_ASSERT(
                index != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
            UNIT_ASSERT(table.WriteRecordData(
                index,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index);
            indices.push_back(index);
        }

        size_t count = 0;
        for (auto it = table.begin(); it != table.end(); ++it) {
            TStringBuf record = *it;

            UNIT_ASSERT(!record.empty());

            TTestData recovered =
                TTestData::Deserialize(record.data(), record.size());

            UNIT_ASSERT(recovered == testRecords[count]);

            ++count;
        }

        UNIT_ASSERT_VALUES_EQUAL(testRecords.size(), count);
    }

    Y_UNIT_TEST(ShouldDeleteRecords)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 32, 1024);

        TTestData data1{1, "first", {1, 2}};
        TTestData data2{2, "second", {3, 4}};

        TString serialized1 = data1.Serialize();
        TString serialized2 = data2.Serialize();

        ui64 index1 = table.AllocRecord(serialized1.size());
        UNIT_ASSERT(table.WriteRecordData(
            index1,
            serialized1.data(),
            serialized1.size()));
        table.CommitRecord(index1);

        ui64 index2 = table.AllocRecord(serialized2.size());
        UNIT_ASSERT(table.WriteRecordData(
            index2,
            serialized2.data(),
            serialized2.size()));
        table.CommitRecord(index2);

        UNIT_ASSERT_VALUES_EQUAL(2, table.CountRecords());

        table.DeleteRecord(index1);
        UNIT_ASSERT_VALUES_EQUAL(1, table.CountRecords());

        UNIT_ASSERT(table.GetRecord(index1).empty());
        UNIT_ASSERT(!table.GetRecord(index2).empty());
    }

    Y_UNIT_TEST(ShouldReuseDeletedRecordSlots)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 4, 1024);

        TVector<ui64> indices;
        for (int i = 0; i < 4; ++i) {
            TTestData data{
                static_cast<ui64>(i),
                TStringBuilder() << "item" << i,
                {static_cast<ui32>(i)}};
            TString serialized = data.Serialize();
            ui64 index = table.AllocRecord(serialized.size());
            UNIT_ASSERT(
                index != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
            UNIT_ASSERT(table.WriteRecordData(
                index,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index);
            indices.push_back(index);
        }

        UNIT_ASSERT_VALUES_EQUAL(4, table.CountRecords());

        TTestData extraData{999, "extra", {999}};
        TString extraSerialized = extraData.Serialize();
        ui64 extraIndex = table.AllocRecord(extraSerialized.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TDynamicPersistentTable<TTestHeader>::InvalidIndex,
            extraIndex);

        table.DeleteRecord(indices[1]);
        UNIT_ASSERT_VALUES_EQUAL(3, table.CountRecords());

        ui64 newIndex = table.AllocRecord(extraSerialized.size());
        UNIT_ASSERT(
            newIndex != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
        UNIT_ASSERT_VALUES_EQUAL(indices[1], newIndex);
    }

    Y_UNIT_TEST(ShouldPersistAcrossRestarts)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        TVector<TTestData> testData = {
            {100, "persistent_first", {10, 20, 30}},
            {200, "persistent_second", {40, 50, 60, 70}}};

        {
            auto table = CreateTable(tablePath, 32, 1024);
            table.HeaderData()->Val = 123;

            for (const auto& data: testData) {
                TString serialized = data.Serialize();
                ui64 index = table.AllocRecord(serialized.size());
                UNIT_ASSERT(table.WriteRecordData(
                    index,
                    serialized.data(),
                    serialized.size()));
                table.CommitRecord(index);
            }

            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());
        }

        {
            auto table = CreateTable(tablePath, 32, 1024);
            UNIT_ASSERT_VALUES_EQUAL(123, table.HeaderData()->Val);
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());

            THashSet<ui64> foundIds;
            for (auto it = table.begin(); it != table.end(); ++it) {
                TStringBuf record = *it;

                TTestData recovered =
                    TTestData::Deserialize(record.data(), record.size());
                foundIds.insert(recovered.Id);

                bool found = false;
                for (const auto& original: testData) {
                    if (recovered == original) {
                        found = true;
                        break;
                    }
                }
                UNIT_ASSERT(found);
            }

            UNIT_ASSERT_VALUES_EQUAL(testData.size(), foundIds.size());
        }
    }

    Y_UNIT_TEST(ShouldCompactCorrectly)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        TVector<TTestData> testData = {
            {100, "persistent_first", {10, 20, 30}},
            {200, "persistent_second", {40, 50, 60, 70}},
            {300, "persistent_third", {80, 90, 100, 110, 120}}};

        ui64 index1 = TDynamicPersistentTable<TTestHeader>::InvalidIndex;
        ui64 index2 = TDynamicPersistentTable<TTestHeader>::InvalidIndex;

        {
            auto table = CreateTable(tablePath, 32, 1024);
            table.HeaderData()->Val = 123;

            TString serialized = testData[0].Serialize();
            index1 = table.AllocRecord(serialized.size());
            UNIT_ASSERT(table.WriteRecordData(
                index1,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index1);

            serialized = testData[1].Serialize();
            index2 = table.AllocRecord(serialized.size());
            UNIT_ASSERT(table.WriteRecordData(
                index2,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index2);

            table.DeleteRecord(index1);

            serialized = testData[2].Serialize();
            ui64 index3 = table.AllocRecord(serialized.size());
            UNIT_ASSERT(table.WriteRecordData(
                index3,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index3);

            UNIT_ASSERT_VALUES_EQUAL(index3, index1);

            UNIT_ASSERT_VALUES_EQUAL(2, table.CountRecords());
        }

        {
            auto table = CreateTable(tablePath, 32, 1024);
            UNIT_ASSERT_VALUES_EQUAL(123, table.HeaderData()->Val);
            UNIT_ASSERT_VALUES_EQUAL(2, table.CountRecords());

            TStringBuf record = table.GetRecord(index1);

            TTestData recovered =
                TTestData::Deserialize(record.data(), record.size());

            UNIT_ASSERT(recovered == testData[2]);

            record = table.GetRecord(index2);
            recovered = TTestData::Deserialize(record.data(), record.size());
            UNIT_ASSERT(recovered == testData[1]);
        }
    }

    Y_UNIT_TEST(ShouldHandleDataAreaExpansion)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 100, 1024);

        TVector<ui64> indices;
        for (int i = 0; i < 50; ++i) {
            TTestData data{
                static_cast<ui64>(i),
                TStringBuilder()
                    << "large_record_name_" << i << "_with_long_suffix",
                TVector<ui32>(100, i)};

            TString serialized = data.Serialize();
            ui64 index = table.AllocRecord(serialized.size());
            UNIT_ASSERT(
                index != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
            UNIT_ASSERT(table.WriteRecordData(
                index,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index);
            indices.push_back(index);
        }

        UNIT_ASSERT_VALUES_EQUAL(50, table.CountRecords());

        for (size_t i = 0; i < indices.size(); ++i) {
            TStringBuf record = table.GetRecord(indices[i]);
            UNIT_ASSERT(!record.empty());

            TTestData recovered =
                TTestData::Deserialize(record.data(), record.size());
            UNIT_ASSERT_VALUES_EQUAL(i, recovered.Id);
        }
    }

    Y_UNIT_TEST(ShouldTriggerFileMapExpansionWhenDataAreaFull)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 10, 256);

        TVector<ui64> indices;
        size_t totalUsed = 0;

        for (int i = 0; i < 20; ++i) {
            TTestData data{
                static_cast<ui64>(i),
                TStringBuilder() << "record" << i,
                {static_cast<ui32>(i)}};
            TString serialized = data.Serialize();

            ui64 index = table.AllocRecord(serialized.size());
            if (index == TDynamicPersistentTable<TTestHeader>::InvalidIndex) {
                break;
            }

            UNIT_ASSERT(table.WriteRecordData(
                index,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index);
            indices.push_back(index);
            totalUsed += serialized.size();
        }

        // Test verifies automatic file expansion beyond initial 256 bytes
        UNIT_ASSERT_LT(256, totalUsed);
        UNIT_ASSERT(indices.size() > 5);

        for (size_t i = 0; i < indices.size(); ++i) {
            TStringBuf record = table.GetRecord(indices[i]);
            UNIT_ASSERT(!record.empty());

            TTestData recovered =
                TTestData::Deserialize(record.data(), record.size());
            UNIT_ASSERT_VALUES_EQUAL(i, recovered.Id);
        }
    }

    Y_UNIT_TEST(ShouldUpdateRecordWithDecreasedSize)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 32, 1024);

        TTestData originalData{
            1,
            "original_large_name_with_extra_content",
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}};
        TString originalSerialized = originalData.Serialize();

        ui64 index = table.AllocRecord(originalSerialized.size());
        UNIT_ASSERT_VALUES_UNEQUAL(
            TDynamicPersistentTable<TTestHeader>::InvalidIndex,
            index);
        UNIT_ASSERT(table.WriteRecordData(
            index,
            originalSerialized.data(),
            originalSerialized.size()));
        table.CommitRecord(index);

        TStringBuf originalRecord = table.GetRecord(index);
        UNIT_ASSERT_VALUES_EQUAL(
            originalSerialized.size(),
            originalRecord.size());

        TTestData updatedData{1, "small", {1, 2}};
        TString updatedSerialized = updatedData.Serialize();
        UNIT_ASSERT(updatedSerialized.size() < originalSerialized.size());

        UNIT_ASSERT(table.WriteRecordData(
            index,
            updatedSerialized.data(),
            updatedSerialized.size()));

        TStringBuf updatedRecord = table.GetRecord(index);
        UNIT_ASSERT_VALUES_EQUAL(
            updatedSerialized.size(),
            updatedRecord.size());

        TTestData recoveredData =
            TTestData::Deserialize(updatedRecord.data(), updatedRecord.size());
        UNIT_ASSERT(recoveredData == updatedData);
    }

    Y_UNIT_TEST(ShouldCompactDataAreaAndReuseSpace)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 20, 512);

        TVector<ui64> indices;
        for (int i = 0; i < 10; ++i) {
            TTestData data{
                static_cast<ui64>(i),
                TStringBuilder() << "record_" << i << "_with_content",
                {static_cast<ui32>(i), static_cast<ui32>(i + 1)}};
            TString serialized = data.Serialize();

            ui64 index = table.AllocRecord(serialized.size());
            if (index == TDynamicPersistentTable<TTestHeader>::InvalidIndex) {
                break;
            }

            UNIT_ASSERT(table.WriteRecordData(
                index,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index);
            indices.push_back(index);
        }

        size_t recordsBeforeDelete = table.CountRecords();
        UNIT_ASSERT(recordsBeforeDelete > 3);

        // Create fragmentation by deleting every other record
        for (size_t i = 1; i < indices.size(); i += 2) {
            table.DeleteRecord(indices[i]);
        }

        size_t recordsAfterDelete = table.CountRecords();
        UNIT_ASSERT(recordsAfterDelete < recordsBeforeDelete);

        // Test that large record fits after compaction despite fragmentation
        TTestData largeData{
            999,
            "large_record_that_needs_compaction_to_fit_in_available_space",
            TVector<ui32>(20, 999)};
        TString largeSerialized = largeData.Serialize();

        ui64 newIndex = table.AllocRecord(largeSerialized.size());
        UNIT_ASSERT(
            newIndex != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
        UNIT_ASSERT(table.WriteRecordData(
            newIndex,
            largeSerialized.data(),
            largeSerialized.size()));
        table.CommitRecord(newIndex);

        TStringBuf largeRecord = table.GetRecord(newIndex);
        UNIT_ASSERT(!largeRecord.empty());

        TTestData recovered =
            TTestData::Deserialize(largeRecord.data(), largeRecord.size());
        UNIT_ASSERT(recovered == largeData);

        for (size_t i = 0; i < indices.size(); i += 2) {
            TStringBuf remainingRecord = table.GetRecord(indices[i]);
            UNIT_ASSERT(!remainingRecord.empty());
        }
    }

    Y_UNIT_TEST(ShouldFailWhenMaxRecordsLimitReached)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 3, 1024);

        TVector<ui64> indices;

        for (int i = 0; i < 3; ++i) {
            TTestData data{
                static_cast<ui64>(i),
                TStringBuilder() << "record" << i,
                {static_cast<ui32>(i)}};
            TString serialized = data.Serialize();

            ui64 index = table.AllocRecord(serialized.size());
            UNIT_ASSERT(
                index != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
            UNIT_ASSERT(table.WriteRecordData(
                index,
                serialized.data(),
                serialized.size()));
            table.CommitRecord(index);
            indices.push_back(index);
        }

        UNIT_ASSERT_VALUES_EQUAL(3, table.CountRecords());

        TTestData extraData{999, "extra", {999}};
        TString extraSerialized = extraData.Serialize();
        ui64 extraIndex = table.AllocRecord(extraSerialized.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TDynamicPersistentTable<TTestHeader>::InvalidIndex,
            extraIndex);

        UNIT_ASSERT_VALUES_EQUAL(3, table.CountRecords());
        for (size_t i = 0; i < indices.size(); ++i) {
            TStringBuf record = table.GetRecord(indices[i]);
            UNIT_ASSERT(!record.empty());
        }

        // Test slot reuse after deletion
        table.DeleteRecord(indices[1]);
        UNIT_ASSERT_VALUES_EQUAL(2, table.CountRecords());

        ui64 newIndex = table.AllocRecord(extraSerialized.size());
        UNIT_ASSERT(
            newIndex != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
        UNIT_ASSERT_VALUES_EQUAL(indices[1], newIndex);
        UNIT_ASSERT(table.WriteRecordData(
            newIndex,
            extraSerialized.data(),
            extraSerialized.size()));
        table.CommitRecord(newIndex);

        UNIT_ASSERT_VALUES_EQUAL(3, table.CountRecords());
    }

    Y_UNIT_TEST(ShouldHandleZeroSizeData)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        auto table = CreateTable(tablePath, 32, 1024);

        ui64 index = table.AllocRecord(0);
        UNIT_ASSERT(
            index == TDynamicPersistentTable<TTestHeader>::InvalidIndex);

        TTestData emptyData{42, "", {}};
        TString emptySerialized = emptyData.Serialize();

        ui64 emptyIndex = table.AllocRecord(emptySerialized.size());
        UNIT_ASSERT(
            emptyIndex != TDynamicPersistentTable<TTestHeader>::InvalidIndex);
        UNIT_ASSERT(table.WriteRecordData(
            emptyIndex,
            emptySerialized.data(),
            emptySerialized.size()));
        table.CommitRecord(emptyIndex);

        TStringBuf emptyRecord = table.GetRecord(emptyIndex);
        UNIT_ASSERT(!emptyRecord.empty());
        TTestData recoveredEmpty =
            TTestData::Deserialize(emptyRecord.data(), emptyRecord.size());
        UNIT_ASSERT(recoveredEmpty == emptyData);

        UNIT_ASSERT_VALUES_EQUAL(1, table.CountRecords());

        TTestData normalData{99, "normal", {1, 2, 3}};
        TString normalSerialized = normalData.Serialize();

        ui64 normalIndex = table.AllocRecord(normalSerialized.size());
        UNIT_ASSERT(!table.WriteRecordData(
            normalIndex,
            nullptr,
            normalSerialized.size()));
        UNIT_ASSERT(table.WriteRecordData(
            normalIndex,
            normalSerialized.data(),
            normalSerialized.size()));

        UNIT_ASSERT(
            table.WriteRecordData(normalIndex, normalSerialized.data(), 0));
        UNIT_ASSERT(!table.WriteRecordData(
            normalIndex,
            normalSerialized.data(),
            normalSerialized.size()));
        table.CommitRecord(normalIndex);

        TStringBuf normalRecord = table.GetRecord(normalIndex);
        UNIT_ASSERT_VALUES_EQUAL(0, normalRecord.size());

        UNIT_ASSERT_VALUES_EQUAL(2, table.CountRecords());
    }

    Y_UNIT_TEST(ShouldShrinkDataAreaOnStartupAfterHistoricalSpike)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "startup_shrink.table";

        const ui64 initialDataAreaSize = 4_KB;
        const ui64 dataSize = 1_KB;
        const ui32 totalRecords = 9;
        const ui32 keptRecords = 2;

        TVector<ui64> indices;
        TVector<TString> keptPayloads;
        ui64 grownDataAreaSize = 0;

        {
            auto table =
                CreateTable(tablePath, 32, initialDataAreaSize, 1_GB, 8_KB);

            for (ui32 i = 0; i < totalRecords; ++i) {
                TString payload(dataSize, static_cast<char>('a' + (i % 26)));

                indices.push_back(AllocAndCommitRecord(table, payload));
                if (i < keptRecords) {
                    keptPayloads.push_back(payload);
                }
            }

            grownDataAreaSize = GetTableHeader(table)->DataAreaSize;
            UNIT_ASSERT_VALUES_EQUAL(16_KB, grownDataAreaSize);

            for (ui32 i = keptRecords; i < indices.size(); ++i) {
                UNIT_ASSERT(table.DeleteRecord(indices[i]));
            }
            UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());
        }

        {
            auto table =
                CreateTable(tablePath, 32, initialDataAreaSize, 1_GB, 8_KB);

            auto* header = GetTableHeader(table);
            UNIT_ASSERT_VALUES_EQUAL(8_KB, header->DataAreaSize);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                header->DataAreaSize % initialDataAreaSize);
            UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());

            for (ui32 i = 0; i < keptRecords; ++i) {
                TStringBuf record = table.GetRecordWithValidation(i);
                UNIT_ASSERT_VALUES_EQUAL(keptPayloads[i], TString(record));
            }
        }
    }

    Y_UNIT_TEST(ShouldShrinkAfterAllocationOnLowMemoryOp)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "runtime_shrink_alloc.table";

        const ui64 initialDataAreaSize = 4_KB;
        const TString payload(1_KB, 'a');
        const ui64 shrinkLowMemoryOpThreshold = 3;
        const ui64 totalRecords = 9;
        const ui64 keptRecords = 3;

        auto table = CreateTable(
            tablePath,
            32,
            initialDataAreaSize,
            1_GB,
            1_KB,
            TTable::DefaultGapSpacePercentageCompactionThreshold,
            shrinkLowMemoryOpThreshold);

        TVector<ui64> indices = FillTable(table, totalRecords, payload);

        auto* header = GetTableHeader(table);
        UNIT_ASSERT_VALUES_EQUAL(16_KB, header->DataAreaSize);
        UNIT_ASSERT_VALUES_EQUAL(totalRecords, indices.size());

        DeleteRecords(table, indices, keptRecords);
        UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());

        const ui64 dataAreaSizeBeforeShrink =
            GetTableHeader(table)->DataAreaSize;
        const ui64 newIndex = AllocAndCommitRecord(table, payload);
        const ui64 dataAreaSizeAfterShrink =
            GetTableHeader(table)->DataAreaSize;

        UNIT_ASSERT_VALUES_EQUAL(16_KB, dataAreaSizeBeforeShrink);
        UNIT_ASSERT_VALUES_EQUAL(8_KB, dataAreaSizeAfterShrink);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            dataAreaSizeAfterShrink % initialDataAreaSize);

        TStringBuf firstRecord = table.GetRecordWithValidation(indices.front());
        UNIT_ASSERT_VALUES_EQUAL(payload, TString(firstRecord));

        TStringBuf newRecord = table.GetRecordWithValidation(newIndex);
        UNIT_ASSERT_VALUES_EQUAL(payload, TString(newRecord));
    }

    Y_UNIT_TEST(ShouldNotShrinkOnManyDeletionsInAllocOnlyModeUntilAllocation)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "runtime_shrink_alloc_only.table";

        const ui64 initialDataAreaSize = 4_KB;
        const TString payload(1_KB, 'p');
        const ui64 shrinkLowMemoryOpThreshold = 1;
        const ui64 totalRecords = 16;
        const ui64 keptRecords = 1;

        auto table = CreateTable(
            tablePath,
            32,
            initialDataAreaSize,
            1_GB,
            1_KB,
            TTable::DefaultGapSpacePercentageCompactionThreshold,
            shrinkLowMemoryOpThreshold);

        TVector<ui64> indices = FillTable(table, totalRecords, payload);

        auto* header = GetTableHeader(table);
        UNIT_ASSERT_VALUES_EQUAL(16_KB, header->DataAreaSize);
        UNIT_ASSERT_VALUES_EQUAL(totalRecords, indices.size());

        DeleteRecords(table, indices, keptRecords);
        UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());

        const ui64 dataAreaSizeAfterDeletes =
            GetTableHeader(table)->DataAreaSize;
        UNIT_ASSERT_VALUES_EQUAL(16_KB, dataAreaSizeAfterDeletes);

        const ui64 newIndex = AllocAndCommitRecord(table, payload);
        const ui64 dataAreaSizeAfterShrink =
            GetTableHeader(table)->DataAreaSize;

        UNIT_ASSERT_VALUES_EQUAL(8_KB, dataAreaSizeAfterShrink);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            dataAreaSizeAfterShrink % initialDataAreaSize);

        TStringBuf firstRecord = table.GetRecordWithValidation(indices.front());
        UNIT_ASSERT_VALUES_EQUAL(payload, TString(firstRecord));

        TStringBuf newRecord = table.GetRecordWithValidation(newIndex);
        UNIT_ASSERT_VALUES_EQUAL(payload, TString(newRecord));
    }

    Y_UNIT_TEST(ShouldShrinkAfterDeletionOnNextLowMemoryOpInAnyOpMode)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "runtime_shrink_threshold.table";

        const ui64 initialDataAreaSize = 4_KB;
        const TString payload(1_KB, 'h');
        const ui64 shrinkLowMemoryOpThreshold = 4;
        const ui64 totalRecords = 9;
        const ui64 keptRecords = 3;

        auto table = CreateTable(
            tablePath,
            32,
            initialDataAreaSize,
            1_GB,
            1_KB,
            TTable::DefaultGapSpacePercentageCompactionThreshold,
            shrinkLowMemoryOpThreshold,
            TTable::DefaultShrinkTriggerPercent,
            TTable::DefaultShrinkReservePercent,
            TTable::EShrinkMode::AnyOp);

        TVector<ui64> indices = FillTable(table, totalRecords, payload);

        auto* header = GetTableHeader(table);
        UNIT_ASSERT_VALUES_EQUAL(16_KB, header->DataAreaSize);
        UNIT_ASSERT_VALUES_EQUAL(totalRecords, indices.size());

        DeleteRecords(table, indices, keptRecords);
        UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());

        const ui64 dataAreaSizeBeforeAlloc =
            GetTableHeader(table)->DataAreaSize;
        const ui64 newIndex = AllocAndCommitRecord(table, payload);
        UNIT_ASSERT_VALUES_EQUAL(
            dataAreaSizeBeforeAlloc,
            GetTableHeader(table)->DataAreaSize);
        UNIT_ASSERT_VALUES_EQUAL(16_KB, dataAreaSizeBeforeAlloc);

        const ui64 dataAreaSizeBeforeShrink =
            GetTableHeader(table)->DataAreaSize;
        UNIT_ASSERT(table.DeleteRecord(newIndex));
        UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());

        const ui64 dataAreaSizeAfterShrink =
            GetTableHeader(table)->DataAreaSize;
        UNIT_ASSERT_VALUES_EQUAL(16_KB, dataAreaSizeBeforeShrink);
        UNIT_ASSERT_VALUES_EQUAL(8_KB, dataAreaSizeAfterShrink);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            dataAreaSizeAfterShrink % initialDataAreaSize);
    }

    Y_UNIT_TEST(
        ShouldForceDeallocateMemoryWhenUsageIsLowButLowMemoryOpThresholdNotReady)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "manual_deallocate.table";

        const ui64 initialDataAreaSize = 4_KB;
        const TString payload(1_KB, 'd');
        const ui64 shrinkLowMemoryOpThreshold = 100;
        const ui64 totalRecords = 9;
        const ui64 keptRecords = 3;

        auto table = CreateTable(
            tablePath,
            32,
            initialDataAreaSize,
            1_GB,
            1_KB,
            TTable::DefaultGapSpacePercentageCompactionThreshold,
            shrinkLowMemoryOpThreshold);

        TVector<ui64> indices = FillTable(table, totalRecords, payload);

        UNIT_ASSERT_VALUES_EQUAL(16_KB, GetTableHeader(table)->DataAreaSize);
        table.TryDeallocateMemory();
        UNIT_ASSERT_VALUES_EQUAL(16_KB, GetTableHeader(table)->DataAreaSize);

        DeleteRecords(table, indices, keptRecords);

        const ui64 dataAreaSizeBeforeShrink =
            GetTableHeader(table)->DataAreaSize;
        UNIT_ASSERT_VALUES_EQUAL(16_KB, dataAreaSizeBeforeShrink);

        table.TryDeallocateMemory();

        const ui64 dataAreaSizeAfterShrink =
            GetTableHeader(table)->DataAreaSize;
        UNIT_ASSERT_VALUES_EQUAL(8_KB, dataAreaSizeAfterShrink);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            dataAreaSizeAfterShrink % initialDataAreaSize);
    }

    Y_UNIT_TEST(ShouldAvoidShrinkOscillationWhenLowUtilizationIsBrief)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "no_oscillation.table";

        const ui64 initialDataAreaSize = 4_KB;
        const TString payload(1_KB, 'o');
        const ui64 shrinkLowMemoryOpThreshold = 2;
        const ui64 totalRecords = 16;
        const ui64 keptRecords = 4;

        auto table = CreateTable(
            tablePath,
            32,
            initialDataAreaSize,
            1_GB,
            1_KB,
            TTable::DefaultGapSpacePercentageCompactionThreshold,
            shrinkLowMemoryOpThreshold);

        TVector<ui64> indices = FillTable(table, totalRecords, payload);
        UNIT_ASSERT_VALUES_EQUAL(totalRecords, indices.size());
        UNIT_ASSERT_VALUES_EQUAL(16_KB, GetTableHeader(table)->DataAreaSize);

        DeleteRecords(table, indices, keptRecords);
        UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());

        const ui64 dataAreaSizeBeforeCompaction =
            GetTableHeader(table)->DataAreaSize;

        // Deleting down to 4 records enters low-memory mode only once, so
        // the next mutation must compact but still must not shrink.
        AllocAndCommitRecord(table, payload);
        const ui64 dataAreaSizeAfterFirstCompaction =
            GetTableHeader(table)->DataAreaSize;
        UNIT_ASSERT_VALUES_EQUAL(
            dataAreaSizeBeforeCompaction,
            dataAreaSizeAfterFirstCompaction);

        // Once allocation leaves the low-memory zone, the counter is reset.
        AllocAndCommitRecord(table, payload);
        const ui64 dataAreaSizeAfterSecondAlloc =
            GetTableHeader(table)->DataAreaSize;
        UNIT_ASSERT_VALUES_EQUAL(
            dataAreaSizeBeforeCompaction,
            dataAreaSizeAfterSecondAlloc);
    }

    Y_UNIT_TEST(ShouldShrinkEmptyTableToInitialDataAreaOnStartup)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "empty_startup_shrink.table";

        const ui64 initialDataAreaSize = 4_KB;
        const TString payload(1_KB, 'e');
        const ui64 totalRecords = 9;

        TVector<ui64> indices;

        {
            auto table =
                CreateTable(tablePath, 32, initialDataAreaSize, 1_GB, 1_KB);
            indices = FillTable(table, totalRecords, payload);

            UNIT_ASSERT_VALUES_EQUAL(
                16_KB,
                GetTableHeader(table)->DataAreaSize);

            DeleteRecords(table, indices, 0);
            UNIT_ASSERT_VALUES_EQUAL(0, table.CountRecords());
        }

        {
            auto table =
                CreateTable(tablePath, 32, initialDataAreaSize, 1_GB, 1_KB);
            auto* header = GetTableHeader(table);

            UNIT_ASSERT_VALUES_EQUAL(0, table.CountRecords());
            UNIT_ASSERT_VALUES_EQUAL(initialDataAreaSize, header->DataAreaSize);
        }
    }

    Y_UNIT_TEST(ShouldUseCurrentDataAreaSizeForCompactionThreshold)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "current_threshold.table";

        const ui64 initialDataAreaSize = 1_KB;
        const TString payload(1_KB, 't');
        const ui64 totalRecords = 8;

        auto table =
            CreateTable(tablePath, 32, initialDataAreaSize, 1_GB, 1_KB);

        TVector<ui64> indices = FillTable(table, totalRecords, payload);
        UNIT_ASSERT_VALUES_EQUAL(totalRecords, indices.size());

        auto* tableHeader = GetTableHeader(table);
        UNIT_ASSERT_VALUES_EQUAL(8_KB, tableHeader->DataAreaSize);

        UNIT_ASSERT(table.DeleteRecord(indices[2]));
        UNIT_ASSERT(table.DeleteRecord(indices[3]));

        const ui64 dataAreaSizeBefore = tableHeader->DataAreaSize;
        AllocAndCommitRecord(table, payload);
        const ui64 dataAreaSizeAfter = GetTableHeader(table)->DataAreaSize;

        UNIT_ASSERT(dataAreaSizeAfter > dataAreaSizeBefore);
    }

    Y_UNIT_TEST(ShouldCompactWhenGapExceedsMaxStepWithoutPercentThreshold)
    {
        TTempDir tempDir;
        TString tablePath =
            tempDir.Path() / "max_step_compaction_threshold.table";

        const ui64 initialDataAreaSize = 1_KB;
        const ui64 maxDataAreaStepSize = 4_KB;
        const TString payload(1_KB, 'm');
        const ui64 totalRecords = 8;
        const ui64 keptRecords = 3;

        auto table = CreateTable(
            tablePath,
            32,
            initialDataAreaSize,
            maxDataAreaStepSize,
            1_KB,
            100);

        TVector<ui64> indices = FillTable(table, totalRecords, payload);
        UNIT_ASSERT_VALUES_EQUAL(totalRecords, indices.size());

        auto* tableHeader = GetTableHeader(table);
        UNIT_ASSERT_VALUES_EQUAL(8_KB, tableHeader->DataAreaSize);

        DeleteRecords(table, indices, keptRecords);

        const ui64 dataAreaSizeBefore = tableHeader->DataAreaSize;
        AllocAndCommitRecord(table, payload);
        const ui64 dataAreaSizeAfter = GetTableHeader(table)->DataAreaSize;

        UNIT_ASSERT_VALUES_EQUAL(dataAreaSizeBefore, dataAreaSizeAfter);
    }

    Y_UNIT_TEST(ShouldLimitDataAreaGrowthStep)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "max_growth_step.table";

        const ui64 initialDataAreaSize = 1_KB;
        const ui64 maxDataAreaStepSize = 4_KB;
        const TString payload(1_KB, 's');

        auto table = CreateTable(
            tablePath,
            32,
            initialDataAreaSize,
            maxDataAreaStepSize,
            4_KB);

        auto assertDataAreaSize = [&](ui64 expectedSize)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                expectedSize,
                GetTableHeader(table)->DataAreaSize);
        };

        AllocAndCommitRecord(table, payload);
        assertDataAreaSize(1_KB);

        AllocAndCommitRecord(table, payload);
        assertDataAreaSize(2_KB);

        AllocAndCommitRecord(table, payload);
        assertDataAreaSize(4_KB);

        for (ui32 i = 0; i < 2; ++i) {
            AllocAndCommitRecord(table, payload);
        }
        assertDataAreaSize(8_KB);

        for (ui32 i = 0; i < 4; ++i) {
            AllocAndCommitRecord(table, payload);
        }
        assertDataAreaSize(12_KB);

        for (ui32 i = 0; i < 4; ++i) {
            AllocAndCommitRecord(table, payload);
        }
        assertDataAreaSize(16_KB);
    }

    Y_UNIT_TEST(ShouldUseHalfMaxStepReserveWhenShrinkTriggeredByMaxStep)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "max_growth_step_shrink.table";

        const ui64 initialDataAreaSize = 1_KB;
        const ui64 maxDataAreaStepSize = 8_KB;
        const TString payload(1_KB, 'r');
        const ui64 shrinkLowMemoryOpThreshold = 4;
        const ui64 totalRecords = 16;
        const ui64 keptRecords = 7;

        auto table = CreateTable(
            tablePath,
            32,
            initialDataAreaSize,
            maxDataAreaStepSize,
            1_KB,
            TTable::DefaultGapSpacePercentageCompactionThreshold,
            shrinkLowMemoryOpThreshold,
            TTable::DefaultShrinkTriggerPercent,
            TTable::DefaultShrinkReservePercent,
            TTable::EShrinkMode::AnyOp);

        TVector<ui64> indices = FillTable(table, totalRecords, payload);

        auto* header = GetTableHeader(table);
        UNIT_ASSERT_VALUES_EQUAL(16_KB, header->DataAreaSize);
        UNIT_ASSERT_VALUES_EQUAL(totalRecords, indices.size());

        DeleteRecords(table, indices, keptRecords);
        UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());

        const ui64 dataAreaSizeBeforeAlloc =
            GetTableHeader(table)->DataAreaSize;
        const ui64 firstShrinkIndex = AllocAndCommitRecord(table, payload);
        UNIT_ASSERT_VALUES_EQUAL(
            dataAreaSizeBeforeAlloc,
            GetTableHeader(table)->DataAreaSize);
        UNIT_ASSERT(table.DeleteRecord(firstShrinkIndex));
        UNIT_ASSERT_VALUES_EQUAL(11_KB, GetTableHeader(table)->DataAreaSize);
        UNIT_ASSERT_VALUES_EQUAL(keptRecords, table.CountRecords());
    }

    Y_UNIT_TEST(ShouldResumeAbortedCompaction)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        TVector<TTestData> testData = TestDataRecords;

        {
            auto table = CreateTable(tablePath, 32, 1024);

            for (const auto& data: testData) {
                TString serialized = data.Serialize();
                ui64 index = table.AllocRecord(serialized.size());
                UNIT_ASSERT(table.WriteRecordData(
                    index,
                    serialized.data(),
                    serialized.size()));
                table.CommitRecord(index);
            }

            auto* tableHeader = GetTableHeader(table);

            UNIT_ASSERT_VALUES_EQUAL(TTable::Version, tableHeader->Version);
            UNIT_ASSERT_VALUES_EQUAL(
                TTable::InvalidIndex,
                tableHeader->CompactedRecordSrcIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                TTable::InvalidIndex,
                tableHeader->CompactedRecordDstIndex);

            // Simulate aborted compaction: record 2 was being moved to position
            // 1 but the operation was interrupted before completion
            tableHeader->CompactedRecordSrcIndex = 2;
            tableHeader->CompactedRecordDstIndex = 1;
            table.DeleteRecord(tableHeader->CompactedRecordDstIndex);

            // Update expected data to reflect the move
            testData[tableHeader->CompactedRecordDstIndex] =
                testData[tableHeader->CompactedRecordSrcIndex];
            testData.erase(
                testData.begin() + tableHeader->CompactedRecordSrcIndex);
        }

        {
            auto table = CreateTable(tablePath, 32, 1024);
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());

            auto* tableHeader = GetTableHeader(table);

            UNIT_ASSERT_VALUES_EQUAL(TTable::Version, tableHeader->Version);
            UNIT_ASSERT_VALUES_EQUAL(
                TTable::InvalidIndex,
                tableHeader->CompactedRecordSrcIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                TTable::InvalidIndex,
                tableHeader->CompactedRecordDstIndex);

            size_t index = 0;
            for (auto it = table.begin(); it != table.end(); ++it, ++index) {
                TStringBuf record = *it;
                TTestData recovered =
                    TTestData::Deserialize(record.data(), record.size());
                UNIT_ASSERT(recovered == testData[index]);
            }
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), index);

            // Test aborted compaction that never started copying data
            tableHeader->CompactedRecordSrcIndex = 2;
            tableHeader->CompactedRecordDstIndex = TTable::InvalidIndex;
        }

        {
            auto table = CreateTable(tablePath, 32, 1024);
            auto* tableHeader = GetTableHeader(table);

            UNIT_ASSERT_VALUES_EQUAL(TTable::Version, tableHeader->Version);
            UNIT_ASSERT_VALUES_EQUAL(
                TTable::InvalidIndex,
                tableHeader->CompactedRecordSrcIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                TTable::InvalidIndex,
                tableHeader->CompactedRecordDstIndex);

            size_t index = 0;
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());
            for (auto it = table.begin(); it != table.end(); ++it) {
                TStringBuf record = *it;
                TTestData recovered =
                    TTestData::Deserialize(record.data(), record.size());
                UNIT_ASSERT(testData[index] == recovered);
                index++;
            }
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());
        }
    }

    Y_UNIT_TEST(ShouldResumeAbortedDataAreaCompactionDuringFirstMemcpy)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        TVector<TTestData> testData = TestDataRecords;

        TVector<ui64> indices;

        {
            auto table = CreateTable(tablePath, 32, 1024, 1_GB, 1);

            for (const auto& data: testData) {
                TString serialized = data.Serialize();
                ui64 index = table.AllocRecord(serialized.size());
                UNIT_ASSERT(table.WriteRecordData(
                    index,
                    serialized.data(),
                    serialized.size()));
                table.CommitRecord(index);
                indices.push_back(index);
            }

            // Make the record smaller to create fragmentation
            testData[1] = TTestData{2, "new_data", {11}};
            TString serialized = testData[1].Serialize();
            table.WriteRecordData(
                indices[1],
                serialized.data(),
                serialized.size());

            // Get table internals to simulate partial compaction
            auto [tableHeader, descriptorsPtr, dataPtr] =
                GetTableInternals(table);

            // Find the record that will be moved during compaction
            ui64 recordToMoveIndex = indices[2];
            ui64 recordSize = descriptorsPtr[recordToMoveIndex].DataSize;

            // SIMULATE ABORTED COMPACTION:
            // Copy the data to tmp buffer using memcpy (as compaction would
            // do), but only half of the record to simulate crash/abort
            std::memcpy(
                dataPtr + tableHeader->DataAreaSize,
                dataPtr + descriptorsPtr[recordToMoveIndex].DataOffset,
                recordSize / 2);
        }

        {
            auto table = CreateTable(tablePath, 32, 1024, 1_GB, 2);

            // All records should be accessible and valid
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());

            for (size_t i = 0; i < testData.size(); ++i) {
                TStringBuf record = table.GetRecordWithValidation(i);
                UNIT_ASSERT_C(!record.empty(), "Record should be accessible");
                TTestData recovered =
                    TTestData::Deserialize(record.data(), record.size());
                UNIT_ASSERT_C(
                    recovered == testData[i],
                    "Record data should be intact");
            }
        }
    }

    Y_UNIT_TEST(ShouldResumeAbortedDataAreaCompactionAfterMovingIndexIsSet)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        TVector<TTestData> testData = TestDataRecords;

        TVector<ui64> indices;

        {
            auto table = CreateTable(tablePath, 32, 1024, 1_GB, 1);

            for (const auto& data: testData) {
                TString serialized = data.Serialize();
                ui64 index = table.AllocRecord(serialized.size());
                UNIT_ASSERT(table.WriteRecordData(
                    index,
                    serialized.data(),
                    serialized.size()));
                table.CommitRecord(index);
                indices.push_back(index);
            }

            // Make the record smaller to create fragmentation
            testData[1] = TTestData{2, "new_data", {11}};
            TString serialized = testData[1].Serialize();
            table.WriteRecordData(
                indices[1],
                serialized.data(),
                serialized.size());

            // Get table internals to simulate partial compaction
            auto [tableHeader, descriptorsPtr, dataPtr] =
                GetTableInternals(table);

            // Find the record that will be moved during compaction
            ui64 recordToMoveIndex = indices[2];
            ui64 recordSize = descriptorsPtr[recordToMoveIndex].DataSize;

            // SIMULATE ABORTED COMPACTION:
            // Copy the data to tmp buffer using memcpy (as compaction would
            // do)
            std::memcpy(
                dataPtr + tableHeader->DataAreaSize,
                dataPtr + descriptorsPtr[recordToMoveIndex].DataOffset,
                recordSize);

            tableHeader->DataCompactionRecordIndex = recordToMoveIndex;

            // DO NOT update the descriptor offset (simulate crash/abort)
            // This creates case: data is copied to the tmp buffer and index is
            // set but offset is old and data will be copied to the old place
            // during restoration
        }

        {
            auto table = CreateTable(tablePath, 32, 1024, 1_GB, 1);

            // All records should be accessible and valid
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());

            for (size_t i = 0; i < testData.size(); ++i) {
                TStringBuf record = table.GetRecordWithValidation(i);
                UNIT_ASSERT_C(!record.empty(), "Record should be accessible");
                TTestData recovered =
                    TTestData::Deserialize(record.data(), record.size());
                UNIT_ASSERT_C(
                    recovered == testData[i],
                    "Record data should be intact");
            }
        }
    }

    Y_UNIT_TEST(ShouldResumeAbortedDataAreaCompactionAfterNewOffsetIsSet)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        TVector<TTestData> testData = TestDataRecords;

        TVector<ui64> indices;

        {
            auto table = CreateTable(tablePath, 32, 1024, 1_GB, 2);

            for (const auto& data: testData) {
                TString serialized = data.Serialize();
                ui64 index = table.AllocRecord(serialized.size());
                UNIT_ASSERT(table.WriteRecordData(
                    index,
                    serialized.data(),
                    serialized.size()));
                table.CommitRecord(index);
                indices.push_back(index);
            }

            // Make the record smaller to create fragmentation
            testData[1] = TTestData{2, "new_data", {11}};
            TString serialized = testData[1].Serialize();
            table.WriteRecordData(
                indices[1],
                serialized.data(),
                serialized.size());

            // Get table internals to simulate partial compaction
            auto [tableHeader, descriptorsPtr, dataPtr] =
                GetTableInternals(table);

            // Find the record that will be moved during compaction
            ui64 recordToMoveIndex = indices[2];
            ui64 recordSize = descriptorsPtr[recordToMoveIndex].DataSize;

            // SIMULATE ABORTED COMPACTION:
            // Copy the data to tmp buffer using memcpy (as compaction would
            // do)
            std::memcpy(
                dataPtr + tableHeader->DataAreaSize,
                dataPtr + descriptorsPtr[recordToMoveIndex].DataOffset,
                recordSize);

            tableHeader->DataCompactionRecordIndex = recordToMoveIndex;

            // Calculate where this record should be moved to fill the gap
            ui64 newOffset = descriptorsPtr[indices[1]].DataOffset +
                             descriptorsPtr[indices[1]].DataSize;

            descriptorsPtr[recordToMoveIndex].DataOffset = newOffset;

            // DO NOT move data to final location (simulate
            // crash/abort) This creates case: data is copied to the tmp buffer
            // and index and offset are prepared, but final step is not
            // performed, during restoration data will be moved to the new
            // offset
        }

        {
            auto table = CreateTable(tablePath, 32, 1024, 1_GB, 2);

            // All records should be accessible and valid
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());

            for (size_t i = 0; i < testData.size(); ++i) {
                TStringBuf record = table.GetRecordWithValidation(i);
                UNIT_ASSERT_C(!record.empty(), "Record should be accessible");
                TTestData recovered =
                    TTestData::Deserialize(record.data(), record.size());
                UNIT_ASSERT_C(
                    recovered == testData[i],
                    "Record data should be intact");
            }
        }
    }

    Y_UNIT_TEST(ShouldResumeAbortedDataAreaCompactionDuringSecondMemcpy)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        TVector<TTestData> testData = TestDataRecords;

        TVector<ui64> indices;

        {
            auto table = CreateTable(tablePath, 32, 1024, 1_GB, 2);

            for (const auto& data: testData) {
                TString serialized = data.Serialize();
                ui64 index = table.AllocRecord(serialized.size());
                UNIT_ASSERT(table.WriteRecordData(
                    index,
                    serialized.data(),
                    serialized.size()));
                table.CommitRecord(index);
                indices.push_back(index);
            }

            // Make the record smaller to create fragmentation
            testData[1] = TTestData{2, "new_data", {11}};
            TString serialized = testData[1].Serialize();
            table.WriteRecordData(
                indices[1],
                serialized.data(),
                serialized.size());

            // Get table internals to simulate partial compaction
            auto [tableHeader, descriptorsPtr, dataPtr] =
                GetTableInternals(table);

            // Find the record that will be moved during compaction
            ui64 recordToMoveIndex = indices[2];
            ui64 originalOffset = descriptorsPtr[recordToMoveIndex].DataOffset;
            ui64 recordSize = descriptorsPtr[recordToMoveIndex].DataSize;

            // Calculate where this record should be moved to fill the gap
            ui64 newOffset = descriptorsPtr[indices[1]].DataOffset +
                             descriptorsPtr[indices[1]].DataSize;

            // SIMULATE ABORTED COMPACTION:
            // Copy the data to tmp buffer using memcpy (as compaction would do)
            std::memcpy(
                dataPtr + tableHeader->DataAreaSize,
                dataPtr + originalOffset,
                recordSize);

            tableHeader->DataCompactionRecordIndex = recordToMoveIndex;

            descriptorsPtr[recordToMoveIndex].DataOffset = newOffset;

            // Copy the data to the new offset, but only half of the record to
            // simulate crash/abort
            std::memcpy(
                dataPtr + newOffset,
                dataPtr + tableHeader->DataAreaSize,
                recordSize / 2);
        }

        {
            auto table = CreateTable(tablePath, 32, 1024, 1_GB, 1);

            // All records should be accessible and valid
            UNIT_ASSERT_VALUES_EQUAL(testData.size(), table.CountRecords());

            for (size_t i = 0; i < testData.size(); ++i) {
                TStringBuf record = table.GetRecordWithValidation(i);
                UNIT_ASSERT_C(!record.empty(), "Record should be accessible");
                TTestData recovered =
                    TTestData::Deserialize(record.data(), record.size());
                UNIT_ASSERT_C(
                    recovered == testData[i],
                    "Record data should be intact");
            }
        }
    }

    Y_UNIT_TEST(ShouldResumeAbortedAddRecord)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        using TTable = TDynamicPersistentTable<TTestHeader>;

        TTestData testData1{1, "first", {10, 20}};
        TTestData testData2{2, "second", {30, 40}};

        {
            auto table = CreateTable(tablePath, 32, 1024);

            TString serialized1 = testData1.Serialize();
            ui64 index1 = table.AllocRecord(serialized1.size());
            UNIT_ASSERT(table.WriteRecordData(
                index1,
                serialized1.data(),
                serialized1.size()));
            table.CommitRecord(index1);

            TString serialized2 = testData2.Serialize();

            // Simulate crash during add operation before state change, but
            // after index list was set
            ui64 index2 = table.AllocRecord(serialized2.size());

            auto [tableHeader, descriptorsPtr, dataPtr] =
                GetTableInternals(table);

            tableHeader->ListOperationState = TTable::EListOperationState::Add;
            tableHeader->ListOperationIndex = index2;
            tableHeader->ListOperationPrevIndex = index1;
            descriptorsPtr[index2].State = TTable::ERecordState::Free;
        }

        {
            auto table = CreateTable(tablePath, 32, 1024);

            auto* tableHeader = GetTableHeader(table);

            // After recovery, list operation state should be cleared
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(TTable::EListOperationState::None),
                static_cast<int>(tableHeader->ListOperationState));
            UNIT_ASSERT_VALUES_EQUAL(
                TTable::InvalidIndex,
                tableHeader->ListOperationIndex);

            // Verify first record is still accessible
            TStringBuf record1 = table.GetRecord(0);
            UNIT_ASSERT(!record1.empty());
            TTestData recovered1 =
                TTestData::Deserialize(record1.data(), record1.size());
            UNIT_ASSERT(testData1 == recovered1);

            // Records should be properly linked after recovery
            UNIT_ASSERT_VALUES_EQUAL(1, table.CountRecords());
        }
    }

    Y_UNIT_TEST(ShouldResumeAbortedRemoveRecord)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        using TTable = TDynamicPersistentTable<TTestHeader>;

        TVector<TTestData> testData = TestDataRecords;
        TVector<ui64> indices;

        {
            auto table = CreateTable(tablePath, 32, 1024);

            for (const auto& data: testData) {
                TString serialized = data.Serialize();
                ui64 index = table.AllocRecord(serialized.size());
                UNIT_ASSERT(table.WriteRecordData(
                    index,
                    serialized.data(),
                    serialized.size()));
                table.CommitRecord(index);
                indices.push_back(index);
            }

            auto [tableHeader, descriptorsPtr, dataPtr] =
                GetTableInternals(table);

            // Simulate aborted remove operation for middle record
            ui64 prevIndex = indices[1];
            ui64 removeIndex = indices[2];
            ui64 nextIndex = indices[3];

            // Simulate crash during remove operation before state change, but
            // after index list was changed
            tableHeader->ListOperationState =
                TTable::EListOperationState::Remove;
            tableHeader->ListOperationIndex = removeIndex;
            tableHeader->ListOperationPrevIndex = prevIndex;
            tableHeader->ListOperationNextIndex = nextIndex;

            descriptorsPtr[prevIndex].NextDataIndex = nextIndex;
        }

        {
            auto table = CreateTable(tablePath, 32, 1024);

            auto* tableHeader = GetTableHeader(table);

            // After recovery, list operation state should be cleared
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(TTable::EListOperationState::None),
                static_cast<int>(tableHeader->ListOperationState));
            UNIT_ASSERT_VALUES_EQUAL(
                TTable::InvalidIndex,
                tableHeader->ListOperationIndex);

            // The middle record should be properly removed from the list and
            // records should be compacted
            UNIT_ASSERT_VALUES_EQUAL(4, table.CountRecords());

            // Verify remaining records are accessible
            auto assertRecord = [&](ui64 tableIndex, ui64 testIndex)
            {
                TStringBuf record = table.GetRecordWithValidation(tableIndex);
                UNIT_ASSERT_C(!record.empty(), "Record should be accessible");
                TTestData recovered =
                    TTestData::Deserialize(record.data(), record.size());
                UNIT_ASSERT_C(
                    testData[testIndex] == recovered,
                    "Record data should be intact");
            };
            assertRecord(indices[0], 0);
            assertRecord(indices[1], 1);
            assertRecord(indices[2], 3);
            assertRecord(indices[3], 4);
        }
    }

    Y_UNIT_TEST(RandomizedAllocDeleteRestore)
    {
        TTempDir tempDir;
        TString tablePath = tempDir.Path() / "test.table";

        const ui32 tableSize = 50;
        const ui32 testRecords = 2000;
        const double restoreProbability = 0.05;

        std::unique_ptr<TTable> table;
        TReferenceImplementation ri(tableSize);

        auto restore = [&]()
        {
            table = std::make_unique<TTable>(
                CreateTable(tablePath, tableSize, 1024));
        };

        restore();

        ui32 remainingRecords = testRecords;
        while (remainingRecords || ri.CountRecords()) {
            const bool shouldAlloc = remainingRecords && RandomNumber<bool>();

            if (shouldAlloc) {
                TTestData testData{
                    RandomNumber<ui64>(),
                    TStringBuilder() << "record_" << RandomNumber<ui32>(1000),
                    TVector<ui32>()};

                ui32 valueCount = RandomNumber<ui32>(15);
                for (ui32 i = 0; i < valueCount; ++i) {
                    testData.Values.push_back(RandomNumber<ui32>());
                }

                TString serialized = testData.Serialize();
                ui64 tableIndex = table->AllocRecord(serialized.size());
                ui64 riIndex = ri.AllocRecord(serialized.size());

                UNIT_ASSERT_VALUES_EQUAL(riIndex, tableIndex);

                if (tableIndex !=
                    TDynamicPersistentTable<TTestHeader>::InvalidIndex)
                {
                    remainingRecords--;

                    UNIT_ASSERT(table->WriteRecordData(
                        tableIndex,
                        serialized.data(),
                        serialized.size()));
                    table->CommitRecord(tableIndex);
                    ri.CommitRecord(riIndex, serialized);
                }
            } else {
                if (ri.CountRecords()) {
                    ui64 index = ri.SomeRecord();
                    if (index !=
                        TDynamicPersistentTable<TTestHeader>::InvalidIndex)
                    {
                        TStringBuf tableRecord = table->GetRecord(index);
                        TString riRecord = ri.GetRecord(index);

                        if (!tableRecord.empty() && !riRecord.empty()) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                riRecord.size(),
                                tableRecord.size());
                            UNIT_ASSERT_VALUES_EQUAL(
                                riRecord,
                                TString(tableRecord));
                        }

                        ri.DeleteRecord(index);
                        table->DeleteRecord(index);
                    }
                }
            }

            if (RandomNumber<double>() < restoreProbability) {
                restore();
                ri.Compact();
            }

            UNIT_ASSERT_VALUES_EQUAL(ri.CountRecords(), table->CountRecords());
        }
    }
}

}   // namespace NCloud
