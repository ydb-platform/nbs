#include "write_data_request_builder.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
IOutputStream& PrintValues(IOutputStream& out, const TVector<T>& values)
{
    out << "[";
    for (size_t i = 0; i != values.size();) {
        out << values[i];
        i++;
        if (i != values.size()) {
            out << ", ";
        }
    }
    out << "]";
    return out;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestCaseWriteDataEntry
{
    ui64 NodeId = 0;
    ui64 Offset = 0;
    ui64 Length = 0;
};

IOutputStream& operator<<(IOutputStream& out, const TTestCaseWriteDataEntry& e)
{
    out << "{"
        << "NodeId: " << e.NodeId << ", "
        << "Offset: " << e.Offset << ", "
        << "Length: " << e.Length << "}";
    return out;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestCaseWriteDataEntries
{
    TVector<TString> Buffers;
    TVector<TTestCaseWriteDataEntry> Entries;

    TTestCaseWriteDataEntries(
        std::initializer_list<TTestCaseWriteDataEntry> testCaseEntries)
    {
        for (const auto& e: testCaseEntries) {
            Buffers.emplace_back(e.Length, 'a');   // dummy buffer
            Entries.push_back(e);
        }
    }

    explicit TTestCaseWriteDataEntries(
        const TVector<TTestCaseWriteDataEntry>& testCaseEntries)
    {
        for (const auto& e: testCaseEntries) {
            Buffers.emplace_back(e.Length, 'a');   // dummy buffer
            Entries.push_back(e);
        }
    }
};

IOutputStream& operator<<(
    IOutputStream& out,
    const TTestCaseWriteDataEntries& entries)
{
    return PrintValues(out, entries.Entries);
}

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    size_t CalculateEntriesCountToFlush(
        const TTestCaseWriteDataEntries& entries,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize)
    {
        auto builder = CreateWriteDataRequestBuilder({
            .FileSystemId = "test_fs",
            .MaxWriteRequestSize = maxWriteRequestSize,
            .MaxWriteRequestsCount = maxWriteRequestsCount,
            .MaxSumWriteRequestsSize = maxSumWriteRequestsSize,
            .ZeroCopyWriteEnabled = false,
        });

        auto batchBuilder =
            builder->CreateWriteDataRequestBatchBuilder(/* nodeId = */ 0);

        size_t requestCount = 0;

        for (size_t i = 0; i < entries.Entries.size(); i++) {
            const auto& e = entries.Entries[i];
            const auto& buffer = entries.Buffers[i];

            if (!batchBuilder->AddRequest(/* handle = */ 0, e.Offset, buffer)) {
                break;
            }
            requestCount++;
        }

        auto res = batchBuilder->Build();

        Y_ABORT_UNLESS(res.AffectedRequestCount == requestCount);
        return res.AffectedRequestCount;
    }

    size_t CalculateEntriesCountToFlushReferenceImpl(
        const TTestCaseWriteDataEntries& entries,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize)
    {
        auto begin = entries.Entries.front().Offset;
        auto end =
            entries.Entries.front().Offset + entries.Entries.front().Length;

        for (const auto& entry: entries.Entries) {
            begin = Min(begin, entry.Offset);
            end = Max(end, entry.Offset + entry.Length);
        }

        TVector<bool> data(end - begin, false);

        for (size_t count = 0; count < entries.Entries.size(); count++) {
            const auto& entry = entries.Entries[count];
            for (auto i = entry.Offset; i < entry.Offset + entry.Length; i++) {
                data[i - begin] = true;
            }

            if (!CheckIfCanFlushWithLimits(
                    data,
                    maxWriteRequestSize,
                    maxWriteRequestsCount,
                    maxSumWriteRequestsSize) &&
                count > 0)
            {
                return count;
            }
        }

        return entries.Entries.size();
    }

private:
    bool CheckIfCanFlushWithLimits(
        const TVector<bool>& data,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize)
    {
        if (Count(data, true) > maxSumWriteRequestsSize) {
            return false;
        }

        ui32 lastRequestSize = 0;
        size_t requestCount = 0;

        for (bool x: data) {
            if (x) {
                lastRequestSize++;
                if (lastRequestSize > maxWriteRequestSize) {
                    lastRequestSize = 1;
                    requestCount++;
                }
            } else if (lastRequestSize > 0) {
                lastRequestSize = 0;
                requestCount++;
            }
        }

        if (lastRequestSize > 0) {
            requestCount++;
        }

        return requestCount <= maxWriteRequestsCount;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteDataRequestBuilderTest)
{
    Y_UNIT_TEST(ShouldCalculateEntriesCountToFlush)
    {
        TBootstrap b;

        TTestCaseWriteDataEntries singleEntry{{1, 0, 3}};

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.CalculateEntriesCountToFlush(singleEntry, 100, 100, 1000));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.CalculateEntriesCountToFlush(singleEntry, 1, 2, 1000));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.CalculateEntriesCountToFlush(singleEntry, 100, 100, 2));

        TTestCaseWriteDataEntries twoOverlappingEntries{{1, 0, 3}, {1, 1, 3}};

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.CalculateEntriesCountToFlush(
                twoOverlappingEntries,
                100,
                100,
                1000));

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.CalculateEntriesCountToFlush(twoOverlappingEntries, 100, 100, 4));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.CalculateEntriesCountToFlush(twoOverlappingEntries, 100, 100, 3));

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.CalculateEntriesCountToFlush(
                twoOverlappingEntries,
                100,
                1,
                1000));

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.CalculateEntriesCountToFlush(
                twoOverlappingEntries,
                3,
                100,
                1000));

        TTestCaseWriteDataEntries twoSeparateEntries{{1, 0, 3}, {1, 4, 3}};

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.CalculateEntriesCountToFlush(twoSeparateEntries, 100, 100, 1000));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.CalculateEntriesCountToFlush(twoSeparateEntries, 100, 100, 4));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.CalculateEntriesCountToFlush(twoSeparateEntries, 100, 1, 1000));

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.CalculateEntriesCountToFlush(twoSeparateEntries, 3, 100, 1000));
    }

    void TestCalculateEntriesCountToFlush(
        const TTestCaseWriteDataEntries& entries,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize)
    {
        TBootstrap b;

        auto actual = b.CalculateEntriesCountToFlush(
            entries,
            maxWriteRequestSize,
            maxWriteRequestsCount,
            maxSumWriteRequestsSize);

        auto expected = b.CalculateEntriesCountToFlushReferenceImpl(
            entries,
            maxWriteRequestSize,
            maxWriteRequestsCount,
            maxSumWriteRequestsSize);

        UNIT_ASSERT_VALUES_EQUAL_C(
            expected,
            actual,
            "CalculateEntriesCountToFlush failed for " << entries);
    }

    Y_UNIT_TEST(ShouldCalculateEntriesCountToFlushRandomized)
    {
        constexpr size_t IterationCount = 100;
        constexpr ui32 MaxWriteRequestSize = 5;
        constexpr ui32 MaxWriteRequestsCount = 3;
        constexpr ui32 MaxSumWriteRequestsSize = 10;
        constexpr ui64 MaxLength = 10;
        constexpr size_t MaxIntervalCount = 10;

        for (size_t iter = 0; iter < IterationCount; iter++) {
            size_t intervalCount = RandomNumber(MaxIntervalCount) + 1;

            TVector<TTestCaseWriteDataEntry> entries;

            while (intervalCount-- > 0) {
                ui64 length = RandomNumber(MaxLength) + 1;
                ui64 offset = RandomNumber(MaxLength - length + 1);

                entries.push_back(
                    {.NodeId = 1, .Offset = offset, .Length = length});
            }

            TTestCaseWriteDataEntries testCase(entries);

            for (ui32 maxWriteRequestSize = 1;
                 maxWriteRequestSize <= MaxWriteRequestSize;
                 maxWriteRequestSize++)
            {
                for (ui32 maxWriteRequestsCount = 1;
                     maxWriteRequestsCount <= MaxWriteRequestsCount;
                     maxWriteRequestsCount++)
                {
                    for (ui32 maxSumWriteRequestsSize = 1;
                         maxSumWriteRequestsSize <= MaxSumWriteRequestsSize;
                         maxSumWriteRequestsSize++)
                    {
                        TestCalculateEntriesCountToFlush(
                            testCase,
                            maxWriteRequestSize,
                            maxWriteRequestsCount,
                            maxSumWriteRequestsSize);
                    }
                }
            }
        }
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
