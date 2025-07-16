#include "write_back_cache_impl.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/intrlist.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/system/tempfile.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
IOutputStream& PrintValues(
    IOutputStream& out,
    const TVector<T>& values)
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TTestCaseWriteDataEntry
{
    ui64 Handle = 0;
    ui64 Offset = 0;
    ui64 Length = 0;
};

IOutputStream& operator<<(
    IOutputStream& out,
    const TTestCaseWriteDataEntry& e)
{
    out << "{"
        << "Handle: " << e.Handle << ", "
        << "Offset: " << e.Offset << ", "
        << "Length: " << e.Length
        << "}";
    return out;
}

IOutputStream& operator<<(
    IOutputStream& out,
    const TVector<TTestCaseWriteDataEntry>& entries)
{
    return PrintValues(out, entries);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestCaseWriteDataEntryPart
{
    // index of TWriteDataEntry which this part refers to
    ui32 SourceIndex = 0;
    ui64 OffsetInSource = 0;
    ui64 Offset = 0;
    ui64 Length = 0;

    bool operator==(const TTestCaseWriteDataEntryPart& rhs) const
    {
        return std::tie(SourceIndex, OffsetInSource, Offset, Length) ==
            std::tie(rhs.SourceIndex, rhs.OffsetInSource, rhs.Offset, rhs.Length);
    }

    bool operator<(const TTestCaseWriteDataEntryPart& rhs) const
    {
        return std::tie(SourceIndex, OffsetInSource, Offset, Length) <
            std::tie(rhs.SourceIndex, rhs.OffsetInSource, rhs.Offset, rhs.Length);
    }
};

IOutputStream& operator<<(
    IOutputStream& out,
    const TTestCaseWriteDataEntryPart& p)
{
    out << "{"
        << "SourceIndex: "    << p.SourceIndex    << ", "
        << "OffsetInSource: " << p.OffsetInSource << ", "
        << "Offset: "         << p.Offset         << ", "
        << "Length: "         << p.Length
        << "}";
    return out;
}

IOutputStream& operator<<(
    IOutputStream& out,
    const TVector<TTestCaseWriteDataEntryPart>& parts)
{
    return PrintValues(out, parts);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestCaseWriteDataRange
{
    ui64 Offset = 0;
    ui64 Length = 0;
};

IOutputStream& operator<<(
    IOutputStream& out,
    const TTestCaseWriteDataRange& e)
{
    out << "{"
        << "Offset: " << e.Offset << ", "
        << "Length: " << e.Length
        << "}";
    return out;
}

IOutputStream& operator<<(
    IOutputStream& out,
    const TVector<TTestCaseWriteDataRange>& entries)
{
    return PrintValues(out, entries);
}

////////////////////////////////////////////////////////////////////////////////

struct TCalculateDataPartsToReadTestBootstrap
{
    using TWriteDataEntry = TWriteBackCache::TWriteDataEntry;
    using TWriteDataEntryPart = TWriteBackCache::TWriteDataEntryPart;

    ILoggingServicePtr Logging;
    TLog Log;

    TCalculateDataPartsToReadTestBootstrap()
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");
    }

    ~TCalculateDataPartsToReadTestBootstrap() = default;

    TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TVector<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length)
    {
        return TWriteBackCache::TUtil::CalculateDataPartsToRead(
            entries,
            startingFromOffset,
            length);
    }

    TVector<TWriteDataEntryPart> InvertDataParts(
        const TVector<TWriteDataEntryPart>& parts,
        ui64 startingFromOffset,
        ui64 length)
    {
        return TWriteBackCache::TUtil::InvertDataParts(
            parts,
            startingFromOffset,
            length);
    }

    TVector<TWriteDataEntryPart> CalculateDataPartsToReadReferenceImpl(
        const TVector<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length)
    {
        const auto endOffset = startingFromOffset + length;

        TVector<TWriteDataEntryPart> parts;
        for (ui64 offset = startingFromOffset; offset != endOffset; offset++) {
            TWriteDataEntry* lastEntry = nullptr;
            for (auto* entry: entries) {
                if (entry->Offset() <= offset && offset < entry->End()) {
                    lastEntry = entry;
                }
            }

            if (lastEntry != nullptr) {
                parts.emplace_back(
                    lastEntry,
                    offset - lastEntry->Offset(),
                    offset,
                    1);
            }
        }

        TVector<TWriteDataEntryPart> res;
        res.push_back(parts.front());

        // merge consecutive parts
        for (size_t partIndex = 1; partIndex < parts.size(); partIndex++) {
            auto offset = res.back().Offset;

            while (partIndex < parts.size()) {
                bool extendsPrevPart =
                    res.back().Source == parts[partIndex].Source;
                extendsPrevPart =
                    extendsPrevPart && offset + 1 == parts[partIndex].Offset;

                if (!extendsPrevPart) {
                    res.push_back(parts[partIndex]);
                    break;
                }

                res.back().Length++;
                partIndex++;
                offset++;
            }
        }

        return res;
    }
};

using TWriteDataEntry = TCalculateDataPartsToReadTestBootstrap::TWriteDataEntry;
using TWriteDataEntryPart =
    TCalculateDataPartsToReadTestBootstrap::TWriteDataEntryPart;

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator<<(
    IOutputStream& out,
    const TWriteDataEntry& e)
{
    out << "{"
        << "Handle: " << e.GetHandle() << ", "
        << "Offset: " << e.Offset() << ", "
        << "Length: " << e.GetBuffer().Size()
        << "}";
    return out;
}

IOutputStream& operator<<(
    IOutputStream& out,
    const TWriteDataEntryPart& p)
{
    const auto printedSource = reinterpret_cast<ui64>(p.Source);
    out << "{"
        << "Source: "         << printedSource    << ", "
        << "OffsetInSource: " << p.OffsetInSource << ", "
        << "Offset: "         << p.Offset         << ", "
        << "Length: "         << p.Length
        << "}";
    return out;
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCalculateDataPartsToReadTest)
{
    constexpr ui64 MaxLength = 100;

    void TestShouldCorrectlyCalculateDataPartsToRead(
        const TVector<TTestCaseWriteDataEntry>& testCaseEntries,
        const TVector<TTestCaseWriteDataEntryPart>& expectedParts)
    {
        TIntrusiveListWithAutoDelete<TWriteDataEntry, TDelete> entries;
        for (const auto& e: testCaseEntries) {
            Y_ABORT_UNLESS(e.Offset + e.Length < MaxLength);

            auto request = std::make_shared<NProto::TWriteDataRequest>();
            request->SetHandle(e.Handle);
            request->SetOffset(e.Offset);
            request->SetBuffer(TString(e.Length, 'a')); // dummy buffer

            auto entry = std::make_unique<TWriteDataEntry>(std::move(request));
            entries.PushBack(entry.release());
        }

        TVector<TWriteDataEntry*> entryPtrs;
        for (auto& entry: entries) {
            entryPtrs.push_back(&entry);
        }

        TCalculateDataPartsToReadTestBootstrap b;
        const auto parts = b.CalculateDataPartsToRead(
            entryPtrs,
            0,
            MaxLength);

        TVector<TTestCaseWriteDataEntryPart> actualParts;
        for (auto& part: parts) {
            std::optional<size_t> sourceIndex;
            for (size_t i = 0; i < entryPtrs.size(); i++) {
                if (part.Source == entryPtrs[i]) {
                    sourceIndex = i;
                    break;
                }
            }
            UNIT_ASSERT_C(
                sourceIndex,
                "read part " << part << " that is not found in entries list");

            actualParts.emplace_back(
                *sourceIndex,
                part.OffsetInSource,
                part.Offset,
                part.Length);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            expectedParts,
            actualParts,
            "failed for test case with entries: " << testCaseEntries);
    }

    void TestShouldCorrectlyCalculateDataPartsToReadWithReferenceImpl(
        const TVector<TTestCaseWriteDataEntry>& testCaseEntries)
    {
        TIntrusiveListWithAutoDelete<TWriteDataEntry, TDelete> entries;
        for (const auto& e: testCaseEntries) {
            Y_ABORT_UNLESS(e.Offset + e.Length <= MaxLength);

            auto request = std::make_shared<NProto::TWriteDataRequest>();
            request->SetHandle(e.Handle);
            request->SetOffset(e.Offset);
            request->SetBuffer(TString(e.Length, 'a')); // dummy buffer

            auto entry = std::make_unique<TWriteDataEntry>(std::move(request));
            entries.PushBack(entry.release());
        }

        TVector<TWriteDataEntry*> entryPtrs;
        for (auto& entry: entries) {
            entryPtrs.push_back(&entry);
        }

        TCalculateDataPartsToReadTestBootstrap b;

        auto expectedParts = b.CalculateDataPartsToReadReferenceImpl(
            entryPtrs,
            0,
            MaxLength);
        auto actualParts = b.CalculateDataPartsToRead(
            entryPtrs,
            0,
            MaxLength);

        UNIT_ASSERT_VALUES_EQUAL_C(
            expectedParts,
            actualParts,
            "failed for test case with entries: " << testCaseEntries);
    }

    void TestShouldCorrectlyInvertDataParts(
        const TVector<TTestCaseWriteDataRange>& testCaseEntries,
        const TVector<TTestCaseWriteDataRange>& expectedResult,
        int rangeOffset,
        int rangeLength)
    {
        TVector<TWriteDataEntryPart> parts;
        for (const auto& e: testCaseEntries) {
            parts.push_back(TWriteDataEntryPart {
                .Offset = e.Offset,
                .Length = e.Length
            });
        }

        TCalculateDataPartsToReadTestBootstrap b;
        const auto actualParts = b.InvertDataParts(
            parts,
            rangeOffset,
            rangeLength);

        TVector<TWriteDataEntryPart> expectedParts;
        for (const auto& e: expectedResult) {
            expectedParts.push_back(TWriteDataEntryPart {
                .Offset = e.Offset,
                .Length = e.Length
            });
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            expectedParts,
            actualParts,
            "failed for test case with entries: " << testCaseEntries <<
            " for offset = " << rangeOffset << ", length = " << rangeLength);
    }

    void ValidateDataParts(
        const TVector<TWriteDataEntryPart>& parts,
        ui64 rangeOffset,
        ui64 rangeLength)
    {
        if (parts.empty()) {
            return;
        }

        UNIT_ASSERT_GE(parts.front().Offset, rangeOffset);
        UNIT_ASSERT_LE(
            parts.back().Offset + parts.back().Length,
            rangeOffset + rangeLength);

        for (const auto& part: parts) {
            UNIT_ASSERT_GT(part.Length, 0);
        }

        for (size_t i = 1; i < parts.size(); i++) {
            UNIT_ASSERT_LT(
                parts[i - 1].Offset + parts[i - 1].Length,
                parts[i].Offset);
        }
    }

    bool IsInAnyOfDataParts(
        const TVector<TWriteDataEntryPart>& parts,
        ui64 offset)
    {
        return std::ranges::any_of(parts, [offset](const auto& part) {
            return part.Offset <= offset && offset < part.Offset + part.Length;
        });
    }

    void TestShouldCorrectlyInvertDataParts(
        const TVector<TTestCaseWriteDataEntry>& testCaseEntries,
        ui64 rangeOffset,
        ui64 rangeLength)
    {
        TIntrusiveListWithAutoDelete<TWriteDataEntry, TDelete> entries;
        for (const auto& e: testCaseEntries) {
            Y_ABORT_UNLESS(e.Offset + e.Length <= MaxLength);

            auto entry = std::make_unique<TWriteDataEntry>(
                e.Handle,
                e.Offset,
                e.Length,
                TStringBuf(),
                TString(),
                NProto::THeaders());
            entries.PushBack(entry.release());
        }

        TVector<TWriteDataEntry*> entryPtrs;
        for (auto& entry: entries) {
            entryPtrs.push_back(&entry);
        }

        TCalculateDataPartsToReadTestBootstrap b;

        auto parts = b.CalculateDataPartsToReadReferenceImpl(
            entryPtrs,
            0,
            MaxLength);

        const auto invertedParts = b.InvertDataParts(
            parts,
            rangeOffset,
            rangeLength);

        ValidateDataParts(invertedParts, rangeOffset, rangeLength);

        for (ui64 i = 0; i < rangeLength; i++) {
            ui64 offset = rangeOffset + i;
            bool isInActualParts = IsInAnyOfDataParts(parts, offset);
            bool isInInvertedParts = IsInAnyOfDataParts(invertedParts, offset);
            UNIT_ASSERT_C(
                isInActualParts != isInInvertedParts,
                "failed for test case with entries: " << testCaseEntries <<
                " for offset = " << offset);
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateDataPartsToRead)
    {
        TestShouldCorrectlyCalculateDataPartsToRead(
            {
                {1, 0, 10}
            },
            {
                {0, 0, 0, 10}
            }
        );
        TestShouldCorrectlyCalculateDataPartsToRead(
            {
                {1, 0, 10}, {1, 0, 10}
            },
            {
                {1, 0, 0, 10}
            }
        );
        TestShouldCorrectlyCalculateDataPartsToRead(
            {
                {1, 0, 10}, {1, 1, 8}
            },
            {
                {0, 0, 0, 1}, {1, 0, 1, 8}, {0, 9, 9, 1}
            }
        );
        TestShouldCorrectlyCalculateDataPartsToRead(
            {
                {1, 0, 10}, {1, 12, 10}, {1, 24, 11}
            },
            {
                {0, 0, 0, 10}, {1, 0, 12, 10}, {2, 0, 24, 11}
            }
        );
        TestShouldCorrectlyCalculateDataPartsToRead(
            {
                {1, 0, 10}, {1, 12, 10}, {1, 24, 11}, {1, 3, 30}
            },
            {
                {0, 0, 0, 3}, {3, 0, 3, 30}, {2, 9, 33, 2}
            }
        );
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateDataPartsToReadRandomized)
    {
        TVector<TTestCaseWriteDataEntry> entries;

        size_t remainingEntries = 111;
        while (remainingEntries--) {
            const auto offset = RandomNumber<ui64>(MaxLength);
            const auto length = RandomNumber<ui64>(MaxLength - offset) + 1;
            entries.emplace_back(1, offset, length);
        }

        TestShouldCorrectlyCalculateDataPartsToReadWithReferenceImpl(entries);
    }

    Y_UNIT_TEST(ShouldCorrectlyInvertDataParts)
    {
        TestShouldCorrectlyInvertDataParts(
            { },
            {
                {2, 10}
            },
            2,
            10
        );
        TestShouldCorrectlyInvertDataParts(
            {
                {2, 10}
            },
            { },
            2,
            10
        );
        TestShouldCorrectlyInvertDataParts(
            {
                {0, 14}
            },
            { },
            2,
            10
        );
        TestShouldCorrectlyInvertDataParts(
            {
                {4, 6}
            },
            {
                {2, 2}, {10, 2}
            },
            2,
            10
        );
        TestShouldCorrectlyInvertDataParts(
            {
                {4, 8}
            },
            {
                {2, 2}
            },
            2,
            10
        );

        TestShouldCorrectlyInvertDataParts(
            {
                {2, 8}
            },
            {
                {10, 2}
            },
            2,
            10
        );
        TestShouldCorrectlyInvertDataParts(
            {
                {2, 4}, {6, 6}
            },
            { },
            2,
            10
        );
        TestShouldCorrectlyInvertDataParts(
            {
                {2, 4}, {8, 4}
            },
            {
                {6, 2}
            },
            2,
            10
        );
        TestShouldCorrectlyInvertDataParts(
            {
                {0, 1}, {1, 3}, {10, 3}, {13, 1}
            },
            {
                {4, 6}
            },
            2,
            10
        );
    }

    Y_UNIT_TEST(ShouldCorrectlyInvertDataPartsRandomized)
    {
        TVector<TTestCaseWriteDataEntry> entries;

        size_t remainingEntries = 111;
        while (remainingEntries--) {
            const auto offset = RandomNumber<ui64>(MaxLength);
            const auto length = RandomNumber<ui64>(MaxLength - offset) + 1;
            entries.emplace_back(1, offset, length);
        }

        const auto regionOffset = RandomNumber<ui64>(MaxLength);
        const auto regionLength =
            RandomNumber<ui64>(MaxLength - regionOffset) + 1;

        TestShouldCorrectlyInvertDataParts(entries, regionOffset, regionLength);
    }
}

}   // namespace NCloud::NFileStore::NFuse
