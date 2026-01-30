#include "node_cache.h"

#include "write_data_request_manager.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_persistent_storage.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_write_back_cache_stats.h>
#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

constexpr ui64 MaxLength = 100;

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

IOutputStream& operator<<(
    IOutputStream& out,
    const TVector<TTestCaseWriteDataEntry>& entries)
{
    return PrintValues(out, entries);
}

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
               std::tie(
                   rhs.SourceIndex,
                   rhs.OffsetInSource,
                   rhs.Offset,
                   rhs.Length);
    }

    bool operator<(const TTestCaseWriteDataEntryPart& rhs) const
    {
        return std::tie(SourceIndex, OffsetInSource, Offset, Length) <
               std::tie(
                   rhs.SourceIndex,
                   rhs.OffsetInSource,
                   rhs.Offset,
                   rhs.Length);
    }
};

IOutputStream& operator<<(
    IOutputStream& out,
    const TTestCaseWriteDataEntryPart& p)
{
    out << "{"
        << "SourceIndex: " << p.SourceIndex << ", "
        << "OffsetInSource: " << p.OffsetInSource << ", "
        << "Offset: " << p.Offset << ", "
        << "Length: " << p.Length << "}";
    return out;
}

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    ITimerPtr Timer;
    std::shared_ptr<TTestWriteBackCacheStats> Stats;
    std::shared_ptr<TTestStorage> Storage;
    TWriteDataRequestManager RequestManager;
    TVector<std::unique_ptr<TCachedWriteDataRequest>> Requests;
    NWriteBackCache::TNodeCache Cache;

    TBootstrap()
        : Timer(CreateWallClockTimer())
        , Stats(std::make_shared<TTestWriteBackCacheStats>())
        , Storage(std::make_shared<TTestStorage>(Stats))
        , RequestManager(
              std::make_shared<TSequenceIdGenerator>(),
              Storage,
              Timer,
              Stats)
    {}

    void PushUnflushed(ui64 offset, TString data)
    {
        Cache.EnqueueUnflushedRequest(MakeRequest(offset, std::move(data)));
    }

    std::unique_ptr<TCachedWriteDataRequest> MakeRequest(
        ui64 offset,
        TString data)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetOffset(offset);
        *request->MutableBuffer() = std::move(data);

        auto res = RequestManager.AddRequest(std::move(request));
        return std::get<1>(std::move(res));
    }

    TString GetCachedData(ui64 offset, ui64 byteCount) const
    {
        auto cachedData = Cache.GetCachedData(offset, byteCount);

        TStringBuilder out;
        for (const auto& part: cachedData.Parts) {
            if (!out.empty()) {
                out << ", ";
            }
            out << part.RelativeOffset + offset << ":" << part.Data;
        }
        return out;
    }

    TString VisitUnflushedRequests() const
    {
        TStringBuilder out;
        Cache.VisitUnflushedRequests(
            [&out](const TCachedWriteDataRequest* entry)
            {
                if (!out.empty()) {
                    out << ", ";
                }
                out << entry->GetOffset() << ":" << entry->GetBuffer();
                return true;
            });
        return out;
    }
};

////////////////////////////////////////////////////////////////////////////////

TVector<TTestCaseWriteDataEntryPart> CalculateDataPartsToRead(
    const TVector<TTestCaseWriteDataEntry>& testCaseEntries,
    ui64 offset,
    ui64 byteCount)
{
    Y_ABORT_UNLESS(testCaseEntries.size() < 255);

    TBootstrap b;
    for (size_t i = 0; i < testCaseEntries.size(); i++) {
        const auto& e = testCaseEntries[i];
        char c = static_cast<char>(i + 1);
        b.PushUnflushed(e.Offset, TString(e.Length, c));   // dummy buffer
    }
    auto cachedData = b.Cache.GetCachedData(offset, byteCount);
    TVector<TTestCaseWriteDataEntryPart> res;
    for (const auto& part: cachedData.Parts) {
        ui32 i = part.Data[0] - 1;
        Y_ABORT_UNLESS(i < testCaseEntries.size());
        ui64 ofs = part.RelativeOffset + offset;
        res.push_back(
            TTestCaseWriteDataEntryPart{
                i,
                ofs - testCaseEntries[i].Offset,
                ofs,
                part.Data.size()});
    }

    return res;
}

TVector<TTestCaseWriteDataEntryPart> CalculateDataPartsToReadReferenceImpl(
    const TVector<TTestCaseWriteDataEntry>& testCaseEntries,
    ui64 offset,
    ui64 byteCount)
{
    TVector<TTestCaseWriteDataEntryPart> parts;

    for (ui64 i = offset; i != offset + byteCount; i++) {
        std::optional<size_t> index;
        for (size_t j = 0; j < testCaseEntries.size(); j++) {
            const auto& e = testCaseEntries[j];
            if (e.Offset <= i && i < e.Offset + e.Length) {
                index = j;
            }
        }
        if (index) {
            const auto& e = testCaseEntries[*index];
            parts.emplace_back(*index, i - e.Offset, i - offset, 1);
        }
    }

    TVector<TTestCaseWriteDataEntryPart> res;
    res.push_back(parts.front());

    // merge consecutive parts
    for (size_t partIndex = 1; partIndex < parts.size(); partIndex++) {
        auto offset = res.back().Offset;

        while (partIndex < parts.size()) {
            bool extendsPrevPart =
                res.back().SourceIndex == parts[partIndex].SourceIndex;
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

void TestShouldCorrectlyCalculateDataPartsToRead(
    const TVector<TTestCaseWriteDataEntry>& testCaseEntries,
    const TVector<TTestCaseWriteDataEntryPart>& expectedParts)
{
    auto actualParts = CalculateDataPartsToRead(testCaseEntries, 0, MaxLength);

    UNIT_ASSERT_VALUES_EQUAL_C(
        expectedParts,
        actualParts,
        "failed for test case with entries: " << testCaseEntries);
}

void TestShouldCorrectlyCalculateDataPartsToReadWithReferenceImpl(
    const TVector<TTestCaseWriteDataEntry>& testCaseEntries)
{
    auto expectedParts =
        CalculateDataPartsToReadReferenceImpl(testCaseEntries, 0, MaxLength);

    auto actualParts = CalculateDataPartsToRead(testCaseEntries, 0, MaxLength);

    UNIT_ASSERT_VALUES_EQUAL_C(
        expectedParts,
        actualParts,
        "failed for test case with entries: " << testCaseEntries);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeCacheTest)
{
    Y_UNIT_TEST(Simple)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 1);
        UNIT_ASSERT(!b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 4:bc, 7:xy", b.GetCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL(
            "3:abc, 1:def, 7:xyz",
            b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlush)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.MoveFrontUnflushedRequestToFlushed();

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 2);
        UNIT_ASSERT(b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 1);
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 4:bc, 7:xy", b.GetCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("1:def, 7:xyz", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlushAndEvict)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.MoveFrontUnflushedRequestToFlushed();
        b.Cache.DequeueFlushedRequest();

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 2);
        UNIT_ASSERT(!b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 7:xy", b.GetCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("1:def, 7:xyz", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlushAll)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.MoveFrontUnflushedRequestToFlushed();
        b.Cache.MoveFrontUnflushedRequestToFlushed();
        b.Cache.MoveFrontUnflushedRequestToFlushed();

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(!b.Cache.HasUnflushedRequests());
        UNIT_ASSERT(b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 1);
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 4:bc, 7:xy", b.GetCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlushAllAndEvict)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.MoveFrontUnflushedRequestToFlushed();
        b.Cache.MoveFrontUnflushedRequestToFlushed();
        b.Cache.MoveFrontUnflushedRequestToFlushed();

        b.Cache.DequeueFlushedRequest();

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(!b.Cache.HasUnflushedRequests());
        UNIT_ASSERT(b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 2);
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 7:xy", b.GetCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlushAllAndEvictAll)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.MoveFrontUnflushedRequestToFlushed();
        b.Cache.MoveFrontUnflushedRequestToFlushed();
        b.Cache.MoveFrontUnflushedRequestToFlushed();

        b.Cache.DequeueFlushedRequest();
        b.Cache.DequeueFlushedRequest();
        b.Cache.DequeueFlushedRequest();

        UNIT_ASSERT(b.Cache.Empty());
        UNIT_ASSERT(!b.Cache.HasUnflushedRequests());
        UNIT_ASSERT(!b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL("", b.GetCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateDataPartsToRead)
    {
        TestShouldCorrectlyCalculateDataPartsToRead(
            {{1, 0, 10}},
            {{0, 0, 0, 10}});
        TestShouldCorrectlyCalculateDataPartsToRead(
            {{1, 0, 10}, {1, 0, 10}},
            {{1, 0, 0, 10}});
        TestShouldCorrectlyCalculateDataPartsToRead(
            {{1, 0, 10}, {1, 1, 8}},
            {{0, 0, 0, 1}, {1, 0, 1, 8}, {0, 9, 9, 1}});
        TestShouldCorrectlyCalculateDataPartsToRead(
            {{1, 0, 10}, {1, 12, 10}, {1, 24, 11}},
            {{0, 0, 0, 10}, {1, 0, 12, 10}, {2, 0, 24, 11}});
        TestShouldCorrectlyCalculateDataPartsToRead(
            {{1, 0, 10}, {1, 12, 10}, {1, 24, 11}, {1, 3, 30}},
            {{0, 0, 0, 3}, {3, 0, 3, 30}, {2, 9, 33, 2}});
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
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
