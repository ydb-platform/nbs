#include "log_record.h"

#include <library/cpp/testing/unittest/registar.h>

#include <cstring>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalMetadataTest)
{
    Y_UNIT_TEST(ShouldRoundTrip)
    {
        for (ui64 lsnLowWatermark: {0ul, 1ul, 42ul, Max<ui64>()}) {
            auto out = DeserializeMetadata(SerializeMetadata(
                {.Version = CurrentFormatVersion,
                 .LsnLowWatermark = lsnLowWatermark}));

            UNIT_ASSERT(out);
            UNIT_ASSERT_VALUES_EQUAL(CurrentFormatVersion, out->Version);
            UNIT_ASSERT_VALUES_EQUAL(lsnLowWatermark, out->LsnLowWatermark);
        }
    }

    Y_UNIT_TEST(ShouldRejectTruncatedBuffer)
    {
        const auto good = SerializeMetadata(
            {.Version = CurrentFormatVersion, .LsnLowWatermark = 7});

        // every prefix must be rejected, never read past its end
        for (size_t len = 0; len < good.Size(); ++len) {
            UNIT_ASSERT_C(
                !DeserializeMetadata(TBuffer(good.Data(), len)),
                "len=" << len);
        }

        UNIT_ASSERT(DeserializeMetadata(good));
    }

    Y_UNIT_TEST(ShouldRejectTrailingGarbage)
    {
        const auto good = SerializeMetadata(
            {.Version = CurrentFormatVersion, .LsnLowWatermark = 7});

        TBuffer extra(good.Data(), good.Size());
        extra.Append("x", 1);

        UNIT_ASSERT(!DeserializeMetadata(extra));
    }

    Y_UNIT_TEST(ShouldRejectAnotherFormatVersion)
    {
        auto buffer = SerializeMetadata(
            {.Version = CurrentFormatVersion, .LsnLowWatermark = 7});

        const ui64 bogus = CurrentFormatVersion + 1;
        memcpy(buffer.Data(), &bogus, sizeof(bogus));

        UNIT_ASSERT(!DeserializeMetadata(buffer));
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogRecordTest)
{
    TLogRecord MakeRecord(ui64 lsn, ui64 prevLsn, ui64 mappingCount)
    {
        TLogRecord record;
        record.Lsn = lsn;
        record.PrevLsn = prevLsn;
        record.PagesCrc = 0xC0FFEE;

        for (ui64 i = 0; i < mappingCount; ++i) {
            record.PageMappings.push_back(
                {.PageNo = 100 + i * 10,
                 .Location = {.FirstPageNo = i * 3, .PageCount = i + 1}});
        }

        return record;
    }

    Y_UNIT_TEST(ShouldRoundTrip)
    {
        for (ui64 mappingCount: {0ul, 1ul, 5ul}) {
            const auto in = MakeRecord(42, 41, mappingCount);

            auto out = DeserializeRecord(SerializeRecord(in));

            UNIT_ASSERT(out);
            UNIT_ASSERT_VALUES_EQUAL(in.Lsn, out->Lsn);
            UNIT_ASSERT_VALUES_EQUAL(in.PrevLsn, out->PrevLsn);
            UNIT_ASSERT_VALUES_EQUAL(in.PagesCrc, out->PagesCrc);
            UNIT_ASSERT_VALUES_EQUAL(
                in.PageMappings.size(),
                out->PageMappings.size());

            for (size_t i = 0; i < in.PageMappings.size(); ++i) {
                const auto& expected = in.PageMappings[i];
                const auto& actual = out->PageMappings[i];

                UNIT_ASSERT_VALUES_EQUAL(expected.PageNo, actual.PageNo);
                UNIT_ASSERT_VALUES_EQUAL(
                    expected.Location.FirstPageNo,
                    actual.Location.FirstPageNo);
                UNIT_ASSERT_VALUES_EQUAL(
                    expected.Location.PageCount,
                    actual.Location.PageCount);
            }

            // a restored record gets a fresh promise to be completed later
            UNIT_ASSERT(out->Promise.Initialized());
            UNIT_ASSERT(!out->Promise.HasValue());
        }
    }

    Y_UNIT_TEST(ShouldRejectTruncatedBuffer)
    {
        const auto good = SerializeRecord(MakeRecord(42, 41, 2));

        // every prefix must be rejected, never read past its end
        for (size_t len = 0; len < good.Size(); ++len) {
            UNIT_ASSERT_C(
                !DeserializeRecord(TBuffer(good.Data(), len)),
                "len=" << len);
        }

        UNIT_ASSERT(DeserializeRecord(good));
    }

    Y_UNIT_TEST(ShouldRejectTrailingGarbage)
    {
        const auto good = SerializeRecord(MakeRecord(42, 41, 2));

        TBuffer extra(good.Data(), good.Size());
        extra.Append("x", 1);

        UNIT_ASSERT(!DeserializeRecord(extra));
    }

    Y_UNIT_TEST(ShouldRejectMismatchedPageMappingCount)
    {
        auto buffer = SerializeRecord(MakeRecord(42, 41, 2));

        // the count is the third field of the header, after lsn and prevLsn
        const ui32 bogus = 3;
        memcpy(buffer.Data() + 2 * sizeof(ui64), &bogus, sizeof(bogus));

        UNIT_ASSERT(!DeserializeRecord(buffer));
    }

    Y_UNIT_TEST(ShouldCalculatePagesCrcOverPagesInOrder)
    {
        auto page = [](TStringBuf content)
        {
            return TBuffer(content.data(), content.size());
        };

        UNIT_ASSERT_VALUES_EQUAL(0, CalculatePagesCrc(TVector<TBuffer>()));

        const ui32 crc =
            CalculatePagesCrc(TVector<TBuffer>{page("abcd"), page("efgh")});
        UNIT_ASSERT_VALUES_UNEQUAL(0, crc);

        // the crc covers the contents as one stream, so it does not depend
        // on how the stream is cut into pages
        UNIT_ASSERT_VALUES_EQUAL(
            crc,
            CalculatePagesCrc(TVector<TBuffer>{page("abcdefgh")}));

        // but it does depend on their order and on every byte
        UNIT_ASSERT_VALUES_UNEQUAL(
            crc,
            CalculatePagesCrc(TVector<TBuffer>{page("efgh"), page("abcd")}));
        UNIT_ASSERT_VALUES_UNEQUAL(
            crc,
            CalculatePagesCrc(TVector<TBuffer>{page("abcd"), page("efgX")}));
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCachedPagesTest)
{
    TVector<TBuffer> MakePages(ui64 pageCount)
    {
        TVector<TBuffer> pages;
        for (ui64 i = 0; i < pageCount; ++i) {
            pages.emplace_back("abcd", 4);
        }

        return pages;
    }

    Y_UNIT_TEST(ShouldCountKeptPagesUntilDestroyed)
    {
        auto counter = std::make_shared<std::atomic<ui64>>(0);

        {
            auto pages = TCachedPages::Reserve(MakePages(2), counter, 8);
            UNIT_ASSERT(!pages.Empty());
            UNIT_ASSERT_VALUES_EQUAL(2, pages.Get().size());
            UNIT_ASSERT_VALUES_EQUAL(8, counter->load());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, counter->load());
    }

    Y_UNIT_TEST(ShouldNotKeepPagesOverLimit)
    {
        auto counter = std::make_shared<std::atomic<ui64>>(0);

        auto first = TCachedPages::Reserve(MakePages(2), counter, 11);
        UNIT_ASSERT(!first.Empty());

        auto second = TCachedPages::Reserve(MakePages(1), counter, 11);
        UNIT_ASSERT(second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(8, counter->load());

        // the pages fit again once the first ones are gone
        first = {};
        UNIT_ASSERT_VALUES_EQUAL(0, counter->load());

        second = TCachedPages::Reserve(MakePages(1), counter, 11);
        UNIT_ASSERT(!second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(4, counter->load());
    }

    Y_UNIT_TEST(ShouldKeepCopiesOfPages)
    {
        auto counter = std::make_shared<std::atomic<ui64>>(0);

        TString content = "abcd";
        const TVector<TStringBuf> views = {content, content};

        auto pages = TCachedPages::Reserve(views, counter, 8);
        UNIT_ASSERT_VALUES_EQUAL(8, counter->load());

        // the kept pages do not depend on the source
        content = "efgh";

        UNIT_ASSERT_VALUES_EQUAL(2, pages.Get().size());
        for (const auto& page: pages.Get()) {
            UNIT_ASSERT_VALUES_EQUAL(
                "abcd",
                TStringBuf(page.Data(), page.Size()));
        }

        UNIT_ASSERT(TCachedPages::Reserve(views, counter, 8).Empty());
        UNIT_ASSERT_VALUES_EQUAL(8, counter->load());
    }

    Y_UNIT_TEST(ShouldCountMovedPagesOnce)
    {
        auto counter = std::make_shared<std::atomic<ui64>>(0);

        {
            auto pages = TCachedPages::Reserve(MakePages(1), counter, 8);
            TCachedPages moved(std::move(pages));

            UNIT_ASSERT_VALUES_EQUAL(1, moved.Get().size());
            UNIT_ASSERT_VALUES_EQUAL(4, counter->load());

            // the new pages replace the old ones in the counter
            moved = TCachedPages::Reserve(MakePages(1), counter, 8);
            UNIT_ASSERT_VALUES_EQUAL(4, counter->load());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, counter->load());
    }
}

}   // namespace NCloud::NJournalled
