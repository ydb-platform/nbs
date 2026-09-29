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

    Y_UNIT_TEST(ShouldRejectATruncatedBuffer)
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

    Y_UNIT_TEST(ShouldRejectATruncatedBuffer)
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

    Y_UNIT_TEST(ShouldRejectAMismatchedPageMappingCount)
    {
        auto buffer = SerializeRecord(MakeRecord(42, 41, 2));

        // the count is the third field of the header, after lsn and prevLsn
        const ui64 bogus = 3;
        memcpy(buffer.Data() + 2 * sizeof(ui64), &bogus, sizeof(bogus));

        UNIT_ASSERT(!DeserializeRecord(buffer));
    }
}

}   // namespace NCloud::NJournalled
