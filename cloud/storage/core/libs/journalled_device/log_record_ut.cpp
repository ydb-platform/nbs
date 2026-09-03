#include "log_record.h"

#include <library/cpp/testing/unittest/registar.h>

#include <cstring>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalMetadataTest)
{
    Y_UNIT_TEST(ShouldRoundTrip)
    {
        for (ui64 lastAckedLsn: {0ul, 1ul, 42ul, Max<ui64>()}) {
            auto out = DeserializeMetadata(SerializeMetadata(
                {.Version = CurrentFormatVersion,
                 .LastAckedLsn = lastAckedLsn}));

            UNIT_ASSERT(out);
            UNIT_ASSERT_VALUES_EQUAL(CurrentFormatVersion, out->Version);
            UNIT_ASSERT_VALUES_EQUAL(lastAckedLsn, out->LastAckedLsn);
        }
    }

    Y_UNIT_TEST(ShouldRejectATruncatedBuffer)
    {
        const auto good = SerializeMetadata(
            {.Version = CurrentFormatVersion, .LastAckedLsn = 7});

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
            {.Version = CurrentFormatVersion, .LastAckedLsn = 7});

        TBuffer extra(good.Data(), good.Size());
        extra.Append("x", 1);

        UNIT_ASSERT(!DeserializeMetadata(extra));
    }

    Y_UNIT_TEST(ShouldRejectAnotherFormatVersion)
    {
        auto buffer = SerializeMetadata(
            {.Version = CurrentFormatVersion, .LastAckedLsn = 7});

        const ui32 bogus = CurrentFormatVersion + 1;
        memcpy(buffer.Data(), &bogus, sizeof(bogus));

        UNIT_ASSERT(!DeserializeMetadata(buffer));
    }
}

}   // namespace NCloud::NJournalled
