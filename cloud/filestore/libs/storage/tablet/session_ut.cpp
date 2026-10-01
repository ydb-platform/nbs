#include "session.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSessionTest)
{
    Y_UNIT_TEST(ShouldEvictDupCacheEntriesWithoutRequestId)
    {
        for (bool committed: {false, true}) {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            InitCriticalEventsCounter(counters);
            auto invalidEntryCount = counters->GetCounter(
                GetCriticalEventForInvalidDupCacheEntry(),
                true);

            TSession session({}, {});

            auto addEntry = [&](ui64 entryId, ui64 requestId) {
                NProto::TDupCacheEntry entry;
                entry.SetEntryId(entryId);
                if (requestId) {
                    entry.SetRequestId(requestId);
                }
                if (committed) {
                    session.LoadDupCacheEntry(std::move(entry));
                } else {
                    session.AddDupCacheEntry(std::move(entry), false);
                }
            };

            addEntry(2, 0);
            UNIT_ASSERT_VALUES_EQUAL(1, invalidEntryCount->Val());
            addEntry(3, 42);
            UNIT_ASSERT_VALUES_EQUAL(1, invalidEntryCount->Val());
            addEntry(4, 0);
            UNIT_ASSERT_VALUES_EQUAL(2, invalidEntryCount->Val());

            UNIT_ASSERT(!session.LookupDupEntry(0));
            UNIT_ASSERT(!session.AccessDupEntry(0));
            auto* entry = session.LookupDupEntry(42);
            UNIT_ASSERT(entry);
            UNIT_ASSERT_VALUES_EQUAL(3, entry->GetEntryId());
            UNIT_ASSERT_VALUES_EQUAL(committed, entry->Committed);

            UNIT_ASSERT_VALUES_EQUAL(0, session.PopDupCacheEntry(3));
            UNIT_ASSERT_VALUES_EQUAL(2, session.PopDupCacheEntry(2));
            UNIT_ASSERT(session.LookupDupEntry(42));
            UNIT_ASSERT_VALUES_EQUAL(3, session.PopDupCacheEntry(1));
            UNIT_ASSERT(!session.LookupDupEntry(42));
            UNIT_ASSERT_VALUES_EQUAL(4, session.PopDupCacheEntry(0));
            UNIT_ASSERT_VALUES_EQUAL(0, session.PopDupCacheEntry(0));

            if (committed) {
                UNIT_ASSERT_VALUES_EQUAL(5, session.GenerateDupCacheEntryId());
            }
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
