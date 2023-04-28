#include "address.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSpdkAddressTest)
{
    Y_UNIT_TEST(ShouldGetNSIDFromTransportId)
    {
        UNIT_ASSERT_VALUES_EQUAL(GetNSIDFromTransportId(""), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetNSIDFromTransportId("ns:42"), 42);
        UNIT_ASSERT_EXCEPTION(GetNSIDFromTransportId("ns:42abc"), yexception);
        UNIT_ASSERT_EXCEPTION(GetNSIDFromTransportId("ns:"), yexception);
        UNIT_ASSERT_EXCEPTION(GetNSIDFromTransportId("ns:-1"), yexception);

        UNIT_ASSERT_VALUES_EQUAL(GetNSIDFromTransportId("ns=42"), 42);
        UNIT_ASSERT_EXCEPTION(GetNSIDFromTransportId("ns=42abc"), yexception);
        UNIT_ASSERT_EXCEPTION(GetNSIDFromTransportId("ns="), yexception);
        UNIT_ASSERT_EXCEPTION(GetNSIDFromTransportId("ns=-1"), yexception);

        UNIT_ASSERT_VALUES_EQUAL(
            GetNSIDFromTransportId("trtype:TCP ns:42 adrfam:IPv6 "), 42);

        UNIT_ASSERT_VALUES_EQUAL(
            GetNSIDFromTransportId("trtype:TCP   ns:42  adrfam:IPv6 "), 42);

        UNIT_ASSERT_VALUES_EQUAL(
            GetNSIDFromTransportId("trtype:TCP ns=42 adrfam:IPv6 "), 42);

        UNIT_ASSERT_VALUES_EQUAL(
            GetNSIDFromTransportId("trtype:TCP adrfam:IPv6 "), 0);
    }
}

}   // namespace NCloud::NBlockStore::NSpdk
