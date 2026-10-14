#include <cloud/blockstore/libs/uring/transport/protocol.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NUring {

Y_UNIT_TEST_SUITE(TUringProtocolTest)
{
    Y_UNIT_TEST(ShouldHaveFixedSizes)
    {
        UNIT_ASSERT_VALUES_EQUAL(24u, sizeof(THello));
        UNIT_ASSERT_VALUES_EQUAL(32u, sizeof(TWelcome));
        UNIT_ASSERT_VALUES_EQUAL(16u, sizeof(TDataHello));
        UNIT_ASSERT_VALUES_EQUAL(8u, sizeof(TDataWelcome));
        UNIT_ASSERT_VALUES_EQUAL(24u, sizeof(TFrameHeader));
        UNIT_ASSERT_VALUES_EQUAL(16u, sizeof(TDataHeader));
        UNIT_ASSERT_VALUES_EQUAL(0x5553424Eu, MAGIC);
    }

    Y_UNIT_TEST(ShouldNegotiateOob)
    {
        using enum EOobMode;
        UNIT_ASSERT(!NegotiateOob(Off, Required).has_value());
        UNIT_ASSERT(!NegotiateOob(Required, Off).has_value());
        UNIT_ASSERT_VALUES_EQUAL(false, *NegotiateOob(Off, Off));
        UNIT_ASSERT_VALUES_EQUAL(false, *NegotiateOob(Off, Auto));
        UNIT_ASSERT_VALUES_EQUAL(false, *NegotiateOob(Auto, Off));
        UNIT_ASSERT_VALUES_EQUAL(true, *NegotiateOob(Auto, Auto));
        UNIT_ASSERT_VALUES_EQUAL(true, *NegotiateOob(Auto, Required));
        UNIT_ASSERT_VALUES_EQUAL(true, *NegotiateOob(Required, Auto));
        UNIT_ASSERT_VALUES_EQUAL(true, *NegotiateOob(Required, Required));
    }

    Y_UNIT_TEST(ShouldRoundTripWire)
    {
        UNIT_ASSERT(FromWire(ToWire(EOobMode::Off)) == EOobMode::Off);
        UNIT_ASSERT(FromWire(ToWire(EOobMode::Auto)) == EOobMode::Auto);
        UNIT_ASSERT(FromWire(ToWire(EOobMode::Required)) == EOobMode::Required);
        UNIT_ASSERT(FromWire(77) == EOobMode::Auto);
    }

    Y_UNIT_TEST(ShouldSerializeError)
    {
        auto bytes = SerializeError(E_REJECTED, "busy");
        auto error = ParseError(bytes);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("busy", error.GetMessage());

        auto bad = ParseError("\x01\x02garbage");
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, bad.GetCode());
    }

    Y_UNIT_TEST(ShouldNotParseSuccessAsError)
    {
        auto empty = ParseError("");
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, empty.GetCode());

        auto success = ParseError(SerializeError(S_OK, "fine"));
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, success.GetCode());
    }
}

}   // namespace NCloud::NBlockStore::NUring
