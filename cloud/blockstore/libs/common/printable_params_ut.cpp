#include "printable_params.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPrintableParams)
{
    Y_UNIT_TEST(PrintKeyValueTest)
    {
        const std::pair<TStringBuf, TPrintableValue> params[] = {
            {"str", TString("hello")},
            {"cstr", static_cast<const char*>("world")},
            {"buf", TStringBuf("bufval")},
            {"i", int{-1}},
            {"u16", ui16{16}},
            {"u32", ui32{32}},
            {"u64", ui64{64}},
            {"range", TBlockRange64::MakeClosedInterval(0, 9)},
            {"flag", std::monostate{}},
        };
        UNIT_ASSERT_VALUES_EQUAL(
            "str:hello cstr:world buf:bufval i:-1 u16:16 u32:32 u64:64"
            " range:[0..9] flag",
            PrintParams(params));
    }
}

}   // namespace NCloud::NBlockStore
