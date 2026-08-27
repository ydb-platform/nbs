#include "utils.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/shuffle.h>
#include <util/stream/format.h>
#include <util/string/join.h>

#include <array>

namespace NCloud::NBlockStore::NNvme {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TVector<ui8>& opcodes)
{
    TStringBuilder out;

    out << "[ ";
    for (ui8 opcode: opcodes) {
        out << Hex(opcode, HF_ADDX) << " ";
    }
    out << "]";

    return out;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNVMeUtilsTest)
{
    Y_UNIT_TEST(ShouldCalculateOpcodesToLock)
    {
        struct TCase
        {
            TVector<ui8> AllowedOpcodes;
            TVector<ui8> Lockable;
            TVector<ui8> Prohibited;
            TVector<ui8> Expected;
        };

        const TCase cases[]{
            {{}, {}, {}, {}},
            {{}, {0x1, 0x2, 0x3}, {}, {0x1, 0x2, 0x3}},
            {{}, {0x1, 0x2, 0x3}, {0x1, 0x2, 0x3}, {}},
            {{}, {0x1, 0x2, 0x3, 0x4}, {0x1, 0x3}, {0x2, 0x4}},
            {{0x1, 0x2, 0x3}, {0x1, 0x2, 0x3}, {0x1, 0x2, 0x3}, {}},
            {{0x1}, {0x1, 0x2, 0x3}, {}, {0x2, 0x3}},
            {{0x1}, {0x1, 0x2, 0x3}, {0x3}, {0x2}},
            {{0x1, 0x2, 0x3, 0x4, 0x5, 0x6}, {0x1, 0x2, 0x3}, {}, {}},
            {{0x1, 0x4, 0x5, 0x6}, {0x1, 0x2, 0x3}, {0x3}, {0x2}},
            {{0x1, 0x3},
             {0x1, 0x2, 0x3, 0x4, 0x5, 0x6},
             {0x5, 0x6},
             {0x2, 0x4}},
        };

        for (size_t i = 0; i != std::size(cases); ++i) {
            auto t = cases[i];

            ShuffleRange(t.AllowedOpcodes);
            ShuffleRange(t.Lockable);
            ShuffleRange(t.Prohibited);

            auto result = CalculateOpcodesToLock(
                t.AllowedOpcodes,
                t.Lockable,
                t.Prohibited);
            UNIT_ASSERT_EQUAL_C(
                t.Expected,
                result,
                " " << ToString(t.Expected) << " != " << ToString(result)
                    << ". Test case #" << (i + 1) << " { "
                    << JoinSeq(
                           ", ",
                           {ToString(t.AllowedOpcodes),
                            ToString(t.Lockable),
                            ToString(t.Prohibited)})
                    << " }");
        }
    }
}

}   // namespace NCloud::NBlockStore::NNvme
