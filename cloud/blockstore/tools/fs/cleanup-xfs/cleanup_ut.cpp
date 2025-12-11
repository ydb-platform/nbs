#include "cleanup.h"

#include "public.h"

#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/stream/file.h>
#include <util/system/mutex.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDev: IDev
{
    mutable TMutex Mtx;
    mutable TVector<std::pair<ui32, i64>> W;
    mutable TVector<std::pair<ui32, i64>> R;

    void Pwrite(const void* buffer, ui32 byteCount, i64 offset) const override
    {
        Y_UNUSED(buffer);

        with_lock (Mtx) {
            W.emplace_back(byteCount, offset);
        }
    }

    void Pload(void* buffer, ui32 byteCount, i64 offset) const override
    {
        Y_UNUSED(buffer);

        with_lock (Mtx) {
            R.emplace_back(byteCount, offset);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCleanupTest)
{
    Y_UNIT_TEST(ShouldCleanup4K)
    {
        TSuperBlock sb{4096, 4, 6094848, 4096};

        TVector<TFreeList> freeSpace{
            {0, 1000, 100},
            {1, 2000, 200},
            {2, 3000, 300},
            {3, 4000, 400},
        };

        TDev dev;

        Cleanup(dev, sb, freeSpace, 2, false);

        UNIT_ASSERT_VALUES_EQUAL(0, dev.R.size());
        UNIT_ASSERT_VALUES_EQUAL(sb.GroupCount, dev.W.size());

        SortBy(dev.W, [](auto& p) { return p.second; });

        for (ui64 i = 0; i != dev.W.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                sb.BlockSize * freeSpace[i].Count,
                dev.W[i].first);

            UNIT_ASSERT_VALUES_EQUAL(
                sb.BlockSize * (i * sb.BlocksPerGroup + freeSpace[i].Offset),
                dev.W[i].second);
        }
    }

    Y_UNIT_TEST(ShouldCleanup512)
    {
        TSuperBlock sb{4096, 4, 6094848, 512};

        TVector<TFreeList> freeSpace{
            {0, 1000, 100},
            {1, 2000, 200},
            {2, 3000, 300},
            {3, 4000, 400},
        };

        TDev dev;

        Cleanup(dev, sb, freeSpace, 2, false);

        TVector<std::pair<ui32, i64>> expectedW{
            {sb.BlockSize, sb.BlocksPerGroup * sb.BlockSize},
            {sb.BlockSize, 2 * sb.BlocksPerGroup * sb.BlockSize},
            {sb.BlockSize, 3 * sb.BlocksPerGroup * sb.BlockSize},

            {100 * sb.BlockSize, sb.BlockSize * freeSpace[0].Offset},
            {200 * sb.BlockSize,
             sb.BlockSize * (sb.BlocksPerGroup + freeSpace[1].Offset)},
            {300 * sb.BlockSize,
             sb.BlockSize * (2 * sb.BlocksPerGroup + freeSpace[2].Offset)},
            {400 * sb.BlockSize,
             sb.BlockSize * (3 * sb.BlocksPerGroup + freeSpace[3].Offset)}};

        UNIT_ASSERT_VALUES_EQUAL(sb.GroupCount - 1, dev.R.size());
        UNIT_ASSERT_VALUES_EQUAL(expectedW.size(), dev.W.size());

        SortBy(expectedW, [](auto& p) { return p.second; });
        SortBy(dev.W, [](auto& p) { return p.second; });

        for (ui64 i = 0; i != dev.W.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(expectedW[i].first, dev.W[i].first);
            UNIT_ASSERT_VALUES_EQUAL(expectedW[i].second, dev.W[i].second);
        }
    }
}
