#include "zero_block_stats.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/random/fast.h>
#include <util/string/builder.h>
#include <util/system/align.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4_KB;

TZeroBlockStats Calculate(ui64 offset, const TString& data)
{
    NProto::TWriteDataRequest request;
    request.SetOffset(offset);
    request.SetBuffer(data);

    const auto stats = CalculateZeroBlockStats(request, BlockSize);
    UNIT_ASSERT_VALUES_EQUAL(data.size(), stats.Bytes);
    return stats;
}

TZeroBlockStats CalculateWithIovecs(
    ui64 offset,
    const TString& data,
    const TVector<ui64>& iovecSizes)
{
    NProto::TWriteDataRequest request;
    request.SetOffset(offset);

    ui64 dataOffset = 0;
    for (const ui64 size: iovecSizes) {
        auto* iovec = request.AddIovecs();
        if (size) {
            iovec->SetBase(reinterpret_cast<ui64>(data.data() + dataOffset));
            iovec->SetLength(size);
        }
        dataOffset += size;
    }
    UNIT_ASSERT_VALUES_EQUAL(data.size(), dataOffset);

    const auto stats = CalculateZeroBlockStats(request, BlockSize);
    UNIT_ASSERT_VALUES_EQUAL(data.size(), stats.Bytes);
    return stats;
}

TZeroBlockStats CalculateNaive(ui64 offset, const TString& data)
{
    TZeroBlockStats stats;
    stats.AllZeroes = data.find_first_not_of('\0') == TString::npos;

    const ui64 end = offset + data.size();
    for (ui64 block = AlignUp<ui64>(offset, BlockSize);
         block + BlockSize <= end;
         block += BlockSize)
    {
        const auto content = TStringBuf(data).SubStr(block - offset, BlockSize);
        if (content.find_first_not_of('\0') == TStringBuf::npos) {
            stats.ZeroBlockBytes += BlockSize;
        }
    }

    return stats;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TZeroBlockStatsTest)
{
    Y_UNIT_TEST(ShouldCountZeroBlocks)
    {
        TString data(16_KB, 0);
        auto stats = Calculate(0, data);
        UNIT_ASSERT_VALUES_EQUAL(16_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(stats.AllZeroes);

        data[4_KB] = 'x';
        stats = Calculate(0, data);
        UNIT_ASSERT_VALUES_EQUAL(12_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(!stats.AllZeroes);

        data[4_KB] = 0;
        data[8_KB - 1] = 'x';
        stats = Calculate(8_KB, data);
        UNIT_ASSERT_VALUES_EQUAL(12_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(!stats.AllZeroes);

        stats = Calculate(0, TString(16_KB, 'x'));
        UNIT_ASSERT_VALUES_EQUAL(0, stats.ZeroBlockBytes);
        UNIT_ASSERT(!stats.AllZeroes);

        stats = Calculate(0, TString());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.ZeroBlockBytes);
        UNIT_ASSERT(stats.AllZeroes);
    }

    Y_UNIT_TEST(ShouldCountOnlyFullyCoveredBlocks)
    {
        // [1_KB, 11_KB) fully covers only [4_KB, 8_KB)
        TString data(10_KB, 0);
        auto stats = Calculate(1_KB, data);
        UNIT_ASSERT_VALUES_EQUAL(4_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(stats.AllZeroes);

        data[0] = 'x';
        stats = Calculate(1_KB, data);
        UNIT_ASSERT_VALUES_EQUAL(4_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(!stats.AllZeroes);

        data[0] = 0;
        data[10_KB - 1] = 'x';
        stats = Calculate(1_KB, data);
        UNIT_ASSERT_VALUES_EQUAL(4_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(!stats.AllZeroes);

        data[10_KB - 1] = 0;
        data[3_KB] = 'x';
        stats = Calculate(1_KB, data);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.ZeroBlockBytes);
        UNIT_ASSERT(!stats.AllZeroes);

        stats = Calculate(100, TString(200, 0));
        UNIT_ASSERT_VALUES_EQUAL(0, stats.ZeroBlockBytes);
        UNIT_ASSERT(stats.AllZeroes);

        stats = Calculate(4_KB - 1, TString(4_KB + 1, 0));
        UNIT_ASSERT_VALUES_EQUAL(4_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(stats.AllZeroes);
    }

    Y_UNIT_TEST(ShouldCountZeroBlocksSplitBetweenIovecs)
    {
        TString data(16_KB, 0);
        auto stats = CalculateWithIovecs(0, data, {1, 0, 4_KB, 5000, 7287});
        UNIT_ASSERT_VALUES_EQUAL(16_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(stats.AllZeroes);

        // the last byte of the second block, in the middle of an iovec
        data[8_KB - 1] = 'x';
        stats = CalculateWithIovecs(0, data, {1, 0, 4_KB, 5000, 7287});
        UNIT_ASSERT_VALUES_EQUAL(12_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(!stats.AllZeroes);

        stats = CalculateWithIovecs(1_KB, data, {3_KB, 13_KB});
        UNIT_ASSERT_VALUES_EQUAL(8_KB, stats.ZeroBlockBytes);
        UNIT_ASSERT(!stats.AllZeroes);
    }

    Y_UNIT_TEST(ShouldMatchNaiveImplementation)
    {
        TReallyFastRng32 rng(42);

        for (ui32 iteration = 0; iteration < 10000; ++iteration) {
            const ui64 offset = rng.Uniform(3 * BlockSize);
            TString data(rng.Uniform(10 * BlockSize), 0);

            // mostly zero data with a few non-zero bytes
            const ui32 nonZeroBytes = rng.Uniform(4);
            for (ui32 i = 0; i < nonZeroBytes && !data.empty(); ++i) {
                data[rng.Uniform(data.size())] = 'x';
            }

            TVector<ui64> iovecSizes;
            ui64 remaining = data.size();
            while (remaining) {
                const ui64 size = rng.Uniform(Min<ui64>(remaining, 6_KB) + 1);
                iovecSizes.push_back(size);
                remaining -= size;
            }

            const auto expected = CalculateNaive(offset, data);
            const auto message = TStringBuilder()
                << "iteration " << iteration << " offset " << offset
                << " size " << data.size();

            auto stats = Calculate(offset, data);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expected.ZeroBlockBytes,
                stats.ZeroBlockBytes,
                message);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expected.AllZeroes,
                stats.AllZeroes,
                message);

            stats = CalculateWithIovecs(offset, data, iovecSizes);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expected.ZeroBlockBytes,
                stats.ZeroBlockBytes,
                message);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expected.AllZeroes,
                stats.AllZeroes,
                message);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
