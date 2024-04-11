#include "read_ahead.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultCache: TReadAheadCache
{
    static constexpr ui32 MaxNodes = 1024;
    static constexpr ui32 MaxResultsPerNode = 32;
    static constexpr ui32 RangeSize = 1_MB;
    static constexpr ui32 MaxGap = 20;

    TDefaultCache()
        : TReadAheadCache(TDefaultAllocator::Instance())
    {
        Reset(MaxNodes, MaxResultsPerNode, RangeSize, MaxGap);
    }
};

////////////////////////////////////////////////////////////////////////////////

TByteRange MakeRange(ui64 offset, ui32 len)
{
    return TByteRange(offset, len, 4_KB);
}

void RegisterResult(TDefaultCache& cache, ui64 nodeId, ui64 offset, ui32 len)
{
    NProtoPrivate::TDescribeDataResponse result;
    auto* f = result.AddFreshDataRanges();
    f->SetContent(Sprintf("n=%lu,o=%lu,l=%u", nodeId, offset, len));

    cache.RegisterResult(nodeId, MakeRange(offset, len), result);
};

TString FillResult(TDefaultCache& cache, ui64 nodeId, ui64 offset, ui32 len)
{
    NProtoPrivate::TDescribeDataResponse result;
    if (cache.TryFillResult(nodeId, MakeRange(offset, len), &result)) {
        const auto& fdr = result.GetFreshDataRanges();
        UNIT_ASSERT_VALUES_EQUAL(1, fdr.size());
        return fdr[0].GetContent();
    }
    return {};
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadAheadTest)
{
    Y_UNIT_TEST(ShouldDetectPureSequentialRead)
    {
        const ui64 nodeId = 111;
        const ui32 blockSize = 4_KB;
        const ui32 requestSize = 32 * blockSize;

        TDefaultCache cache;

        TMaybe<TByteRange> r;
        ui64 offset = 0;
        while (offset < 1_MB - requestSize) {
            r = cache.RegisterDescribe(
                nodeId,
                TByteRange(offset, requestSize, blockSize));
            UNIT_ASSERT_C(!r, r.GetRef().Describe());
            offset += requestSize;
        }

        while (offset < 10_MB) {
            r = cache.RegisterDescribe(
                nodeId,
                TByteRange(offset, requestSize, blockSize));
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(
                TByteRange(offset, 1_MB, blockSize).Describe(),
                r->Describe());
            offset += requestSize;
        }

        r = cache.RegisterDescribe(
            nodeId,
            TByteRange(100_MB, requestSize, blockSize));
        UNIT_ASSERT_C(!r, r.GetRef().Describe());
    }

    Y_UNIT_TEST(ShouldDetectAlmostSequentialRead)
    {
        const ui64 nodeId = 111;
        const ui32 blockSize = 4_KB;

        TDefaultCache cache;
        TMaybe<TByteRange> r;
        r = cache.RegisterDescribe(nodeId, TByteRange(0, 128_KB, blockSize));
        UNIT_ASSERT_C(!r, r.GetRef().Describe());
        r = cache.RegisterDescribe(nodeId, TByteRange(128_KB, 128_KB, blockSize));
        UNIT_ASSERT_C(!r, r.GetRef().Describe());
        r = cache.RegisterDescribe(nodeId, TByteRange(512_KB, 256_KB, blockSize));
        UNIT_ASSERT_C(!r, r.GetRef().Describe());
        r = cache.RegisterDescribe(nodeId, TByteRange(384_KB, 128_KB, blockSize));
        UNIT_ASSERT_C(!r, r.GetRef().Describe());
        r = cache.RegisterDescribe(nodeId, TByteRange(768_KB, 256_KB, blockSize));
        UNIT_ASSERT(r);
        UNIT_ASSERT_VALUES_EQUAL(
            TByteRange(768_KB, 1_MB, blockSize).Describe(),
            r->Describe());
        r = cache.RegisterDescribe(
            nodeId,
            TByteRange(1_MB + 256_KB, 256_KB, blockSize));
        UNIT_ASSERT_C(!r, r.GetRef().Describe());
        r = cache.RegisterDescribe(
            nodeId,
            TByteRange(1_MB + 512_KB, 384_KB, blockSize));
        UNIT_ASSERT(r);
        UNIT_ASSERT_VALUES_EQUAL(
            TByteRange(1_MB + 512_KB, 1_MB, blockSize).Describe(),
            r->Describe());
    }

    Y_UNIT_TEST(ShouldCacheResults)
    {
        TDefaultCache cache;

        RegisterResult(cache, 111, 0, 1_MB);
        RegisterResult(cache, 111, 1_MB, 1_MB);
        RegisterResult(cache, 111, 2_MB, 1_MB);
        RegisterResult(cache, 222, 100_MB, 1_MB);
        RegisterResult(cache, 222, 105_MB, 1_MB);

        UNIT_ASSERT_VALUES_EQUAL("", FillResult(cache, 333, 0, 1_MB));

        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=0,l=1048576",
            FillResult(cache, 111, 0, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=0,l=1048576",
            FillResult(cache, 111, 1_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=1048576,l=1048576",
            FillResult(cache, 111, 1_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=1048576,l=1048576",
            FillResult(cache, 111, 2_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=2097152,l=1048576",
            FillResult(cache, 111, 2_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=2097152,l=1048576",
            FillResult(cache, 111, 3_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL("", FillResult(cache, 111, 3_MB, 128_KB));

        UNIT_ASSERT_VALUES_EQUAL(
            "n=222,o=104857600,l=1048576",
            FillResult(cache, 222, 100_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=222,o=104857600,l=1048576",
            FillResult(cache, 222, 101_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL("", FillResult(cache, 222, 101_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            FillResult(cache, 222, 105_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=222,o=110100480,l=1048576",
            FillResult(cache, 222, 105_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=222,o=110100480,l=1048576",
            FillResult(cache, 222, 106_MB - 128_KB, 128_KB));
    }

    Y_UNIT_TEST(ShouldEvictNodesAndResults)
    {
        TDefaultCache cache;

        ui64 nodeId = 1;
        while (nodeId < TDefaultCache::MaxNodes + 1) {
            for (ui32 rangeId = 0;
                    rangeId < 2 * TDefaultCache::MaxResultsPerNode; ++rangeId) {
                RegisterResult(cache, nodeId, rangeId * 1_MB, 1_MB);
            }

            ++nodeId;
        }

        UNIT_ASSERT_VALUES_EQUAL(TDefaultCache::MaxNodes, cache.CacheSize());

        while (nodeId < 2 * TDefaultCache::MaxNodes + 1) {
            for (ui32 rangeId = 0;
                    rangeId < 2 * TDefaultCache::MaxResultsPerNode; ++rangeId) {
                RegisterResult(cache, nodeId, rangeId * 1_MB, 1_MB);
            }

            ++nodeId;
        }

        const ui64 firstNodeId = nodeId - TDefaultCache::MaxNodes;
        const ui64 lastNodeId = nodeId - 1;
        const ui64 firstOffset = TDefaultCache::MaxResultsPerNode * 1_MB;
        const ui64 lastOffset =
            (2 * TDefaultCache::MaxResultsPerNode - 1) * 1_MB;

        // nothing should be cached for the nodes with id < firstNodeId
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            FillResult(cache, 1, lastOffset, 1_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            FillResult(cache, firstNodeId - 1, lastOffset, 1_MB));

        // nothing should be cached for the ranges with offsets < firstOffset
        UNIT_ASSERT_VALUES_EQUAL("", FillResult(cache, firstNodeId, 0, 1_MB));
        UNIT_ASSERT_VALUES_EQUAL("", FillResult(
            cache,
            firstNodeId,
            (firstOffset - 1_MB),
            1_MB));
        UNIT_ASSERT_VALUES_EQUAL("", FillResult(cache, lastNodeId, 0, 1_MB));
        UNIT_ASSERT_VALUES_EQUAL("", FillResult(
            cache,
            lastNodeId,
            (firstOffset - 1_MB),
            1_MB));

        // ranges with offsets >= firstOffsets for the nodes with
        // id >= firstNodeId should be cached
        const auto expected = [] (const ui64 nodeId, const ui64 offset) {
            return Sprintf("n=%lu,o=%lu,l=1048576", nodeId, offset);
        };

        UNIT_ASSERT_VALUES_EQUAL(
            expected(firstNodeId, firstOffset),
            FillResult(cache, firstNodeId, firstOffset, 1_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            expected(firstNodeId, lastOffset),
            FillResult(cache, firstNodeId, lastOffset, 1_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            expected(lastNodeId, firstOffset),
            FillResult(cache, lastNodeId, firstOffset, 1_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            expected(lastNodeId, lastOffset),
            FillResult(cache, lastNodeId, lastOffset, 1_MB));
    }
}

}   // namespace NCloud::NFileStore::NStorage
