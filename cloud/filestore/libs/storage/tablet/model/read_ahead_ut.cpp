#include "read_ahead.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultCache: TReadAheadCache
{
    TDefaultCache()
        : TReadAheadCache(TDefaultAllocator::Instance())
    {
        Reset(1024, 32, 1_MB, 20);
    }
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

        auto makeRange = [] (ui64 offset, ui32 len) {
            return TByteRange(offset, len, 4_KB);
        };
        auto registerResult = [&] (ui64 nodeId, ui64 offset, ui32 len) {
            NProtoPrivate::TDescribeDataResponse result;
            auto* f = result.AddFreshDataRanges();
            f->SetContent(Sprintf("n=%lu,o=%lu,l=%u", nodeId, offset, len));

            cache.RegisterResult(nodeId, makeRange(offset, len), result);
        };
        auto fillResult = [&] (ui64 nodeId, ui64 offset, ui32 len) {
            NProtoPrivate::TDescribeDataResponse result;
            if (cache.TryFillResult(nodeId, makeRange(offset, len), &result)) {
                const auto& fdr = result.GetFreshDataRanges();
                UNIT_ASSERT_VALUES_EQUAL(1, fdr.size());
                return fdr[0].GetContent();
            }
            return TString();
        };
        registerResult(111, 0, 1_MB);
        registerResult(111, 1_MB, 1_MB);
        registerResult(111, 2_MB, 1_MB);
        registerResult(222, 100_MB, 1_MB);
        registerResult(222, 105_MB, 1_MB);

        UNIT_ASSERT_VALUES_EQUAL("", fillResult(333, 0, 1_MB));

        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=0,l=1048576",
            fillResult(111, 0, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=0,l=1048576",
            fillResult(111, 1_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=1048576,l=1048576",
            fillResult(111, 1_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=1048576,l=1048576",
            fillResult(111, 2_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=2097152,l=1048576",
            fillResult(111, 2_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=111,o=2097152,l=1048576",
            fillResult(111, 3_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL("", fillResult(111, 3_MB, 128_KB));

        UNIT_ASSERT_VALUES_EQUAL(
            "n=222,o=104857600,l=1048576",
            fillResult(222, 100_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=222,o=104857600,l=1048576",
            fillResult(222, 101_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL("", fillResult(222, 101_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL("", fillResult(222, 105_MB - 128_KB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=222,o=110100480,l=1048576",
            fillResult(222, 105_MB, 128_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            "n=222,o=110100480,l=1048576",
            fillResult(222, 106_MB - 128_KB, 128_KB));
    }
}

}   // namespace NCloud::NFileStore::NStorage
