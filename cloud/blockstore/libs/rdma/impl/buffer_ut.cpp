#include "buffer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(
    TBufferPoolTest){Y_UNIT_TEST(ShouldAllocateAlignedBuffers){TBufferPool pool;
pool.Init(nullptr, nullptr, 0);

auto buffer = pool.AcquireBuffer(1234);
UNIT_ASSERT(buffer.Length == 4_KB);
}   // namespace NCloud::NBlockStore::NRdma

Y_UNIT_TEST(ShouldAllocateSmallBuffersInOneChunk)
{
    TBufferPool pool;
    pool.Init(nullptr, nullptr, 0);

    const auto& stats = pool.GetStats();

    auto buffer = pool.AcquireBuffer(4_KB);
    UNIT_ASSERT(buffer.Length == 4_KB);

    UNIT_ASSERT_EQUAL(stats.ActiveChunksCount, 1);
    UNIT_ASSERT_EQUAL(stats.CustomChunksCount, 0);
    UNIT_ASSERT_EQUAL(stats.FreeChunksCount, 0);

    pool.ReleaseBuffer(buffer);

    UNIT_ASSERT_EQUAL(stats.ActiveChunksCount, 1);
    UNIT_ASSERT_EQUAL(stats.CustomChunksCount, 0);
    UNIT_ASSERT_EQUAL(stats.FreeChunksCount, 0);
}

Y_UNIT_TEST(ShouldAllocateCustomChunkForLargeBuffer)
{
    TBufferPool pool;
    pool.Init(nullptr, nullptr, 0);

    const auto& stats = pool.GetStats();

    auto buffer = pool.AcquireBuffer(4_MB);
    UNIT_ASSERT(buffer.Length == 4_MB);

    UNIT_ASSERT_EQUAL(stats.ActiveChunksCount, 0);
    UNIT_ASSERT_EQUAL(stats.CustomChunksCount, 1);
    UNIT_ASSERT_EQUAL(stats.FreeChunksCount, 0);

    pool.ReleaseBuffer(buffer);

    UNIT_ASSERT_EQUAL(stats.ActiveChunksCount, 0);
    UNIT_ASSERT_EQUAL(stats.CustomChunksCount, 0);
    UNIT_ASSERT_EQUAL(stats.FreeChunksCount, 0);
}

Y_UNIT_TEST(ShouldCacheFreeChunks)
{
    TBufferPool pool;
    pool.Init(nullptr, nullptr, 0);

    const auto& stats = pool.GetStats();

    TVector<TPooledBuffer> buffers = {
        pool.AcquireBuffer(1_MB),
        pool.AcquireBuffer(1_MB),
        pool.AcquireBuffer(1_MB),
        pool.AcquireBuffer(1_MB),
        // trigger chunk switching
        pool.AcquireBuffer(1_MB),
    };

    UNIT_ASSERT_EQUAL(stats.ActiveChunksCount, 2);
    UNIT_ASSERT_EQUAL(stats.CustomChunksCount, 0);
    UNIT_ASSERT_EQUAL(stats.FreeChunksCount, 0);

    for (auto& buffer: buffers) {
        pool.ReleaseBuffer(buffer);
    }

    UNIT_ASSERT_EQUAL(stats.ActiveChunksCount, 1);
    UNIT_ASSERT_EQUAL(stats.CustomChunksCount, 0);
    UNIT_ASSERT_EQUAL(stats.FreeChunksCount, 1);
}
}
;

}   // namespace NCloud::NBlockStore::NRdma
