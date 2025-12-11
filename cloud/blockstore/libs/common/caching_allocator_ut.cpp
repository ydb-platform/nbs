#include "caching_allocator.h"

#include <cloud/storage/core/libs/common/alloc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultPageSize = 4 * 1024 * 1024;
constexpr ui32 DefaultPageCount = 256;
constexpr ui32 DefaultPageDropSize = 512 * 1024;

////////////////////////////////////////////////////////////////////////////////

auto CreateTestAllocator(IAllocator* upstream)
{
    return CreateCachingAllocator(
        upstream,
        DefaultPageSize,
        DefaultPageCount,
        DefaultPageDropSize);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCachingAllocatorTest)
{
    Y_UNIT_TEST(ShouldAllocate)
    {
        TProfilingAllocator upstream;

        auto allocator = CreateTestAllocator(&upstream);

        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), 0);

        auto b = allocator->Allocate(4096);
        UNIT_ASSERT(b.Data);
        UNIT_ASSERT(b.Node);
        UNIT_ASSERT_VALUES_EQUAL(b.Len, 4096);
        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), DefaultPageSize);

        allocator->Release(b);
        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), DefaultPageSize);
        allocator = nullptr;

        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), 0);
    }

    Y_UNIT_TEST(ShouldFallback)
    {
        TProfilingAllocator upstream;

        auto allocator = CreateTestAllocator(&upstream);

        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), 0);

        auto b = allocator->Allocate(DefaultPageSize + 1);
        UNIT_ASSERT(b.Data);
        UNIT_ASSERT(!b.Node);
        UNIT_ASSERT_VALUES_EQUAL(b.Len, DefaultPageSize + 1);
        UNIT_ASSERT_VALUES_EQUAL(
            upstream.GetBytesAllocated(),
            DefaultPageSize + 1);

        allocator->Release(b);
        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), 0);
    }

    Y_UNIT_TEST(ShouldReusePage)
    {
        TProfilingAllocator upstream;

        auto allocator = CreateTestAllocator(&upstream);

        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), 0);

        auto b = allocator->Allocate(DefaultPageSize - 1);
        UNIT_ASSERT(b.Data);
        UNIT_ASSERT(b.Node);
        UNIT_ASSERT_VALUES_EQUAL(b.Len, DefaultPageSize - 1);
        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), DefaultPageSize);

        allocator->Release(b);
        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), DefaultPageSize);

        b = allocator->Allocate(DefaultPageSize - 1);
        UNIT_ASSERT(b.Data);
        UNIT_ASSERT(b.Node);
        UNIT_ASSERT_VALUES_EQUAL(b.Len, DefaultPageSize - 1);
        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), DefaultPageSize);

        allocator->Release(b);
        allocator = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(upstream.GetBytesAllocated(), 0);
    }
}

}   // namespace NCloud::NBlockStore
