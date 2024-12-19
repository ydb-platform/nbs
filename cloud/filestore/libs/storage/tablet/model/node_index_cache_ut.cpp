#include "node_index_cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TNodeAttr MakeNodeAttr(ui64 id, ui32 mode = 0, ui64 mtime = 0)
{
    NProto::TNodeAttr attr;
    attr.SetId(id);
    attr.SetMode(mode);
    attr.SetMTime(mtime);
    return attr;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeIndexCache)
{
    Y_UNIT_TEST(ShouldCacheNodeAttributes)
    {
        TNodeIndexCache cache(TDefaultAllocator::Instance());
        cache.Reset(10);

        cache.RegisterGetNodeAttrResult(1, "name1", MakeNodeAttr(2, 123));
        cache.RegisterGetNodeAttrResult(1, "name2", MakeNodeAttr(3, 456, 789));

        NProto::TNodeAttr attr;
        UNIT_ASSERT(cache.TryFillGetNodeAttrResult(1, "name1", &attr));
        UNIT_ASSERT_VALUES_EQUAL(attr.GetId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(attr.GetMode(), 123);
        UNIT_ASSERT_VALUES_EQUAL(attr.GetMTime(), 0);

        UNIT_ASSERT(cache.TryFillGetNodeAttrResult(1, "name2", &attr));
        UNIT_ASSERT_VALUES_EQUAL(attr.GetId(), 3);
        UNIT_ASSERT_VALUES_EQUAL(attr.GetMode(), 456);
        UNIT_ASSERT_VALUES_EQUAL(attr.GetMTime(), 789);

        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().NodeCount, 2);
    }

    Y_UNIT_TEST(ShouldInvalidateCache)
    {
        TNodeIndexCache cache(TDefaultAllocator::Instance());
        cache.Reset(10);

        cache.RegisterGetNodeAttrResult(1, "name1", MakeNodeAttr(2));
        cache.RegisterGetNodeAttrResult(1, "name2", MakeNodeAttr(3));

        cache.InvalidateCache(1, "name1");

        NProto::TNodeAttr attr;
        UNIT_ASSERT(!cache.TryFillGetNodeAttrResult(1, "name1", &attr));
        UNIT_ASSERT(cache.TryFillGetNodeAttrResult(1, "name2", &attr));
        UNIT_ASSERT_VALUES_EQUAL(attr.GetId(), 3);

        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().NodeCount, 1);
    }

    Y_UNIT_TEST(ShouldInvalidateCacheByNodeId)
    {
        TNodeIndexCache cache(TDefaultAllocator::Instance());
        cache.Reset(1);

        cache.RegisterGetNodeAttrResult(1, "name1", MakeNodeAttr(2));
        cache.RegisterGetNodeAttrResult(1, "name2", MakeNodeAttr(3));

        cache.InvalidateCache(2);

        NProto::TNodeAttr attr;
        UNIT_ASSERT(!cache.TryFillGetNodeAttrResult(1, "name1", &attr));
        UNIT_ASSERT(cache.TryFillGetNodeAttrResult(1, "name2", &attr));
        UNIT_ASSERT_VALUES_EQUAL(attr.GetId(), 3);

        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().NodeCount, 1);
    }

    Y_UNIT_TEST(ShouldDisplaceCacheUponOverflow)
    {
        TNodeIndexCache cache(TDefaultAllocator::Instance());
        cache.Reset(2);

        cache.RegisterGetNodeAttrResult(1, "name1", MakeNodeAttr(2));
        cache.RegisterGetNodeAttrResult(1, "name2", MakeNodeAttr(3));
        cache.RegisterGetNodeAttrResult(1, "name3", MakeNodeAttr(4));

        NProto::TNodeAttr attr;
        UNIT_ASSERT(!cache.TryFillGetNodeAttrResult(1, "name1", &attr));
        UNIT_ASSERT(!cache.TryFillGetNodeAttrResult(1, "name2", &attr));
        UNIT_ASSERT(cache.TryFillGetNodeAttrResult(1, "name3", &attr));

        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().NodeCount, 1);
    }

    Y_UNIT_TEST(ShouldLockAndUnlockNodes)
    {
        TNodeIndexCache cache(TDefaultAllocator::Instance());
        cache.Reset(10);

        cache.LockNode(2);

        // name1 is locked and should not be cached, while name2 is not locked

        cache.RegisterGetNodeAttrResult(1, "name1", MakeNodeAttr(2));
        cache.RegisterGetNodeAttrResult(1, "name2", MakeNodeAttr(3));

        NProto::TNodeAttr attr;
        UNIT_ASSERT(!cache.TryFillGetNodeAttrResult(1, "name1", &attr));
        UNIT_ASSERT(cache.TryFillGetNodeAttrResult(1, "name2", &attr));

        // there can be multiple locks on the same node

        cache.LockNode(2);
        cache.RegisterGetNodeAttrResult(1, "name1", MakeNodeAttr(2));
        UNIT_ASSERT(!cache.TryFillGetNodeAttrResult(1, "name1", &attr));

        cache.UnlockNode(2);
        cache.UnlockNode(2);

        // now the node is unlocked and should be cached

        cache.RegisterGetNodeAttrResult(1, "name1", MakeNodeAttr(2));
        UNIT_ASSERT(cache.TryFillGetNodeAttrResult(1, "name1", &attr));
    }
}

}   // namespace NCloud::NFileStore::NStorage
