#include "xattr_cache.h"

#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 NodeId = 10;
constexpr ui64 SourceId = 100;
constexpr ui32 ShardCount = 4;
const TDuration Timeout = TDuration::Seconds(15);
const TDuration CacheTTL = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    TXAttrCache Cache;

    explicit TEnv(ui32 maxEntryCount)
        : Cache(Timer, maxEntryCount, ShardCount, Timeout)
    {}

    void Add(const TString& name, const TString& value, ui64 version)
    {
        Cache.Add(
            NodeId,
            name,
            value,
            version,
            1 /* cacheVersion */,
            SourceId,
            CacheTTL);
    }

    TMaybe<TString> GetValue(const TString& name)
    {
        const auto xattr = Cache.Get(NodeId, name);
        UNIT_ASSERT(xattr);
        return xattr->Value;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TXAttrCacheTest)
{
    Y_UNIT_TEST(ShouldCacheValuesAndAbsences)
    {
        TEnv env(100 /* maxEntryCount */);

        env.Add("a", "value_a", 1 /* version */);
        env.Cache.AddAbsent(
            NodeId,
            "b",
            1 /* cacheVersion */,
            SourceId,
            CacheTTL);

        UNIT_ASSERT_VALUES_EQUAL("value_a", *env.GetValue("a"));
        UNIT_ASSERT(!env.GetValue("b"));
        UNIT_ASSERT(!env.Cache.Get(NodeId, "c"));
        UNIT_ASSERT(!env.Cache.Get(NodeId + 1, "a"));
        UNIT_ASSERT_VALUES_EQUAL(2, env.Cache.GetEntryCount());
    }

    Y_UNIT_TEST(ShouldNotReplaceNewerValues)
    {
        TEnv env(100 /* maxEntryCount */);

        env.Add("a", "value_2", 2 /* version */);
        env.Add("a", "value_1", 1 /* version */);
        UNIT_ASSERT_VALUES_EQUAL("value_2", *env.GetValue("a"));

        env.Add("a", "value_3", 3 /* version */);
        UNIT_ASSERT_VALUES_EQUAL("value_3", *env.GetValue("a"));
    }

    Y_UNIT_TEST(ShouldEvictLeastRecentlyUsedEntries)
    {
        //
        // All the xattrs of a node are in the same shard - 1 entry per shard.
        //

        TEnv env(ShardCount /* maxEntryCount */);

        env.Add("a", "value_a", 1 /* version */);
        env.Add("b", "value_b", 1 /* version */);
        UNIT_ASSERT(!env.Cache.Get(NodeId, "a"));
        UNIT_ASSERT_VALUES_EQUAL("value_b", *env.GetValue("b"));
        UNIT_ASSERT_VALUES_EQUAL(1, env.Cache.GetEntryCount());
    }

    Y_UNIT_TEST(ShouldReplaceStaleValueOfReusedNode)
    {
        TEnv env(100 /* maxEntryCount */);

        env.Add("a", "old", 5 /* version */);
        env.Cache.Invalidate(NodeId, 2 /* version */);

        //
        // The node id is reused - the xattr versions of the new node are not
        // comparable to the versions of the old one.
        //

        env.Cache.Add(
            NodeId,
            "a",
            "new",
            1 /* version */,
            2 /* cacheVersion */,
            SourceId,
            CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL("new", *env.GetValue("a"));
    }
}

}   // namespace NCloud::NFileStore::NFuse
