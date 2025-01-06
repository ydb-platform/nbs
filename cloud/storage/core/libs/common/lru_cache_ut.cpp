#include "lru_cache.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLRUCache)
{
    Y_UNIT_TEST(ShouldEnforceCapacity)
    {
        TLRUCache<TString, TString> hashMap(TDefaultAllocator::Instance());
        hashMap.SetCapacity(2);

        UNIT_ASSERT_VALUES_EQUAL(0, hashMap.size());
        UNIT_ASSERT_VALUES_EQUAL(2, hashMap.capacity());

        hashMap.emplace("key1", "value1");
        hashMap.emplace("key2", "value2");

        UNIT_ASSERT_VALUES_EQUAL(2, hashMap.size());
        UNIT_ASSERT_VALUES_EQUAL("value1", hashMap.find("key1")->second);
        UNIT_ASSERT_VALUES_EQUAL("value2", hashMap.find("key2")->second);

        hashMap.emplace("key3", "value3");   // Should evict "key1"

        UNIT_ASSERT_VALUES_EQUAL(2, hashMap.size());
        UNIT_ASSERT_EQUAL(hashMap.end(), hashMap.find("key1"));
        UNIT_ASSERT_VALUES_EQUAL("value2", hashMap.find("key2")->second);
        UNIT_ASSERT_VALUES_EQUAL("value3", hashMap.find("key3")->second);
    }

    Y_UNIT_TEST(ShouldHandleAccessOrder)
    {
        TLRUCache<TString, TString> hashMap(TDefaultAllocator::Instance());
        hashMap.SetCapacity(3);

        hashMap.emplace("key1", "value1");
        hashMap.emplace("key2", "value2");
        hashMap.emplace("key3", "value3");

        // Access key2 to make it most recently used
        hashMap.find("key2");

        // Insert a new key, evicting the least recently used (key1)
        hashMap.emplace("key4", "value4");

        UNIT_ASSERT_EQUAL(hashMap.end(), hashMap.find("key1"));
        UNIT_ASSERT_VALUES_EQUAL("value2", hashMap.find("key2")->second);
        UNIT_ASSERT_VALUES_EQUAL("value3", hashMap.find("key3")->second);
        UNIT_ASSERT_VALUES_EQUAL("value4", hashMap.find("key4")->second);
    }

    Y_UNIT_TEST(ShouldHandleErase)
    {
        TLRUCache<TString, TString> hashMap(TDefaultAllocator::Instance());
        hashMap.SetCapacity(3);

        hashMap.emplace("key1", "value1");
        hashMap.emplace("key2", "value2");
        hashMap.emplace("key3", "value3");

        UNIT_ASSERT_VALUES_EQUAL(3, hashMap.size());

        // Erase key2 and ensure order is preserved
        hashMap.erase(hashMap.find("key2"));

        UNIT_ASSERT_VALUES_EQUAL(2, hashMap.size());
        UNIT_ASSERT_EQUAL(hashMap.end(), hashMap.find("key2"));
        UNIT_ASSERT_VALUES_EQUAL("value1", hashMap.find("key1")->second);
        UNIT_ASSERT_VALUES_EQUAL("value3", hashMap.find("key3")->second);

        // Erase remaining keys
        hashMap.erase(hashMap.find("key1"));
        hashMap.erase(hashMap.find("key3"));

        UNIT_ASSERT_VALUES_EQUAL(0, hashMap.size());
        UNIT_ASSERT_EQUAL(hashMap.end(), hashMap.find("key1"));
        UNIT_ASSERT_EQUAL(hashMap.end(), hashMap.find("key3"));
    }

    Y_UNIT_TEST(ShouldThrowOnAtForNonExistentKey)
    {
        TLRUCache<TString, TString> hashMap(TDefaultAllocator::Instance());
        hashMap.SetCapacity(2);

        hashMap.emplace("key1", "value1");

        UNIT_ASSERT_VALUES_EQUAL("value1", hashMap.at("key1"));

        UNIT_ASSERT_EXCEPTION(hashMap.at("key2"), yexception);
    }

    Y_UNIT_TEST(ShouldHandleEdgeCases)
    {
        TLRUCache<TString, TString> hashMap(TDefaultAllocator::Instance());
        hashMap.SetCapacity(0);

        // Test capacity 0
        auto [it, inserted1] = hashMap.emplace("key1", "value1");
        UNIT_ASSERT_VALUES_EQUAL(false, inserted1);
        UNIT_ASSERT_EQUAL(hashMap.end(), it);
        UNIT_ASSERT_VALUES_EQUAL(0, hashMap.size());
        UNIT_ASSERT_EQUAL(hashMap.end(), hashMap.find("key1"));

        hashMap.SetCapacity(2);
        UNIT_ASSERT_VALUES_EQUAL(0, hashMap.size());
        UNIT_ASSERT_VALUES_EQUAL(2, hashMap.capacity());

        // Test inserting duplicate keys - emplace should not overwrite the
        // value
        auto [it1, inserted2] = hashMap.emplace("key1", "value1");
        UNIT_ASSERT_VALUES_EQUAL(true, inserted2);
        auto [it2, inserted3] = hashMap.emplace("key1", "value2");
        UNIT_ASSERT_VALUES_EQUAL(false, inserted3);

        UNIT_ASSERT_VALUES_EQUAL(1, hashMap.size());
        UNIT_ASSERT_VALUES_EQUAL("value1", hashMap.find("key1")->second);
    }
}

}   // namespace NCloud
