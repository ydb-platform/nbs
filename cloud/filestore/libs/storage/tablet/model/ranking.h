#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <
    typename TKey,
    typename TValue,
    typename TComparator,
    typename TKeyExtractor,
    typename TKeyHash = THash<TKey>>
class TBoundedRanking
{
private:
    using TEntries = TSet<TValue, TComparator>;
    using TIterator = typename TEntries::iterator;

    size_t MaxEntries;
    TKeyExtractor ExtractKey;
    TEntries Entries;
    THashMap<TKey, TIterator, TKeyHash> KeyToRanking;

private:
    void EvictFirst()
    {
        while (Entries.size() > MaxEntries) {
            auto first = Entries.begin();
            const TKey key = ExtractKey(*first);

            KeyToRanking.erase(key);
            Entries.erase(first);
        }
    }

public:
    TBoundedRanking(
        size_t maxEntries,
        TComparator comparator,
        TKeyExtractor keyExtractor)
        : MaxEntries(maxEntries)
        , ExtractKey(std::move(keyExtractor))
        , Entries(std::move(comparator))
    {}

    const TValue* Find(const TKey& key) const
    {
        auto it = KeyToRanking.find(key);
        if (it == KeyToRanking.end()) {
            return nullptr;
        }

        return &*it->second;
    }

    bool InsertOrUpdate(TValue value)
    {
        const auto key = ExtractKey(value);
        auto it = KeyToRanking.find(key);

        if (it != KeyToRanking.end()) {
            Entries.erase(it->second);
            KeyToRanking.erase(it);
        }

        auto [newIt, inserted] = Entries.insert(std::move(value));
        if (!inserted) {
            return false;
        }

        KeyToRanking.emplace(key, newIt);

        EvictFirst();

        return true;
    }

    TVector<TValue> GetLastN(ui32 n) const
    {
        TVector<TValue> result;
        result.reserve(Min<size_t>(n, Entries.size()));

        for (auto it = Entries.rbegin();
             it != Entries.rend() && result.size() < n;
             ++it)
        {
            result.push_back(*it);
        }

        return result;
    }
};

}   // namespace NCloud::NFileStore::NStorage
