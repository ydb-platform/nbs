#pragma once

#include <util/digest/multi.h>
#include <util/generic/hash.h>

#include <cstdint>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires requires(T v) {
        {
            v++
        } -> std::same_as<T>;
    }
class TFreeKey
{
private:
    T NextFreeKey;

public:
    explicit TFreeKey(T base = {})
        : NextFreeKey(std::move(base))
    {}

    T GenerateNextFreeKey()
    {
        return NextFreeKey++;
    }
};

using TMetricNextFreeKey = TFreeKey<ui64>;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires requires(T v) {
        {
            v == v
        } -> std::same_as<bool>;
        {
            THash<T>()(v)
        } -> std::same_as<size_t>;
    }
class TKey
{
    friend struct THash<TKey>;

private:
    std::uintptr_t UniqueId;
    T Key;

public:
    TKey(std::uintptr_t uniqueId, T key)
        : UniqueId(uniqueId)
        , Key(std::move(key))
    {}

    explicit TKey(T key = {})
        : TKey(static_cast<std::uintptr_t>(0), std::move(key))
    {}

    TKey(const void* const uniquePtr, T key)
        : TKey(reinterpret_cast<std::uintptr_t>(uniquePtr), std::move(key))
    {}

    T& operator*()
    {
        return Key;
    }

    const T& operator*() const
    {
        return Key;
    }

    T* operator->()
    {
        return &Key;
    }

    const T* const operator->() const
    {
        return &Key;
    }

    friend bool operator==(const TKey& lhv, const TKey& rhv) = default;
};

using TMetricKey = TKey<ui64>;

}   // namespace NCloud::NFileStore::NMetrics

template <typename T>
struct THash<NCloud::NFileStore::NMetrics::TKey<T>>
{
    size_t operator()(
        const NCloud::NFileStore::NMetrics::TKey<T>& key) const noexcept
    {
        return MultiHash(key.UniqueId, key.Key);
    }
};
