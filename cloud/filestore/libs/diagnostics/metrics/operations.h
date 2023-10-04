#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/types.h>

#include <atomic>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
i64 Load(const T& source);

template <>
inline i64 Load(const std::atomic<i64>& source)
{
    return source.load(std::memory_order_relaxed);
}

template <>
inline i64 Load(const TAtomic& source)
{
    return AtomicGet(source);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void Store(T& source, i64 value);

template <>
inline void Store(std::atomic<i64>& source, i64 value)
{
    return source.store(value, std::memory_order_relaxed);
}

template <>
inline void Store(TAtomic& source, i64 value)
{
    AtomicSet(source, value);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
i64 Inc(T& source);

template <>
inline i64 Inc(std::atomic<i64>& source)
{
    return source.fetch_add(1, std::memory_order_relaxed);
}

template <>
inline i64 Inc(TAtomic& source)
{
    return AtomicGetAndIncrement(source);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
i64 Dec(T& source);

template <>
inline i64 Dec(std::atomic<i64>& source)
{
    return source.fetch_sub(1, std::memory_order_relaxed);
}

template <>
inline i64 Dec(TAtomic& source)
{
    return AtomicGetAndDecrement(source);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
i64 Add(T& source, i64 value);

template <>
inline i64 Add(std::atomic<i64>& source, i64 value)
{
    return source.fetch_add(value, std::memory_order_relaxed);
}

template <>
inline i64 Add(TAtomic& source, i64 value)
{
    return AtomicGetAndAdd(source, value);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
i64 Sub(T& source, i64 value);

template <>
inline i64 Sub(std::atomic<i64>& source, i64 value)
{
    return source.fetch_sub(value, std::memory_order_relaxed);
}

template <>
inline i64 Sub(TAtomic& source, i64 value)
{
    return AtomicGetAndSub(source, value);
}

}   // namespace NCloud::NFileStore::NMetrics
