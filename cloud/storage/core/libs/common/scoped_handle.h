#pragma once

#include <util/generic/fwd.h>

#include <concepts>
#include <functional>
#include <utility>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <
    std::integral T,
    T InvalidValue,
    typename UniqueTag
>
class TScopedHandle
{
private:
    T Value = InvalidValue;

public:
    using TUnderlyingType = T;

    constexpr TScopedHandle() = default;

    constexpr explicit TScopedHandle(T value)
        : Value {value}
    {}

    constexpr explicit operator T () const noexcept
    {
        return Value;
    }

    constexpr explicit operator bool () const noexcept
    {
        return Value != InvalidValue;
    }

    // TODO: std::to_underlying (C++23)
    friend constexpr T ToUnderlying(TScopedHandle handle) noexcept
    {
        return static_cast<T>(handle);
    }

    friend constexpr auto operator <=> (TScopedHandle, TScopedHandle) = default;
};

}   // namespace NCloud

////////////////////////////////////////////////////////////////////////////////

template <typename T, T V, typename Tag>
struct std::hash<NCloud::TScopedHandle<T, V, Tag>>
{
    std::size_t operator () (NCloud::TScopedHandle<T, V, Tag> handle) const
    {
        return std::hash<T>{}(ToUnderlying(handle));
    }
};

template <typename T, T V, typename Tag>
struct THash<NCloud::TScopedHandle<T, V, Tag>>
    : public std::hash<NCloud::TScopedHandle<T, V, Tag>>
{};

////////////////////////////////////////////////////////////////////////////////

template <typename T, T V, typename Tag>
struct std::equal_to<NCloud::TScopedHandle<T, V, Tag>>
{
    bool operator () (
        NCloud::TScopedHandle<T, V, Tag> lhs,
        NCloud::TScopedHandle<T, V, Tag> rhs) const
    {
        return lhs == rhs;
    }
};

template <typename T, T V, typename Tag>
struct TEqualTo<NCloud::TScopedHandle<T, V, Tag>>
    : public std::equal_to<NCloud::TScopedHandle<T, V, Tag>>
{};

////////////////////////////////////////////////////////////////////////////////
