#pragma once

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename F>
struct TPipeableProperty
{
    F Set;
};

template <typename F>
TPipeableProperty(F&&) -> TPipeableProperty<F>;

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename F>
decltype(auto) operator|(T&& config, TPipeableProperty<F> p)
{
    p.Set(config);

    return std::forward<T>(config);
}

}   // namespace NCloud::NBlockStore::NStorage
