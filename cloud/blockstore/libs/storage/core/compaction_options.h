#pragma once

#include <util/system/types.h>

#include <bitset>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ECompactionOption: size_t
{
    Forced,
    External,
    MaxFieldNumber
};

constexpr size_t ToBit(ECompactionOption option)
{
    return static_cast<size_t>(option);
}

using TCompactionOptions =
    std::bitset<ToBit(ECompactionOption::MaxFieldNumber)>;

}   // namespace NCloud::NBlockStore::NStorage
