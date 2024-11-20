#pragma once

#include <util/system/types.h>

#include <bitset>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum ECompactionOptions: ui32
{
    ForceFullCompaction,
    ExternalCompaction,
    MaxFieldNumber
};

using TCompactionOptions =
    std::bitset<ECompactionOptions::MaxFieldNumber>;

}   // namespace NCloud::NBlockStore::NStorage
