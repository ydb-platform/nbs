#pragma once

#include <cstddef>

namespace NCloud::NBlockStore::NRdma {

class TPageSize
{
    static size_t Get();

public:
    const static size_t Value;
};

}   // namespace NCloud::NBlockStore::NRdma
