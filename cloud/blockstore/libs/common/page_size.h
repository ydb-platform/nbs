#pragma once

#include <cstddef>

namespace NCloud::NBlockStore {

class TPageSize
{
    const static size_t Value;
    static size_t Init();

public:
    static size_t GetPageSize();
};

}   // namespace NCloud::NBlockStore
