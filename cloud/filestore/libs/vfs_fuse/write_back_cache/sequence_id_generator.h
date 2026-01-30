#pragma once

#include <util/system/types.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct ISequenceIdGenerator
{
    virtual ~ISequenceIdGenerator() = default;

    virtual ui64 Generate() = 0;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
