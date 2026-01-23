#pragma once

#include <util/system/types.h>

#include <atomic>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct ISequenceIdGenerator
{
    virtual ~ISequenceIdGenerator() = default;

    virtual ui64 Generate() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSequenceIdGenerator: public ISequenceIdGenerator
{
private:
    std::atomic<ui64> CurrentId;

public:
    explicit TSequenceIdGenerator(ui64 initial = 1);
    ui64 Generate() override;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
