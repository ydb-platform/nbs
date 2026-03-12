#pragma once

#include <util/system/types.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct ISequenceIdGenerator
{
    virtual ~ISequenceIdGenerator() = default;

    virtual ui64 GenerateId() = 0;
};

using ISequenceIdGeneratorPtr = std::shared_ptr<ISequenceIdGenerator>;

////////////////////////////////////////////////////////////////////////////////

class TSequenceIdGenerator: public ISequenceIdGenerator
{
private:
    std::atomic<ui64> CurrentId = 1;

public:
    ui64 GenerateId() override;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
