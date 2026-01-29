#pragma once

#include "sequence_id_generator.h"

#include <atomic>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

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
