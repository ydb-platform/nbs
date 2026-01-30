#include "sequence_id_generator_impl.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TSequenceIdGenerator::TSequenceIdGenerator(ui64 initial)
    : CurrentId(initial)
{}

ui64 TSequenceIdGenerator::Generate()
{
    return CurrentId++;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
