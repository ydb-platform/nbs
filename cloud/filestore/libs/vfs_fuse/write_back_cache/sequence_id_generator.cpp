#include "sequence_id_generator.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

ui64 TSequenceIdGenerator::GenerateId()
{
    return CurrentId++;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
