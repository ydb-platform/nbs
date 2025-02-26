#include "write_back_cache.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteBackCache(const TString& filePath, ui32 size)
    : RequestsToProcess(filePath, size)
{}

////////////////////////////////////////////////////////////////////////////////

TWriteBackCachePtr CreateWriteBackCache(const TString& filePath, ui32 size)
{
    return std::make_unique<TWriteBackCache>(filePath, size);
}

}   // namespace NCloud::NFileStore::NFuse
