#include "host_write_back_cache.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

THostWriteBackCache::THostWriteBackCache(const TString& filePath, ui32 size)
    : RequestsToProcess(filePath, size)
{}

////////////////////////////////////////////////////////////////////////////////

THostWriteBackCachePtr CreateHostWriteBackCache(
    const TString& filePath,
    ui32 size)
{
    return std::make_unique<THostWriteBackCache>(filePath, size);
}

}   // namespace NCloud::NFileStore::NFuse
