#include "core.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

void Init()
{}

void Init(const cpu_set_t& cpuMask)
{
    (void)cpuMask;
}

void Destroy()
{}

void EnableDebugLogging()
{}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
