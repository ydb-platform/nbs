#pragma once

#include <sched.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

// Starts the silk runtime with one scheduler thread (and one worker
// thread) per CPU of the process affinity mask.
void Init();
// Same, but only on the CPUs set in cpuMask, which must intersect the
// affinity mask.
void Init(const cpu_set_t& cpuMask);

void Destroy();
void EnableDebugLogging();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
