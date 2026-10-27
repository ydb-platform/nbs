#pragma once

#include <sched.h>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

void Init();
void Init(const cpu_set_t& cpuMask);

void Destroy();
void EnableDebugLogging();

}   // namespace NCloud::NFastShard
