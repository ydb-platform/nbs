#pragma once

#ifdef FASTSHARD_IPC_STUB

namespace NCloud::NFastShard {

struct TFastShardContext
{};

}   // namespace NCloud::NFastShard

#else

#define FASTSHARD_INCLUDE_SILK_CONTEXT
#include "context_i.h"
#undef FASTSHARD_INCLUDE_SILK_CONTEXT

#endif
