#pragma once
#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud {

NActors::IActor* CreateMockAccessServiceWithCache(bool enableV2Interface); // for compatibility with older code

}
