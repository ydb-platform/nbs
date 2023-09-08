#pragma once

#if defined(__ANDROID__)
#   include "config-android.h"
#elif defined(__IOS__)
#   include "config-ios.h"
#elif defined(__APPLE__)
#   include "config-osx.h"
#else
#   include "config-linux.h"
#endif
