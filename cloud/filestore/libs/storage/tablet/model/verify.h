#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/verify.h>

#define TABLET_VERIFY_C(expr, message)                                          \
    STORAGE_VERIFY_C(                                                           \
        expr,                                                                   \
        TWellKnownEntityTypes::FILESYSTEM,                                      \
        LogTag,                                                                 \
        message)                                                                \
// TABLET_VERIFY_C

#define TABLET_VERIFY(expr)                                                     \
    TABLET_VERIFY_C(expr, "")                                                   \
// TABLET_VERIFY

#define TABLET_VERIFY_DEBUG_C(expr, message)                                    \
    STORAGE_VERIFY_DEBUG_C(                                                     \
        expr,                                                                   \
        TWellKnownEntityTypes::FILESYSTEM,                                      \
        LogTag,                                                                 \
        message)                                                                \
// TABLET_VERIFY_DEBUG_C

#define TABLET_VERIFY_DEBUG(expr)                                               \
    TABLET_VERIFY_DEBUG_C(expr, "")                                             \
// TABLET_VERIFY_DEBUG
