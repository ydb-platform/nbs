#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/verify.h>

#define SERVICE_VERIFY_C(expr, message)                                        \
    STORAGE_VERIFY_C(expr, TWellKnownEntityTypes::FILESYSTEM, LogTag, message) \
    // SERVICE_VERIFY_C

#define SERVICE_VERIFY(expr)   \
    SERVICE_VERIFY_C(expr, "") \
    // SERVICE_VERIFY

#define SERVICE_VERIFY_DEBUG_C(expr, message) \
    STORAGE_VERIFY_DEBUG_C(                   \
        expr,                                 \
        TWellKnownEntityTypes::FILESYSTEM,    \
        LogTag,                               \
        message)                              \
    // SERVICE_VERIFY_DEBUG_C

#define SERVICE_VERIFY_DEBUG(expr)   \
    SERVICE_VERIFY_DEBUG_C(expr, "") \
// SERVICE_VERIFY_DEBUG
