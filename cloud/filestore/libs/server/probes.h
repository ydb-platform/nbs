#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SERVER_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)          \
    PROBE(ExecuteRequest,                                                      \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64, ui32, TString),                                   \
        NAMES(                                                                 \
            "requestType",                                                     \
            "requestId",                                                       \
            NCloud::NProbeParam::MediaKind,                                    \
            "fsId"))                                                           \
    PROBE(SendResponse,                                                        \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("requestType", "requestId")                                      \
    )                                                                          \
    PROBE(RequestCompleted,                                                    \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("requestType", "requestId")                                      \
    )                                                                          \
// FILESTORE_SERVER_PROVIDER

LWTRACE_DECLARE_PROVIDER(FILESTORE_SERVER_PROVIDER)
