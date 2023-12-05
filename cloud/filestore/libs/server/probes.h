#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SERVER_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)          \
    PROBE(ExecuteRequest,                                                      \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64, ui32, TString, ui64),                             \
        NAMES(                                                                 \
            NCloud::NProbeParam::RequestType,                                  \
            "requestId",                                                       \
            NCloud::NProbeParam::MediaKind,                                    \
            "fsId",                                                            \
            NCloud::NProbeParam::RequestSize)                                  \
    )                                                                          \
    PROBE(SendResponse,                                                        \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId")                   \
    )                                                                          \
    PROBE(RequestCompleted,                                                    \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId")                   \
    )                                                                          \
// FILESTORE_SERVER_PROVIDER

LWTRACE_DECLARE_PROVIDER(FILESTORE_SERVER_PROVIDER)
