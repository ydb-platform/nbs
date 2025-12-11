#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CLIENT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(                                                            \
        SendRequest,                                                  \
        GROUPS("NFSRequest"),                                         \
        TYPES(TString, ui64),                                         \
        NAMES(                                                        \
            NCloud::NProbeParam::RequestType,                         \
            NCloud::NProbeParam::RequestId))                          \
    PROBE(                                                            \
        ResponseReceived,                                             \
        GROUPS("NFSRequest"),                                         \
        TYPES(TString, ui64),                                         \
        NAMES(                                                        \
            NCloud::NProbeParam::RequestType,                         \
            NCloud::NProbeParam::RequestId))                          \
    PROBE(                                                            \
        RequestCompleted,                                             \
        GROUPS("NFSRequest"),                                         \
        TYPES(TString, ui64, ui64, ui64),                             \
        NAMES(                                                        \
            NCloud::NProbeParam::RequestType,                         \
            NCloud::NProbeParam::RequestId,                           \
            NCloud::NProbeParam::RequestTime,                         \
            NCloud::NProbeParam::RequestExecutionTime))               \
    // FILESTORE_CLIENT_PROVIDER

LWTRACE_DECLARE_PROVIDER(FILESTORE_CLIENT_PROVIDER)
