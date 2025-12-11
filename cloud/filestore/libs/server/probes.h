#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SERVER_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(                                                            \
        ExecuteRequest,                                               \
        GROUPS("NFSRequest"),                                         \
        TYPES(TString, ui64, TString, ui32, ui64),                    \
        NAMES(                                                        \
            NCloud::NProbeParam::RequestType,                         \
            NCloud::NProbeParam::RequestId,                           \
            NCloud::NProbeParam::FsId,                                \
            NCloud::NProbeParam::MediaKind,                           \
            NCloud::NProbeParam::RequestSize))                        \
    PROBE(                                                            \
        SendResponse,                                                 \
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
    // FILESTORE_SERVER_PROVIDER

LWTRACE_DECLARE_PROVIDER(FILESTORE_SERVER_PROVIDER)
