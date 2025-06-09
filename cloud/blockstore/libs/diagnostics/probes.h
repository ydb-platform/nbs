#pragma once

#include <cloud/blockstore/libs/common/public.h>

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SERVER_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)         \
    PROBE(RequestReceived,                                                     \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui32, ui64, TString),                                   \
        NAMES(                                                                 \
            NCloud::NProbeParam::RequestType,                                  \
            NCloud::NProbeParam::MediaKind,                                    \
            "requestId",                                                       \
            "diskId"))                                                         \
    PROBE(RequestStarted,                                                      \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui32, ui64, TString, ui64, ui64),                       \
        NAMES(                                                                 \
            NCloud::NProbeParam::RequestType,                                  \
            NCloud::NProbeParam::MediaKind,                                    \
            NCloud::NProbeParam::RequestId,                                    \
            NCloud::NProbeParam::DiskId,                                       \
            NCloud::NProbeParam::StartBlock,                                   \
            NCloud::NProbeParam::RequestSize))                                 \
    PROBE(RequestAcquired,                                                     \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId"))        \
    PROBE(RequestPostponed,                                                    \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId"))        \
    PROBE(RequestAdvanced,                                                     \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId"))        \
    PROBE(RequestSent,                                                         \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId"))        \
    PROBE(ResponseReceived,                                                    \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId"))        \
    PROBE(ResponseSent,                                                        \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId"))        \
    PROBE(RequestCompleted,                                                    \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString, ui64, ui64, ui64, ui64),                 \
        NAMES(                                                                 \
            NCloud::NProbeParam::RequestType,                                  \
            "requestId",                                                       \
            "diskId",                                                          \
            "requestSize",                                                     \
            "requestTime",                                                     \
            NCloud::NProbeParam::RequestExecutionTime,                         \
            "error"))                                                          \
    PROBE(RequestRetry,                                                        \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString, ui64, ui64, ui64),                       \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId",         \
            "retryCount", "timeout", "error"))                                 \
    PROBE(RequestReceived_ThrottlingService,                                   \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui32, ui64, TString),                                   \
        NAMES(                                                                 \
            NCloud::NProbeParam::RequestType,                                  \
            NCloud::NProbeParam::MediaKind,                                    \
            "requestId",                                                       \
            "diskId"))                                                         \
    PROBE(RequestPostponed_ThrottlingService,                                  \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId"))        \
    PROBE(RequestAdvanced_ThrottlingService,                                   \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "diskId"))        \
// BLOCKSTORE_SERVER_PROVIDER

LWTRACE_DECLARE_PROVIDER(BLOCKSTORE_SERVER_PROVIDER)
