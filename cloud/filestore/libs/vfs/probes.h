#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_VFS_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(                                                         \
        RequestReceived,                                           \
        GROUPS("NFSRequest"),                                      \
        TYPES(TString, ui64, TString, ui32, ui64),                 \
        NAMES(                                                     \
            NCloud::NProbeParam::RequestType,                      \
            NCloud::NProbeParam::RequestId,                        \
            NCloud::NProbeParam::FsId,                             \
            NCloud::NProbeParam::MediaKind,                        \
            NCloud::NProbeParam::RequestSize))                     \
    PROBE(                                                         \
        RequestSent,                                               \
        GROUPS("NFSRequest"),                                      \
        TYPES(TString, ui64),                                      \
        NAMES(                                                     \
            NCloud::NProbeParam::RequestType,                      \
            NCloud::NProbeParam::RequestId))                       \
    PROBE(                                                         \
        ResponseReceived,                                          \
        GROUPS("NFSRequest"),                                      \
        TYPES(TString, ui64),                                      \
        NAMES(                                                     \
            NCloud::NProbeParam::RequestType,                      \
            NCloud::NProbeParam::RequestId))                       \
    PROBE(                                                         \
        ResponseSent,                                              \
        GROUPS("NFSRequest"),                                      \
        TYPES(TString, ui64),                                      \
        NAMES(                                                     \
            NCloud::NProbeParam::RequestType,                      \
            NCloud::NProbeParam::RequestId))                       \
    PROBE(                                                         \
        RequestCompleted,                                          \
        GROUPS("NFSRequest"),                                      \
        TYPES(TString, ui64, ui64, ui64, i64),                     \
        NAMES(                                                     \
            "replyType",                                           \
            NCloud::NProbeParam::RequestId,                        \
            NCloud::NProbeParam::RequestTime,                      \
            NCloud::NProbeParam::RequestExecutionTime,             \
            "result"))                                             \
    PROBE(                                                         \
        RequestCompletedError,                                     \
        GROUPS("NFSRequest"),                                      \
        TYPES(TString, ui64, ui64, ui64, i64, i64),                \
        NAMES(                                                     \
            "replyType",                                           \
            NCloud::NProbeParam::RequestId,                        \
            NCloud::NProbeParam::RequestTime,                      \
            NCloud::NProbeParam::RequestExecutionTime,             \
            "error",                                               \
            "result"))                                             \
    // FILESTORE_VFS_PROVIDER

LWTRACE_DECLARE_PROVIDER(FILESTORE_VFS_PROVIDER)
