#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_STORAGE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(                                                             \
        BackgroundTaskStarted_Tablet,                                  \
        GROUPS("NFSBackground"),                                       \
        TYPES(TString, ui64, TString, ui32),                           \
        NAMES(                                                         \
            NCloud::NProbeParam::RequestType,                          \
            NCloud::NProbeParam::RequestId,                            \
            NCloud::NProbeParam::FsId,                                 \
            NCloud::NProbeParam::MediaKind))                           \
    PROBE(                                                             \
        RequestReceived_Tablet,                                        \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64, TString, ui32),                           \
        NAMES(                                                         \
            NCloud::NProbeParam::RequestType,                          \
            NCloud::NProbeParam::RequestId,                            \
            NCloud::NProbeParam::FsId,                                 \
            NCloud::NProbeParam::MediaKind))                           \
    PROBE(                                                             \
        BackgroundRequestReceived_Tablet,                              \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES(                                                         \
            NCloud::NProbeParam::RequestType,                          \
            NCloud::NProbeParam::RequestId))                           \
    PROBE(                                                             \
        ResponseSent_Tablet,                                           \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES(                                                         \
            NCloud::NProbeParam::RequestType,                          \
            NCloud::NProbeParam::RequestId))                           \
    PROBE(                                                             \
        RequestReceived_TabletWorker,                                  \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES(                                                         \
            NCloud::NProbeParam::RequestType,                          \
            NCloud::NProbeParam::RequestId))                           \
    PROBE(                                                             \
        ResponseSent_TabletWorker,                                     \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES(                                                         \
            NCloud::NProbeParam::RequestType,                          \
            NCloud::NProbeParam::RequestId))                           \
    PROBE(                                                             \
        RequestPostponed_Tablet,                                       \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES(                                                         \
            NCloud::NProbeParam::RequestType,                          \
            NCloud::NProbeParam::RequestId))                           \
    PROBE(                                                             \
        RequestAdvanced_Tablet,                                        \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES(                                                         \
            NCloud::NProbeParam::RequestType,                          \
            NCloud::NProbeParam::RequestId))                           \
    PROBE(                                                             \
        TxInit,                                                        \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        TxPrepare,                                                     \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        TxPrepareRestarted,                                            \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        TxPrepareDone,                                                 \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        TxExecute,                                                     \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        TxExecuteDone,                                                 \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        TxComplete,                                                    \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        TxCompleteDone,                                                \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        ForkFailed,                                                    \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        AuthRequestSent_Proxy,                                         \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    PROBE(                                                             \
        AuthResponseReceived_Proxy,                                    \
        GROUPS("NFSRequest"),                                          \
        TYPES(TString, ui64),                                          \
        NAMES("name", NCloud::NProbeParam::RequestId))                 \
    // FILESTORE_STORAGE_PROVIDER

LWTRACE_DECLARE_PROVIDER(FILESTORE_STORAGE_PROVIDER)
