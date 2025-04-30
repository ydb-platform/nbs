#pragma once

#include <cloud/blockstore/libs/common/public.h>

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_STORAGE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)        \
    PROBE(AuthRequestSent_Proxy,                                               \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(AuthResponseReceived_Proxy,                                          \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestSent_Proxy,                                                   \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestReceived_Service,                                             \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestSentPipe,                                                     \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(ResponseReceivedPipe,                                                \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(ResponseSent_Service,                                                \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestReceived_Volume,                                              \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(DuplicatedRequestReceived_Volume,                                    \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, ui64),                                            \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "parentId"))      \
    PROBE(RequestReceived_VolumeWorker,                                        \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestPostponed_Volume,                                             \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestAdvanced_Volume,                                              \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(ResponseSent_Volume,                                                 \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(ResponseSent_VolumeWorker,                                           \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestReceived_Partition,                                           \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestReceived_PartitionWorker,                                     \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(RequestReceived_NonreplPartitionWorker,                              \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, TString),                                         \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "device"))        \
    PROBE(RequestReceived_PartitionWorker_DSProxy,                             \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64, ui32),                                            \
        NAMES(NCloud::NProbeParam::RequestType, "requestId", "groupId"))       \
    PROBE(ResponseSent_Partition,                                              \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(ResponseSent_PartitionWorker,                                        \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(ResponseSent_NonreplPartitionWorker,                                 \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(ResponseReceived_Proxy,                                              \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(BackgroundTaskStarted_Partition,                                     \
        GROUPS("NBSBackground"),                                               \
        TYPES(TString, ui32, ui64, TString),                                   \
        NAMES(                                                                 \
            NCloud::NProbeParam::RequestType,                                  \
            NCloud::NProbeParam::MediaKind,                                    \
            "requestId",                                                       \
            "diskId"))                                                         \
    PROBE(ForkFailed,                                                          \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
    PROBE(TxInit,                                                              \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("name", "requestId"))                                            \
    PROBE(TxPrepare,                                                           \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("name", "requestId"))                                            \
    PROBE(TxPrepareRestarted,                                                  \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("name", "requestId"))                                            \
    PROBE(TxPrepareDone,                                                       \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("name", "requestId"))                                            \
    PROBE(TxExecute,                                                           \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("name", "requestId"))                                            \
    PROBE(TxExecuteDone,                                                       \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("name", "requestId"))                                            \
    PROBE(TxComplete,                                                          \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("name", "requestId"))                                            \
    PROBE(TxCompleteDone,                                                      \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("name", "requestId"))                                            \
    PROBE(RequestReceived_DiskAgent,                                           \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui32, ui64, TString),                                   \
        NAMES(                                                                 \
            NCloud::NProbeParam::RequestType,                                  \
            NCloud::NProbeParam::MediaKind,                                    \
            "requestId",                                                       \
            "deviceId"))                                                       \
    PROBE(ResponseSent_DiskAgent,                                              \
        GROUPS("NBSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES(NCloud::NProbeParam::RequestType, "requestId"))                  \
// BLOCKSTORE_STORAGE_PROVIDER

LWTRACE_DECLARE_PROVIDER(BLOCKSTORE_STORAGE_PROVIDER)
