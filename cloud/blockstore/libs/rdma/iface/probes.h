#pragma once

#include "public.h"

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_RDMA_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)           \
    PROBE(RequestEnqueued,                                                     \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(SendRequestStarted,                                                  \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(SendRequestCompleted,                                                \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(RecvRequestCompleted,                                                \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(ReadRequestDataStarted,                                              \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(ReadRequestDataCompleted,                                            \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(ExecuteRequest,                                                      \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(ResponseEnqueued,                                                    \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(WriteResponseDataStarted,                                            \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(WriteResponseDataCompleted,                                          \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(SendResponseStarted,                                                 \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(SendResponseCompleted,                                               \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(RecvResponseCompleted,                                               \
        GROUPS("NBSRequest"),                                                  \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(RequestReceived_Cells,                                               \
        GROUPS("NBSRequest"),                                                  \
        TYPES(),                                                               \
        NAMES())                                                               \
// BLOCKSTORE_RDMA_PROVIDER

LWTRACE_DECLARE_PROVIDER(BLOCKSTORE_RDMA_PROVIDER)
