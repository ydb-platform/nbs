#pragma once

#include "private.h"

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_TEST_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(                                                           \
        RequestStarted,                                              \
        GROUPS("NBSRequest"),                                        \
        TYPES(ui64, TString),                                        \
        NAMES("requestId", "requestType"))                           \
    PROBE(                                                           \
        IORequestStarted,                                            \
        GROUPS("NBSRequest"),                                        \
        TYPES(ui64),                                                 \
        NAMES("requestId"))                                          \
    PROBE(                                                           \
        IORequestCompleted,                                          \
        GROUPS("NBSRequest"),                                        \
        TYPES(ui64),                                                 \
        NAMES("requestId"))                                          \
    PROBE(                                                           \
        RdmaPrepareRequest,                                          \
        GROUPS("NBSRequest"),                                        \
        TYPES(ui64),                                                 \
        NAMES("requestId"))                                          \
    PROBE(                                                           \
        RdmaHandleResponse,                                          \
        GROUPS("NBSRequest"),                                        \
        TYPES(ui64),                                                 \
        NAMES("requestId"))                                          \
    PROBE(                                                           \
        RequestCompleted,                                            \
        GROUPS("NBSRequest"),                                        \
        TYPES(ui64),                                                 \
        NAMES("requestId"))                                          \
    // BLOCKSTORE_TEST_PROVIDER

LWTRACE_DECLARE_PROVIDER(BLOCKSTORE_TEST_PROVIDER)
