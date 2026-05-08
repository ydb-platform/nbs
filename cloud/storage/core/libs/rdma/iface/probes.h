#pragma once

#include "public.h"

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_RDMA_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)              \
    PROBE(RequestEnqueued,                                                     \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(SendRequestStarted,                                                  \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(SendRequestCompleted,                                                \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(RecvRequestCompleted,                                                \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(ReadRequestDataStarted,                                              \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(ReadRequestDataCompleted,                                            \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(ExecuteRequest,                                                      \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(ResponseEnqueued,                                                    \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(WriteResponseDataStarted,                                            \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(WriteResponseDataCompleted,                                          \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(SendResponseStarted,                                                 \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(SendResponseCompleted,                                               \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(RecvResponseCompleted,                                               \
        GROUPS("StorageRequest"),                                              \
        TYPES(ui64),                                                           \
        NAMES("requestId"))                                                    \
    PROBE(RequestReceived_RdmaTarget,                                          \
        GROUPS("StorageRequest"),                                              \
        TYPES(),                                                               \
        NAMES())                                                               \
// STORAGE_RDMA_PROVIDER

LWTRACE_DECLARE_PROVIDER(STORAGE_RDMA_PROVIDER)
