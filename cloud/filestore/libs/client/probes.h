#pragma once

#include "public.h"

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CLIENT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)          \
    PROBE(SendRequest,                                                         \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("requestType", "requestId")                                      \
    )                                                                          \
    PROBE(ResponseReceived,                                                    \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("requestType", "requestId")                                      \
    )                                                                          \
    PROBE(RequestCompleted,                                                    \
        GROUPS("NFSRequest"),                                                  \
        TYPES(TString, ui64),                                                  \
        NAMES("requestType", "requestId")                                      \
    )                                                                          \
// FILESTORE_CLIENT_PROVIDER

LWTRACE_DECLARE_PROVIDER(FILESTORE_CLIENT_PROVIDER)
