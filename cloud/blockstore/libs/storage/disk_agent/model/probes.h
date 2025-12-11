#pragma once

#include <library/cpp/lwtrace/all.h>

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_AGENT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(                                                                 \
        FaultInjection,                                                    \
        GROUPS("NBSRequest"),                                              \
        TYPES(TString, TString),                                           \
        NAMES("name", "deviceId"))                                         \
    // BLOCKSTORE_DISK_AGENT_PROVIDER

#define BLOCKSTORE_DISK_AGENT_FAULT_INJECTION(NAME, DEVICE_ID) \
    GLOBAL_LWPROBE(                                            \
        BLOCKSTORE_DISK_AGENT_PROVIDER,                        \
        FaultInjection,                                        \
        NAME,                                                  \
        DEVICE_ID)                                             \
    // BLOCKSTORE_DISK_AGENT_FAULT_INJECTION

LWTRACE_DECLARE_PROVIDER(BLOCKSTORE_DISK_AGENT_PROVIDER)
