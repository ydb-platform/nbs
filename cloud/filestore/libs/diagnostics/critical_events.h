#pragma once

#include "public.h"

namespace NCloud::NFileStore{

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CRITICAL_EVENTS(xxx)                                         \
    xxx(TabletUpdateConfigError)                                               \
    xxx(InvalidTabletStorageInfo)                                              \
    xxx(CollectGarbageError)                                                   \
    xxx(TabletBSFailure)                                                       \
    xxx(TabletCommitIdOverflow)                                                \
    xxx(VfsQueueRunningError)                                                  \
    xxx(EndpointStartingError)                                                 \
    xxx(MissingSessionId)                                                      \
    xxx(CreateSessionError)                                                    \
    xxx(DescribeFileStoreError)                                                \
// FILESTORE_CRITICAL_EVENTS

////////////////////////////////////////////////////////////////////////////////

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

#define FILESTORE_DECLARE_CRITICAL_EVENT_ROUTINE(name)                         \
    void Report##name();                                                       \
// FILESTORE_DECLARE_CRITICAL_EVENT_ROUTINE

    FILESTORE_CRITICAL_EVENTS(FILESTORE_DECLARE_CRITICAL_EVENT_ROUTINE)
#undef FILESTORE_DECLARE_CRITICAL_EVENT_ROUTINE

}   // namespace NCloud::NFileStore
