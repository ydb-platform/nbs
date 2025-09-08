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
    xxx(NodeNotFoundInShard)                                                   \
    xxx(NotEnoughResultsInGetNodeAttrBatchResponses)                           \
    xxx(AsyncDestroyHandleFailed)                                              \
    xxx(HandleOpsQueueProcessError)                                            \
    xxx(HandleOpsQueueCreatingOrDeletingError)                                 \
    xxx(DuplicateRequestId)                                                    \
    xxx(InvalidDupCacheEntry)                                                  \
    xxx(GeneratedOrphanNode)                                                   \
    xxx(ReceivedNodeOpErrorFromShard)                                          \
    xxx(LocalFsMaxSessionNodesInUse)                                           \
    xxx(LocalFsMaxSessionFileHandlesInUse)                                     \
    xxx(LocalFsMissingHandleNode)                                              \
    xxx(ShardStatsRetrievalTimeout)                                            \
    xxx(CreateNodeRequestResponseMismatchInShard)                              \
    xxx(RenameNodeRequestSentToWrongShard)                                     \
    xxx(RenameNodeRequestForLocalNode)                                         \
    xxx(InvalidShardNo)                                                        \
    xxx(WriteBackCacheCreatingOrDeletingError)                                 \
    xxx(ErrorWasSentToTheGuest)                                                \
// FILESTORE_CRITICAL_EVENTS

#define FILESTORE_IMPOSSIBLE_EVENTS(xxx)                                       \
    xxx(CancelRoutineIsNotSet)                                                 \
    xxx(ChildNodeWithoutRef)                                                   \
    xxx(SessionNotFoundInTx)                                                   \
    xxx(InvalidNodeIdForLocalNode)                                             \
    xxx(ChildNodeIsNull)                                                       \
    xxx(TargetNodeWithoutRef)                                                  \
    xxx(ParentNodeIsNull)                                                      \
    xxx(FailedToCreateHandle)                                                  \
    xxx(ChildRefIsNull)                                                        \
    xxx(NewChildNodeIsNull)                                                    \
    xxx(IndexOutOfBounds)                                                      \
    xxx(CheckFreshBytesFailed)                                                 \
    xxx(LocalFsDuplicateFileHandle)                                            \
    xxx(UnexpectedLocalNode)                                                   \
    xxx(NoRenameNodeInDestinationRequest)                                      \
    xxx(BadChildRefUponCommitRenameNodeInSource)                               \
    xxx(FailedToLockNodeRef)                                                   \
    xxx(InvalidNodeRefUponCompleteUnlinkNode)                                  \
    xxx(UnknownOpLogEntry)                                                     \
// FILESTORE_IMPOSSIBLE_EVENTS

////////////////////////////////////////////////////////////////////////////////

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

#define FILESTORE_DECLARE_CRITICAL_EVENT_ROUTINE(name)                         \
    TString Report##name(const TString& message = "");                         \
    const TString GetCriticalEventFor##name();                                 \
// FILESTORE_DECLARE_CRITICAL_EVENT_ROUTINE

    FILESTORE_CRITICAL_EVENTS(FILESTORE_DECLARE_CRITICAL_EVENT_ROUTINE)
#undef FILESTORE_DECLARE_CRITICAL_EVENT_ROUTINE

#define FILESTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE(name)                       \
    TString Report##name(const TString& message = "");                         \
    const TString GetCriticalEventFor##name();                                 \
// FILESTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE

    FILESTORE_IMPOSSIBLE_EVENTS(FILESTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE)
#undef FILESTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE

}   // namespace NCloud::NFileStore
