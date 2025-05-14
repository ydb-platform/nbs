#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CRITICAL_EVENTS(xxx)                                        \
    xxx(InvalidTabletConfig)                                                   \
    xxx(ReassignTablet)                                                        \
    xxx(TabletBSFailure)                                                       \
    xxx(DiskAllocationFailure)                                                 \
    xxx(CollectGarbageError)                                                   \
    xxx(VhostQueueRunningError)                                                \
    xxx(MigrationFailed)                                                       \
    xxx(BadMigrationConfig)                                                    \
    xxx(InitFreshBlocksError)                                                  \
    xxx(TrimFreshLogError)                                                     \
    xxx(NrdDestructionError)                                                   \
    xxx(FailedToStartVolumeLocally)                                            \
    xxx(PublishDiskStateError)                                                 \
    xxx(EndpointRestoringError)                                                \
    xxx(HangingYdbStatsRequest)                                                \
    xxx(UserNotificationError)                                                 \
    xxx(BackupPathDescriptionsFailure)                                         \
    xxx(RdmaError)                                                             \
    xxx(MirroredDiskAllocationCleanupFailure)                                  \
    xxx(MirroredDiskAllocationPlacementGroupCleanupFailure)                    \
    xxx(MirroredDiskDeviceReplacementForbidden)                                \
    xxx(MirroredDiskDeviceReplacementFailure)                                  \
    xxx(MirroredDiskDeviceReplacementRateLimitExceeded)                        \
    xxx(MirroredDiskMinorityChecksumMismatch)                                  \
    xxx(MirroredDiskMajorityChecksumMismatch)                                  \
    xxx(MirroredDiskChecksumMismatchUponRead)                                  \
    xxx(MirroredDiskChecksumMismatchUponWrite)                                 \
    xxx(MirroredDiskAddTagFailed)                                              \
    xxx(CounterUpdateRace)                                                     \
    xxx(EndpointStartingError)                                                 \
    xxx(ResyncFailed)                                                          \
    xxx(DiskRegistryBackupFailed)                                              \
    xxx(FailedToParseRdmaError)                                                \
    xxx(FailedToSerializeRdmaError)                                            \
    xxx(RegisterAgentWithEmptyRackName)                                        \
    xxx(AddConfirmedBlobsError)                                                \
    xxx(ConfirmBlobsError)                                                     \
    xxx(ManuallyPreemptedVolumesFileError)                                     \
    xxx(ServiceProxyWakeupTimerHit)                                            \
    xxx(ReceivedUnknownTaskId)                                                 \
    xxx(MigrationSourceNotFound)                                               \
    xxx(UnexpectedBatchMigration)                                              \
    xxx(FreshDeviceNotFoundInConfig)                                           \
    xxx(DiskRegistryDeviceNotFoundSoft)                                        \
    xxx(DiskRegistrySourceDiskNotFound)                                        \
    xxx(EndpointSwitchFailure)                                                 \
    xxx(ExternalEndpointUnexpectedExit)                                        \
    xxx(BlockDigestMismatchInBlob)                                             \
    xxx(DiskRegistryResumeDeviceFailed)                                        \
    xxx(DiskRegistryAgentDevicePoolConfigMismatch)                             \
    xxx(DiskRegistryPurgeHostError)                                            \
    xxx(DiskRegistryCleanupAgentConfigError)                                   \
    xxx(DiskRegistryOccupiedDeviceConfigurationHasChanged)                     \
    xxx(DiskRegistryWrongMigratedDeviceOwnership)                              \
    xxx(DiskRegistryInitialAgentRejectionThresholdExceeded)                    \
    xxx(ErrorWasSentToTheGuestForReliableDisk)                                 \
    xxx(ErrorWasSentToTheGuestForNonReliableDisk)                              \
    xxx(MirroredDiskResyncChecksumMismatch)                                    \
    xxx(DiskAgentInconsistentMultiWriteResponse)                               \
// BLOCKSTORE_CRITICAL_EVENTS

#define DISK_AGENT_CRITICAL_EVENTS(xxx)                                        \
    xxx(AcquiredDiskEraseAttempt)                                              \
    xxx(DiskAgentConfigMismatch)                                               \
    xxx(DiskAgentIoDuringSecureErase)                                          \
    xxx(DiskAgentSecureEraseDuringIo)                                          \
    xxx(DiskAgentSessionCacheRestoreError)                                     \
    xxx(DiskAgentSessionCacheUpdateError)                                      \
    xxx(UnexpectedIdentifierRepetition)                                        \
// DISK_AGENT_CRITICAL_EVENTS

#define BLOCKSTORE_IMPOSSIBLE_EVENTS(xxx)                                      \
    xxx(TabletCommitIdOverflow)                                                \
    xxx(TabletCollectCounterOverflow)                                          \
    xxx(DiskRegistryLogicalPhysicalBlockSizeMismatch)                          \
    xxx(DiskRegistryAgentDeviceNodeIdMismatch)                                 \
    xxx(DiskRegistryPoolDeviceRackMismatch)                                    \
    xxx(DiskRegistryAgentNotFound)                                             \
    xxx(DiskRegistryBadDeviceSizeAdjustment)                                   \
    xxx(DiskRegistryBadDeviceStateAdjustment)                                  \
    xxx(DiskRegistryDuplicateDiskInPlacementGroup)                             \
    xxx(DiskRegistryInvalidPlacementGroupPartition)                            \
    xxx(DiskRegistryDeviceLocationNotFound)                                    \
    xxx(DiskRegistryDiskNotFound)                                              \
    xxx(DiskRegistryPlacementGroupNotFound)                                    \
    xxx(DiskRegistryDeviceListReferencesNonexistentDisk)                       \
    xxx(DiskRegistryPlacementGroupDiskNotFound)                                \
    xxx(DiskRegistryDeviceNotFound)                                            \
    xxx(DiskRegistryNoScheduledNotification)                                   \
    xxx(DiskRegistryDeviceDoesNotBelongToDisk)                                 \
    xxx(DiskRegistryCouldNotAddLaggingDevice)                                  \
    xxx(ResyncUnexpectedWriteOrZeroCounter)                                    \
    xxx(MonitoringSvgTemplatesNotFound)                                        \
    xxx(DiskRegistryUnexpectedAffectedDisks)                                   \
    xxx(ReadBlockCountMismatch)                                                \
    xxx(CancelRoutineIsNotSet)                                                 \
    xxx(FieldDescriptorNotFound)                                               \
    xxx(DiskRegistryInsertToPendingCleanupFailed)                              \
    xxx(OverlappingRangesDuringMigrationDetected)                              \
    xxx(StartExternalEndpointError)                                            \
    xxx(EmptyRequestSgList)                                                    \
    xxx(LaggingAgentsProxyWrongRecipientActor)                                 \
    xxx(UnexpectedCookie)                                                      \
// BLOCKSTORE_IMPOSSIBLE_EVENTS

////////////////////////////////////////////////////////////////////////////////

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

#define BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE(name)                        \
    TString Report##name(const TString& message = "");                         \
    const TString GetCriticalEventFor##name();                                 \
// BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_CRITICAL_EVENTS(BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE(name)             \
    TString Report##name(const TString& message = "");                         \
    const TString GetCriticalEventFor##name();                                 \
// BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE

    DISK_AGENT_CRITICAL_EVENTS(
        BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE(name)                      \
    TString Report##name(const TString& message = "");                         \
    const TString GetCriticalEventFor##name();                                 \
// BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE
    BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE

}   // namespace NCloud::NBlockStore
