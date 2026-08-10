#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/printable_params.h>
#include <cloud/blockstore/libs/common/volume_labels.h>

#include <utility>

namespace NCloud::NBlockStore {

using TCritEventParams =
    std::initializer_list<std::pair<TStringBuf, TPrintableValue>>;

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
    xxx(MirroredDiskAddTagFailed)                                              \
    xxx(CounterUpdateRace)                                                     \
    xxx(EndpointStartingError)                                                 \
    xxx(ResyncFailed)                                                          \
    xxx(DiskRegistryBackupFailed)                                              \
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
    xxx(DiskRegistryOccupiedDeviceConfigurationHasChanged)                     \
    xxx(DiskRegistryWrongMigratedDeviceOwnership)                              \
    xxx(DiskRegistryInitialAgentRejectionThresholdExceeded)                    \
    xxx(ErrorWasSentToTheGuestForReliableDisk)                                 \
    xxx(ErrorWasSentToTheGuestForNonReliableDisk)                              \
    xxx(MirroredDiskResyncChecksumMismatch)                                    \
    xxx(DiskAgentInconsistentMultiWriteResponse)                               \
    xxx(ReleaseShadowDiskError)                                                \
    xxx(WrongCellIdInDescribeVolume)                                           \
    xxx(TrimFreshLogTimeout)                                                   \
    xxx(DiskRegistryStateIntegrityBroken)                                      \
    xxx(AddFreshBlocksResultedInError)                                         \
    xxx(OverlappingRequestsDetected)                                           \
    xxx(CrossPartitionRequestDetected)                                         \
// BLOCKSTORE_CRITICAL_EVENTS

#define BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(xxx)                             \
    xxx(AcquiredDiskEraseAttempt)                                              \
    xxx(DiskAgentConfigMismatch)                                               \
    xxx(DiskAgentDeviceSymlinkMismatch)                                        \
    xxx(DiskAgentIoDuringSecureErase)                                          \
    xxx(DiskAgentSecureEraseDuringIo)                                          \
    xxx(DiskAgentSessionCacheRestoreError)                                     \
    xxx(DiskAgentSessionCacheUpdateError)                                      \
    xxx(UnexpectedIdentifierRepetition)                                        \
    xxx(ChaosGeneratedError)                                                   \
// BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS

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
    xxx(DiskRegistryCouldNotAddOutdatedLaggingDevice)                          \
    xxx(DiskRegistryReplicaTableReplaceError)                                  \
    xxx(ResyncUnexpectedWriteOrZeroCounter)                                    \
    xxx(MonitoringResourceNotFound)                                            \
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
    xxx(MultiAgentRequestAffectsTwoDevices)                                    \
    xxx(ChecksumCalculationError)                                              \
    xxx(LogicalDiskIdMismatch)                                                 \
    xxx(DeviceReplacementContractBroken)                                       \
    xxx(InflightRequestInvariantViolation)                                     \
    xxx(SetupChannelsOnWrongMediaKindVolume)                                   \
    xxx(DiskRegistryDetachPathWithDependentDisk)                               \
    xxx(DiskDevicesSizeViolation)                                              \
    xxx(RdmaMessageTypeMismatch)                                               \
    xxx(BlockChecksumAbsent)                                                   \
    xxx(CleanupBlobMetaBlocksMismatch)                                         \
// BLOCKSTORE_IMPOSSIBLE_EVENTS

#define BLOCKSTORE_VOLUME_CRITICAL_EVENTS(xxx)                                 \
    xxx(InvalidTabletConfig)                                                   \
    xxx(ReassignTablet)                                                        \
    xxx(TabletBSFailure)                                                       \
    xxx(DiskAllocationFailure)                                                 \
    xxx(CollectGarbageError)                                                   \
    xxx(MigrationFailed)                                                       \
    xxx(BadMigrationConfig)                                                    \
    xxx(InitFreshBlocksError)                                                  \
    xxx(TrimFreshLogError)                                                     \
    xxx(NrdDestructionError)                                                   \
    xxx(FailedToStartVolumeLocally)                                            \
    xxx(MirroredDiskAllocationCleanupFailure)                                  \
    xxx(MirroredDiskAllocationPlacementGroupCleanupFailure)                    \
    xxx(MirroredDiskDeviceReplacementForbidden)                                \
    xxx(MirroredDiskDeviceReplacementFailure)                                  \
    xxx(MirroredDiskDeviceReplacementRateLimitExceeded)                        \
    xxx(MirroredDiskMinorityChecksumMismatch)                                  \
    xxx(MirroredDiskMajorityChecksumMismatch)                                  \
    xxx(MirroredDiskChecksumMismatchUponRead)                                  \
    xxx(MirroredDiskAddTagFailed)                                              \
    xxx(ResyncFailed)                                                          \
    xxx(AddConfirmedBlobsError)                                                \
    xxx(ConfirmBlobsError)                                                     \
    xxx(BlockDigestMismatchInBlob)                                             \
    xxx(ErrorWasSentToTheGuestForReliableDisk)                                 \
    xxx(ErrorWasSentToTheGuestForNonReliableDisk)                              \
    xxx(MirroredDiskResyncChecksumMismatch)                                    \
    xxx(ReleaseShadowDiskError)                                                \
    xxx(WrongCellIdInDescribeVolume)                                           \
    xxx(TrimFreshLogTimeout)                                                   \
    xxx(AddFreshBlocksResultedInError)                                         \
    xxx(OverlappingRequestsDetected)                                           \
    xxx(CrossPartitionRequestDetected)                                         \
    // BLOCKSTORE_VOLUME_CRITICAL_EVENTS

////////////////////////////////////////////////////////////////////////////////

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);
void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

NCloud::IStatsHandlerPtr CreateCriticalEventsStatsHandler();

// For unit test purposes
void ResetVolumeCriticalEventsCounter();

#define BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE(name)                        \
    TString Report##name(const TString& message = "");                         \
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues);                                    \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues);                                    \
    const TString GetCriticalEventFor##name();                                 \
// BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_CRITICAL_EVENTS(BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE(name)             \
    TString Report##name(const TString& message = "");                         \
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues);                                    \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues);                                    \
    const TString GetCriticalEventFor##name();                                 \
// BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(
        BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE(name)                      \
    TString Report##name(const TString& message = "");                         \
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues);                                    \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues);                                    \
    const TString GetCriticalEventFor##name();                                 \
// BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE
    BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE

#define BLOCKSTORE_DECLARE_VOLUME_CRITICAL_EVENT_ROUTINE(name)             \
        TString Report##name(                                                  \
            const TString& diskId,                                             \
            const TString& cloudId,                                            \
            const TString& folderId,                                           \
            const TString& message = "");                                      \
        TString Report##name(                                                  \
            const TString& diskId,                                             \
            const TString& cloudId,                                            \
            const TString& folderId,                                           \
            const TString& message,                                            \
            const TCritEventParams& keyValues);                                \
        TString Report##name(                                                  \
            const TString& diskId,                                             \
            const TString& cloudId,                                            \
            const TString& folderId,                                           \
            const TCritEventParams& keyValues);                                \
        template <typename... TArgs>                                           \
        TString Report##name(                                                  \
            const TVolumeLabels& volumeLabels,                                 \
            TArgs&&... args)                                                   \
        {                                                                      \
            return Report##name(                                               \
                volumeLabels.DiskId,                                           \
                volumeLabels.CloudId,                                          \
                volumeLabels.FolderId,                                         \
                std::forward<TArgs>(args)...);                                 \
        }                                                                      \
        template <typename... TArgs>                                           \
        TString Report##name(                                                  \
            const TVolumeLabelsConstPtr& volumeLabels,                         \
            TArgs&&... args)                                                   \
        {                                                                      \
            Y_ABORT_UNLESS(volumeLabels);                                      \
            return Report##name(*volumeLabels, std::forward<TArgs>(args)...);  \
        }                                                                      \
        const TString GetVolumeCriticalEventFor##name();                       \
        const TString GetDeprecatedCriticalEventFor##name();                   \
        // BLOCKSTORE_DECLARE_VOLUME_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_VOLUME_CRITICAL_EVENTS(
        BLOCKSTORE_DECLARE_VOLUME_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_VOLUME_CRITICAL_EVENT_ROUTINE

}   // namespace NCloud::NBlockStore
