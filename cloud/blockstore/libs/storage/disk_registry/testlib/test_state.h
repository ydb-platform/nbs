#pragma once

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_database.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_state.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/testlib/common_properties.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

#define UNIT_ASSERT_DISK_STATE(diskId, state, update)                          \
    UNIT_ASSERT_VALUES_EQUAL(diskId, (update).State.GetDiskId());              \
    UNIT_ASSERT_VALUES_EQUAL_C(                                                \
        NProto::EDiskState_Name(NProto::state),                                \
        NProto::EDiskState_Name((update).State.GetState()),                    \
        diskId                                                                 \
    );                                                                         \

#define UNIT_ASSERT_SUCCESS(expr)                                              \
    [error = (expr)] () {                                                      \
        UNIT_ASSERT_C(!HasError(error), error);                                \
    }()                                                                        \

namespace NCloud::NBlockStore::NStorage::NDiskRegistryStateTest {

using NProto::TDeviceConfig;

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 512;
constexpr ui32 DefaultLogicalBlockSize = 4_KB;
constexpr ui64 DefaultDeviceSize = 10_GB;

////////////////////////////////////////////////////////////////////////////////

struct TByDeviceUUID
{
    template <typename T>
    bool operator () (const T& lhs, const T& rhs) const
    {
        return lhs.GetDeviceUUID() < rhs.GetDeviceUUID();
    }
};

struct TByDiskId
{
    bool operator () (const auto& lhs, const auto& rhs) const
    {
        return lhs.GetDiskId() < rhs.GetDiskId();
    }

    bool operator () (
        const TDiskStateUpdate& lhs,
        const TDiskStateUpdate& rhs) const
    {
        return lhs.State.GetDiskId() < rhs.State.GetDiskId();
    }
};

struct TByNodeId
{
    bool operator () (const auto& lhs, const auto& rhs) const
    {
        return lhs.GetNodeId() < rhs.GetNodeId();
    }
};

struct TByAgentId
{
    bool operator () (const auto& lhs, const auto& rhs) const
    {
        return lhs.GetAgentId() < rhs.GetAgentId();
    }
};

////////////////////////////////////////////////////////////////////////////////

TDeviceConfig Device(
    TString name,
    TString uuid,
    TString rack = "",
    ui32 blockSize = DefaultBlockSize,
    ui64 totalSize = DefaultDeviceSize,
    TString transportId = {},
    NProto::EDeviceState state = NProto::DEVICE_STATE_ONLINE,
    NProto::TRdmaEndpoint rdmaEndpoint = {});

TDeviceConfig Device(
    TString name,
    TString uuid,
    NProto::EDeviceState state);

NProto::TAgentConfig AgentConfig(
    ui32 nodeId,
    NProto::EAgentState state,
    std::initializer_list<TDeviceConfig> devices);

NProto::TAgentConfig AgentConfig(
    ui32 nodeId,
    std::initializer_list<TDeviceConfig> devices);

NProto::TAgentConfig AgentConfig(
    ui32 nodeId,
    TString agentId,
    ui64 seqNumber,
    TVector<TDeviceConfig> devices);

NProto::TAgentConfig AgentConfig(
    ui32 nodeId,
    TString agentId,
    TVector<TDeviceConfig> devices);

NProto::TDiskRegistryConfig MakeConfig(
    const TVector<NProto::TAgentConfig>& agents,
    const TVector<NProto::TDeviceOverride>& deviceOverrides = {});

NProto::TDiskRegistryConfig MakeConfig(
    ui32 version,
    const TVector<NProto::TAgentConfig>& agents);

NProto::TPlacementGroupConfig SpreadPlacementGroup(
    TString id,
    TVector<TString> disks);

NProto::TPlacementGroupConfig PartitionPlacementGroup(
    TString id,
    TVector<TVector<TString>> partitions);

NProto::TDiskConfig Disk(
    const TString& diskId,
    std::initializer_list<TString> uuids,
    NProto::EDiskState state = NProto::DISK_STATE_ONLINE);

TVector<NProto::TDiskConfig> MirrorDisk(
    const TString& diskId,
    TVector<TVector<TString>> uuids,
    NProto::EDiskState state = NProto::DISK_STATE_ONLINE);

NProto::TDiskConfig ShadowDisk(
    const TString& sourceDiskId,
    const TString& checkpointId,
    std::initializer_list<TString> uuids,
    NProto::EDiskState state = NProto::DISK_STATE_ONLINE);

NProto::TError AllocateMirroredDisk(
    TDiskRegistryDatabase& db,
    TDiskRegistryState& state,
    const TString& diskId,
    ui64 totalSize,
    ui32 replicaCount,
    TVector<TDeviceConfig>& devices,
    TVector<TVector<TDeviceConfig>>& replicas,
    TVector<NProto::TDeviceMigration>& migrations,
    TVector<TString>& deviceReplacementIds,
    TInstant now = TInstant::Seconds(100),
    NProto::EStorageMediaKind mediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR2,
    ui32 logicalBlockSize = DefaultLogicalBlockSize);

NProto::TError AllocateDisk(
    TDiskRegistryDatabase& db,
    TDiskRegistryState& state,
    const TString& diskId,
    const TString& placementGroupId,
    ui32 placementPartitionIndex,
    ui64 totalSize,
    TVector<TDeviceConfig>& devices,
    TInstant now = TInstant::Seconds(100),
    NProto::EStorageMediaKind mediaKind =
        NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
    TString poolName = "",
    ui32 logicalBlockSize = DefaultLogicalBlockSize);

NProto::TError AllocateCheckpoint(
    TInstant now,
    TDiskRegistryDatabase& db,
    TDiskRegistryState& state,
    const TString& sourceDiskId,
    const TString& checkpointId,
    TString* shadowDiskId,
    TVector<TDeviceConfig>* devices);

NProto::TStorageServiceConfig CreateDefaultStorageConfigProto();

TStorageConfigPtr CreateStorageConfig(NProto::TStorageServiceConfig proto);

TStorageConfigPtr CreateStorageConfig();

void UpdateConfig(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const TVector<NProto::TAgentConfig>& agents,
    const TVector<NProto::TDeviceOverride>& deviceOverrides = {});

NProto::TError RegisterAgent(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& config,
    TInstant timestamp);

NProto::TError RegisterAgent(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& config);

void CleanDevices(TDiskRegistryState& state, TDiskRegistryDatabase& db);

TVector<TString> UpdateAgentState(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const TString& agentId,
    NProto::EAgentState desiredState);

TVector<TString> UpdateAgentState(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& config,
    NProto::EAgentState desiredState);

////////////////////////////////////////////////////////////////////////////////

TString GetReplicaTableRepr(
    const TDiskRegistryState& state,
    const TString& diskId);

////////////////////////////////////////////////////////////////////////////////

struct TDiskRegistryStateBuilder
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    TStorageConfigPtr StorageConfig = CreateStorageConfig();
    NMonitoring::TDynamicCountersPtr Counters;
    NProto::TDiskRegistryConfig Config;
    TVector<NProto::TAgentConfig> Agents;
    TVector<NProto::TDiskConfig> Disks;
    TVector<NProto::TPlacementGroupConfig> PlacementGroups;
    TVector<TBrokenDiskInfo> BrokenDisks;
    TVector<TString> DisksToReallocate;
    TVector<TString> ErrorNotifications;
    TVector<NProto::TUserNotification> UserNotifications;
    TVector<TDiskStateUpdate> DiskStateUpdates;
    ui64 DiskStateSeqNo = 0;
    TVector<TDirtyDevice> DirtyDevices;
    TVector<TString> DisksToCleanup;
    TVector<TString> OutdatedVolumeConfigs;
    TVector<NProto::TSuspendedDevice> SuspendedDevices;
    TDeque<TAutomaticallyReplacedDeviceInfo> AutomaticallyReplacedDevices;
    THashMap<TString, NProto::TDiskRegistryAgentParams> DiskRegistryAgentListParams;

    static TDiskRegistryStateBuilder LoadState(TDiskRegistryDatabase& db);

    std::unique_ptr<TDiskRegistryState> Build();

    TDiskRegistryStateBuilder& With(TStorageConfigPtr config);

    TDiskRegistryStateBuilder& WithDisksToReallocate(TVector<TString> ids);

    TDiskRegistryStateBuilder& WithStorageConfig(
        NProto::TStorageServiceConfig proto);

    TDiskRegistryStateBuilder& With(NMonitoring::TDynamicCountersPtr counters);

    TDiskRegistryStateBuilder& With(ui64 diskStateSeqNo);

    TDiskRegistryStateBuilder& WithConfig(NProto::TDiskRegistryConfig config);

    TDiskRegistryStateBuilder& WithConfig(TVector<NProto::TAgentConfig> agents);

    TDiskRegistryStateBuilder& WithConfig(
        ui32 version,
        TVector<NProto::TAgentConfig> agents);

    TDiskRegistryStateBuilder& WithKnownAgents(
        TVector<NProto::TAgentConfig> agents);

    TDiskRegistryStateBuilder& WithAgents(TVector<NProto::TAgentConfig> agents);

    TDiskRegistryStateBuilder& WithDisks(TVector<NProto::TDiskConfig> disks);

    TDiskRegistryStateBuilder& WithDirtyDevices(TVector<TDirtyDevice> dirtyDevices);

    TDiskRegistryStateBuilder& WithSuspendedDevices(
        TVector<TString> suspendedDevices);

    TDiskRegistryStateBuilder& WithSpreadPlacementGroups(
        TVector<TString> groupIds);

    TDiskRegistryStateBuilder& WithPlacementGroups(
        TVector<NProto::TPlacementGroupConfig> groups);

    TDiskRegistryStateBuilder& WithErrorNotifications(
        TVector<TString> notifications);

    TDiskRegistryStateBuilder& AddDevicePoolConfig(
        TString name,
        ui64 allocationUnit,
        NProto::EDevicePoolKind kind);
};

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistryStateTest
