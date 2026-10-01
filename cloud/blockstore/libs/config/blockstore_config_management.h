/*******************************************************************************

Blockstore configuration construction and source processing. Startup and runtime
factories build independently owned snapshots; runtime updates follow the field
markers. Callers publish completed snapshots and update live ICB defaults
separately.

*******************************************************************************/

#pragma once

#include "blockstore_config.h"

#include <cloud/blockstore/config/blockstore.pb.h>

#include <cloud/storage/core/libs/config/runtime_config.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Non-protobuf inputs used to build selected runtime configuration sections.
struct TBlockstoreConfigExtraParameters
{
    // Host-specific inputs used to build the DiskAgent wrapper.
    struct
    {
        // Local DiskAgent rack; empty if the rack is not specified.
        TString Rack;

        // Local DiskAgent network throughput in megabits per second; zero if
        // the throughput is not specified.
        ui32 NetworkMbitThroughput = 0;
    } DiskAgent;
};

////////////////////////////////////////////////////////////////////////////////

// Remove static-only fields from NProto::TBlockstoreConfig.
void RemoveStaticOnlyBlockstoreFields(NProto::TBlockstoreConfig& config);

// Normalize dynamic overrides against the prepared static configuration
// before building a snapshot; preserve equal static
// server/discovery ports unless discovery is explicitly overridden.
void NormalizeDynamicBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    NProto::TBlockstoreConfig& dynamicConfig);

// Merge dynamicConfig into a copy of staticConfig according to protobuf rules,
// except for Features. Keep the first feature with each name within a source;
// replace a matching static feature with the complete dynamic record at its
// position, and append dynamic-only features in their source order.
// Pass dynamicConfig normalized by NormalizeDynamicBlockstoreConfig().
NProto::TBlockstoreConfig MergeBlockstoreConfigSources(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig);

// Copy non-protobuf parameters from the current Blockstore configuration.
TBlockstoreConfigExtraParameters GetBlockstoreConfigExtraParameters(
    const IBlockstoreConfig& currentConfig);

// Build adapters from an already merged and filtered proto without merging
// sources again; require non-null controls and leave their defaults unchanged.
IBlockstoreConfigPtr MakeBlockstoreConfig(
    const NProto::TBlockstoreConfig& config,
    NStorage::TStorageConfigControlsPtr controls,
    TBlockstoreConfigExtraParameters extraParameters = {});

// Merge the source protos and create independently owned runtime adapters. The
// controls pointer must be non-null and becomes the live ICB overlay of the
// Storage wrapper. Extra parameters supply host-specific DiskAgent values.
// dynamicConfig is normalized with NormalizeDynamicBlockstoreConfig() before
// merging.
IBlockstoreConfigPtr MakeStartupBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NStorage::TStorageConfigControlsPtr controls,
    TBlockstoreConfigExtraParameters extraParameters = {});

// Create a bootstrap configuration from merged source protos and copies of the
// initialized Storage and DiskAgent adapters. The Storage copy shares its
// live ICB controls and uses Features from the merged sources.
// Build every other section from the merged sources.
// dynamicConfig is normalized with NormalizeDynamicBlockstoreConfig() before
// merging.
IBlockstoreConfigPtr MakeStartupBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    const NStorage::TStorageConfig& storageConfig,
    const NStorage::TDiskAgentConfig& diskAgentConfig);

// Prepare runtimeConfig from static, complete startup, and dynamicConfig
// normalized by NormalizeDynamicBlockstoreConfig(); return diagnostics.
// Use a separate message for runtimeConfig and publish it only on success.
NConfig::TRuntimeConfigDiagnostics PrepareRuntimeBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& startupConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NProto::TBlockstoreConfig& runtimeConfig);

// Build a runtime snapshot from static, complete startup, and received CMS
// protos; return diagnostics separately and leave sources and controls intact.
// dynamicConfig is normalized with NormalizeDynamicBlockstoreConfig() before
// merging. Require non-null controls; replace diagnostics only on success.
IBlockstoreConfigPtr MakeRuntimeBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& startupConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NStorage::TStorageConfigControlsPtr controls,
    NConfig::TRuntimeConfigDiagnostics& diagnostics,
    TBlockstoreConfigExtraParameters extraParameters = {});

}   // namespace NCloud::NBlockStore
