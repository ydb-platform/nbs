/*******************************************************************************

Blockstore configuration construction and source processing.

  * Startup and runtime factories retain the accepted sections of each
    publication.

  * Runtime updates follow field markers.

  * Callers build the runtime snapshot, then apply the update in this order.

    1. Apply the accepted RW values and feature rules to the shared
       TStorageConfig.
    2. Update the shared ICB defaults from the snapshot's StorageServiceConfig.
       Use compiled-in defaults for unset fields. Reset ICB overrides for RW
       parameters whose defaults changed.
    3. Publish the new snapshot.

*******************************************************************************/

#pragma once

#include "blockstore_config.h"

#include <cloud/blockstore/config/blockstore.pb.h>

#include <cloud/storage/core/libs/config/config_preparation_diagnostics.h>

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

// Remove fields reserved for static-only configuration and sections left empty.
NConfig::TConfigPreparationDiagnostics RemoveStaticOnlyBlockstoreFields(
    NProto::TBlockstoreConfig& config);

// Normalize dynamic overrides against static configuration before merging
// sources.
//
// staticConfig - Static configuration used as the normalization base.
// dynamicConfig - Dynamic overrides to modify in place.
NConfig::TConfigPreparationDiagnostics NormalizeDynamicBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    NProto::TBlockstoreConfig& dynamicConfig);

// Check whether startup may process PrivateDatabaseConfig.
//
// dynamicYamlConfigurationEnabled - Whether dynamic YAML configuration is
//      enabled in the static configuration.
// temporaryServer - Whether this is a temporary server instance.
bool IsPrivateDatabaseConfigEnabled(
    bool dynamicYamlConfigurationEnabled,
    bool temporaryServer);

// Merge dynamic overrides into a copy of the static configuration using
// protobuf rules and section-specific composition policies.
//
// staticConfig - static configuration used as the merge base.
// dynamicConfig - dynamic configuration prepared with
//      PrepareDynamicConfigSectionsUpdate(), then normalized with
//      NormalizeDynamicBlockstoreConfig() using the same staticConfig.
//      For preparation, use nullptr at startup or the last accepted dynamic
//      source at runtime.
NProto::TBlockstoreConfig MergeBlockstoreConfigSources(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig);

// Obtain extra parameters from an existing configuration.
TBlockstoreConfigExtraParameters GetBlockstoreConfigExtraParameters(
    const IBlockstoreConfig& currentConfig);

// Build a configuration.
//
// config - Complete proto after any required source merging and runtime
//      filtering; used as supplied.
// controls - Shared live ICB controls for Storage; non-null. Defaults and
//      operator overrides are left unchanged.
// extraParameters - parameters not stored in the configuration proto.
//
// Returns - A new, non-null configuration.
IBlockstoreConfigMutablePtr MakeBlockstoreConfig(
    const NProto::TBlockstoreConfig& config,
    NStorage::TStorageConfigControlsPtr controls,
    TBlockstoreConfigExtraParameters extraParameters);

// Build a configuration with a TStorageConfig copy.
//
// config - Complete proto for Features and all sections except Storage and
//      DiskAgent; used as supplied.
// storageConfig - Source of Storage values before ICB overrides and of shared
//      live ICB controls. Features come from config; ICB defaults and operator
//      overrides stay unchanged.
// diskAgentConfig - Source of the DiskAgent configuration and its
//      non-protobuf parameters
//
// Returns - A new, non-null configuration.
IBlockstoreConfigMutablePtr MakeBlockstoreConfig(
    const NProto::TBlockstoreConfig& config,
    const NStorage::TStorageConfig& storageConfig,
    const NStorage::TDiskAgentConfig& diskAgentConfig);

// Build a configuration with an existing TStorageConfig.
//
// config - Complete proto for the snapshot sections, including
//      StorageServiceConfig and FeaturesConfig.
// sharedStorageConfig - Non-null TStorageConfig shared between snapshots.
// extraParameters - parameters not stored in the configuration proto.
//
// Returns - A new, non-null snapshot.
IBlockstoreConfigMutablePtr MakeBlockstoreConfig(
    const NProto::TBlockstoreConfig& config,
    NStorage::TStorageConfigPtr sharedStorageConfig,
    TBlockstoreConfigExtraParameters extraParameters);

// Blockstore configuration built from static and normalized dynamic sources.
// The result owns the merged proto and independently constructed wrappers.
struct TBlockstoreConfigBuildResult
{
    // Merged proto before ICB overrides; owned by this result.
    NProto::TBlockstoreConfig Proto;

    // Wrappers built from Proto, sharing the supplied ICB controls; non-null.
    // Later changes to Proto do not change these wrappers.
    IBlockstoreConfigMutablePtr Config;

    // Storage configuration held by Config; non-null.
    NStorage::TStorageConfigMutablePtr StorageConfig;

    // Presence of any known top-level field in the normalized dynamic source.
    bool HasDynamicOverrides = false;

    // Static-only fields removed from the supplied dynamic source; owned by
    // this result and independent of the merged proto.
    NConfig::TConfigPreparationDiagnostics NormalizationDiagnostics;
};

// Build a startup configuration by merging static and dynamic sources.
// Accept startup values regardless of runtime-update markers.
//
// staticConfig - Static configuration used as the merge base.
// dynamicConfig - Dynamic configuration prepared with
//      PrepareDynamicConfigSectionsUpdate() using nullptr as the previous
//      source.
// controls - Shared live ICB controls for Storage; non-null. Defaults and
//      operator overrides are left unchanged.
// extraParameters - parameters not stored in the configuration proto.
//
// Returns - The configuration build result.
TBlockstoreConfigBuildResult MakeStartupBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NStorage::TStorageConfigControlsPtr controls,
    TBlockstoreConfigExtraParameters extraParameters);

// Merge configuration sources and enforce runtime-update markers.
//
// staticConfig - Static configuration used as the merge base and fallback
//      when dynamic overrides are removed.
// startupConfig - Complete configuration accepted at node startup. Keep it
//      unchanged across updates; use it to restore forbidden runtime changes.
// dynamicConfig - Dynamic configuration prepared with
//      PrepareDynamicConfigSectionsUpdate() using the last accepted dynamic
//      source, then normalized with NormalizeDynamicBlockstoreConfig() using
//      staticConfig.
// runtimeConfig - Separate output message for the effective runtime
//      configuration. Use its contents only after a successful return.
//
// Returns - A report of rejected runtime changes and their reasons. Rejections
//      do not fail preparation; permitted changes remain in runtimeConfig.
NConfig::TConfigPreparationDiagnostics PrepareRuntimeBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& startupConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NProto::TBlockstoreConfig& runtimeConfig);

// Build a runtime configuration and report ignored dynamic values and rejected
// runtime changes. Prefer StaticOnly for paths reported by both stages.
//
// staticConfig - Static configuration used as the merge base and fallback
//      when dynamic overrides are removed.
// startupConfig - Complete configuration accepted at node startup. Keep it
//      unchanged across updates; use it to restore forbidden runtime changes.
// dynamicConfig - Dynamic configuration prepared with
//      PrepareDynamicConfigSectionsUpdate() using the last accepted dynamic
//      source.
// sharedStorageConfig - Non-null TStorageConfig retained by the returned
//      snapshot.
// extraParameters - parameters not stored in the configuration proto.
// diagnostics - Combined report from normalization and runtime filtering.
//      Replaced when a configuration is returned; preserved if creation fails.
//
// Returns - A new, non-null configuration.
IBlockstoreConfigMutablePtr MakeRuntimeBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& startupConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NStorage::TStorageConfigPtr sharedStorageConfig,
    TBlockstoreConfigExtraParameters extraParameters,
    NConfig::TConfigPreparationDiagnostics& diagnostics);

}   // namespace NCloud::NBlockStore
