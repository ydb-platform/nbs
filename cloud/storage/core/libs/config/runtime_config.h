/*******************************************************************************

Filtering of runtime protobuf configurations.

1. Filtering

   - Require AllowRuntimeUpdate=true along the whole field path. Restore
     forbidden values and presence from startup.
   - Treat repeated/map fields and oneofs with AllowRuntimeSwitch=true as
     whole values; ignore their inner markers.
   - For other oneofs (with AllowRuntimeSwitch!=true, except proto3 optional
     fields), keep the startup selection, including unset. On a rejected
     switch, restore the whole oneof from startup. Allow permitted field
     updates when the selection is unchanged.
   - Remove unknown fields recursively without diagnostics.
   - Return a TConfigPreparationDiagnostics report with rejected fields and
     rejection reasons.

2. Default values and required fields

   - Keep a field set even when its explicitly assigned value equals the
     protobuf default.
   - Keep the selected oneof message even when all its fields are unset.
   - Fill missing allowed required fields, including child messages, with
     protobuf defaults. Do not recreate removed optional messages.
   - Reject adding a message absent at startup if it or its required children
     have a required field without runtime update permission. Apply other
     permitted changes.
     Example:

         syntax = "proto2";

         message TParent
         {
             required uint32 Value = 1 [(NMarkers.AllowRuntimeUpdate) = true];
             required uint32 Frozen = 2 [(NMarkers.AllowRuntimeUpdate) = false];
         }

         message TConfig
         {
             optional TParent Parent = 1 [(NMarkers.AllowRuntimeUpdate) = true];
         }

         startup: {}
         runtime: { Parent: { Value: 10, Frozen: 20 } }
         result:  {}  (Parent rejected)

3. Usage (pseudocode)

    1. At startup; keep staticConfig and startupConfig unchanged afterwards.

        staticConfig = ReadStaticConfig();
        startupDynamicOverrides = RecvDynamicOverrides();

        // MergeStaticAndDynamic() returns a fresh static-config copy
        // with dynamic overrides applied using the caller's merge rules.
        startupConfig = MergeStaticAndDynamic(
            staticConfig, startupDynamicOverrides);

        // Use startup configuration
        Publish(startupConfig);

    2. At runtime

        // Receive the full current dynamic overrides (or {} on removal).
        runtimeDynamicOverrides = RecvDynamicOverrides();

        // Merge sources - static configuration and new dynamic overrides -
        // before filtering. Keep runtime separate from the unchanged static
        // and startup sources.
        runtimeCandidate = MergeStaticAndDynamic(
            staticConfig, runtimeDynamicOverrides);

        try:
            // Filter
            diagnostics = FilterRuntimeConfig(
                staticConfig, startupConfig, runtimeCandidate);
        catch:
            // Discard runtime on exceptions: it may be partly modified.
            // Keep the previous published configuration.
            ... (log, critical event, etc)
            return;

        // Use new runtime configuration
        Publish(runtimeCandidate, diagnostics);

    3. Add a schema validation unit test for every new configuration type
       used with this module.

        ValidateRuntimeConfigSchema(*TConfig::descriptor());

*******************************************************************************/

#pragma once

#include "config_preparation_diagnostics.h"

#include <google/protobuf/message.h>

namespace NCloud::NConfig {

// Reject inconsistent oneof markers outside compound parameter values.
// Reject messages that allow extensions, except inside repeated/map fields
// and oneofs with AllowRuntimeSwitch=true.
// Call this function in a test for every configuration type used with
// FilterRuntimeConfig(), passing the root descriptor. Throw for an invalid
// schema; FilterRuntimeConfig() does not perform this check.
void ValidateRuntimeConfigSchema(
    const google::protobuf::Descriptor& descriptor);

// Filter the runtime configuration in place and report rejected changes.
//
// Inputs:
// - staticConfig: the base configuration used to build runtimeConfig.
// - startupConfig: the complete configuration accepted at startup. Keep it
//   unchanged across runtime updates.
// - runtimeConfig: the complete requested runtime state, with merging and
//   absent overrides resolved by the caller before this call.
//
// Outputs:
// - runtimeConfig retains permitted changes and restores forbidden state from
//   startupConfig.
// - The return value maps rejected paths to reasons in IgnoredPaths.
//
// Pass runtimeConfig as a separate message from staticConfig and startupConfig
// (no aliasing); runtimeConfig is modified by this call.
// Require identical message descriptors, startupConfig.IsInitialized() == true,
// and a marker schema checked in tests with ValidateRuntimeConfigSchema().
// Unknown data in staticConfig and startupConfig is ignored.
// Throw on descriptor mismatch or missing required startup fields before
// modifying runtimeConfig. Other exceptions may leave it partly modified:
// discard it on failure. Both source configurations stay unchanged.
TConfigPreparationDiagnostics FilterRuntimeConfig(
    const google::protobuf::Message& staticConfig,
    const google::protobuf::Message& startupConfig,
    google::protobuf::Message& runtimeConfig);

}   // namespace NCloud::NConfig
