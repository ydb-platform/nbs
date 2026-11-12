/*******************************************************************************

Service volume-throttling configuration composition and input validation.

*******************************************************************************/

#pragma once

#include "section_updater.h"

#include <cloud/blockstore/config/volume_throttling.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

// Return the shared stateless VolumeThrottling updater with process lifetime.
const IConfigSectionUpdater& GetVolumeThrottlingSectionUpdater();

// Ignore unknown fields and unsupported selectors; report descriptions of
// detected issues and leave config unchanged if a retained coefficient is
// invalid.
NCloud::NProto::TError PrepareStaticVolumeThrottlingConfig(
    NProto::TVolumeThrottlingServiceConfig& config,
    TVector<TString>& issues);

// Prepare dynamic rules, skipping empty Coefficients and retaining deletions;
// report descriptions of detected issues and leave config unchanged on invalid
// active rules.
NCloud::NProto::TError PrepareDynamicVolumeThrottlingConfig(
    NProto::TVolumeThrottlingServiceConfig& config,
    TVector<TString>& issues);

// Build merged config from static and dynamic rules.
// Put dynamic rules with coefficients before static rules not replaced or
// deleted by dynamic rules.
void MergeVolumeThrottlingConfig(
    const NProto::TVolumeThrottlingServiceConfig& staticConfig,
    const NProto::TVolumeThrottlingServiceConfig& dynamicConfig,
    NProto::TVolumeThrottlingServiceConfig* result);

}   // namespace NCloud::NBlockStore
