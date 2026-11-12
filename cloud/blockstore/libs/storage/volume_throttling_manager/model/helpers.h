#pragma once

#include <cloud/blockstore/libs/storage/api/volume_throttling_manager.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Validate one rule's coefficients and report its original configuration index.
NProto::TError ValidateThrottlingRule(
    const NProto::TVolumeThrottlingRule& rule,
    int index);

NProto::TError ValidateThrottlingConfigRules(
    const google::protobuf::RepeatedPtrField<NProto::TVolumeThrottlingRule>& rules);

// Select and copy the first matching Disks rule, otherwise the first Filter;
// return an empty rule if neither matches.
NProto::TVolumeThrottlingRule GetThrottlingRuleForVolume(
    const google::protobuf::RepeatedPtrField<NProto::TVolumeThrottlingRule>& rules,
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    const NProto::EStorageMediaKind& mediaKind);

}   // namespace NCloud::NBlockStore::NStorage
