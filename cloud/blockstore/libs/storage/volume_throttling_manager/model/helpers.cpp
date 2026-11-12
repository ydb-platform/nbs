#include "helpers.h"

#include <cloud/blockstore/libs/storage/api/volume_throttling_manager.h>

#include <util/generic/algorithm.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Find an invalid coefficient; return a null field name if all are valid.
std::pair<const char*, double> GetInvalidCoefficient(
    const NProto::TVolumeThrottlingRule& rule)
{
    if (!rule.HasCoefficients()) {
        return {};
    }

    const auto& coefficients = rule.GetCoefficients();
    if (coefficients.HasMaxReadBandwidth() &&
        !(coefficients.GetMaxReadBandwidth() >= 0.0 &&
          coefficients.GetMaxReadBandwidth() <= 1.0))
    {
        return {"MaxReadBandwidth", coefficients.GetMaxReadBandwidth()};
    }
    if (coefficients.HasMaxPostponedWeight() &&
        !(coefficients.GetMaxPostponedWeight() >= 0.0 &&
          coefficients.GetMaxPostponedWeight() <= 1.0))
    {
        return {"MaxPostponedWeight", coefficients.GetMaxPostponedWeight()};
    }
    if (coefficients.HasMaxReadIops() &&
        !(coefficients.GetMaxReadIops() >= 0.0 &&
          coefficients.GetMaxReadIops() <= 1.0))
    {
        return {"MaxReadIops", coefficients.GetMaxReadIops()};
    }
    if (coefficients.HasBoostTime() &&
        !(coefficients.GetBoostTime() >= 0.0 &&
          coefficients.GetBoostTime() <= 1.0))
    {
        return {"BoostTime", coefficients.GetBoostTime()};
    }
    if (coefficients.HasBoostRefillTime() &&
        !(coefficients.GetBoostRefillTime() >= 0.0 &&
          coefficients.GetBoostRefillTime() <= 1.0))
    {
        return {"BoostRefillTime", coefficients.GetBoostRefillTime()};
    }
    if (coefficients.HasBoostPercentage() &&
        !(coefficients.GetBoostPercentage() >= 0.0 &&
          coefficients.GetBoostPercentage() <= 1.0))
    {
        return {"BoostPercentage", coefficients.GetBoostPercentage()};
    }
    if (coefficients.HasMaxWriteBandwidth() &&
        !(coefficients.GetMaxWriteBandwidth() >= 0.0 &&
          coefficients.GetMaxWriteBandwidth() <= 1.0))
    {
        return {"MaxWriteBandwidth", coefficients.GetMaxWriteBandwidth()};
    }
    if (coefficients.HasMaxWriteIops() &&
        !(coefficients.GetMaxWriteIops() >= 0.0 &&
          coefficients.GetMaxWriteIops() <= 1.0))
    {
        return {"MaxWriteIops", coefficients.GetMaxWriteIops()};
    }
    if (coefficients.HasBurstPercentage() &&
        !(coefficients.GetBurstPercentage() >= 0.0 &&
          coefficients.GetBurstPercentage() <= 1.0))
    {
        return {"BurstPercentage", coefficients.GetBurstPercentage()};
    }
    return {};
}

// Match any listed disk ID; an empty list matches no disks.
bool SpecificDiskFilterApplies(
    const NProto::TSpecificDiskFilter& filter,
    const TString& diskId)
{
    const auto& diskIds = filter.GetDiskIds();
    return Find(diskIds, diskId) != diskIds.end();
}

// Match all nonempty filter lists, accepting any value within each list.
bool DiskFilterApplies(
    const NProto::TDiskFilter& filter,
    const TString& cloudId,
    const TString& folderId,
    const NProto::EStorageMediaKind& mediaKind)
{
    const auto& cloudIds = filter.GetCloudIds();
    if (!cloudIds.empty() && Find(cloudIds, cloudId) == cloudIds.end()) {
        return false;
    }

    const auto& folderIds = filter.GetFolderIds();
    if (!folderIds.empty() && Find(folderIds, folderId) == folderIds.end()) {
        return false;
    }

    const auto& mediaKinds = filter.GetMediaKinds();
    return mediaKinds.empty() ||
           Find(mediaKinds, mediaKind) != mediaKinds.end();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// Validate known coefficients without including selector values in errors.
NProto::TError ValidateThrottlingRule(
    const NProto::TVolumeThrottlingRule& rule,
    int index)
{
    const auto [name, value] = GetInvalidCoefficient(rule);
    if (name) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Rules[" << index << "].Coefficients." << name
                             << " = " << value
                             << "; expected a finite value in [0, 1]");
    }
    return {};
}

NProto::TError ValidateThrottlingConfigRules(
    const google::protobuf::RepeatedPtrField<NProto::TVolumeThrottlingRule>& rules)
{
    for (int i = 0; i < rules.size(); ++i) {
        if (auto error = ValidateThrottlingRule(rules[i], i); HasError(error)) {
            return error;
        }
    }
    return {};
}

// Select a rule without borrowing the input; keep the first Filter match
// while searching for a Disks match, which takes priority.
NProto::TVolumeThrottlingRule GetThrottlingRuleForVolume(
    const google::protobuf::RepeatedPtrField<NProto::TVolumeThrottlingRule>& rules,
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    const NProto::EStorageMediaKind& mediaKind)
{
    const NProto::TVolumeThrottlingRule* resultPtr = nullptr;

    for (const auto& rule: rules) {
        switch (rule.GetSelectorCase()) {
            case NProto::TVolumeThrottlingRule::kDisks:
                if (SpecificDiskFilterApplies(rule.GetDisks(), diskId)) {
                    return rule;
                }
                break;
            case NProto::TVolumeThrottlingRule::kFilter:
                if (!resultPtr &&
                    DiskFilterApplies(
                        rule.GetFilter(),
                        cloudId,
                        folderId,
                        mediaKind))
                {
                    // Pointer is used to avoid copying here
                    resultPtr = &rule;
                }
                break;
            default:
                break;
        }
    }

    return resultPtr ? *resultPtr : NProto::TVolumeThrottlingRule{};
}

}   // namespace NCloud::NBlockStore::NStorage
