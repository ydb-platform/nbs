/*******************************************************************************

Selector identity ignores list order and duplicates. Dynamic config rules are
composed with the original static config rules, preserving source order and
removing all static copies of replaced or deleted selectors before Volume
matching.

*******************************************************************************/

#include "volume_throttling.h"

#include <cloud/blockstore/libs/storage/volume_throttling_manager/model/helpers.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

namespace {

class TVolumeThrottlingSectionUpdater final: public IConfigSectionUpdater
{
public:
    [[nodiscard]] NCloud::NProto::TError Prepare(
        NProto::TBlockstoreConfig& dynamicConfigCandidate,
        const NProto::TBlockstoreConfig* previousAcceptedDynamicConfig,
        TVector<TString>& issues) const override;

    void Merge(
        const NProto::TBlockstoreConfig& staticConfig,
        const NProto::TBlockstoreConfig& dynamicConfig,
        NProto::TBlockstoreConfig& result) const override;
};

// Return a skip reason for unsupported selectors or empty dynamic coefficients.
TStringBuf GetSkippedRuleReason(
    const NProto::TVolumeThrottlingRule& rule,
    bool dynamic)
{
    if (rule.GetSelectorCase() ==
        NProto::TVolumeThrottlingRule::SELECTOR_NOT_SET)
    {
        return "no known selector (Filter or Disks)";
    }

    // Preserve empty static rules, which can select the original disk profile.
    if (dynamic && rule.HasCoefficients() &&
        !HasKnownFields(rule.GetCoefficients()))
    {
        return "Coefficients contains no known fields";
    }
    return {};
}

// Build a comparison key without changing the published selector's list order.
TString GetSelectorKey(const NProto::TVolumeThrottlingRule& rule)
{
    auto normalized = rule;
    normalized.ClearCoefficients();
    normalized.DiscardUnknownFields();
    if (normalized.HasDisks()) {
        SortUnique(*normalized.MutableDisks()->MutableDiskIds());
    } else if (normalized.HasFilter()) {
        auto* filter = normalized.MutableFilter();
        SortUnique(*filter->MutableCloudIds());
        SortUnique(*filter->MutableFolderIds());
        SortUnique(*filter->MutableMediaKinds());
    }
    return normalized.SerializeAsString();
}

// Prepare known rules without changing config on errors; retain original
// rule indices in reported issues and validation errors.
NCloud::NProto::TError PrepareVolumeThrottlingConfig(
    NProto::TVolumeThrottlingServiceConfig& config,
    bool dynamic,
    TVector<TString>& issues)
{
    // Select usable rules before validating coefficients or indexing selectors.
    issues.clear();
    auto prepared = config;
    prepared.ClearRules();
    THashMap<TString, int> selectors;
    for (int i = 0; i < config.GetRules().size(); ++i) {
        const auto& rule = config.GetRules(i);
        if (const auto reason = GetSkippedRuleReason(rule, dynamic)) {
            issues.push_back(
                TStringBuilder()
                << "Skip VolumeThrottling.Rules[" << i << "]: " << reason);
            continue;
        }
        if (auto error = NStorage::ValidateThrottlingRule(rule, i);
            HasError(error))
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Invalid volume throttling coefficients at "
                                 << "VolumeThrottling." << error.GetMessage());
        }
        if (dynamic) {
            if (auto [it, inserted] =
                    selectors.emplace(GetSelectorKey(rule), i);
                !inserted)
            {
                const auto* selector = rule.HasDisks() ? "Disks" : "Filter";
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "Duplicate volume throttling selector at "
                        << "VolumeThrottling.Rules[" << i << "]." << selector
                        << ": same selector as "
                        << "VolumeThrottling.Rules[" << it->second << "]."
                        << selector
                        << " (list order and duplicates are ignored)");
            }
        }
        prepared.AddRules()->CopyFrom(rule);
    }

    // Commit the known portion only after every retained operation is valid.
    prepared.DiscardUnknownFields();
    config.Swap(&prepared);
    return {};
}

// Preserve raw dynamic operations, including deletions, when restoring the
// accepted VolumeThrottling section.
NCloud::NProto::TError TVolumeThrottlingSectionUpdater::Prepare(
    NProto::TBlockstoreConfig& dynamicConfigCandidate,
    const NProto::TBlockstoreConfig* previousAcceptedDynamicConfig,
    TVector<TString>& issues) const
{
    issues.clear();
    if (!dynamicConfigCandidate.HasVolumeThrottling()) {
        return {};
    }

    // Validate the complete section before deciding whether to retain it.
    auto error = PrepareDynamicVolumeThrottlingConfig(
        *dynamicConfigCandidate.MutableVolumeThrottling(),
        issues);
    if (!HasError(error)) {
        return {};
    }

    // Restore the accepted dynamic source rather than its merged static rules.
    if (previousAcceptedDynamicConfig &&
        previousAcceptedDynamicConfig->HasVolumeThrottling())
    {
        dynamicConfigCandidate.MutableVolumeThrottling()->CopyFrom(
            previousAcceptedDynamicConfig->GetVolumeThrottling());
    } else {
        dynamicConfigCandidate.ClearVolumeThrottling();
    }
    return error;
}

// Apply the throttling overlay only when at least one source has the section.
void TVolumeThrottlingSectionUpdater::Merge(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NProto::TBlockstoreConfig& result) const
{
    if (staticConfig.HasVolumeThrottling() ||
        dynamicConfig.HasVolumeThrottling())
    {
        MergeVolumeThrottlingConfig(
            staticConfig.GetVolumeThrottling(),
            dynamicConfig.GetVolumeThrottling(),
            result.MutableVolumeThrottling());
    }
}

}   // namespace

NCloud::NProto::TError PrepareStaticVolumeThrottlingConfig(
    NProto::TVolumeThrottlingServiceConfig& config,
    TVector<TString>& issues)
{
    return PrepareVolumeThrottlingConfig(config, false, issues);
}

NCloud::NProto::TError PrepareDynamicVolumeThrottlingConfig(
    NProto::TVolumeThrottlingServiceConfig& config,
    TVector<TString>& issues)
{
    return PrepareVolumeThrottlingConfig(config, true, issues);
}

void MergeVolumeThrottlingConfig(
    const NProto::TVolumeThrottlingServiceConfig& staticConfig,
    const NProto::TVolumeThrottlingServiceConfig& dynamicConfig,
    NProto::TVolumeThrottlingServiceConfig* result)
{
    // Ignore unusable rules before their keys can displace static rules.
    THashSet<TString> dynamicSelectors;
    result->ClearRules();
    for (const auto& rule: dynamicConfig.GetRules()) {
        if (GetSkippedRuleReason(rule, true)) {
            continue;
        }
        dynamicSelectors.insert(GetSelectorKey(rule));
        if (rule.HasCoefficients()) {
            result->AddRules()->CopyFrom(rule);
        }
    }

    // Preserve static order while dropping all copies of overridden selectors.
    for (const auto& rule: staticConfig.GetRules()) {
        if (!GetSkippedRuleReason(rule, false) &&
            !dynamicSelectors.contains(GetSelectorKey(rule)))
        {
            result->AddRules()->CopyFrom(rule);
        }
    }
    result->DiscardUnknownFields();
}

const IConfigSectionUpdater& GetVolumeThrottlingSectionUpdater()
{
    static const TVolumeThrottlingSectionUpdater updater;
    return updater;
}

}   // namespace NCloud::NBlockStore
