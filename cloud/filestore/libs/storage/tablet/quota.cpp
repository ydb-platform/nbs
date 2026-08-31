#include "quota.h"

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

inline void ClampedAdd(ui64& value, i64 delta)
{
    if (delta >= 0) {
        value += static_cast<ui64>(delta);
        return;
    }

    const ui64 decrement = static_cast<ui64>(-delta);
    value = Y_UNLIKELY(decrement > value) ? 0 : value - decrement;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TQuotaStore::UpdateQuota(const NProto::TQuota& quota)
{
    QuotaById[quota.GetQuotaId()] = quota;
}

void TQuotaStore::RemoveQuota(ui32 quotaId)
{
    QuotaById.erase(quotaId);
    UsageByQuotaId.erase(quotaId);
}

const NProto::TQuota* TQuotaStore::FindQuota(ui32 quotaId) const
{
    return QuotaById.FindPtr(quotaId);
}

TVector<NProto::TQuota> TQuotaStore::GetQuotas() const
{
    TVector<NProto::TQuota> quotas;
    quotas.reserve(QuotaById.size());
    for (const auto& [_, quota]: QuotaById) {
        quotas.push_back(quota);
    }
    // Make sure the quotas are returned in a deterministic order
    Sort(quotas.begin(), quotas.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.GetQuotaId() < rhs.GetQuotaId();
    });

    return quotas;
}

ui32 TQuotaStore::GetQuotaCount() const
{
    return static_cast<ui32>(QuotaById.size());
}

void TQuotaStore::LoadUsage(const TQuotaUsage& usage)
{
    UsageByQuotaId[usage.QuotaId] = usage;
}

const TQuotaUsage* TQuotaStore::UpdateUsage(
    ui32 quotaId,
    i64 bytesDelta,
    i64 nodesDelta)
{
    if (quotaId == 0) {
        return nullptr;
    }

    auto [it, inserted] = UsageByQuotaId.try_emplace(quotaId);
    if (inserted) {
        it->second.QuotaId = quotaId;
    }
    ClampedAdd(it->second.UsedBytes, bytesDelta);
    ClampedAdd(it->second.UsedNodes, nodesDelta);

    return &it->second;
}

const TQuotaUsage* TQuotaStore::FindUsage(ui32 quotaId) const
{
    return UsageByQuotaId.FindPtr(quotaId);
}

TVector<TQuotaUsage> TQuotaStore::GetUsages() const
{
    TVector<TQuotaUsage> usages;
    usages.reserve(UsageByQuotaId.size());
    for (const auto& [_, usage]: UsageByQuotaId) {
        usages.push_back(usage);
    }
    // Make sure the usages are returned in a deterministic order
    Sort(usages.begin(), usages.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.QuotaId < rhs.QuotaId;
    });

    return usages;
}

}   // namespace NCloud::NFileStore::NStorage
