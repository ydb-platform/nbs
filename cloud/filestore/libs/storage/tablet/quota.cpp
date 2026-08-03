#include "quota.h"

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TQuotaStore::UpdateQuota(const NProto::TQuota& quota)
{
    QuotaById[quota.GetQuotaId()] = quota;
}

void TQuotaStore::RemoveQuota(ui32 quotaId)
{
    QuotaById.erase(quotaId);
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

}   // namespace NCloud::NFileStore::NStorage
