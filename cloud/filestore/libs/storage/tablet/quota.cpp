#include "quota.h"

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
    for (const auto& [quotaId, quota]: QuotaById) {
        quotas.push_back(quota);
    }

    return quotas;
}

ui32 TQuotaStore::GenerateQuotaId() const
{
    ui32 quotaId = 1;
    for (const auto& [id, quota]: QuotaById) {
        quotaId = Max(quotaId, id + 1);
    }

    return quotaId;
}

}   // namespace NCloud::NFileStore::NStorage
