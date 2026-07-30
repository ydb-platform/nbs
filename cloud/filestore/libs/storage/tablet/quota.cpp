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
    auto it = QuotaById.find(quotaId);
    return it != QuotaById.end() ? &it->second : nullptr;
}

}   // namespace NCloud::NFileStore::NStorage
