#include "tablet_state_impl.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletState::LoadQuotas(const TVector<NProto::TQuota>& quotas)
{
    for (const auto& quota: quotas) {
        Impl->Quotas.UpdateQuota(quota);
    }
}

TVector<NProto::TQuota> TIndexTabletState::GetQuotas() const
{
    return Impl->Quotas.GetQuotas();
}

const NProto::TQuota* TIndexTabletState::FindQuota(ui32 quotaId) const
{
    return Impl->Quotas.FindQuota(quotaId);
}

const NProto::TQuota& TIndexTabletState::CreateQuota(
    IIndexTabletDatabase& db,
    ui64 maxBytes,
    ui64 maxNodes,
    TInstant creationTimestamp)
{
    const ui32 quotaId = Impl->Quotas.GenerateQuotaId();

    NProto::TQuota quota;
    quota.SetQuotaId(quotaId);
    quota.SetMaxBytes(maxBytes);
    quota.SetMaxNodes(maxNodes);
    quota.SetCreationTimestampUs(creationTimestamp.MicroSeconds());

    db.WriteQuota(quota);
    Impl->Quotas.UpdateQuota(quota);

    return *Impl->Quotas.FindQuota(quotaId);
}

void TIndexTabletState::DeleteQuota(IIndexTabletDatabase& db, ui32 quotaId)
{
    db.DeleteQuota(quotaId);
    Impl->Quotas.RemoveQuota(quotaId);
}

}   // namespace NCloud::NFileStore::NStorage
