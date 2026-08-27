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

const NProto::TQuota& TIndexTabletState::SetQuota(
    IIndexTabletDatabase& db,
    ui32 quotaId,
    ui64 maxBytes,
    ui64 maxNodes,
    TInstant now)
{
    const auto* existing = Impl->Quotas.FindQuota(quotaId);

    NProto::TQuota quota;
    quota.SetQuotaId(quotaId);
    quota.SetMaxBytes(maxBytes);
    quota.SetMaxNodes(maxNodes);
    if (existing) {
        quota.SetCreationTimestampUs(existing->GetCreationTimestampUs());
        // TODO(6608): add a UT for NodeId preservation once AttachQuota
        // exists and can populate it
        *quota.MutableNodeId() = existing->GetNodeId();
    } else {
        quota.SetCreationTimestampUs(now.MicroSeconds());
    }

    db.WriteQuota(quota);
    Impl->Quotas.UpdateQuota(quota);

    return *Impl->Quotas.FindQuota(quotaId);
}

void TIndexTabletState::DeleteQuota(IIndexTabletDatabase& db, ui32 quotaId)
{
    db.DeleteQuota(quotaId);
    db.DeleteQuotaUsage(quotaId);
    Impl->Quotas.RemoveQuota(quotaId);
}

void TIndexTabletState::LoadQuotaUsages(const TVector<TQuotaUsage>& usages)
{
    for (const auto& usage: usages) {
        Impl->Quotas.LoadUsage(usage);
    }
}

const TQuotaUsage* TIndexTabletState::FindQuotaUsage(ui32 quotaId) const
{
    return Impl->Quotas.FindUsage(quotaId);
}

TVector<TQuotaUsage> TIndexTabletState::GetQuotaUsages() const
{
    return Impl->Quotas.GetUsages();
}

void TIndexTabletState::UpdateQuotaUsage(
    IIndexTabletDatabase& db,
    ui32 quotaId,
    i64 bytesDelta,
    i64 nodesDelta)
{
    if (quotaId == 0 || (bytesDelta == 0 && nodesDelta == 0)) {
        return;
    }

    if (!Impl->Quotas.FindQuota(quotaId)) {
        return;
    }

    const auto* usage =
        Impl->Quotas.UpdateUsage(quotaId, bytesDelta, nodesDelta);
    if (usage) {
        db.WriteQuotaUsage(quotaId, usage->UsedBytes, usage->UsedNodes);
    }
}

}   // namespace NCloud::NFileStore::NStorage
