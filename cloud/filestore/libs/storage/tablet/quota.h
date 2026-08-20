#pragma once

#include "public.h"

#include "model/quota.h"

#include <cloud/filestore/public/api/protos/quota.pb.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TQuotaStore
{
private:
    THashMap<ui32, NProto::TQuota> QuotaById;
    THashMap<ui32, TQuotaUsage> UsageByQuotaId;

public:
    void UpdateQuota(const NProto::TQuota& quota);
    void RemoveQuota(ui32 quotaId);

    [[nodiscard]] const NProto::TQuota* FindQuota(ui32 quotaId) const;
    [[nodiscard]] TVector<NProto::TQuota> GetQuotas() const;

    void LoadUsage(const TQuotaUsage& usage);

    // Returns the updated usage, or nullptr for quotaId == 0 (a no-op).
    const TQuotaUsage* UpdateUsage(
        ui32 quotaId,
        i64 bytesDelta,
        i64 nodesDelta);

    [[nodiscard]] const TQuotaUsage* FindUsage(ui32 quotaId) const;
    [[nodiscard]] TVector<TQuotaUsage> GetUsages() const;
};

}   // namespace NCloud::NFileStore::NStorage
