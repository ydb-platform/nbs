#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TQuotaStore
{
private:
    THashMap<ui32, NProto::TQuota> QuotaById;

public:
    void UpdateQuota(const NProto::TQuota& quota);
    void RemoveQuota(ui32 quotaId);

    [[nodiscard]] const NProto::TQuota* FindQuota(ui32 quotaId) const;
};

}   // namespace NCloud::NFileStore::NStorage
