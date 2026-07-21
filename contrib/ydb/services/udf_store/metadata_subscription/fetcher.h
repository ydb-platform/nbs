#pragma once

#include "snapshot.h"
#include <contrib/ydb/services/metadata/abstract/common.h>
#include <contrib/ydb/library/accessor/accessor.h>

namespace NKikimr::NUdfStore {

class TSnapshotsFetcher: public NMetadata::NFetcher::TSnapshotsFetcher<TSnapshot> {
    virtual std::vector<NMetadata::IClassBehaviour::TPtr> DoGetManagers() const override {
        return  {
            TUdfMeta::GetBehaviour()
        };
    }
};

} // namespace NKikimr::NUdfStore
