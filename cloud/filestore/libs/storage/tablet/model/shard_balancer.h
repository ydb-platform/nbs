#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TShardBalancer
{
private:
    TVector<TString> ShardIds;
    ui32 ShardSelector = 0;

public:
    void UpdateShards(TVector<TString> shardIds)
    {
        ShardIds = std::move(shardIds);
        ShardSelector = 0;
    }

    TString SelectShard(ui64 fileSize)
    {
        Y_UNUSED(fileSize);

        if (!ShardIds) {
            return {};
        }

        return ShardIds[ShardSelector++ % ShardIds.size()];
    }
};

}   // namespace NCloud::NFileStore::NStorage
