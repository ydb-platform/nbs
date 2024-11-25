#pragma once

#include <cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Helps to process the TEvGetChangedBlocksRequest message. It respond to
// request with an error.
class TGetChangedBlocksCompanion
{

public:
    TGetChangedBlocksCompanion() = default;

    void DeclineGetChangedBlocks(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
