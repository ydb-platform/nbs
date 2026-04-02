#pragma once

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct INodeStateHolderStats
{
    virtual ~INodeStateHolderStats() = default;

    virtual void IncrementNodeCount() = 0;
    virtual void DecrementNodeCount() = 0;
};

using INodeStateHolderStatsPtr = std::shared_ptr<INodeStateHolderStats>;

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
