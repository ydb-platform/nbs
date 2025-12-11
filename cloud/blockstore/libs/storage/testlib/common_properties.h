#pragma once

#include "properties.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline auto WithPool(const TString& name)
{
    return TPipeableProperty{[=](auto& config) mutable
                             {
                                 config.SetPoolName(std::move(name));
                             }};
}

inline auto WithPool(const TString& name, auto kind)
{
    return TPipeableProperty{[=](auto& config) mutable
                             {
                                 config.SetPoolName(std::move(name));
                                 config.SetPoolKind(kind);
                             }};
}

inline auto WithPoolConfig(const TString& name, auto kind, ui64 allocationUnit)
{
    return TPipeableProperty{[=](auto& config) mutable
                             {
                                 auto* pool = config.AddDevicePoolConfigs();
                                 pool->SetName(std::move(name));
                                 pool->SetKind(kind);
                                 pool->SetAllocationUnit(allocationUnit);
                             }};
}

inline auto WithTotalSize(ui64 size, ui32 blockSize = 4096)
{
    return TPipeableProperty{[=](auto& config)
                             {
                                 config.SetBlocksCount(size / blockSize);
                                 config.SetBlockSize(blockSize);
                             }};
}

}   // namespace NCloud::NBlockStore::NStorage
