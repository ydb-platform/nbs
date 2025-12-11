#include "block.h"

#include <util/digest/numeric.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TLegacyHasher: IBlockLocation2RangeIndex
{
    ui32 Calc(ui64 nodeId, ui32 blockIndex) const override
    {
        // 1. just ignore high bits of node index

        // 2. split nodes in groups
        ui32 nodeGroup = static_cast<ui32>(nodeId) / NodeGroupSize;

        // 3. split blocks in groups
        ui32 blockGroup = blockIndex / BlockGroupSize;

        // 4. shuffle around
        return IntHash(nodeGroup) ^ IntHash(blockGroup);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockLocalityHasher: IBlockLocation2RangeIndex
{
    ui32 Calc(ui64 nodeId, ui32 blockIndex) const override
    {
        // 1. just ignore high bits of node index

        // 2. split nodes in groups
        ui32 nodeGroup = static_cast<ui32>(nodeId) / NodeGroupSize;

        // 3. split blocks in groups
        ui32 blockGroup = blockIndex / BlockGroupSize;

        // 4. shuffle around
        return (0xFFFF0000 & IntHash(nodeGroup)) | (0xFFFF & blockGroup);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockLocation2RangeIndexPtr CreateRangeIdHasher(ui32 type)
{
    switch (type) {
        case 0:
            return std::make_shared<TLegacyHasher>();
        case 1:
            return std::make_shared<TBlockLocalityHasher>();
    }

    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage
