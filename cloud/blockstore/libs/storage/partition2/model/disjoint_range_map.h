#pragma once

#include "public.h"

#include "block.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/vector.h>

#include <functional>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

using TRangeVisitor = std::function<void(const TBlockRange32& range)>;

////////////////////////////////////////////////////////////////////////////////

class TDisjointRangeMap
{
private:
    struct TBinaryTreeMap;
    std::unique_ptr<TBinaryTreeMap> BinaryTreeMap;

    struct TWideTreeMap;
    std::unique_ptr<TWideTreeMap> WideTreeMap;

public:
    TDisjointRangeMap(EOptimizationMode mode);
    ~TDisjointRangeMap();

    void Mark(TBlockRange32 blockRange, ui64 mark);
    void FindMarks(
        TBlockRange32 blockRange,
        ui64 maxMark,
        TVector<TDeletedBlock>* marks) const;
    void Visit(const TRangeVisitor& visitor) const;

    void Clear();
    bool IsEmpty() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
