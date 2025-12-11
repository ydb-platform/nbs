#include "disjoint_range_map.h"

#include "alloc.h"

#include <util/generic/map.h>

#include <array>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct TDisjointRangeMap::TBinaryTreeMap
{
    struct TItem
    {
        ui32 Start;
        ui64 Mark;
    };

    using TData =
        TMap<ui32, TItem, TLess<ui32>, TStlAllocator>;   // End -> {Begin, Mark}
    TData Data;

    TBinaryTreeMap()
        : Data(GetAllocatorByTag(EAllocatorTag::DisjointRangeMap))
    {}

    void Mark(TBlockRange32 br, ui64 mark)
    {
        auto lo = Data.lower_bound(br.Start);
        auto hi = Data.upper_bound(br.End);

        // *----a----*
        //       *----b----*
        // means b.Mark > a.Mark

        if (lo == Data.end()) {
            // *--*      *--*
            //                 *--br--*
            //      *--*
            // just insert new range
            Data[br.End] = {br.Start, mark};
            return;
        }

        if (lo->second.Start < br.Start) {
            if (lo->second.Mark < mark) {
                if (lo->first <= br.End) {
                    // *--lo--*
                    //    *--br--*
                    Data[br.Start - 1] = lo->second;
                    Data.erase(lo++);
                    // *old_lo* *--br--*
                } else {
                    // *----lo----*
                    //   *--br--*
                    Y_DEBUG_ABORT_UNLESS(lo == hi);

                    Data[br.Start - 1] = lo->second;
                    Data[br.End] = {br.Start, mark};
                    lo->second.Start = br.End + 1;
                    // *lo* *--br--* *lo*
                    return;
                }
            } else {
                if (lo->first >= br.End) {
                    //    *--br--*
                    // *----lo----*
                    // drop this mark
                    return;
                } else {
                    //    *--br--*
                    // *--lo--*
                    br.Start = lo->first + 1;
                    ++lo;
                    // *--old_lo--* *--br--*
                }
            }
        }

        if (hi != Data.end() && hi->second.Start <= br.End) {
            Y_DEBUG_ABORT_UNLESS(hi->second.Start >= br.Start);

            if (hi->second.Mark < mark) {
                //    *--hi--*
                // *--br--*
                hi->second.Start = br.End + 1;
                // *--br--* *hi*
            } else {
                if (hi->second.Start == br.Start) {
                    // *--br--*
                    // *----hi----*
                    Y_DEBUG_ABORT_UNLESS(lo == hi);
                    // drop this mark
                    return;
                } else {
                    // *--br--*
                    //    *--hi--*
                    Y_DEBUG_ABORT_UNLESS(hi->second.Start > br.Start);
                    br.End = hi->second.Start - 1;
                    // *br* *--hi--*
                }
            }
        }

        //     *aaa*  *bbb*                  *eee*
        // *--------------------br--------------------*
        //                     *ccc*    *ddd*
        //
        // should turn into
        //
        // *--------br--------* *ccc* *br* *ddd* *---br---*

        ui32 start = br.Start;

        while (lo != hi) {
            if (lo->second.Mark < mark) {
                //   *--lo--*
                // *------br------*
                Data.erase(lo++);
                continue;
            }

            if (start != lo->second.Start) {
                // *---------br---------*
                //    *--*    *--lo--*
                //        ^
                //        |
                //      start
                Y_DEBUG_ABORT_UNLESS(start < lo->second.Start);
                Data[lo->second.Start - 1] = {start, mark};
            }

            start = lo->first + 1;

            ++lo;
        }

        if (start <= br.End) {
            Data[br.End] = {start, mark};
        }
    }

    void FindMarks(
        TBlockRange32 blockRange,
        ui64 maxMark,
        TVector<TDeletedBlock>* marks) const
    {
        auto it = Data.lower_bound(blockRange.Start);
        while (it != Data.end()) {
            const auto range =
                TBlockRange32::MakeClosedInterval(it->second.Start, it->first);
            if (!blockRange.Overlaps(range)) {
                break;
            }

            if (it->second.Mark <= maxMark) {
                const auto intersection = blockRange.Intersect(range);
                for (ui32 i = intersection.Start; i <= intersection.End; ++i) {
                    marks->push_back({i, it->second.Mark});
                }
            }

            ++it;
        }
    }

    void Visit(const TRangeVisitor& visitor) const
    {
        for (const auto& x: Data) {
            visitor(TBlockRange32::MakeClosedInterval(x.second.Start, x.first));
        }
    }

    void Clear()
    {
        Data.clear();
    }

    bool IsEmpty() const
    {
        return Data.empty();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDisjointRangeMap::TWideTreeMap
{
    struct TNode: TNonCopyable
    {
        union TValue {
            TNode* Node = nullptr;
            ui64 Mark;
        };

        IAllocator* Allocator;
        std::array<TValue, 256> Children;
        bool IsLeaf = false;

        ~TNode()
        {
            Clear();
        }

        void Clear()
        {
            if (!IsLeaf) {
                for (const auto x: Children) {
                    if (x.Node) {
                        DeleteNode(x.Node);
                    }
                }
            }

            Children.fill(TValue());
        }

        TNode* NewNode()
        {
            return NewImpl<TNode>(
                GetAllocatorByTag(EAllocatorTag::DisjointRangeMap));
        }

        void DeleteNode(TNode* node)
        {
            DeleteImpl(
                GetAllocatorByTag(EAllocatorTag::DisjointRangeMap),
                node);
        }
    };

    static_assert(sizeof(ui64) >= sizeof(TNode*));

    TNode Root;
    bool Empty = true;

    template <class TNodePtr>
    struct TIteratorBase
    {
        using TPath = std::array<ui32, 4>;

        TBlockRange32 Range;
        ui64 BlockIndex;
        TNodePtr Root;

        TIteratorBase(TBlockRange32 range, TNodePtr root)
            : Range(range)
            , BlockIndex(Range.Start)
            , Root(root)
        {}

        bool IsValid() const
        {
            return BlockIndex <= Range.End;
        }

        ui32 Index() const
        {
            return BlockIndex;
        }

        TPath Path() const
        {
            TPath path;
            path[0] = (BlockIndex & 0xFF000000) >> 24;
            path[1] = (BlockIndex & 0x00FF0000) >> 16;
            path[2] = (BlockIndex & 0x0000FF00) >> 8;
            path[3] = (BlockIndex & 0x000000FF);
            return path;
        }

        ui64 Get() const
        {
            auto path = Path();
            TNodePtr node = Root;
            for (ui32 i = 0; i < 3; ++i) {
                auto c = node->Children[path[i]];
                if (c.Node) {
                    node = c.Node;
                } else {
                    node = nullptr;
                    break;
                }
            }

            return node ? node->Children[path[3]].Mark : 0;
        }
    };

    struct TConstIterator: TIteratorBase<const TNode*>
    {
        TPath CurrentPath;
        ui32 CurrentDepth;
        const TNode* CurrentNode;

        TConstIterator(TBlockRange32 range, const TNode* root)
            : TIteratorBase<const TNode*>(range, root)
            , CurrentDepth(0)
            , CurrentNode(nullptr)
        {
            DoNext(true);
        }

        void Next()
        {
            if (!IsValid()) {
                return;
            }

            if (!Move()) {
                return;
            }

            DoNext(false);
        }

        const TNode* Node(const TPath& path, ui32 pos) const
        {
            const TNode* node = Root;
            for (ui32 i = 0; i < pos; ++i) {
                auto c = node->Children[path[i]];
                if (c.Node) {
                    node = c.Node;
                } else {
                    return nullptr;
                }
            }

            return node;
        }

        void DoNext(bool init)
        {
            if (init) {
                CurrentPath = Path();
                CurrentNode = Root;
            }

            while (true) {
                auto c = CurrentNode->Children[CurrentPath[CurrentDepth]];
                if (!CurrentNode->IsLeaf && c.Node) {
                    Y_DEBUG_ABORT_UNLESS(CurrentDepth < 3);
                    CurrentNode = c.Node;
                    ++CurrentDepth;
                } else if (CurrentNode->IsLeaf && c.Mark) {
                    BlockIndex = (CurrentPath[0] << 24) +
                                 (CurrentPath[1] << 16) +
                                 (CurrentPath[2] << 8) + CurrentPath[3];

                    return;
                } else {
                    if (!Move()) {
                        return;
                    }

                    if (init) {
                        for (ui32 j = CurrentDepth + 1; j < 4; ++j) {
                            CurrentPath[j] = 0;
                        }
                        init = false;
                    }
                }
            }
        }

        bool Move()
        {
            ++CurrentPath[CurrentDepth];
            while (CurrentPath[CurrentDepth] == 256) {
                if (CurrentDepth) {
                    CurrentPath[CurrentDepth] = 0;
                    --CurrentDepth;
                    ++CurrentPath[CurrentDepth];
                } else {
                    BlockIndex = 1ULL << 32;
                    return false;
                }
            }

            CurrentNode = Node(CurrentPath, CurrentDepth);
            Y_DEBUG_ABORT_UNLESS(CurrentNode);

            return true;
        }
    };

    struct TIterator: TIteratorBase<TNode*>
    {
        TIterator(TBlockRange32 range, TNode* root)
            : TIteratorBase<TNode*>(range, root)
        {}

        void Set(ui64 mark)
        {
            auto path = Path();
            TNode* node = Root;
            for (ui32 i = 0; i < 3; ++i) {
                auto c = node->Children[path[i]];
                if (c.Node) {
                    node = c.Node;
                } else {
                    auto* child = node->NewNode();
                    if (i == 2) {
                        child->IsLeaf = true;
                    }
                    node->Children[path[i]].Node = child;
                    node = child;
                }
            }

            Y_DEBUG_ABORT_UNLESS(node);
            Y_DEBUG_ABORT_UNLESS(node->IsLeaf);

            auto& value = node->Children[path[3]].Mark;
            value = Max(value, mark);
        }

        void Next()
        {
            if (!IsValid()) {
                return;
            }

            ++BlockIndex;
        }
    };

    void Mark(TBlockRange32 blockRange, ui64 mark)
    {
        TIterator it(blockRange, &Root);
        while (it.IsValid()) {
            it.Set(mark);
            it.Next();
        }

        Empty = false;
    }

    void FindMarks(
        TBlockRange32 blockRange,
        ui64 maxMark,
        TVector<TDeletedBlock>* marks) const
    {
        TConstIterator it(blockRange, &Root);
        while (it.IsValid()) {
            const auto m = it.Get();
            Y_DEBUG_ABORT_UNLESS(m);
            if (m <= maxMark) {
                marks->push_back({it.Index(), m});
            }
            it.Next();
        }
    }

    void Visit(const TRangeVisitor& visitor) const
    {
        TConstIterator it(TBlockRange32::Max(), &Root);

        if (!it.IsValid()) {
            return;
        }

        auto currentRange = TBlockRange32::MakeOneBlock(it.Index());
        it.Next();
        while (it.IsValid()) {
            if (it.Index() == currentRange.End + 1) {
                ++currentRange.End;
            } else {
                visitor(currentRange);
                currentRange = TBlockRange32::MakeOneBlock(it.Index());
            }

            it.Next();
        }

        visitor(currentRange);
    }

    void Clear()
    {
        Root.Clear();
        Empty = true;
    }

    bool IsEmpty() const
    {
        return Empty;
    }
};

////////////////////////////////////////////////////////////////////////////////

TDisjointRangeMap::TDisjointRangeMap(EOptimizationMode mode)
{
    switch (mode) {
        case EOptimizationMode::OptimizeForLongRanges: {
            BinaryTreeMap.reset(new TBinaryTreeMap());
            break;
        }

        case EOptimizationMode::OptimizeForShortRanges: {
            WideTreeMap.reset(new TWideTreeMap());
            break;
        }

        default:
            Y_ABORT_UNLESS(0);
    }
}

TDisjointRangeMap::~TDisjointRangeMap()
{}

void TDisjointRangeMap::Mark(TBlockRange32 blockRange, ui64 mark)
{
    if (BinaryTreeMap) {
        BinaryTreeMap->Mark(blockRange, mark);
    } else {
        WideTreeMap->Mark(blockRange, mark);
    }
}

void TDisjointRangeMap::FindMarks(
    TBlockRange32 blockRange,
    ui64 maxMark,
    TVector<TDeletedBlock>* marks) const
{
    if (BinaryTreeMap) {
        BinaryTreeMap->FindMarks(blockRange, maxMark, marks);
    } else {
        WideTreeMap->FindMarks(blockRange, maxMark, marks);
    }
}

void TDisjointRangeMap::Visit(const TRangeVisitor& visitor) const
{
    if (BinaryTreeMap) {
        BinaryTreeMap->Visit(visitor);
    } else {
        WideTreeMap->Visit(visitor);
    }
}

void TDisjointRangeMap::Clear()
{
    if (BinaryTreeMap) {
        BinaryTreeMap->Clear();
    } else {
        WideTreeMap->Clear();
    }
}

bool TDisjointRangeMap::IsEmpty() const
{
    if (BinaryTreeMap) {
        return BinaryTreeMap->IsEmpty();
    } else {
        return WideTreeMap->IsEmpty();
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
