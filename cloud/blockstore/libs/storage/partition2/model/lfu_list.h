#pragma once

#include "alloc.h"

#include <util/generic/intrlist.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {
namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TLFUItemGroup: TIntrusiveListItem<TLFUItemGroup<T>>
{
    TVector<T*> Data;
    size_t UseCount = 1;
};

}   // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TLFUListItemBase
{
    NPrivate::TLFUItemGroup<T>* Group = nullptr;
    ui32 Pos = Max<ui32>();
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TLFUList
{
    using TGroup = NPrivate::TLFUItemGroup<T>;
    using TData = TIntrusiveList<TGroup>;

private:
    IAllocator* Allocator;
    TData Data;
    ui32 Count = 0;
    ui64 Weight = 0;

public:
    TLFUList(IAllocator* allocator)
        : Allocator(allocator)
    {}

    ~TLFUList()
    {
        auto it = Data.Begin();
        while (it != Data.End()) {
            auto* g = it->Node();
            it.Next();
            DeleteImpl(Allocator, g);
        }
    }

    ui32 GetCount() const
    {
        return Count;
    }

    ui64 GetWeight() const
    {
        return Weight;
    }

    void Add(T* t)
    {
        TGroup* group = nullptr;
        if (!Data.Empty()) {
            auto* last = Data.Back();
            if (last->UseCount == 1) {
                group = last;
            }
        }

        if (!group) {
            group = NewImpl<TGroup>(Allocator);
            Data.PushBack(group);
        }

        group->Data.push_back(t);
        t->Group = group;
        t->Pos = group->Data.size() - 1;

        ++Count;
        Weight += t->Weight();
    }

    void Remove(T* t)
    {
        DeleteFromGroup(t);
        --Count;
        Weight -= t->Weight();
    }

    void Use(T* t)
    {
        auto* group = t->Group;
        auto* prev = group->Prev();

        const auto useCount = group->UseCount;

        if (prev == Data.End().Item()) {
            if (group->Data.size() == 1) {
                ++group->UseCount;
            } else {
                prev = NewImpl<TGroup>(Allocator);
                prev->Node()->UseCount = useCount + 1;
                prev->LinkBefore(group);
                Transfer(t, prev->Node());
            }
        } else {
            if (prev->Node()->UseCount == useCount + 1) {
                Transfer(t, prev->Node());
            } else {
                if (group->Data.size() == 1) {
                    ++group->UseCount;
                } else {
                    auto* newGroup = NewImpl<TGroup>(Allocator);
                    newGroup->UseCount = useCount + 1;
                    newGroup->LinkAfter(prev);
                    Transfer(t, newGroup);
                }
            }
        }
    }

    T* Prune()
    {
        if (!Count) {
            Y_DEBUG_ABORT_UNLESS(0);
            return nullptr;
        }

        auto* last = Data.Back();
        auto* t = last->Data.back();
        DeleteFromGroup(t);
        --Count;
        Weight -= t->Weight();

        return t;
    }

private:
    void Transfer(T* t, TGroup* g)
    {
        DeleteFromGroup(t);

        g->Data.push_back(t);
        t->Pos = g->Data.size() - 1;
        t->Group = g;
    }

    void DeleteFromGroup(T* t)
    {
        auto* group = t->Group;

        if (group->Data.size() == 1) {
            group->Unlink();
            DeleteImpl(Allocator, group);
        } else {
            auto* last = group->Data.back();
            last->Pos = t->Pos;
            group->Data[last->Pos] = last;
            group->Data.resize(group->Data.size() - 1);
        }

        t->Group = nullptr;
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
