#pragma once

#include "public.h"

#include "sglist.h"

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IGuardedObject
    : public TAtomicRefCount<IGuardedObject>
    , private TMoveOnly
{
    virtual ~IGuardedObject() = default;

    virtual bool Acquire() = 0;
    virtual void Release() = 0;

    virtual void Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TGuardedSgList final
{
private:
    class TDependentGuardedObject;
    class TUnionGuardedObject;

    TIntrusivePtr<IGuardedObject> GuardedObject;
    TSgList Sglist;

public:
    static TGuardedSgList CreateUnion(TVector<TGuardedSgList> sgLists);

    static TGuardedSgList CreateUnion(
        TVector<TGuardedSgList> guardedObjects,
        TSgList joinedSglist);

    TGuardedSgList(TIntrusivePtr<IGuardedObject> guarded, TSgList sglist);

    TGuardedSgList();
    explicit TGuardedSgList(TSgList sglist);

    bool Empty() const;

    TGuardedSgList CreateDepender() const;

    TGuardedSgList Create(TSgList sglist) const;

    void SetSgList(TSgList sglist);

    class TGuard
    {
    private:
        TIntrusivePtr<IGuardedObject> GuardedObject;
        const TSgList& Sglist;

    public:
        TGuard(TIntrusivePtr<IGuardedObject> guarded, const TSgList& sglist);
        TGuard(TGuard&&) = default;
        TGuard& operator=(TGuard&&) = delete;
        ~TGuard();

        operator bool() const;
        const TSgList& Get() const;
    };

    TGuard Acquire() const;
    void Destroy();
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TGuardedBuffer
{
private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

public:
    TGuardedBuffer() = default;
    explicit TGuardedBuffer(T buffer);

    const T& Get() const;
    T Extract();

    TGuardedSgList GetGuardedSgList() const;
    TGuardedSgList CreateGuardedSgList(TSgList sglist) const;
};

}   // namespace NCloud::NBlockStore

#define BLOCKSTORE_INCLUDE_GUARDED_SGLIST_INL
#include "guarded_sglist_inl.h"
#undef BLOCKSTORE_INCLUDE_GUARDED_SGLIST_INL
