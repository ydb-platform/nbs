#include "guarded_sglist.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TGuardedObject final
    : public IGuardedObject
{
private:
    TAtomic Refs = 1;

public:
    bool Acquire() override
    {
        while (true) {
            const intptr_t curValue = AtomicGet(Refs);

            if (!curValue) {
                return false;
            }

            if (AtomicCas(&Refs, curValue + 1, curValue)) {
                return true;
            }
        }
    }

    void Release() override
    {
        AtomicSub(Refs, 1);
    }

    void Close() override
    {
        while (true) {
            const intptr_t curValue = AtomicGet(Refs);

            if (!curValue) {
                return;
            }

            if (curValue != 1) {
                continue;
            }

            if (AtomicCas(&Refs, 0, 1)) {
                return;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGuardedSgList::TDependentGuardedObject final
    : public IGuardedObject
{
private:
    TGuardedObject GuardedObject;
    TIntrusivePtr<IGuardedObject> Dependee;

public:
    explicit TDependentGuardedObject(
            TIntrusivePtr<IGuardedObject> dependee)
        : Dependee(std::move(dependee))
    {}

    bool Acquire() override
    {
        if (GuardedObject.Acquire()) {
            if (Dependee->Acquire()) {
                return true;
            }

            GuardedObject.Release();
            GuardedObject.Close();
        }
        return false;
    }

    void Release() override
    {
        Dependee->Release();
        GuardedObject.Release();
    }

    void Close() override
    {
        GuardedObject.Close();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGuardedSgList::TUnionGuardedObject final
    : public IGuardedObject
{
private:
    TGuardedObject GuardedObject;
    TVector<TGuardedSgList> GuardedSgLists;

public:
    explicit TUnionGuardedObject(TVector<TGuardedSgList> guardedSgLists)
        : GuardedSgLists(std::move(guardedSgLists))
    {}

    bool Acquire() override
    {
        if (GuardedObject.Acquire()) {
            if (AcquireObjects()) {
                return true;
            }

            GuardedObject.Release();
            GuardedObject.Close();
        }
        return false;
    }

    void Release() override
    {
        for (auto& guardedSgList: GuardedSgLists) {
            guardedSgList.GuardedObject->Release();
        }
        GuardedObject.Release();
    }

    void Close() override
    {
        GuardedObject.Close();
    }

private:
    bool AcquireObjects()
    {
        for (size_t i = 0; i < GuardedSgLists.size(); ++i) {
            auto& guardedSgList = GuardedSgLists[i];

            if (!guardedSgList.GuardedObject->Acquire()) {
                // release acquired objects
                for (size_t j = 0; j < i; ++j) {
                    GuardedSgLists[j].GuardedObject->Release();
                }
                return false;
            }
        }
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TGuardedSgList::TGuard::TGuard(
        TIntrusivePtr<IGuardedObject> guardedObject,
        const TSgList& sglist)
    : Sglist(sglist)
{
    if (guardedObject->Acquire()) {
        GuardedObject = std::move(guardedObject);
    }
}

TGuardedSgList::TGuard::~TGuard()
{
    if (GuardedObject) {
        GuardedObject->Release();
    }
}

TGuardedSgList::TGuard::operator bool() const
{
    return GuardedObject != nullptr;
}

const TSgList& TGuardedSgList::TGuard::Get() const
{
    Y_VERIFY(GuardedObject);
    return Sglist;
}

////////////////////////////////////////////////////////////////////////////////

TGuardedSgList TGuardedSgList::CreateUnion(
    TVector<TGuardedSgList> guardedSgLists)
{
    if (guardedSgLists.empty()) {
        return TGuardedSgList();
    }

    if (guardedSgLists.size() == 1) {
        return std::move(guardedSgLists.front());
    }

    TSgList joinedSgList;
    size_t cap = 0;
    for (const auto& guardedSgList: guardedSgLists) {
        cap += guardedSgList.Sglist.size();
    }
    joinedSgList.reserve(cap);

    for (const auto& guardedSgList: guardedSgLists) {
        const auto& sgList = guardedSgList.Sglist;
        joinedSgList.insert(joinedSgList.end(), sgList.begin(), sgList.end());
    }

    return TGuardedSgList(
        MakeIntrusive<TUnionGuardedObject>(std::move(guardedSgLists)),
        std::move(joinedSgList));
}

TGuardedSgList TGuardedSgList::CreateUnion(
    TVector<TGuardedSgList> guardedSgLists,
    TSgList joinedSgList)
{
    if (guardedSgLists.empty()) {
        return TGuardedSgList(std::move(joinedSgList));
    }

    if (guardedSgLists.size() == 1) {
        auto& guardedSgList = guardedSgLists.front();
        guardedSgList.SetSgList(std::move(joinedSgList));
        return std::move(guardedSgList);
    }

    return TGuardedSgList(
        MakeIntrusive<TUnionGuardedObject>(std::move(guardedSgLists)),
        std::move(joinedSgList));
}

TGuardedSgList::TGuardedSgList()
    : GuardedObject(MakeIntrusive<TGuardedObject>())
{}

TGuardedSgList::TGuardedSgList(TSgList sglist)
    : GuardedObject(MakeIntrusive<TGuardedObject>())
    , Sglist(std::move(sglist))
{}

TGuardedSgList::TGuardedSgList(
        TIntrusivePtr<IGuardedObject> guardedObject,
        TSgList sglist)
    : GuardedObject(std::move(guardedObject))
    , Sglist(std::move(sglist))
{}

bool TGuardedSgList::Empty() const
{
    return Sglist.empty();
}

TGuardedSgList TGuardedSgList::CreateDepender() const
{
    Y_VERIFY(GuardedObject);
    return TGuardedSgList(
        MakeIntrusive<TDependentGuardedObject>(GuardedObject),
        Sglist);
}

TGuardedSgList TGuardedSgList::Create(TSgList sglist) const
{
    Y_VERIFY(GuardedObject);
    return TGuardedSgList(GuardedObject, std::move(sglist));
}

void TGuardedSgList::SetSgList(TSgList sglist)
{
    Y_VERIFY(GuardedObject);
    Sglist = std::move(sglist);
}

TGuardedSgList::TGuard TGuardedSgList::Acquire() const
{
    Y_VERIFY(GuardedObject);
    return TGuard(GuardedObject, Sglist);
}

void TGuardedSgList::Destroy()
{
    Y_VERIFY(GuardedObject);
    GuardedObject->Close();
}

}   // namespace NCloud::NBlockStore
