#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ELockMode
{
    Shared = 0,
    Exclusive = 1,
};

////////////////////////////////////////////////////////////////////////////////

struct TLockRange
{
    ui64 NodeId = -1;
    ui64 OwnerId = -1;
    ui64 Offset = 0;
    ui64 Length = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TRangeLocks
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TRangeLocks();
    ~TRangeLocks();

    bool Acquire(
        const TString& sessionId,
        ui64 lockId,
        TLockRange range,
        ELockMode mode,
        TVector<ui64>& removedLocks);

    void Release(
        const TString& sessionId,
        TLockRange range,
        TVector<ui64>& removedLocks);

    bool Test(
        const TString& sessionId,
        TLockRange range,
        ELockMode mode,
        TLockRange* conflicting) const;
};

}   // namespace NCloud::NFileStore::NStorage
