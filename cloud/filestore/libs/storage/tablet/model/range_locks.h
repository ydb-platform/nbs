#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ELockMode
{
    Shared = 0,
    Exclusive = 1,
    Unlock = 2,
};

////////////////////////////////////////////////////////////////////////////////

enum class ELockOrigin
{
    Flock = 0,
    Fcntl,
};

////////////////////////////////////////////////////////////////////////////////

struct TLockRange
{
    ui64 NodeId = -1;
    ui64 OwnerId = -1;
    ui64 Offset = 0;
    ui64 Length = 0;
    pid_t Pid = -1;
    ELockMode LockMode = ELockMode::Shared;
    ELockOrigin LockOrigin = ELockOrigin::Fcntl;
};

IOutputStream& operator<<(IOutputStream& out, const TLockRange& range);

////////////////////////////////////////////////////////////////////////////////

using TLockIncompatibleInfo = std::variant<TLockRange, ELockOrigin, ELockMode>;

struct TRangeLockOperationResult
{
    NProto::TError Error;
    std::variant<TLockIncompatibleInfo, TVector<ui64>> Value;

    explicit TRangeLockOperationResult(TVector<ui64> removedLockIds = {});
    explicit TRangeLockOperationResult(
        NProto::TError error,
        TLockIncompatibleInfo incompatible = {});
    static TRangeLockOperationResult MakeSucceeded();

    bool Succeeded() const;
    bool Failed() const;

    TVector<ui64>& RemovedLockIds();
    TLockIncompatibleInfo& Incompatible();

    template <typename T>
    bool IncompatibleHolds() const
    {
        return std::holds_alternative<T>(
            std::get<TLockIncompatibleInfo>(Value));
    }
    template <typename T>
    T& IncompatibleAs()
    {
        return std::get<T>(Incompatible());
    }
};

class TRangeLocks
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TRangeLocks();
    ~TRangeLocks();

    TRangeLockOperationResult
    Acquire(const TString& sessionId, ui64 lockId, const TLockRange& range);

    TRangeLockOperationResult Release(
        const TString& sessionId,
        const TLockRange& range);

    TRangeLockOperationResult Test(
        const TString& sessionId,
        const TLockRange& range) const;
};

}   // namespace NCloud::NFileStore::NStorage
