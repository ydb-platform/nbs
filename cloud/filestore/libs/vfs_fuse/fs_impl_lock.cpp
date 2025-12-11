#include "fs_impl.h"

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct flock MakeCFlock(const NProto::TTestLockResponse& response)
{
    return {
        .l_type = LockTypeToFcntlMode(response.GetLockType()),
        .l_whence = SEEK_SET,
        .l_start = static_cast<off_t>(response.GetOffset()),
        .l_len = static_cast<off_t>(response.GetLength()),
        .l_pid = static_cast<pid_t>(response.GetPid()),
    };
}

template <typename T>
concept HasSetLockType = requires(T t) {
    {
        t.SetLockType(std::declval<NProto::ELockType>())
    } -> std::same_as<void>;
};

template <typename T>
void InitRequest(
    std::shared_ptr<T>& request,
    const TRangeLock& range,
    NProto::ELockOrigin origin)
{
    request->SetHandle(range.Handle);
    request->SetOwner(range.Owner);
    request->SetOffset(range.Offset);
    request->SetLength(range.Length);
    if constexpr (HasSetLockType<T>) {
        request->SetLockType(range.LockType);
    }
    request->SetPid(range.Pid);
    request->SetLockOrigin(origin);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////
// locking

void TFileSystem::HandleLock(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    const TRangeLock& range,
    NProto::ELockOrigin origin,
    bool sleep)
{
    switch (range.LockType) {
        case NProto::ELockType::E_SHARED:
        case NProto::ELockType::E_EXCLUSIVE: {
            AcquireLock(std::move(callContext), req, ino, range, sleep, origin);
            break;
        }
        case NProto::ELockType::E_UNLOCK: {
            ReleaseLock(std::move(callContext), req, ino, range, origin);
            break;
        }
        default: {
            ReplyError(
                *callContext,
                ErrorIncompatibleLockType(range.LockType),
                req,
                EINVAL);
            break;
        }
    }
}

void TFileSystem::GetLock(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi,
    struct flock* lock)
{
    if (lock->l_whence != SEEK_SET) {
        ReplyError(
            *callContext,
            ErrorIncompatibleLockWhence(lock->l_whence),
            req,
            EINVAL);
        return;
    }

    auto lockType = FcntlModesToLockType(lock->l_type);
    if (!lockType.has_value()) {
        ReplyError(
            *callContext,
            ErrorIncompatibleLockType(lock->l_type),
            req,
            EINVAL);
        return;
    }

    TRangeLock range{
        .Handle = fi->fh,
        .Owner = fi->lock_owner,
        .Pid = lock->l_pid,
        .Offset = lock->l_start,
        .Length = lock->l_len,
        .LockType = *lockType,
    };
    TestLock(callContext, req, ino, range);
}

void TFileSystem::SetLock(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi,
    struct flock* lock,
    bool sleep)
{
    if (lock->l_whence != SEEK_SET) {
        ReplyError(
            *callContext,
            ErrorIncompatibleLockWhence(lock->l_whence),
            req,
            EINVAL);
        return;
    }
    auto lockType = FcntlModesToLockType(lock->l_type);
    if (!lockType.has_value()) {
        ReplyError(
            *callContext,
            ErrorIncompatibleLockType(lock->l_type),
            req,
            EINVAL);
        return;
    }

    TRangeLock range{
        .Handle = fi->fh,
        .Owner = fi->lock_owner,
        .Pid = lock->l_pid,
        .Offset = lock->l_start,
        .Length = lock->l_len,
        .LockType = *lockType,
    };

    HandleLock(std::move(callContext), req, ino, range, NProto::E_FCNTL, sleep);
}

void TFileSystem::FLock(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi,
    int op)
{
    int realOp = op & (LOCK_EX | LOCK_SH | LOCK_UN);
    auto lockType = FlockModesToLockType(realOp);
    if (!lockType.has_value()) {
        ReplyError(
            *callContext,
            ErrorIncompatibleLockType(realOp),
            req,
            EINVAL);
        return;
    }

    TRangeLock range{
        .Handle = fi->fh,
        .Owner = fi->lock_owner,
        .Pid = -1,
        .Offset = 0,
        .Length = 0,
        .LockType = *lockType,
    };
    bool sleep = (op & LOCK_NB) == 0;
    HandleLock(std::move(callContext), req, ino, range, NProto::E_FLOCK, sleep);
}

////////////////////////////////////////////////////////////////////////////////

void TFileSystem::AcquireLock(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    const TRangeLock& range,
    bool sleep,
    NProto::ELockOrigin origin)
{
    STORAGE_DEBUG(
        "AcquireLock #" << ino << " @" << range.Handle << ", pid: " << range.Pid
                        << ", origin: " << NProto::ELockOrigin_Name(origin));

    auto request = StartRequest<NProto::TAcquireLockRequest>(ino);
    InitRequest(request, range, origin);

    Session->AcquireLock(callContext, std::move(request))
        .Subscribe(
            [=, ptr = weak_from_this()](const auto& future) mutable
            {
                auto self = ptr.lock();
                if (!self) {
                    return;
                }

                const auto& response = future.GetValue();
                const auto& error = response.GetError();
                if (SUCCEEDED(error.GetCode())) {
                    self->ReplyError(*callContext, error, req, 0);
                } else if (error.GetCode() == E_FS_WOULDBLOCK && sleep) {
                    if (callContext->Cancelled) {
                        STORAGE_DEBUG("Acquire lock cancelled");
                        // error code shouldn't really matter for the guest
                        // since if we are in FUSE_SUSPEND mode we won't respond
                        // to the guest anyway
                        CancelRequest(std::move(callContext), req);
                    } else {
                        // locks should be acquired synchronously if asked so
                        // retry attempt
                        self->ScheduleAcquireLock(
                            std::move(callContext),
                            error,
                            req,
                            ino,
                            range,
                            sleep,
                            origin);
                    }
                } else {
                    STORAGE_DEBUG(
                        "Failed to acquire lock: " << error.GetMessage());
                    self->ReplyError(
                        *callContext,
                        error,
                        req,
                        ErrnoFromError(error.GetCode()));
                }
            });
}

void TFileSystem::ScheduleAcquireLock(
    TCallContextPtr callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    fuse_ino_t ino,
    const TRangeLock& range,
    bool sleep,
    NProto::ELockOrigin origin)
{
    RequestStats->RequestCompleted(Log, *callContext, error);
    Scheduler->Schedule(
        Timer->Now() + Config->GetLockRetryTimeout(),
        [=, ptr = weak_from_this()]()
        {
            if (auto self = ptr.lock()) {
                self->RequestStats->RequestStarted(self->Log, *callContext);
                self->AcquireLock(callContext, req, ino, range, sleep, origin);
            }
        });
}

void TFileSystem::ReleaseLock(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    const TRangeLock& range,
    NProto::ELockOrigin origin)
{
    STORAGE_DEBUG(
        "ReleaseLock #" << ino << " @" << range.Handle << ", pid: " << range.Pid
                        << ", origin: " << NProto::ELockOrigin_Name(origin));

    auto request = StartRequest<NProto::TReleaseLockRequest>(ino);
    InitRequest(request, range, origin);

    Session->ReleaseLock(callContext, std::move(request))
        .Subscribe(
            [=, ptr = weak_from_this()](const auto& future)
            {
                const auto& response = future.GetValue();
                if (auto self = ptr.lock();
                    CheckResponse(self, *callContext, req, response))
                {
                    self->ReplyError(*callContext, response.GetError(), req, 0);
                }
            });
}

void TFileSystem::TestLock(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    const TRangeLock& range)
{
    STORAGE_DEBUG(
        "TestLock #" << ino << " @" << range.Handle << ", pid" << range.Pid);

    auto request = StartRequest<NProto::TTestLockRequest>(ino);
    InitRequest(request, range, NProto::E_FCNTL);

    Session->TestLock(callContext, std::move(request))
        .Subscribe(
            [=, ptr = weak_from_this()](const auto& future)
            {
                auto self = ptr.lock();
                if (!self) {
                    return;
                }

                const auto& response = future.GetValue();
                const auto& error = response.GetError();
                if (!HasError(error)) {
                    struct flock lock = {
                        .l_type = F_UNLCK,
                    };
                    self->ReplyLock(*callContext, error, req, &lock);
                } else if (error.GetCode() == E_FS_WOULDBLOCK) {
                    auto lock = MakeCFlock(response);
                    self->ReplyLock(*callContext, error, req, &lock);
                } else {
                    self->ReplyError(
                        *callContext,
                        error,
                        req,
                        ErrnoFromError(error.GetCode()));
                }
            });
}

}   // namespace NCloud::NFileStore::NFuse
