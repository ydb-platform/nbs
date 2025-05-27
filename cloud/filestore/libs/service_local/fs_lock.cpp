#include "fs.h"

#include "lowlevel.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

NProto::TAcquireLockResponse TLocalFileSystem::AcquireLock(
    const NProto::TAcquireLockRequest& request)
{
    STORAGE_TRACE("AcquireLock " << DumpMessage(request));

    auto session = GetSession(request);
    auto handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return TErrorResponse(ErrorInvalidHandle(request.GetHandle()));
    }

    if (request.GetLockOrigin() == NProto::E_FLOCK) {
        auto lockType = LockTypeToFlockMode(request.GetLockType()) | LOCK_NB;
        if (!NLowLevel::Flock(*handle, lockType)) {
            return TErrorResponse(
                E_FS_WOULDBLOCK,
                TStringBuilder()
                    << "flock denied for " << static_cast<FHANDLE>(*handle));
        }
    } else {
        bool shared = request.GetLockType() == NProto::E_SHARED;
        bool wouldBlock = !NLowLevel::AcquireLock(
            *handle,
            request.GetOffset(),
            request.GetLength(),
            shared);
        if (wouldBlock) {
            return TErrorResponse(
                E_FS_WOULDBLOCK,
                TStringBuilder()
                    << "lock denied for " << static_cast<FHANDLE>(*handle)
                    << " at (" << request.GetOffset() << ", "
                    << request.GetLength() << ")");
        }
    }

    return {};
}

NProto::TReleaseLockResponse TLocalFileSystem::ReleaseLock(
    const NProto::TReleaseLockRequest& request)
{
    STORAGE_TRACE("ReleaseLock " << DumpMessage(request));

    auto session = GetSession(request);
    auto handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return TErrorResponse(ErrorInvalidHandle(request.GetHandle()));
    }

    if (request.GetLockOrigin() == NProto::E_FLOCK) {
        if (!NLowLevel::Flock(*handle, LOCK_UN)) {
            return TErrorResponse(
                E_FS_WOULDBLOCK,
                TStringBuilder()
                    << "flock denied for " << static_cast<FHANDLE>(*handle));
        }
    } else {
        NLowLevel::ReleaseLock(
            *handle,
            request.GetOffset(),
            request.GetLength());
    }

    return {};
}

NProto::TTestLockResponse TLocalFileSystem::TestLock(
    const NProto::TTestLockRequest& request)
{
    STORAGE_TRACE("TestLock " << DumpMessage(request));

    auto session = GetSession(request);
    auto handle = session->LookupHandle(request.GetHandle());
    if (!handle || !handle->IsOpen()) {
        return TErrorResponse(ErrorInvalidHandle(request.GetHandle()));
    }

    bool shared = request.GetLockType() == NProto::E_SHARED;
    const bool tested = NLowLevel::TestLock(
        *handle,
        request.GetOffset(),
        request.GetLength(),
        shared);
    if (!tested) {
        return TErrorResponse(E_FS_WOULDBLOCK, TStringBuilder()
            << "lock denied for " << static_cast<FHANDLE>(*handle)
            << " at (" << request.GetOffset()
            << ", " << request.GetLength() << ")");
    }

    return {};
}

}   // namespace NCloud::NFileStore
