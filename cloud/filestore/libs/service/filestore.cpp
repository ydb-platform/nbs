#include "filestore.h"

#include <util/generic/vector.h>
#include <util/string/builder.h>

// WARNING: DO NOT REPLACE THIS INCLUDE AFTER <fcntl.h>
#include <linux/falloc.h>
#include <fcntl.h>
#include <linux/fs.h>

#include <array>
#include <span>

namespace NCloud::NFileStore {

using namespace NProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TFlag2Proto = std::pair<int, ui32>;

////////////////////////////////////////////////////////////////////////////////

constexpr std::array SUPPORTED_HANDLE_FLAGS = {
    TFlag2Proto{O_CREAT,     TCreateHandleRequest::E_CREATE},
    TFlag2Proto{O_EXCL,      TCreateHandleRequest::E_EXCLUSIVE},
    TFlag2Proto{O_APPEND,    TCreateHandleRequest::E_APPEND},
    TFlag2Proto{O_TRUNC,     TCreateHandleRequest::E_TRUNCATE},
    TFlag2Proto{O_DIRECTORY, TCreateHandleRequest::E_DIRECTORY},
    TFlag2Proto{O_NOATIME,   TCreateHandleRequest::E_NOATIME},
    TFlag2Proto{O_NOFOLLOW,  TCreateHandleRequest::E_NOFOLLOW},
    TFlag2Proto{O_NONBLOCK,  TCreateHandleRequest::E_NONBLOCK},
    TFlag2Proto{O_PATH,      TCreateHandleRequest::E_PATH},
    TFlag2Proto{O_DIRECT,    TCreateHandleRequest::E_DIRECT},
};

constexpr std::array SUPPORTED_RENAME_FLAGS = {
    TFlag2Proto{RENAME_EXCHANGE,   TRenameNodeRequest::F_EXCHANGE},
    TFlag2Proto{RENAME_NOREPLACE,  TRenameNodeRequest::F_NOREPLACE},
};

constexpr std::array SUPPORTED_FALLOCATE_FLAGS = {
    TFlag2Proto{FALLOC_FL_KEEP_SIZE,      TAllocateDataRequest::F_KEEP_SIZE},
    TFlag2Proto{FALLOC_FL_PUNCH_HOLE,     TAllocateDataRequest::F_PUNCH_HOLE},
    TFlag2Proto{FALLOC_FL_COLLAPSE_RANGE, TAllocateDataRequest::F_COLLAPSE_RANGE},
    TFlag2Proto{FALLOC_FL_ZERO_RANGE,     TAllocateDataRequest::F_ZERO_RANGE},
    TFlag2Proto{FALLOC_FL_INSERT_RANGE,   TAllocateDataRequest::F_INSERT_RANGE},
    TFlag2Proto{FALLOC_FL_UNSHARE_RANGE,  TAllocateDataRequest::F_UNSHARE_RANGE},
};

////////////////////////////////////////////////////////////////////////////////

ui32 SystemFlagsToRequest(int flags, std::span<const TFlag2Proto> supportedFlags)
{
    ui32 value = 0;
    for (const auto& [flag, proto]: supportedFlags) {
        if (flag & flags) {
            value |= ProtoFlag(proto);
            flags &= ~flag;
        }
    }
    return value;
}

int RequestFlagsToSystem(ui32 flags, std::span<const TFlag2Proto> supportedFlags)
{
    int value = 0;
    for (const auto& [flag, proto]: supportedFlags) {
        if (HasFlag(flags, proto)) {
            value |= flag;
        }
    }
    return value;
}

template <typename TRequest>
TString RequestFlagsToString(ui32 flags)
{
    TStringBuilder ss;
    for (ui32 flag = TRequest::EFlags_MIN; flag < TRequest::EFlags_ARRAYSIZE; ++flag) {
        if (HasFlag(flags, flag)) {
            ss << TRequest::EFlags_Name(static_cast<typename TRequest::EFlags>(flag)) << "|";
        }
    }
    if (ss.EndsWith("|")) {
        ss.pop_back();
    }
    return std::move(ss);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::pair<ui32, int> SystemFlagsToHandle(int flags)
{
    const int mode = flags & O_ACCMODE;
    flags &= ~O_ACCMODE;

    ui32 value = SystemFlagsToRequest(flags, SUPPORTED_HANDLE_FLAGS);

    if (mode == O_RDWR) {
        value |= ProtoFlag(TCreateHandleRequest::E_READ) | ProtoFlag(TCreateHandleRequest::E_WRITE);
    } else if (mode == O_WRONLY) {
        value |= ProtoFlag(TCreateHandleRequest::E_WRITE);
    } else if (mode == O_RDONLY) {
        value |= ProtoFlag(TCreateHandleRequest::E_READ);
    }

    return {value, flags};
}

int HandleFlagsToSystem(ui32 flags)
{
    int value = RequestFlagsToSystem(flags, SUPPORTED_HANDLE_FLAGS);

    const bool read = HasFlag(flags, TCreateHandleRequest::E_READ);
    const bool write = HasFlag(flags, TCreateHandleRequest::E_WRITE);

    if (read && write) {
        value |= O_RDWR;
    } else if (write) {
        value |= O_WRONLY;
    } else if (read) {
        value |= O_RDONLY;
    }

    return value;
}

TString HandleFlagsToString(ui32 flags)
{
    return RequestFlagsToString<TCreateHandleRequest>(flags);

}

////////////////////////////////////////////////////////////////////////////////

ui32 SystemFlagsToRename(int flags)
{
    return SystemFlagsToRequest(flags, SUPPORTED_RENAME_FLAGS);
}

int RenameFlagsToSystem(ui32 flags)
{
    return RequestFlagsToSystem(flags, SUPPORTED_RENAME_FLAGS);
}

TString RenameFlagsToString(ui32 flags)
{
    return RequestFlagsToString<TRenameNodeRequest>(flags);
}

////////////////////////////////////////////////////////////////////////////////

ui32 SystemFlagsToFallocate(int flags)
{
    return SystemFlagsToRequest(flags, SUPPORTED_FALLOCATE_FLAGS);
}

int FallocateFlagsToSystem(ui32 flags)
{
    return RequestFlagsToSystem(flags, SUPPORTED_FALLOCATE_FLAGS);
}

TString FallocateFlagsToString(ui32 flags)
{
    return RequestFlagsToString<TAllocateDataRequest>(flags);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<NProto::ELockType> FcntlModesToLockType(int source)
{
    switch (source) {
        case F_RDLCK:
            return NProto::E_SHARED;
        case F_WRLCK:
            return NProto::E_EXCLUSIVE;
        case F_UNLCK:
            return NProto::E_UNLOCK;
        default:
            break;
    }
    return std::nullopt;
}

std::optional<NProto::ELockType> FlockModesToLockType(int source)
{
    switch (source) {
        case LOCK_SH:
            return NProto::E_SHARED;
        case LOCK_EX:
            return NProto::E_EXCLUSIVE;
        case LOCK_UN:
            return NProto::E_UNLOCK;
        default:
            break;
    }
    return std::nullopt;
}

i16 LockTypeToFcntlMode(NProto::ELockType source)
{
    switch (source) {
        case NProto::E_SHARED:
            return F_RDLCK;
        case NProto::E_EXCLUSIVE:
            return F_WRLCK;
        case NProto::E_UNLOCK:
            return F_UNLCK;
        default:
            break;
    }
    Y_ABORT("Unsupported NProto::ELockType's type field value %d", source);
}

i32 LockTypeToFlockMode(NProto::ELockType source)
{
    switch (source) {
        case NProto::E_SHARED:
            return LOCK_SH;
        case NProto::E_EXCLUSIVE:
            return LOCK_EX;
        case NProto::E_UNLOCK:
            return LOCK_UN;
        default:
            break;
    }
    Y_ABORT("Unsupported NProto::ELockType's type field value %d", source);
}

}   // namespace NCloud::NFileStore
