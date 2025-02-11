#include "error.h"

#include <cloud/filestore/public/api/protos/const.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 QuoteLimit = 64;

static const TVector<std::pair<int, int>> SystemErrors = {
    {NProto::E_FS_ACCESS,       EACCES},
    {NProto::E_FS_NOENT,        ENOENT},
    {NProto::E_FS_INVAL,        EINVAL},
    {NProto::E_FS_NAMETOOLONG,  ENAMETOOLONG},
    {NProto::E_FS_EXIST,        EEXIST},
    {NProto::E_FS_NOTDIR,       ENOTDIR},
    {NProto::E_FS_ISDIR,        EISDIR},
    {NProto::E_FS_MLINK,        EMLINK},
    {NProto::E_FS_IO,           EIO},
    {NProto::E_FS_PERM,         EPERM},
    {NProto::E_FS_NXIO,         ENXIO},
    {NProto::E_FS_XDEV,         EXDEV},
    {NProto::E_FS_NODEV,        ENODEV},
    {NProto::E_FS_FBIG,         EFBIG},
    {NProto::E_FS_NOSPC,        ENOSPC},
    {NProto::E_FS_ROFS,         EROFS},
    {NProto::E_FS_NOTEMPTY,     ENOTEMPTY},
    {NProto::E_FS_DQUOT,        EDQUOT},
    {NProto::E_FS_STALE,        ESTALE},
    {NProto::E_FS_REMOTE,       EREMOTE},
    {NProto::E_FS_BADHANDLE,    EBADF},
    {NProto::E_FS_NOTSUPP,      EOPNOTSUPP},
    {NProto::E_FS_NOXATTR,      ENODATA},
    {NProto::E_FS_XATTR2BIG,    E2BIG},
    {NProto::E_FS_WOULDBLOCK,   EWOULDBLOCK},
    {NProto::E_FS_NOLCK,        ENOLCK},
    {NProto::E_FS_RANGE,        ERANGE},
};

TString MessageSafeStr(const TString& str)
{
    return str.substr(0, QuoteLimit).Quote();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError ErrorNotSupported(const TString& message)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NOTSUPP),
        message);
}

NProto::TError ErrorInvalidNodeType(ui64 nodeId)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NODEV),
        TStringBuilder()
            << "invalid node type #" << nodeId);
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ErrorInvalidParent(ui64 nodeId)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NOENT),
        TStringBuilder()
            << "invalid parent node #" << nodeId);
}

NProto::TError ErrorInvalidTarget(ui64 nodeId, const TString& name)
{
    TStringBuilder ss;
    ss << "invalid target node #" << nodeId;

    if (name) {
        ss << " " << name.Quote();
    }

    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NOENT),
        std::move(ss));
}

NProto::TError ErrorAlreadyExists(const TString& path)
{
    return MakeError(
        E_FS_EXIST,
        TStringBuilder()
            << "path " << path.Quote() << " already exists");
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ErrorIsDirectory(ui64 nodeId)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_ISDIR),
        TStringBuilder()
            << "directory node #" << nodeId);
}

NProto::TError ErrorIsNotDirectory(ui64 nodeId)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NOTDIR),
        TStringBuilder()
            << "not a directory node #" << nodeId);
}

NProto::TError ErrorIsNotEmpty(ui64 nodeId)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NOTEMPTY),
        TStringBuilder()
            << "not empty directory node #" << nodeId);
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ErrorNameTooLong(const TString& name)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NAMETOOLONG),
        TStringBuilder()
            << "name too long " << MessageSafeStr(name));
}

NProto::TError ErrorMaxLink(ui64 nodeId)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_MLINK),
        TStringBuilder()
            << "max link count for node #" << nodeId);
}

NProto::TError ErrorFileTooBig()
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_FBIG),
        TStringBuilder()
            << "file size too big");
}

NProto::TError ErrorNoSpaceLeft()
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NOSPC),
        TStringBuilder()
            << "no space left");
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ErrorInvalidArgument()
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_INVAL),
        TStringBuilder()
            << "invalid arguments specified");
}

NProto::TError ErrorInvalidHandle()
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_BADHANDLE),
        TStringBuilder()
            << "invalid handle");
}

NProto::TError ErrorInvalidHandle(ui64 handle)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_BADHANDLE),
        TStringBuilder()
            << "invalid handle #" << handle);
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ErrorAttributeDoesNotExist(const TString& name)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NOXATTR),
        TStringBuilder()
            << "attribute does not exist " << name.Quote());
}

NProto::TError ErrorAttributeAlreadyExists(const TString& name)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_EXIST),
        TStringBuilder()
            << "attribute already exists " << name.Quote());
}

NProto::TError ErrorAttributeNameTooLong(const TString& name)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_RANGE),
        TStringBuilder()
            << "attribute name " << MessageSafeStr(name) << "too big");
}

NProto::TError ErrorAttributeValueTooBig(const TString& name)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_XATTR2BIG),
        TStringBuilder()
            << "attribute " << MessageSafeStr(name) << " value too large");
}

NProto::TError ErrorInvalidAttribute(const TString& name)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NOTSUPP),
        TStringBuilder()
            << "invalid attribute name " << name.Quote());
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ErrorInvalidSession(
    const TString& clientId,
    const TString& sessionId,
    ui64 sessionSeqNo)
{
    return MakeError(E_FS_INVALID_SESSION, TStringBuilder()
        << "invalid session ("
        << "clientId: " << clientId.Quote()
        << ", sessionId: " << sessionId.Quote()
        << ", sessionSeqNo: " << sessionSeqNo
        << ")");
}

NProto::TError ErrorInvalidCheckpoint(const TString& checkpointId)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_NXIO),
        TStringBuilder()
            << "invalid checkpoint " << checkpointId.Quote());
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ErrorIncompatibleLocks()
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_WOULDBLOCK),
        TStringBuilder()
            << "incompatible locks");
}

NProto::TError ErrorIncompatibleLockType(int type)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_INVAL),
        TStringBuilder()
            << "incompatible lock type " << type);
}

NProto::TError ErrorIncompatibleLockWhence(int whence)
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_INVAL),
        TStringBuilder()
            << "incompatible whence " << whence);
}

NProto::TError ErrorIncompatibleLockOriginLocks()
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_ACCESS),
        TStringBuilder()
            << "incompatible lock origin (flock/fcntl)");
}

NProto::TError ErrorIncompatibleFileOpenMode()
{
    return MakeError(
        MAKE_FILESTORE_ERROR(NProto::E_FS_INVAL),
        TStringBuilder()
            << "incompatible file open mode");
}


NProto::TError ErrorDuplicate()
{
    return MakeError(
        E_REJECTED,
        TStringBuilder()
            << "non-idempotent request is being processed");
}

////////////////////////////////////////////////////////////////////////////////

int FileStoreErrorToErrno(int error)
{
    auto it = FindIf(
        SystemErrors.begin(),
        SystemErrors.end(),
        [&] (const auto& pair) {
            return pair.first == error;
        });
    return (it != SystemErrors.end()) ? it->second : EIO;
}

int ErrnoToFileStoreError(int error)
{
    auto it = FindIf(
        SystemErrors.begin(),
        SystemErrors.end(),
        [&] (const auto& pair) {
            return pair.second == error;
        });
    return (it != SystemErrors.end()) ? it->first : NProto::E_FS_IO;
}

}   // namespace NCloud::NFileStore
