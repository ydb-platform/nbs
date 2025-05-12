#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/file.h>
#include <util/system/fstat.h>

namespace NCloud::NFileStore {
namespace NLowLevel {

////////////////////////////////////////////////////////////////////////////////

class UnixCredentialsGuard {
private:
    uid_t OriginalUid = -1;
    gid_t OriginalGid = -1;
    bool IsRestoreNeeded = false;

public:
    UnixCredentialsGuard(uid_t uid, gid_t gid);
    ~UnixCredentialsGuard();
};

////////////////////////////////////////////////////////////////////////////////

struct TFileId
{
    enum class EFileIdType
    {
        Lustre = 0x97,
        Weka = 0x27,
        VastNfs = 0x8,
    };

    struct file_handle FileHandle;
    union {
        struct Y_PACKED
        {
            ui64 Seq;
            ui32 Oid;
            ui32 Ver;
            ui64 ParentSeq;
            ui32 ParentOid;
            ui32 ParentVer;
        } LustreFid;
        struct Y_PACKED
        {
            ui64 Id;
            ui64 Context;
            ui64 ParentId;
            ui64 ParentContext;
        } WekaInodeId;
        struct Y_PACKED
        {
            uint32_t IdHigh32;
            uint32_t IdLow32;
            uint16_t FileType;
            uint16_t Unused;
            uint16_t ServerFhSize;
            uint64_t ServerId;
            uint64_t ServerView;
            uint16_t ServerUnused;
        } VastNfsInodeId;
        char Buffer[MAX_HANDLE_SZ] = {};
    };

    TFileId(const TFileHandle& handle);
    TFileId(const TFileId& fileId) = default;

    TFileHandle Open(const TFileHandle& mountHandle, int flags);
    TString ToString() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemStat
{
    i64 Type = 0;           // Type of filesystem
    i64 BlockSize = 0;      // Optimal transfer block size
    ui64 TotalBlocks = 0;   // Total data blocks in filesystem
    ui64 FreeBlocks = 0;    // Free blocks in filesystem
    ui64 AvailBlocks = 0;   // Free blocks available to unprivileged user
    ui64 TotalFiles = 0;    // Total file nodes in filesystem
    ui64 FreeFiles = 0;     // Free file nodes in filesystem
    i32 FsId[2] = {0, 0};   // Filesystem ID
    i64 MaxNameLen = 0;     // Maximum length of filenames
    i64 FragmentSize = 0;   // Fragment size
    i64 MountFlags = 0;     // Mount flags of filesystem
};

////////////////////////////////////////////////////////////////////////////////

TFileHandle Open(const TString& path, int flags, int mode);
TFileHandle Open(const TFileHandle& handle, int flags, int mode);
TFileHandle OpenAt(
    const TFileHandle& handle,
    const TString& name,
    int flags,
    int mode);

void MkDirAt(const TFileHandle& handle, const TString& name, int mode);
void MkSockAt(const TFileHandle& handle, const TString& name, int mode);

void RenameAt(
    const TFileHandle& handle,
    const TString& name,
    const TFileHandle& newhandle,
    const TString& newname,
    unsigned int flags);

void LinkAt(
    const TFileHandle& node,
    const TFileHandle& parent,
    const TString& name);
void SymLinkAt(
    const TString& target,
    const TFileHandle& handle,
    const TString& name);
void UnlinkAt(const TFileHandle& handle, const TString& name, bool directory);

TString ReadLink(const TFileHandle& handle);

TFileStat Stat(const TFileHandle& handle);
TFileStat StatAt(const TFileHandle& handle, const TString& name);
TFileSystemStat StatFs(const TFileHandle& handle);

TVector<std::pair<TString, TFileStat>> ListDirAt(
    const TFileHandle& handle,
    bool ignoreErrors);

//
// Attrs
//

void Access(const TFileHandle& handle, int mode);
void Chmod(const TFileHandle& handle, int mode);
void Chown(const TFileHandle& handle, unsigned int uid, unsigned int gid);
void Utimes(const TFileHandle& handle, TInstant atime, TInstant mtime);
void Truncate(const TFileHandle& handle, size_t size);
void Allocate(const TFileHandle& handle, int flags, off_t offset, off_t length);

//
// X Attrs
//

TString GetXAttr(const TFileHandle& handle, const TString& name);
TVector<TString> ListXAttrs(const TFileHandle& handle);
void RemoveXAttr(const TFileHandle& handle, const TString& name);
void SetXAttr(
    const TFileHandle& handle,
    const TString& name,
    const TString& value);

//
// Locks
//

bool AcquireLock(
    const TFileHandle& handle,
    off_t offset,
    off_t len,
    bool shared);
bool TestLock(const TFileHandle& handle, off_t offset, off_t len, bool shared);
void ReleaseLock(const TFileHandle& handle, off_t offset, off_t len);
bool Flock(const TFileHandle& handle, int operation);

}   // namespace NLowLevel
}   // namespace NCloud::NFileStore
