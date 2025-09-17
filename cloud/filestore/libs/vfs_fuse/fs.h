#pragma once

#include "public.h"

#include "fuse.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

struct stat;
struct statvfs;
struct flock;

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

#define FILESYSTEM_REPLY_METHOD(xxx, ...) \
    xxx(None,     __VA_ARGS__)            \
    xxx(Error,    __VA_ARGS__)            \
    xxx(Entry,    __VA_ARGS__)            \
    xxx(Create,   __VA_ARGS__)            \
    xxx(Attr,     __VA_ARGS__)            \
    xxx(ReadLink, __VA_ARGS__)            \
    xxx(Open,     __VA_ARGS__)            \
    xxx(Write,    __VA_ARGS__)            \
    xxx(Buf,      __VA_ARGS__)            \
    xxx(StatFs,   __VA_ARGS__)            \
    xxx(XAttr,    __VA_ARGS__)            \
    xxx(Lock,     __VA_ARGS__)            \

// FILESYSTEM_REPLY_METHOD

////////////////////////////////////////////////////////////////////////////////

int ReplyNone(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req);
int ReplyError(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    int errorCode);
int ReplyEntry(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const fuse_entry_param *e);
int ReplyCreate(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const fuse_entry_param *e,
    const fuse_file_info *fi);
int ReplyAttr(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const struct stat *attr,
    double attr_timeout);
int ReplyReadLink(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const char *link);
int ReplyOpen(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const fuse_file_info *fi);
int ReplyWrite(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    size_t count);
int ReplyBuf(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const char *buf,
    size_t size);
int ReplyStatFs(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const struct statvfs *stbuf);
int ReplyXAttr(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    size_t count);
int ReplyLock(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const struct flock *lock);
void CancelRequest(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    fuse_req_t req);

////////////////////////////////////////////////////////////////////////////////

struct ICompletionQueue
    : public virtual TThrRefBase
{
    using TCompletionCallback = std::function<int(fuse_req_t)>;

    virtual int Complete(
        fuse_req_t req,
        TCompletionCallback cb) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IFileSystem
{
    virtual ~IFileSystem() = default;

    virtual void Reset() = 0;

    virtual void Init() = 0;

    //
    // FileSystem information
    //

    // Get file system statistics
    virtual void StatFs(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino) = 0;

    //
    // Nodes
    //

    // Look up a directory entry by name and get its attributes
    virtual void Lookup(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name) = 0;

    // Forget about an inode
    virtual void Forget(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        unsigned long nlookup) = 0;

    // Forget about multiple inodes
    virtual void ForgetMulti(
        TCallContextPtr callContext,
        fuse_req_t req,
        size_t count,
        fuse_forget_data* forgets) = 0;

    // Create a directory
    virtual void MkDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name,
        mode_t mode) = 0;

    // Remove a directory
    virtual void RmDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name) = 0;

    // Create a file node
    virtual void MkNode(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name,
        mode_t mode,
        dev_t rdev) = 0;

    // Remove a file
    virtual void Unlink(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name) = 0;

    // Rename a file
    virtual void Rename(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name,
        fuse_ino_t newparent,
        TString newname,
        int flags) = 0;

    // Create a symbolic link
    virtual void SymLink(
        TCallContextPtr callContext,
        fuse_req_t req,
        TString link,
        fuse_ino_t parent,
        TString name) = 0;

    // Create a hard link to a file
    virtual void Link(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_ino_t newparent,
        TString name) = 0;

    // Read the target of a symbolic link
    virtual void ReadLink(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino) = 0;

    //
    // Node attributes
    //

    // Set file attributes
    virtual void SetAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        struct stat* attr,
        int to_set,
        fuse_file_info* fi) = 0;

    // Get file attributes
    virtual void GetAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) = 0;

    // Check file access permissions
    virtual void Access(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        int mask) = 0;

    //
    // Extended node attributes
    //

    // Set extended attributes
    virtual void SetXAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        TString name,
        TString value,
        int flags) = 0;

    // Get extended attributes
    virtual void GetXAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        TString name,
        size_t size) = 0;

    // List extended attributes
    virtual void ListXAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size) = 0;

    // Remove extended attributes
    virtual void RemoveXAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        TString name) = 0;

    //
    // Directory listing
    //

    // Open directory
    virtual void OpenDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) = 0;

    // Read directory
    virtual void ReadDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size,
        off_t offset,
        fuse_file_info* fi) = 0;

    // Release directory
    virtual void ReleaseDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) = 0;

    //
    // Read & write files
    //

    // Create and open a file
    virtual void Create(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name,
        mode_t mode,
        fuse_file_info* fi) = 0;

    // File open operation
    virtual void Open(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) = 0;

    // Read data from an open file
    virtual void Read(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size,
        off_t offset,
        fuse_file_info* fi) = 0;

    // Write data to an open file
    virtual void Write(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        TStringBuf buffer,
        off_t offset,
        fuse_file_info* fi) = 0;

    // Write contents of buffer to an open file
    virtual void WriteBuf(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_bufvec* bufv,
        off_t offset,
        fuse_file_info* fi) = 0;

    // Allocates space for an open file
    virtual void FAllocate(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        int mode,
        off_t offset,
        off_t length,
        fuse_file_info* fi) = 0;

    // Possibly flush cached data
    virtual void Flush(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) = 0;

    // Synchronize file contents
    virtual void FSync(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        int datasync,
        fuse_file_info* fi) = 0;

    // Synchronize directory contents
    virtual void FSyncDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        int datasync,
        fuse_file_info* fi) = 0;

    // Release an open file
    virtual void Release(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) = 0;

    //
    // Locking
    //

    // Test for a POSIX file lock
    virtual void GetLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi,
        struct flock* lock) = 0;

    // Acquire, modify or release a POSIX file lock
    virtual void SetLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi,
        struct flock* lock,
        bool sleep) = 0;

    // Acquire, modify or release a BSD file lock
    virtual void FLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi,
        int op) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemPtr CreateFileSystem(
    ILoggingServicePtr logging,
    IProfileLogPtr profileLog,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    TFileSystemConfigPtr config,
    IFileStorePtr session,
    IRequestStatsPtr stats,
    ICompletionQueuePtr queue,
    THandleOpsQueuePtr handleOpsQueue,
    TDirectoryHandlesStoragePtr directoryHandlesStorage,
    TWriteBackCache writeBackCache);

}   // namespace NCloud::NFileStore::NFuse
