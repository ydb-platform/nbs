#include "lowlevel.h"

#include <cloud/filestore/libs/service/error.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <array>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <utime.h>

// FIXME
#if !defined(F_OFD_GETLK)
#define F_OFD_GETLK     36
#define F_OFD_SETLK     37
#endif

namespace NCloud::NFileStore::NLowLevel {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf Dot = ".";
constexpr TStringBuf Dotdot = "..";

////////////////////////////////////////////////////////////////////////////////

ui32 GetSystemErrorCode()
{
    int error = ErrnoToFileStoreError(LastSystemError());
    return MAKE_FILESTORE_ERROR(error);
}

TFileStat GetFileStat(struct stat fs)
{
    TFileStat st;
    st.Mode = fs.st_mode;
    st.NLinks = fs.st_nlink;
    st.Uid = fs.st_uid;
    st.Gid = fs.st_gid;
    st.Size = fs.st_size;
    st.ATime = fs.st_atime;
    st.MTime = fs.st_mtime;
    st.CTime = fs.st_ctime;
    st.INode = fs.st_ino;
    return st;
}

TVector<TString> SplitStrings(const char* buf, size_t len)
{
    TVector<TString> result;
    for (size_t i = 0; i < len; i += result.back().size() + 1) {
        result.push_back(buf + i);
    }

    Sort(result);
    return result;
}

timespec ToTimeSpec(TInstant ts)
{
    timespec spec;
    if (ts) {
        spec.tv_sec = ts.Seconds();
        spec.tv_nsec = ts.MicroSecondsOfSecond() * 1000;
    } else {
        spec.tv_sec = 0;
        spec.tv_nsec = UTIME_OMIT;
    }

    return spec;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFile Open(const TString& path, int flags, int mode)
{
    int fd = open(path.data(), flags, mode);
    Y_ENSURE_EX(fd != -1, TServiceError(GetSystemErrorCode())
        << "failed to open " << path.Quote()
        << ": " << LastSystemErrorText());

    return TFile(fd);
}

TFile Open(const TFile& handle, int flags, int mode)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    int fd = open(path, flags, mode);
    Y_ENSURE_EX(fd != -1, TServiceError(GetSystemErrorCode())
        << "failed to open: " << LastSystemErrorText());

    return TFile(fd);
}

TFile OpenAt(const TFile& handle, const TString& name, int flags, int mode)
{
    int fd = openat(handle.GetHandle(), name.data(), flags, mode);
    Y_ENSURE_EX(fd != -1, TServiceError(GetSystemErrorCode())
        << "failed to open " << name.Quote()
        << ": " << LastSystemErrorText());

    return TFile(fd);
}

void MkDirAt(const TFile& handle, const TString& name, int mode)
{
    int res = mkdirat(handle.GetHandle(), name.data(), mode);
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "failed to create dir: " << name.Quote()
        << ": " << LastSystemErrorText());
}

void MkSockAt(const TFile& handle, const TString& name, int mode)
{
    int res = mknodat(handle.GetHandle(), name.data(), mode | S_IFSOCK, dev_t{});
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "failed to create socket: " << name.Quote()
        << ": " << LastSystemErrorText());
}

void LinkAt(const TFile& node, const TFile& parent, const TString& name)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", node.GetHandle());

    int res = linkat(AT_FDCWD, path, parent.GetHandle(), name.data(), AT_SYMLINK_FOLLOW);
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "failed to create link: " << name.Quote()
        << ": " << LastSystemErrorText());
}

void SymLinkAt(const TString target, const TFile& handle, const TString& name)
{
    int res = symlinkat(target.data(), handle.GetHandle(), name.data());
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "failed to create symlink: " << name.Quote() << " -> " << target.Quote()
        << ": " << LastSystemErrorText());
}

void RenameAt(
    const TFile& handle,
    const TString& name,
    const TFile& newhandle,
    const TString& newname,
    unsigned int flags)
{
    // https://man7.org/linux/man-pages/man2/rename.2.html
    // https://lwn.net/Articles/655028/
    int res = syscall(
        SYS_renameat2,
        handle.GetHandle(),
        name.data(),
        newhandle.GetHandle(),
        newname.data(),
        flags);
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "failed to rename " << name.Quote() << " -> " << newname.Quote()
        << ": " << LastSystemErrorText());
}

void UnlinkAt(const TFile& handle, const TString& name, bool directory)
{
    int flags = directory ? AT_REMOVEDIR : 0;
    int res = unlinkat(handle.GetHandle(), name.data(), flags);
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "failed to remove " << (directory ? "file " : "dir ") << name.Quote()
        << ": " << LastSystemErrorText());
}

TFileStat Stat(const TFile& handle)
{
    struct stat fs = {};
    int res = fstat(handle.GetHandle(), &fs);
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "failed to stat: " << LastSystemErrorText());

    return GetFileStat(fs);
}

TFileStat StatAt(const TFile& handle, const TString& name)
{
    struct stat fs = {};
    int res = fstatat(handle.GetHandle(), name.data(), &fs, AT_SYMLINK_NOFOLLOW);
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "failed to stat " << name.Quote()
        << ": " << LastSystemErrorText());

    return GetFileStat(fs);
}

TVector<std::pair<TString, TFileStat>> ListDirAt(const TFile& handle, bool ignoreErrors)
{
    auto fd = openat(handle.GetHandle(), ".", O_RDONLY);
    Y_ENSURE_EX(fd != -1, TServiceError(GetSystemErrorCode())
        << "failed to open: " << LastSystemErrorText());

    auto dir = fdopendir(fd);
    if (!dir) {
        close(fd);
        ythrow TServiceError(GetSystemErrorCode())
            << "failed to list dir: "
            << LastSystemErrorText();
    }

    Y_DEFER {
        if (closedir(dir) != 0) {
            // best effort
            close(fd);
        }
    };

    TVector<std::pair<TString, TFileStat>> results;

    errno = 0;
    while (auto entry = readdir(dir)) {
        TString name(entry->d_name);
        if (name == Dot || name == Dotdot) {
            continue;
        }

        if (ignoreErrors) {
            try {
                auto stat = StatAt(handle, name);
                results.emplace_back(std::move(name), stat);
            } catch (const TServiceError& err) {
                errno = 0;
                continue;
            }
        } else {
            auto stat = StatAt(handle, name);
            results.emplace_back(std::move(name), stat);
        }
    }

    Y_ENSURE_EX(errno == 0, TServiceError(GetSystemErrorCode())
        << "failed to list: "
        << LastSystemErrorText());

    return results;
}

////////////////////////////////////////////////////////////////////////////////

void Access(const TFile& handle, int mode)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    int res = access(path, mode);
    if (res != 0) {
        ythrow TServiceError(GetSystemErrorCode())
            << "access failed: "
            << LastSystemErrorText();
    }
}

void Chmod(const TFile& handle, int mode)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    int res = chmod(path, mode);
    if (res != 0) {
        ythrow TServiceError(GetSystemErrorCode())
            << "chmod failed: "
            << LastSystemErrorText();
    }
}

void Chown(const TFile& handle, unsigned int uid, unsigned int gid)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    int res = chown(path, uid, gid);
    if (res != 0) {
        ythrow TServiceError(GetSystemErrorCode())
            << "chown failed: "
            << LastSystemErrorText();
    }
}

void Utimes(const TFile& handle, TInstant atime, TInstant mtime)
{
    timespec tv[2] = {ToTimeSpec(atime), ToTimeSpec(mtime)};

    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    int res = utimensat(AT_FDCWD, path, tv, 0);
    if (res != 0) {
        ythrow TServiceError(GetSystemErrorCode())
            << "utime failed: "
            << LastSystemErrorText();
    }
}

void Truncate(const TFile& handle, size_t size)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    int res = truncate(path, size);
    if (res != 0) {
        ythrow TServiceError(GetSystemErrorCode())
            << "truncate failed: "
            << LastSystemErrorText();
    }
}

void Allocate(const TFile& handle, int flags, off_t offset, off_t length)
{
    int res = fallocate(handle.GetHandle(), flags, offset, length);
    Y_ENSURE_EX(res != -1, TServiceError(GetSystemErrorCode())
        << "allocate failed: "
        << LastSystemErrorText());
}

TString ReadLink(const TFile& handle)
{
    TString link(PATH_MAX, '\0');

    int res = readlinkat(handle.GetHandle(), "", &link[0], link.size());
    if (res == -1) {
        ythrow TServiceError(GetSystemErrorCode())
            << "readlink failed: "
            << LastSystemErrorText();
    }

    if ((size_t)res == link.size()) {
        ythrow TServiceError(GetSystemErrorCode())
            << "readlink failed: name is too long";
    }

    link.resize(res);
    return link;
}

////////////////////////////////////////////////////////////////////////////////

TString GetXAttr(const TFile& handle, const TString& name)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    TVector<char> buf;

    while (true) {
        int res = getxattr(
            path,
            name.c_str(),
            nullptr,
            0);

        if (res < 0) {
            // TODO MAKE_FILESTORE_ERROR(NProto::E_FS_NOXATTR),
            ythrow TServiceError(E_NOT_FOUND)
                << "failed to get attribute (" << name.Quote() << "): "
                << LastSystemErrorText();
        }

        buf.resize(res + 1);

        res = getxattr(
            path,
            name.c_str(),
            buf.data(),
            buf.size());

        if (res < 0) {
            if (errno == ERANGE) {
                continue;
            }

            ythrow TServiceError(E_IO)
                << "failed to get attribute (" << name.Quote() << "): "
                << LastSystemErrorText();
        }

        buf.resize(res);
        return {buf.begin(), buf.end()};
    }
}

void SetXAttr(const TFile& handle, const TString& name, const TString& value)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    int res = setxattr(
        path,
        name.c_str(),
        value.c_str(),
        value.size(),
        0 /*create or replace*/);

    if (res != 0) {
        ythrow TServiceError(E_IO)
            << "failed to set attribute (" << name.Quote() << ", " << value.Quote() << "): "
            << LastSystemErrorText();
    }
}

void RemoveXAttr(const TFile& handle, const TString& name)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    int res = removexattr(path, name.c_str());
    if (res != 0) {
        ythrow TServiceError(E_IO)
            << "failed to remove attribute (" << name.Quote() << "): "
            << LastSystemErrorText();
    }
}

TVector<TString> ListXAttrs(const TFile& handle)
{
    char path[64] = {0};
    sprintf(path, "/proc/self/fd/%i", handle.GetHandle());

    TVector<char> buf;

    while (true) {
        int res = listxattr(
            path,
            nullptr,
            0);

        if (res < 0) {
            ythrow TServiceError(E_IO)
                << "failed to list attributes: "
                << LastSystemErrorText();
        }

        buf.resize(res + 1);

        res = listxattr(
            path,
            buf.data(),
            buf.size());

        if (res < 0) {
            if (errno == ERANGE) {
                continue;
            }

            ythrow TServiceError(E_IO)
                << "failed to list attributes: "
                << LastSystemErrorText();
        }

        if (res == 0) {
            return {}; // no attributes
        }

        buf.resize(res);
        return SplitStrings(buf.data(), buf.size());
    }
}

////////////////////////////////////////////////////////////////////////////////

bool AcquireLock(const TFile& handle, int offset, int len, bool shared)
{
    struct flock lck = {};
    lck.l_whence = SEEK_SET,
    lck.l_start = offset;
    lck.l_len = len;
    lck.l_type = shared ? F_RDLCK : F_WRLCK;

    int res = fcntl(handle.GetHandle(), F_OFD_SETLK, &lck);
    if (res != 0) {
        if (errno != EAGAIN) {
            ythrow TServiceError(GetSystemErrorCode())
                << "lock failed at (" << offset << ", " << len << "): "
                << LastSystemErrorText();
        }

        return false;
    }

    return true;
}

bool TestLock(const TFile& handle, int offset, int len, bool shared)
{
    struct flock lck = {};
    lck.l_whence = SEEK_SET,
    lck.l_start = offset;
    lck.l_len = len;
    lck.l_type = shared ? F_RDLCK : F_WRLCK;

    int res = fcntl(handle.GetHandle(), F_OFD_GETLK, &lck);
    if (res != 0) {
        ythrow TServiceError(GetSystemErrorCode())
            << "test lock failed at (" << offset << ", " << len << "): "
            << LastSystemErrorText();
    }

    return lck.l_type == F_UNLCK;
}

void ReleaseLock(const TFile& handle, int offset, int len)
{
    struct flock lck = {};
    lck.l_whence = SEEK_SET,
    lck.l_start = offset;
    lck.l_len = len;
    lck.l_type = F_UNLCK;

    int res = fcntl(handle.GetHandle(), F_OFD_SETLK, &lck);
    if (res != 0) {
        ythrow TServiceError(GetSystemErrorCode())
            << "unlock failed at (" << offset << ", " << len << "): "
            << LastSystemErrorText();
    }
}

bool Flock(const TFile& handle, int operation)
{
    int res = flock(handle.GetHandle(), operation);
    if (res != 0) {
        if (errno != EAGAIN) {
            ythrow TServiceError(GetSystemErrorCode())
                << "flock failed: " << LastSystemErrorText();
        }
        return false;
    }

    return true;
}

}   // namespace NCloud::NFileStore::NLowLevel
