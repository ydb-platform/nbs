#include "convert.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/filestore/public/api/protos/fs.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/libs/linux-headers/linux/fuse.h>

#include <errno.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

namespace NCloud::NFileStore::NVFS {

////////////////////////////////////////////////////////////////////////////////

int ErrnoFromError(ui32 code)
{
    if (FACILITY_FROM_CODE(code) == FACILITY_SYSTEM) {
        return STATUS_FROM_CODE(code);
    }

    if (FACILITY_FROM_CODE(code) == FACILITY_FILESTORE) {
        return FileStoreErrorToErrno(STATUS_FROM_CODE(code));
    }

    return EIO;
}

void ConvertAttr(ui32 blockSize, const NProto::TNodeAttr& attr, struct stat& st)
{
    Zero(st);

    st.st_ino = attr.GetId();

    st.st_mode = attr.GetMode() & ~S_IFMT;
    switch (attr.GetType()) {
        case NProto::E_DIRECTORY_NODE:
            st.st_mode |= S_IFDIR;
            break;
        case NProto::E_LINK_NODE:
            st.st_mode |= S_IFLNK;
            break;
        case NProto::E_REGULAR_NODE:
            st.st_mode |= S_IFREG;
            break;
        case NProto::E_SOCK_NODE:
            st.st_mode |= S_IFSOCK;
            break;
    }

    st.st_blksize = blockSize;
    st.st_uid = attr.GetUid();
    st.st_gid = attr.GetGid();
    st.st_size = attr.GetSize();
    // FIXME: number of actually allocated 512 blocks
    st.st_blocks = AlignUp<ui64>(st.st_size, 512) / 512;
    st.st_nlink = attr.GetLinks();
    st.st_atim = ConvertTimeSpec(TInstant::MicroSeconds(attr.GetATime()));
    st.st_mtim = ConvertTimeSpec(TInstant::MicroSeconds(attr.GetMTime()));
    st.st_ctim = ConvertTimeSpec(TInstant::MicroSeconds(attr.GetCTime()));
}

void ConvertAttr(ui32 blockSize, const NProto::TNodeAttr& attr, struct fuse_attr& st)
{
    Zero(st);

    st.ino = attr.GetId();

    st.mode = attr.GetMode() & ~S_IFMT;
    switch (attr.GetType()) {
        case NProto::E_DIRECTORY_NODE:
            st.mode |= S_IFDIR;
            break;
        case NProto::E_LINK_NODE:
            st.mode |= S_IFLNK;
            break;
        case NProto::E_REGULAR_NODE:
            st.mode |= S_IFREG;
            break;
        case NProto::E_SOCK_NODE:
            st.mode |= S_IFSOCK;
            break;
    }

    st.blksize = blockSize;
    st.uid = attr.GetUid();
    st.gid = attr.GetGid();
    st.size = attr.GetSize();
    // FIXME: number of actually allocated 512 blocks
    st.blocks = AlignUp<ui64>(st.size, 512) / 512;
    st.nlink = attr.GetLinks();
    st.atime = ConvertTimeSpec(TInstant::MicroSeconds(attr.GetATime())).tv_nsec;
    st.mtime = ConvertTimeSpec(TInstant::MicroSeconds(attr.GetMTime())).tv_nsec;
    st.ctime = ConvertTimeSpec(TInstant::MicroSeconds(attr.GetCTime())).tv_nsec;
}

void ConvertStat(
    const NProto::TFileStore& info,
    const NProto::TFileStoreStats& stats,
    struct statvfs& st)
{
    Zero(st);

    ui64 bfree = 0;
    // it is possible to use a little bit more blocks
    if (info.GetBlocksCount() > stats.GetUsedBlocksCount()) {
        bfree = info.GetBlocksCount() - stats.GetUsedBlocksCount();
    }

    ui64 ffree = info.GetNodesCount() - stats.GetUsedNodesCount();

    // Optimal transfer block size
    st.f_bsize = info.GetBlockSize();

    // Total data blocks in filesystem
    st.f_blocks = info.GetBlocksCount();

    // Free blocks in filesystem
    st.f_bfree = bfree;

    // Free blocks available to unprivileged user
    st.f_bavail = bfree;

    // Total inodes in filesystem
    st.f_files = info.GetNodesCount();

    // Free inodes in filesystem
    st.f_ffree = ffree;

    // Free inodes available to unprivileged user
    st.f_favail = ffree;

    // Maximum length of filenames
    st.f_namemax = MaxName;

    // Fragment size (since Linux 2.6)
    st.f_frsize = info.GetBlockSize();
}

}   // namespace NCloud::NFileStore::NVFS
