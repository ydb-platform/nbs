#pragma once
#include "public.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/tablet/model/range_locks.h>
#include <cloud/filestore/libs/storage/tablet/model/throttling_policy.h>
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>
#include <cloud/filestore/libs/storage/tablet/session.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/logoblob.h>

#include <util/system/align.h>
#include <util/system/yassert.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] inline ui64 SafeIncrement(ui64 value, size_t delta)
{
    Y_ABORT_UNLESS(value <= Max<ui64>() - delta, "v: %lu, d: %lu", value, Max<ui64>() - delta);
    return value + delta;
}

[[nodiscard]] inline ui64 SafeDecrement(ui64 value, size_t delta)
{
    Y_ABORT_UNLESS(value >= delta, "v: %lu, d: %lu", value, delta);
    return value - delta;
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] inline NKikimr::TLogoBlobID MakeBlobId(ui64 tabletId, const TPartialBlobId& blobId)
{
    return NKikimr::TLogoBlobID(
        tabletId,
        blobId.Generation(),
        blobId.Step(),
        blobId.Channel(),
        blobId.BlobSize(),
        blobId.Cookie());
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] inline ui32 SizeToBlocks(ui64 size, ui32 block)
{
    return AlignUp<ui64>(size, block) / block;
}

[[nodiscard]] inline i64 GetBlocksDifference(ui64 prevSize, ui64 curSize, ui32 block)
{
    return static_cast<i64>(SizeToBlocks(curSize, block)) -
        static_cast<i64>(SizeToBlocks(prevSize, block));
}

////////////////////////////////////////////////////////////////////////////////

enum ECopyAttrsMode
{
    E_CM_CTIME = 1,     // CTime

    E_CM_MTIME = 2,     // MTime
    E_CM_CMTIME = 3,    // CTime + MTime

    E_CM_ATIME = 4,     // ATime
    E_CM_CATIME = 5,    // CTime + ATime

    E_CM_REF = 8,       // Links +1
    E_CM_UNREF = 16,    // Links -1
};

NProto::TNode CreateRegularAttrs(ui32 mode, ui32 uid, ui32 gid);
NProto::TNode CreateDirectoryAttrs(ui32 mode, ui32 uid, ui32 gid);
NProto::TNode CreateLinkAttrs(const TString& link, ui32 uid, ui32 gid);
NProto::TNode CreateSocketAttrs(ui32 mode, ui32 uid, ui32 gid);

NProto::TNode CopyAttrs(const NProto::TNode& src, ui32 mode = E_CM_CTIME);

void ConvertNodeFromAttrs(
    NProto::TNodeAttr& dst,
    ui64 id,
    const NProto::TNode& src);

void ConvertAttrsToNode(const NProto::TNodeAttr& src, NProto::TNode* dst);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TTag
{
};

/**
 * @brief ConvertTo provides single interface to convert from anything to
 * anything. To use this interface you must provide your converter in form of a
 * function named `ConvertToImpl`, that accepts as last parameter struct `To`
 * with return value specialization. Example:
 * @code {.cpp}
 * double ConverToImpl(const std::string& s, TTag<double>)
 * {
 *     // implementation
 * }
 * @endcode
 *
 *
 * @tparam TDestination
 * @tparam TSources
 * @param sources
 * @return TDestination
 */
template<typename TDestination, typename... TSources>
TDestination ConvertTo(TSources&& ...sources)
{
    return ConvertToImpl(
        std::forward<TSources>(sources)...,
        TTag<TDestination>{});
}

////////////////////////////////////////////////////////////////////////////////

ELockOrigin ConvertToImpl(NProto::ELockOrigin source, TTag<ELockOrigin>);
NProto::ELockOrigin ConvertToImpl(ELockOrigin source, TTag<NProto::ELockOrigin>);
ELockMode ConvertToImpl(NProto::ELockType source, TTag<ELockMode>);
NProto::ELockType ConvertToImpl(ELockMode source, TTag<NProto::ELockType>);

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateNodeName(const TString& name);
NProto::TError ValidateXAttrName(const TString& name);
NProto::TError ValidateXAttrValue(const TString& name, const TString& value);
NProto::TError ValidateRange(TByteRange byteRange, ui32 maxFileBlocks);

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& src,
    NProto::TFileSystem& dst);

void Convert(
    const NProto::TFileSystem& src,
    NProtoPrivate::TFileSystemConfig& dst);

void Convert(
    const NProto::TFileStorePerformanceProfile& src,
    TThrottlerConfig& dst);

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief
 * man fcntl:
 * > In order to place a read lock, fd must be open for reading. In order to
 * > place a write lock, fd must  be  open  for  writing. To place both types of
 * > lock, open a file read-write.
 *
 * man 2 flock
 * > A shared or exclusive lock can be placed on a file regardless of the mode
 * > in which the file was opened.
 *
 * @param handle
 * @param range
 * @return true could be in two cases:
 * - flock origin
 * - fcntl origin and node opened in write mode for exclusive lock or either
 *   write or read mode for shared lock
 * @return false when above conditions are not met
 */
bool IsLockingAllowed(const TSessionHandle* handle, const TLockRange& range);

template <typename T>
concept HasGetLockType = requires(T t) {
    { t.GetLockType() } -> std::same_as<NProto::ELockType>;
};

template<typename TRequest>
TLockRange MakeLockRange(const TRequest& request, ui64 nodeId)
{
    TLockRange range{
        .NodeId = nodeId,
        .OwnerId = request.GetOwner(),
        .Offset = request.GetOffset(),
        .Length = request.GetLength(),
        .Pid = request.GetPid(),
        .LockMode = ELockMode::Unlock,
        .LockOrigin = ConvertTo<ELockOrigin>(request.GetLockOrigin()),
    };
    if constexpr (HasGetLockType<TRequest>) {
        range.LockMode = ConvertTo<ELockMode>(request.GetLockType());
    }
    return range;
}

TLockRange ConvertToImpl(const NProto::TSessionLock& proto, TTag<TLockRange>);

}   // namespace NCloud::NFileStore::NStorage
