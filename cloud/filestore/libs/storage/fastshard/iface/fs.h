#pragma once

#include "public.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/locks.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

#define FAST_SHARD_PRIVATE_METHODS(xxx, ...)                                   \
    xxx(GetNodeAttrBatch,                   __VA_ARGS__)                       \
// FAST_SHARD_PRIVATE_METHODS

#define FAST_SHARD_PUBLIC_METHODS(xxx, ...)                                    \
    xxx(GetNodeAttr,                        __VA_ARGS__)                       \
    xxx(SetNodeAttr,                        __VA_ARGS__)                       \
    xxx(AccessNode,                         __VA_ARGS__)                       \
    xxx(CreateNode,                         __VA_ARGS__)                       \
    xxx(UnlinkNode,                         __VA_ARGS__)                       \
    xxx(CreateHandle,                       __VA_ARGS__)                       \
    xxx(DestroyHandle,                      __VA_ARGS__)                       \
    xxx(AllocateData,                       __VA_ARGS__)                       \
    xxx(AcquireLock,                        __VA_ARGS__)                       \
    xxx(ReleaseLock,                        __VA_ARGS__)                       \
    xxx(TestLock,                           __VA_ARGS__)                       \
    xxx(WriteData,                          __VA_ARGS__)                       \
    xxx(ReadData,                           __VA_ARGS__)                       \
    xxx(RemoveNodeXAttr,                    __VA_ARGS__)                       \
    xxx(GetNodeXAttr,                       __VA_ARGS__)                       \
    xxx(SetNodeXAttr,                       __VA_ARGS__)                       \
    xxx(ListNodeXAttr,                      __VA_ARGS__)                       \
// FAST_SHARD_PUBLIC_METHODS

struct IFileSystemShard
{
    virtual ~IFileSystemShard() = default;

#define FAST_SHARD_DECLARE_METHOD(name, ns, ...)                               \
    virtual NThreading::TFuture<ns::T##name##Response> name(                   \
        ns::T##name##Request request) = 0;                                     \
// FAST_SHARD_DECLARE_METHOD

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_DECLARE_METHOD, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_DECLARE_METHOD, NProto)

#undef FAST_SHARD_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateFileSystemShardStub();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
