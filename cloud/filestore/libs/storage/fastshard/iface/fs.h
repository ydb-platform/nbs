#pragma once

#include "public.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/locks.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/stream/fwd.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

#define FAST_SHARD_PRIVATE_METHODS(xxx, ...)                                   \
    xxx(GetNodeAttrBatch, __VA_ARGS__)                                         \
    // FAST_SHARD_PRIVATE_METHODS

#define FAST_SHARD_PUBLIC_METHODS(xxx, ...)                                    \
    xxx(GetNodeAttr, __VA_ARGS__)                                              \
    xxx(SetNodeAttr, __VA_ARGS__)                                              \
    xxx(AccessNode, __VA_ARGS__)                                               \
    xxx(CreateNode, __VA_ARGS__)                                               \
    xxx(UnlinkNode, __VA_ARGS__)                                               \
    xxx(CreateHandle, __VA_ARGS__)                                             \
    xxx(DestroyHandle, __VA_ARGS__)                                            \
    xxx(AllocateData, __VA_ARGS__)                                             \
    xxx(AcquireLock, __VA_ARGS__)                                              \
    xxx(ReleaseLock, __VA_ARGS__)                                              \
    xxx(TestLock, __VA_ARGS__)                                                 \
    xxx(WriteData, __VA_ARGS__)                                                \
    xxx(ReadData, __VA_ARGS__)                                                 \
    xxx(RemoveNodeXAttr, __VA_ARGS__)                                          \
    xxx(GetNodeXAttr, __VA_ARGS__)                                             \
    xxx(SetNodeXAttr, __VA_ARGS__)                                             \
    xxx(ListNodeXAttr, __VA_ARGS__)

// FAST_SHARD_PUBLIC_METHODS

struct TFileSystemShardStats
{
    ui64 UsedNodeCount = 0;
    ui64 TotalNodeCount = 0;
    ui64 UsedNameCount = 0;
    ui64 TotalNameCount = 0;
    ui64 UsedHandleCount = 0;
    ui64 TotalHandleCount = 0;
    ui64 UsedPageCount = 0;
    ui64 TotalPageCount = 0;
};

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

    [[nodiscard]] virtual NThreading::TFuture<NCloud::NProto::TError>
    CollectStats(TFileSystemShardStats* stats) const = 0;

    //
    // Monitoring. The layout of the shard's persistent data structures
    // is fixed after construction, so these methods are synchronous and
    // callable from any thread. A follow-up will add a per-component
    // statistics method taking a component tag (NodeTable, NameTable,
    // etc) and returning json - the layout page will request it via
    // ajax.
    //

    /**
     * Writes an html page describing the layout of the shard's
     * persistent data structures.
     *
     * @param out - (out) Stream the page is written to.
     */
    virtual void DumpLayoutHtml(IOutputStream& out) const = 0;

    /**
     * Writes a json document with the same layout data items as
     * DumpLayoutHtml.
     *
     * @param out - (out) Stream the document is written to.
     */
    virtual void DumpLayoutJson(IOutputStream& out) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateFileSystemShardStub();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
