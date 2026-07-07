#pragma once

#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

#define SN_METHODS(xxx, ...)                                                   \
    xxx(AcquireDevices,   __VA_ARGS__)                                         \
    xxx(ReleaseDevices,   __VA_ARGS__)                                         \
    xxx(ReadPages,        __VA_ARGS__)                                         \
    xxx(WriteLogRecord,   __VA_ARGS__)                                         \
// SN_METHODS

struct IStorageNode
{
    virtual ~IStorageNode() = default;

#define SN_DECLARE_METHOD(name, ...)                                           \
    virtual NThreading::TFuture<NCloud::NProto::T##name##Response> name(       \
        NCloud::NProto::T##name##Request request) = 0;                         \
// SN_DECLARE_METHOD

    SN_METHODS(SN_DECLARE_METHOD)

#undef SN_DECLARE_METHOD
};

using IStorageNodePtr = std::shared_ptr<IStorageNode>;

// A stub IStorageNode that returns default (S_OK) responses.
IStorageNodePtr CreateStorageNodeStub();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
