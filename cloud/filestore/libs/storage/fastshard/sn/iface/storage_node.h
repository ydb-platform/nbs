#pragma once

#include <cloud/storage/core/protos/device.pb.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

#define SN_METHODS(xxx, ...)                                                   \
    xxx(AcquireDevices,   __VA_ARGS__)                                         \
    xxx(ReleaseDevices,   __VA_ARGS__)                                         \
    xxx(ReadPages,        __VA_ARGS__)                                         \
    xxx(WriteLogRecord,   __VA_ARGS__)                                         \
// SN_METHODS

/**
 * Storage node backend that handles TDeviceProtocolRequest messages
 * decoded off the wire by the sn server. One method per case of the
 * TDeviceProtocolRequest.Request oneof — see SN_METHODS above.
 *
 * All methods are synchronous: the caller (server dispatch, or the
 * client's Exchange path) always runs inside a silk fiber, so a slow
 * implementation just cooperatively suspends the fiber.
 * Implementations that need to wait on external I/O should do so via
 * silk primitives (FiberFuture::wait, FiberScheduler::poll, etc.).
 */
struct IStorageNode
{
    virtual ~IStorageNode() = default;

#define SN_DECLARE_METHOD(name, ...)                                           \
    virtual NCloud::NProto::T##name##Response name(                            \
        NCloud::NProto::T##name##Request request) = 0;                         \
// SN_DECLARE_METHOD

    SN_METHODS(SN_DECLARE_METHOD)

#undef SN_DECLARE_METHOD
};

using IStorageNodePtr = std::shared_ptr<IStorageNode>;

/**
 * Returns an IStorageNode whose every method resolves to a default-
 * constructed response with Error.Code = E_NOT_IMPLEMENTED. Intended
 * for tests, stub bootstraps and the not-yet-wired path in production
 * builds.
 *
 * @return - Shared owner of the stub instance.
 */
IStorageNodePtr CreateStorageNodeStub();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
