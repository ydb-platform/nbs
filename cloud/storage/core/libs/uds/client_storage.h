#pragma once

#include <cloud/storage/core/protos/request_source.pb.h>

#include <util/network/socket.h>

namespace NCloud::NStorage::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IClientStorage
{
    virtual ~IClientStorage() = default;

    virtual void AddClient(
        const TSocketHolder& clientSocket,
        NCloud::NProto::ERequestSource source) = 0;

    virtual void RemoveClient(const TSocketHolder& clientSocket) = 0;
};

using IClientStoragePtr = std::shared_ptr<IClientStorage>;

////////////////////////////////////////////////////////////////////////////////

IClientStoragePtr CreateClientStorageStub();

}   // namespace NCloud::NStorage::NServer
