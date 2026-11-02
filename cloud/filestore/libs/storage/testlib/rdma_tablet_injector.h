#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <contrib/ydb/core/testlib/actors/test_runtime.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <memory>

namespace google::protobuf {
    class Message;
}

namespace NCloud::NFileStore::NStorage {

class TTestRdmaServer;

////////////////////////////////////////////////////////////////////////////////

// Runs tablet requests through the tablet side of the RDMA side channel:
// envelope -> RDMA target (in-process server) -> request handler -> forwarder
// -> tablet. The typed response (or the error) is delivered to the sender
// with the original cookie, as if it came over the pipe.
class TRdmaTabletInjector
{
    struct TState;

private:
    NKikimr::TTestActorRuntime& Runtime;
    const ui32 NodeIdx;

    std::shared_ptr<TState> State;
    NActors::TActorId ForwarderId;
    std::shared_ptr<TTestRdmaServer> Server;
    IStartablePtr Target;

public:
    TRdmaTabletInjector(NKikimr::TTestActorRuntime& runtime, ui32 nodeIdx);
    ~TRdmaTabletInjector();

    void Send(
        ui64 tabletId,
        ui32 methodId,
        const google::protobuf::Message& record,
        NActors::TActorId sender,
        ui64 cookie);
};

}   // namespace NCloud::NFileStore::NStorage
