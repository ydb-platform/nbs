#pragma once

#include <contrib/ydb/library/actors/core/event_pb.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <cloud/blockstore/libs/icv2/protos/diagnostic.pb.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// These IDs belong to the dedicated diagnostic IC V2 protocol. Events are
// dispatched by its request channel and never routed through an actor system.
struct TEvDiagnostic
{
    enum EEvents
    {
        EvPingRequest = EventSpaceBegin(NActors::TEvents::ES_USERSPACE) + 1,
        EvPingResponse,
    };

    struct TEvPingRequest final
        : NActors::
              TEventPB<TEvPingRequest, NProto::TIcV2PingRequest, EvPingRequest>
    {
    };

    struct TEvPingResponse final
        : NActors::TEventPB<
              TEvPingResponse,
              NProto::TIcV2PingResponse,
              EvPingResponse>
    {
    };
};

}   // namespace NCloud::NBlockStore::NIcV2
