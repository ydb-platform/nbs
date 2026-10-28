#pragma once

#include <cloud/blockstore/libs/icv2/transport/request_channel.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <array>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Existing NBS actor event IDs and protobuf serialization, carried by a
// standalone channel. Local request types containing SG lists stay local.
// Write requests and successful read responses carry exactly one payload; their
// protobuf Blocks field is empty. Write block count is inferred from payload
// size and BlockSize. Error read responses have no payload. This representation
// is the same with and without XDC; the engine chooses the data channel.
inline constexpr std::array<TRequestEventTypes, 3> DataEventTypes = {
    TRequestEventTypes{.RequestType=NStorage::TEvService::EvReadBlocksRequest,
                       .ResponseType=NStorage::TEvService::EvReadBlocksResponse},
    TRequestEventTypes{.RequestType=NStorage::TEvService::EvWriteBlocksRequest,
                       .ResponseType=NStorage::TEvService::EvWriteBlocksResponse},
    TRequestEventTypes{.RequestType=NStorage::TEvService::EvZeroBlocksRequest,
                       .ResponseType=NStorage::TEvService::EvZeroBlocksResponse}};

}   // namespace NCloud::NBlockStore::NIcV2
