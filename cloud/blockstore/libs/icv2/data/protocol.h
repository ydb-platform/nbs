#pragma once

#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

#include <array>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Use these pairs in TRequestChannelConfig::EventTypes for data requests.
// Write requests and successful read responses must carry one payload and an
// empty protobuf Blocks field. Read errors carry no payload. Write block count
// is payload size divided by BlockSize. Use the same format with or without
// XDC.
inline constexpr std::array<NCloud::NIcV2::TRequestEventTypes, 3>
    DataEventTypes = {
        NCloud::NIcV2::TRequestEventTypes{
            .RequestType = NStorage::TEvService::EvReadBlocksRequest,
            .ResponseType = NStorage::TEvService::EvReadBlocksResponse,
        },
        NCloud::NIcV2::TRequestEventTypes{
            .RequestType = NStorage::TEvService::EvWriteBlocksRequest,
            .ResponseType = NStorage::TEvService::EvWriteBlocksResponse,
        },
        NCloud::NIcV2::TRequestEventTypes{
            .RequestType = NStorage::TEvService::EvZeroBlocksRequest,
            .ResponseType = NStorage::TEvService::EvZeroBlocksResponse,
        },
};

}   // namespace NCloud::NBlockStore::NIcV2
