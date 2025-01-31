 #pragma once

#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

auto ParseWriteDeviceBlocksRequest(TAutoPtr<NActors::IEventHandle>& ev)
    -> std::unique_ptr<TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>;

auto CopyWriteDeviceBlocksRequest(TAutoPtr<NActors::IEventHandle>& ev)
    -> std::unique_ptr<TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>;

auto DefaultWriteDeviceBlocksRequest(TAutoPtr<NActors::IEventHandle>& ev)
    -> std::unique_ptr<TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>;

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
