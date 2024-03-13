#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum EDeviceStateFlags : uint16_t
{
    NONE = 0,
    FRESH = 1 << 0,
    DISABLED = 1 << 1,
    DIRTY = 1 << 2,
    SUSPENDED = 1 << 3,
};

////////////////////////////////////////////////////////////////////////////////

IOutputStream& DumpAgentState(
    IOutputStream& out,
    NProto::EAgentState state);

IOutputStream& DumpDiskState(
    IOutputStream& out,
    NProto::EDiskState state);

IOutputStream& DumpDeviceState(
    IOutputStream& out,
    NProto::EDeviceState state,
    uint16_t flags = EDeviceStateFlags::NONE,
    TString suffix = "");

}   // namespace NCloud::NBlockStore::NStorage
