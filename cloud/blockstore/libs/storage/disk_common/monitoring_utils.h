#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum EDeviceStateFlags : uint16_t
{
    NONE      = 0,
    FRESH     = 1 << 0,
    DISABLED  = 1 << 1,
    DIRTY     = 1 << 2,
    SUSPENDED = 1 << 3,
    LAGGING   = 1 << 4,
};

inline EDeviceStateFlags operator|(EDeviceStateFlags a, EDeviceStateFlags b)
{
    return static_cast<EDeviceStateFlags>(
        static_cast<uint16_t>(a) | static_cast<uint16_t>(b));
}

inline EDeviceStateFlags& operator|=(EDeviceStateFlags& a, EDeviceStateFlags b)
{
    return a = a | b;
}

////////////////////////////////////////////////////////////////////////////////

IOutputStream& DumpAgentState(
    IOutputStream& out,
    NProto::EAgentState state,
    bool connected);

IOutputStream& DumpDiskState(
    IOutputStream& out,
    NProto::EDiskState state);

IOutputStream& DumpDeviceState(
    IOutputStream& out,
    NProto::EDeviceState state,
    EDeviceStateFlags flags = EDeviceStateFlags::NONE,
    TString suffix = "");

}   // namespace NCloud::NBlockStore::NStorage
