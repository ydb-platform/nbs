#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& DumpState(
    IOutputStream& out,
    NProto::EAgentState state);

IOutputStream& DumpState(
    IOutputStream& out,
    NProto::EDiskState state);

IOutputStream& DumpState(
    IOutputStream& out,
    NProto::EDeviceState state);

}   // namespace NCloud::NBlockStore::NStorage
