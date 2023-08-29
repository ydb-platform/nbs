#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage {

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
    bool isFresh = false,
    bool isDisabled = false,
    TString suffix = "");

}   // namespace NCloud::NBlockStore::NStorage
