#pragma once

#include "public.h"

#include <cloud/fastshard/protos/device.pb.h>

#include <cloud/fastshard/sn/iface/storage_node.h>

#include <util/generic/string.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

// Describes a request for the log: the client, the devices and the main
// arguments. Page contents are never printed.

#define JOURNAL_DECLARE_DESCRIBE_REQUEST(name, ...)                            \
    TString DescribeRequest(const NProto::T##name##Request& request);          \
    // JOURNAL_DECLARE_DESCRIBE_REQUEST

SN_METHODS(JOURNAL_DECLARE_DESCRIBE_REQUEST)

#undef JOURNAL_DECLARE_DESCRIBE_REQUEST

}   // namespace NCloud::NJournalled
