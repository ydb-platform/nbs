#pragma once

#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TString NodeTypeToString(NProto::ENodeType nodeType);

}  // namespace NCloud::NFileStore::NClient
