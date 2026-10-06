#pragma once

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NVHostServer {

// Zero means persistence is unavailable; the receiver rejects that generation.
ui64 NextLatencyGeneration(const TString& socketPath);

}   // namespace NCloud::NBlockStore::NVHostServer
