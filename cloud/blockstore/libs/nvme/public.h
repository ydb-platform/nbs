#pragma once

#include <memory>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

struct INvmeManager;
using INvmeManagerPtr = std::shared_ptr<INvmeManager>;

}   // namespace NCloud::NBlockStore::NNvme
