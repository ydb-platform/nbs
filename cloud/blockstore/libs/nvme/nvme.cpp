#include "nvme.h"

#include <cloud/blockstore/libs/nvme/nvme_linux.h>
#include <cloud/blockstore/libs/nvme/nvme_systemd.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

namespace NCloud::NBlockStore::NNvme {

INvmeManagerPtr CreateNvmeManager(
    NProto::EDiskAgentNvmeManagerType type,
    TDuration timeout)
{
#if defined(_linux_)
    switch (type) {
        case NProto::DISK_AGENT_NVME_MANAGER_DEFAULT:
            return std::make_shared<TNvmeManager>(
                CreateLongRunningTaskExecutor("SecureErase"),
                timeout);
            break;
        case NProto::DISK_AGENT_NVME_MANAGER_SYSTEMD:
            return std::make_shared<TSystemdNvmeManager>(
                CreateLongRunningTaskExecutor("SecureErase"),
                timeout);
            break;
    }
#else
    return nullptr;
#endif
}

}   // namespace NCloud::NBlockStore::NNvme
