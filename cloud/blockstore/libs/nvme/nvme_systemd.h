#include "nvme_linux.h"

namespace NCloud::NBlockStore::NNvme {

class TSystemdNvmeManager: public TNvmeManager
{
public:
    TSystemdNvmeManager(ITaskQueuePtr executor, TDuration timeout);
    ~TSystemdNvmeManager() override;

    TResultOrError<TString> GetSerialNumber(const TString& path) override;
    TResultOrError<bool> IsSsd(const TString& path) override;
};

}   // namespace NCloud::NBlockStore::NNvme
