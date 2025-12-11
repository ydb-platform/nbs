#pragma once

#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/vfs_fuse/log.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

class TBufferedClient;

}   // namespace NVHost

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TStarter
{
private:
    ILoggingServicePtr Logging;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

    IFileStoreServicePtr Service;
    NVFS::IFileSystemLoopPtr Loop;

    TString SocketPath;
    std::unique_ptr<NVHost::TBufferedClient> Client;

public:
    static TStarter* GetStarter();

    void Start();
    void Stop();

    int Run(const ui8* data, size_t size);

private:
    TStarter();
};

}   // namespace NCloud::NFileStore::NFuse
