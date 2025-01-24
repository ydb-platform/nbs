#include "dump.h"


#include "starter.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/service_null/service.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/loop.h>
#include <cloud/filestore/libs/vfs_fuse/loop.h>
#include <cloud/filestore/libs/vhost/server.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/vhost-client/vhost-buffered-client.h>

#include <cloud/contrib/vhost/virtio/virtio_fs_spec.h>

#include <contrib/libs/virtiofsd/fuse.h>

#include <util/system/getpid.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NCloud::NFileStore::NVhost;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool ReadData(
    void* dst_data,
    size_t dst_size,
    const ui8* src_data,
    size_t src_size,
    size_t& offset)
{
    if ((src_size - offset) < dst_size) {
        return false;
    }
    memcpy(dst_data, src_data + offset, dst_size);
    offset += dst_size;
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

TStarter* TStarter::GetStarter()
{
    static std::unique_ptr<TStarter> Impl = nullptr;

    if (!Impl) {
        signal(SIGUSR1, SIG_IGN);   // see vfs_fuse/loop for details

        Impl = std::unique_ptr<TStarter>(new TStarter());

        Impl->Start();

        std::set_terminate([]{
            TBackTrace bt;
            bt.Capture();
            Cerr << bt.PrintToString() << Endl;
        });

        atexit([]()
        {
            TStarter::GetStarter()->Stop();
        });
    }

    return Impl.get();
}

TStarter::TStarter()
    : SocketPath(Sprintf("/tmp/vhost.socket_%d", GetPID()))
    , Client(std::make_unique<NVHost::TBufferedClient>(SocketPath))
{
    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();

    Logging = CreateLoggingService("console", { TLOG_NOTICE });
    InitLog(Logging);

    NVhost::InitLog(Logging);
    NVhost::StartServer();

    Service = CreateNullFileStore();

    auto sessionConfig = std::make_shared<TSessionConfig>(
        NProto::TSessionConfig{});
    auto session = CreateSession(
        Logging,
        Timer,
        Scheduler,
        Service,
        std::move(sessionConfig));

    NProto::TVFSConfig proto;
    proto.SetDebug(true);
    proto.SetSocketPath(SocketPath.c_str());

    auto config = std::make_shared<TVFSConfig>(std::move(proto));

DUMP(config->GetFileSystemId());

    Loop = NFuse::CreateFuseLoop(
        std::move(config),
        Logging,
        CreateRequestStatsRegistryStub(),
        Scheduler,
        Timer,
        CreateProfileLogStub(),
        std::move(session));
}


void TStarter::Start()
{
    if (Scheduler) {
        Scheduler->Start();
    }

    if (Loop) {
        Loop->StartAsync().GetValueSync();
    }

    Client->Init();

    static constexpr int DefaultFlags =
        FUSE_ASYNC_READ | FUSE_POSIX_LOCKS | FUSE_ATOMIC_O_TRUNC |
        FUSE_EXPORT_SUPPORT | FUSE_BIG_WRITES | FUSE_DONT_MASK |
        FUSE_SPLICE_WRITE | FUSE_SPLICE_MOVE | FUSE_SPLICE_READ |
        FUSE_FLOCK_LOCKS | FUSE_HAS_IOCTL_DIR | FUSE_AUTO_INVAL_DATA |
        FUSE_DO_READDIRPLUS | FUSE_READDIRPLUS_AUTO | FUSE_ASYNC_DIO |
        FUSE_WRITEBACK_CACHE | FUSE_NO_OPEN_SUPPORT |
        FUSE_PARALLEL_DIROPS | FUSE_HANDLE_KILLPRIV | FUSE_POSIX_ACL |
        FUSE_ABORT_ERROR | FUSE_MAX_PAGES | FUSE_CACHE_SYMLINKS |
        FUSE_NO_OPENDIR_SUPPORT | FUSE_EXPLICIT_INVAL_DATA;

    struct fuse_init {
        fuse_in_header in;
        fuse_init_in init;
    };

    TVector<char> inData(sizeof(fuse_init), '0');
    *reinterpret_cast<fuse_init*>(inData.data()) = {
        .in = {
            .len = sizeof(fuse_init),
            .opcode = FUSE_INIT,
        },
        .init = {
            .major = FUSE_KERNEL_VERSION,
            .minor = FUSE_KERNEL_MINOR_VERSION,
            .flags = DefaultFlags,
        }
    };

    TVector<char> outData;
    Client->Write(inData, outData);
}

void TStarter::Stop()
{
    Client->DeInit();

    if (Loop) {
        Loop->StopAsync().GetValueSync();
    }

    if (Scheduler) {
        Scheduler->Stop();
    }

    NVhost::StopServer();
}

int TStarter::Run(const ui8* data, size_t size)
{
    static constexpr size_t MaxBufferLen = 512;

    if (MaxBufferLen < size) {
        return 0;
    }

    size_t offset = 0;
    while (offset < size) {
        virtio_fs_in_header in;
        if (!ReadData(&in.opcode,  sizeof(in.opcode),  data, size, offset) ||
            !ReadData(&in.unique,  sizeof(in.unique),  data, size, offset) ||
            !ReadData(&in.nodeid,  sizeof(in.nodeid),  data, size, offset) ||
            !ReadData(&in.uid,     sizeof(in.uid),     data, size, offset) ||
            !ReadData(&in.gid,     sizeof(in.gid),     data, size, offset) ||
            !ReadData(&in.pid,     sizeof(in.pid),     data, size, offset) ||
            !ReadData(&in.padding, sizeof(in.padding), data, size, offset) )
        {
            return 0;
        }
        in.len = sizeof(virtio_fs_in_header) + size - offset;

        TVector<char> inData(sizeof(virtio_fs_in_header) + size - offset);
        memcpy(inData.data(), &in, sizeof(virtio_fs_in_header));
        memcpy(inData.data() + sizeof(virtio_fs_in_header), data + offset, size - offset);
        // to break cycle
        offset = size;

        TVector<char> outData(MaxBufferLen);
        if (Client->Write(inData, outData)) {
            virtio_fs_out_header out;
            memcpy(&out, outData.data(), sizeof(virtio_fs_out_header));
            if (out.unique != in.unique) {
                return -1;
            }
        }
    }

    return 0;
}

} // namespace NCloud::NFileStore::NFuse
