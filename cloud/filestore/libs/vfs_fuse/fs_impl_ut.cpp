#include "fs.h"

#include "cloud/filestore/libs/diagnostics/request_stats.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/vfs_fuse/config.h>
#include <cloud/filestore/libs/vfs_fuse/handle_ops_queue.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <contrib/libs/fuse/include/fuse_lowlevel.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>

namespace NCloud::NFileStore::NFuse {

namespace {

using namespace NCloud::NStorage::NUserStats;
using namespace NMonitoring;
using namespace NCloud;

////////////////////////////////////////////////////////////////////////////////
class TCompletionQueueStub final: public ICompletionQueue
{
public:
    TCompletionQueueStub() = default;

    int Complete(fuse_req_t req, TCompletionCallback cb) noexcept override
    {
        std::cout << "TCompletionQueueStub::Complete called" << std::endl;
        Y_UNUSED(req);
        Y_UNUSED(cb);
        return 0;
    }
};

class TBootstrap
{
public:
    TBootstrap()
    {
        ILoggingServicePtr logFs = CreateLoggingService("fs");
        NCloud::NFileStore::IProfileLogPtr profileLogFs(CreateProfileLogStub());
        ISchedulerPtr schedulerFs(new TTestScheduler());
        ITimerPtr timerFs(CreateWallClockTimer());

        NProto::TFileSystemConfig protoConfig;
        protoConfig.SetFileSystemId("tst_fs_id");
        TFileSystemConfigPtr configFs =
            std::make_shared<TFileSystemConfig>(protoConfig);

        ILoggingServicePtr logSession = CreateLoggingService("session");
        ITimerPtr timerSession(CreateWallClockTimer());
        ISchedulerPtr schedulerSession(new TTestScheduler());
        auto serviceSession = std::make_shared<TFileStoreTest>();
        auto sessionConfig =
            std::make_shared<NCloud::NFileStore::NClient::TSessionConfig>(
                NProto::TSessionConfig{});
        IFileStorePtr sessionFs = NCloud::NFileStore::NClient::CreateSession(
            logSession,
            timerSession,
            schedulerSession,
            serviceSession,
            sessionConfig);

        TDynamicCountersPtr countersStats = MakeIntrusive<TDynamicCounters>();
        ITimerPtr timerStats = CreateWallClockTimer();
        IRequestStatsRegistryPtr registryStats =
            CreateRequestStatsRegistryStub();

        IRequestStatsPtr statsFs = registryStats->GetFileSystemStats(
            configFs->GetFileSystemId(),
            "tst_client");

        ICompletionQueuePtr queueFs = std::make_shared<TCompletionQueueStub>();

        //THandleOpsQueuePtr handleOpsQueue(CreateHandleOpsQueue("tst2",0));
        //TWriteBackCachePtr writeBackCache(CreateWriteBackCache("tst3", 0));

        FileSystem = CreateFileSystem(
            logFs,
            profileLogFs,
            schedulerFs,
            timerFs,
            configFs,
            sessionFs,
            statsFs,
            queueFs,
            std::move(nullptr),
            std::move(nullptr));
    }

    ~TBootstrap() = default;

    IFileSystemPtr GetFs()
    {
        return FileSystem;
    }

private:
    std::shared_ptr<TFileStoreTest> Service;

    IFileSystemPtr FileSystem;
};


}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileSystemImplTest)
{
    Y_UNIT_TEST(ShouldCritOnCreate)
    {
        TBootstrap bootstrap;
        IFileSystemPtr fs = bootstrap.GetFs();
        TCallContextPtr callContext = MakeIntrusive<TCallContext>(CreateRequestId());

        fuse_req req {
            //struct fuse_session *
            .se = nullptr,
            //uint64_t
            .unique = 10,
            //int
            .ctr = 0,
            //pthread_mutex_t
            .lock = {},
            //struct fuse_ctx
            .ctx = {.uid = 42, .gid = 420, .pid = 421, .umask = 0},
            //struct fuse_chan *
            .ch = nullptr,
            //int
            .interrupted = 0,
            // unsigned int
            .ioctl_64bit = 1,
            //union
            .u = {.i = {.unique = 10}},
            //struct fuse_req *
            .next = nullptr,
            //struct fuse_req *
            .prev = nullptr
        };

        fuse_ino_t parent = 1;              // Create a file in root directory
        TString name = "test_file";
        mode_t mode = S_IFREG;

        fuse_file_info fi{};
        fi.flags |= O_CREAT;    // create file
        fi.flags |= O_EXCL;     // fail if file already exists
        fi.flags |= O_RDWR;     // open for read/write

        fs->Create(callContext, &req, parent, name,  mode, &fi);
    }
}

}   // namespace NCloud::NFileStore::NFuse
