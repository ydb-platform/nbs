#include "dump.h"

#include "command.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/loop.h>
#include <cloud/filestore/libs/vfs_fuse/loop.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMountCommand final
    : public TFileStoreCommand
{
private:
    TString MountPath;
    bool MountReadOnly = false;
    ui64 MountSeqNo = 0;

    NVFS::IFileSystemLoopPtr FileSystemLoop;

public:
    TMountCommand()
    {
        Opts.AddLongOption("mount-path")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&MountPath);

        Opts.AddLongOption("mount-readonly")
            .NoArgument()
            .SetFlag(&MountReadOnly);

        Opts.AddLongOption("mount-seqno")
            .RequiredArgument("NUM")
            .StoreResult(&MountSeqNo);
    }

    void Init() override
    {
        TFileStoreCommand::Init();

        NProto::TSessionConfig sessionConfig;
        sessionConfig.SetFileSystemId(FileSystemId);
        sessionConfig.SetClientId(ClientId);
        auto session = NClient::CreateSession(
            Logging,
            Timer,
            Scheduler,
            Client,
            std::make_shared<TSessionConfig>(sessionConfig));

        NProto::TVFSConfig proto;
        proto.SetFileSystemId(FileSystemId);
        proto.SetClientId(ClientId);
        proto.SetMountPath(MountPath);
        proto.SetReadOnly(MountReadOnly);
        proto.SetMountSeqNumber(MountSeqNo);
        if (LogSettings.FiltrationLevel > TLOG_DEBUG) {
            proto.SetDebug(true);
        }

        auto config = std::make_shared<NVFS::TVFSConfig>(std::move(proto));

DUMP(config->GetFileSystemId());

        FileSystemLoop = NFuse::CreateFuseLoop(
            config,
            Logging,
            CreateRequestStatsRegistryStub(),
            Scheduler,
            Timer,
            CreateProfileLogStub(),
            session);
    }

    void Start() override
    {
        TFileStoreCommand::Start();

        if (FileSystemLoop) {
            auto error = FileSystemLoop->StartAsync().GetValueSync();
            if (FAILED(error.GetCode())) {
                STORAGE_ERROR("failed to start driver: " << FormatError(error))
                ythrow TServiceError(error);
            }
        }
    }

    void Stop() override
    {
        if (FileSystemLoop) {
            FileSystemLoop->StopAsync().GetValueSync();
        }

        TFileStoreCommand::Stop();
    }

    bool Execute() override
    {
        // wait until stopped by user
        return false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewMountCommand()
{
    return std::make_shared<TMountCommand>();
}

}   // namespace NCloud::NFileStore::NClient
