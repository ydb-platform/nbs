#pragma once

#include "public.h"

#include <cloud/filestore/libs/client/client.h>
#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <contrib/ydb/library/actors/util/should_continue.h>
#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TCommand
{
protected:
    NLastGetopt::TOpts Opts;
    TMaybe<NLastGetopt::TOptsParseResultException> OptsParseResult;

    TString VerboseLevel;
    TLogSettings LogSettings;
    ILoggingServicePtr Logging;
    TLog Log;

    TString MonitoringAddress;
    ui32 MonitoringPort = 0;
    ui32 MonitoringThreads = 0;
    IMonitoringServicePtr Monitoring;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

    TString ServerAddress;
    ui32 ServerPort = 0;
    ui32 SecurePort = 0;
    bool SkipCertVerification = false;
    TString IamTokenFile;
    TString ConfigFile;

    TClientConfigPtr ClientConfig;

    TMutex WaitMutex;
    TCondVar WaitCondVar;

    bool JsonOutput = false;

    TProgramShouldContinue ProgramShouldContinue;

public:
    TCommand();

    virtual ~TCommand() = default;

    int Run(int argc, char** argv);
    void Stop(int exitCode);

    NLastGetopt::TOpts& GetOpts()
    {
        return Opts;
    }

    TLog& AccessLog()
    {
        return Log;
    }

protected:
    virtual void Init();

    virtual void Start();
    virtual void Stop();

    virtual bool Execute() = 0;

    static TCallContextPtr PrepareCallContext()
    {
        return MakeIntrusive<TCallContext>(CreateRequestId());
    }

    template <typename T>
    T WaitFor(NThreading::TFuture<T> future)
    {
        if (!future.HasValue()) {
            auto* ptr = reinterpret_cast<NThreading::TFuture<void>*>(&future);
            if (!WaitForI(*ptr)) {
                return TErrorResponse(E_REJECTED, "request cancelled");
            }
        }
        return future.ExtractValue();
    }

private:
    bool WaitForI(const NThreading::TFuture<void>& future);
};

////////////////////////////////////////////////////////////////////////////////

class TFileStoreServiceCommand
    : public TCommand
{
protected:
    TString ClientId;
    TString FileSystemId;
    bool DisableMultiTabletForwarding = false;

    IFileStoreServicePtr Client;

    NProto::THeaders Headers;

public:
    TFileStoreServiceCommand() = default;

protected:
    void Init() override;

    void Start() override;
    void Stop() override;
};

////////////////////////////////////////////////////////////////////////////////

class TFileStoreCommand
    : public TFileStoreServiceCommand
{
public:
    TFileStoreCommand();

    void Start() override;
    void Stop() override;

protected:
    template <typename T>
    std::shared_ptr<T> CreateRequest()
    {
        auto request = std::make_shared<T>();
        request->SetFileSystemId(FileSystemId);
        request->MutableHeaders()->CopyFrom(Headers);

        return request;
    }

    template <typename T>
    void CheckResponse(const T& response)
    {
        if (HasError(response)) {
            throw TServiceError(response.GetError());
        }
    }

    struct TPathEntry
    {
        NProto::TNodeAttr Node;
        TStringBuf Name;
    };

    NProto::TNodeAttr ResolveNode(
        ui64 parentNodeId,
        TString name,
        bool ignoreMissing);
    TVector<TPathEntry> ResolvePath(
        TStringBuf path,
        bool ignoreMissing);

    class TSessionGuard final
    {
        TFileStoreCommand& FileStoreCmd;
        TLog& Log;

    public:
        explicit TSessionGuard(TFileStoreCommand& fileStoreCmd)
            : FileStoreCmd(fileStoreCmd)
            , Log(FileStoreCmd.AccessLog())
        {
        }

        ~TSessionGuard()
        {
            try {
                FileStoreCmd.DestroySession();
            } catch (...) {
                STORAGE_ERROR("~TSessionGuard: " << CurrentExceptionMessage());
            }
        }
    };

    [[nodiscard]] TSessionGuard CreateSession();

private:
    void DestroySession();
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointCommand
    : public TCommand
{
protected:
    IEndpointManagerPtr Client;

public:
    TEndpointCommand();

protected:
    void Init() override;

    void Start() override;
    void Stop() override;
};

}   // namespace NCloud::NFileStore::NClient
