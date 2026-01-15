#pragma once

#include "bootstrap.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <contrib/ydb/library/actors/util/should_continue.h>
#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/system/progname.h>

#include <memory>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TCommand
    : public TAtomicRefCount<TCommand>
{
protected:
    // When client attempts to read or write too many blocks, several read/write
    // blocks requests are executed instead of a single one (unless Proto option
    // is used)
    ui64 BatchBlocksCount;

    TString ConfigFile;
    TString IamConfigFile;

    TString Host;
    ui32 InsecurePort = 0;
    ui32 SecurePort = 0;
    TString ServerUnixSocketPath;
    bool SkipCertVerification = false;

    TString MonitoringConfig;
    TString MonitoringAddress;
    ui32 MonitoringPort = 0;
    ui32 MonitoringThreads = 0;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IEncryptionClientFactoryPtr EncryptionClientFactory;

    NCloud::NIamClient::IIamTokenClientPtr IamClient;

    mutable TLog Log;

    IMonitoringServicePtr Monitoring;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    IServerStatsPtr ClientStats;
    IStatsUpdaterPtr StatsUpdater;

    TClientAppConfigPtr ClientConfig;
    IClientPtr Client;
    IBlockStorePtr ClientEndpoint;

    IThrottlerPtr Throttler;

    TString VerboseLevel;
    bool EnableGrpcTracing = false;

    TDuration Timeout = TDuration::Zero();

    // Use protobuf syntax for request/response
    bool Proto = false;

    TString IamTokenFile;

    TString ClientPerformanceProfile;

    TString InputFile;
    std::unique_ptr<IInputStream> InputStream;

    TString ErrorFile;
    std::unique_ptr<IOutputStream> ErrorStream;

    TString OutputFile;
    std::shared_ptr<IOutputStream> OutputStream;

    NLastGetopt::TOpts Opts;
    std::unique_ptr<NLastGetopt::TOptsParseResultException> ParseResultPtr;

    TProgramShouldContinue ShouldContinue;

    std::shared_ptr<TClientFactories> ClientFactories;

    static constexpr TDuration WaitTimeout = TDuration::MilliSeconds(100);

public:
    TCommand(IBlockStorePtr client);
    virtual ~TCommand();

    TCommand(const TCommand& other) = delete;
    TCommand & operator=(const TCommand& other) = delete;

    void Prepare(const int argc, const char* argv[]);

    bool Execute();

    void Shutdown();

    bool Run(const int argc, const char* argv[]);

    void PrintUsage() const
    {
        Opts.PrintUsage(GetProgramName());
    }

    IInputStream& GetInputStream();
    IOutputStream& GetErrorStream();
    IOutputStream& GetOutputStream();

    void SetOutputStream(std::shared_ptr<IOutputStream> os);
    void SetClientFactories(std::shared_ptr<TClientFactories> clientFactories);

    static TString NormalizeCommand(TString command);

protected:
    virtual bool DoExecute() = 0;

    // For read/write/zero blocks requests
    NProto::TMountVolumeResponse MountVolume(
        TString diskId,
        TString mountToken,
        ISessionPtr& session,
        NProto::EVolumeAccessMode accessMode,
        bool mountLocal,
        bool throttlingDisabled,
        const NProto::TEncryptionSpec& encryptionSpec);

    bool UnmountVolume(ISession& session);

    template <typename T>
    T WaitFor(NThreading::TFuture<T> future)
    {
        if (!future.HasValue()) {
            auto* ptr = reinterpret_cast<NThreading::TFuture<void>*>(&future);
            if (!WaitForI(*ptr)) {
                return TErrorResponse(E_CANCELLED, "command was stopped");
            }
        }
        return future.GetValue();
    }

    void PrepareHeaders(NProto::THeaders& headers) const;

private:
    bool WaitForI(const NThreading::TFuture<void>& future);

    void Parse(const int argc, const char* argv[]);

    void Init();
    void InitLWTrace();
    void InitIamTokenClient();
    void InitClientConfig();

    TString GetIamTokenFromClient();

    void Start();
    void Stop();
};

using TCommandPtr = TIntrusivePtr<TCommand>;

}   // namespace NCloud::NBlockStore::NClient
