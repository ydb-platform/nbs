#include "target.h"

#include "options.h"
#include "runnable.h"

#include <cloud/blockstore/libs/nbd/error_handler.h>
#include <cloud/blockstore/libs/nbd/server.h>
#include <cloud/blockstore/libs/nbd/server_handler.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/guid.h>
#include <util/network/address.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestTarget final
    : public IRunnable
{
private:
    TAtomic ShouldStop = 0;
    TAtomic ExitCode = 0;

    TMutex WaitMutex;
    TCondVar WaitCondVar;

    TOptionsPtr Options;

    ILoggingServicePtr Logging;
    TLog Log;

    NBD::IServerPtr Server;
    NBD::IServerHandlerFactoryPtr ServerHandlerFactory;

public:
    TTestTarget(TOptionsPtr options)
        : Options(std::move(options))
    {}

    int Run() override;
    void Stop(int exitCode) override;

private:
    void InitLogging();
    void Init();
    void Term();
    void WaitForShutdown();
};

////////////////////////////////////////////////////////////////////////////////

int TTestTarget::Run()
{
    InitLogging();

    STORAGE_INFO("Initializing...");
    try {
        Init();
        WaitForShutdown();
    } catch (...) {
        STORAGE_ERROR(
            "Error during initialization: " << CurrentExceptionMessage());
    }

    STORAGE_INFO("Terminating...");
    try {
        Term();
    } catch (...) {
        STORAGE_ERROR(
            "Error during shutdown: " << CurrentExceptionMessage());
    }

    return AtomicGet(ExitCode);
}

void TTestTarget::InitLogging()
{
    TLogSettings settings;

    if (Options->VerboseLevel) {
        auto level = GetLogLevel(Options->VerboseLevel);
        Y_ENSURE(level, "unknown log level: " << Options->VerboseLevel.Quote());
        settings.FiltrationLevel = *level;
    }

    Logging = CreateLoggingService("console", settings);
    Logging->Start();

    Log = Logging->CreateLog("BLOCKSTORE_TEST");
}

void TTestTarget::WaitForShutdown()
{
    with_lock (WaitMutex) {
        while (AtomicGet(ShouldStop) == 0) {
            WaitCondVar.WaitT(WaitMutex, WaitTimeout);
        }
    }
}

void TTestTarget::Stop(int exitCode)
{
    AtomicSet(ExitCode, exitCode);
    AtomicSet(ShouldStop, 1);

    WaitCondVar.Signal();
}

void TTestTarget::Init()
{
    NBD::TStorageOptions storageOptions = {
        .DiskId = CreateGuidAsString(),
        .ClientId = CreateGuidAsString(),
        .BlockSize = Options->BlockSize,
        .BlocksCount = Options->BlocksCount,
    };

    ServerHandlerFactory = NBD::CreateServerHandlerFactory(
        CreateDefaultDeviceHandlerFactory(),
        Logging,
        CreateStorageStub(),
        CreateServerStatsStub(),
        NBD::CreateErrorHandlerStub(),
        storageOptions);

    NBD::TServerConfig serverConfig = {
        .ThreadsCount = Options->ThreadsCount,
    };

    Server = CreateServer(Logging, serverConfig);
    Server->Start();

    auto listenAddress = TNetworkAddress(
        TUnixSocketPath(Options->SocketPath));

    auto future = Server->StartEndpoint(
        listenAddress,
        ServerHandlerFactory);
    CheckError(future.GetValue(WaitTimeout));
}

void TTestTarget::Term()
{
    if (Server) {
        Server->Stop();
    }

    if (Logging) {
        Logging->Stop();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRunnablePtr CreateTestTarget(TOptionsPtr options)
{
    return std::make_shared<TTestTarget>(std::move(options));
}

}   // namespace NCloud::NBlockStore
