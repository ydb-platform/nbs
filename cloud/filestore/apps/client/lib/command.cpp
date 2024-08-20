#include "command.h"

#include <cloud/filestore/libs/client/durable.h>
#include <cloud/filestore/libs/client/probes.h>
#include <cloud/filestore/libs/vfs/probes.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/hostname.h>
#include <util/system/sysstat.h>

#include <filesystem>

namespace NCloud::NFileStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(1);

const TString DefaultConfigFile = "/Berkanavt/nfs-server/cfg/nfs-client.txt";
const TString DefaultIamTokenFile = "~/.nfs-client/iam-token";

////////////////////////////////////////////////////////////////////////////////

TString GetIamTokenFromFile(const TString& iamTokenFile)
{
    auto path = TFsPath(iamTokenFile).RealPath();
    TFile file(
        path.GetPath(),
        EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);

    auto stats = std::filesystem::status(
            std::filesystem::path(path.GetPath().c_str()));
    auto perms = stats.permissions();

    Y_ENSURE(
        (perms & std::filesystem::perms::others_all) == std::filesystem::perms::none,
        TStringBuilder() << "bad Mode: " << static_cast<ui32>(perms));

    if (!file.IsOpen()) {
        return {};
    }

    return Strip(TFileInput(file).ReadAll());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommand::TCommand()
{
    Opts.AddHelpOption('h');
    Opts.AddVersionOption();

    Opts.AddLongOption("verbose")
        .OptionalArgument("STR")
        .DefaultValue("warn")
        .StoreResult(&VerboseLevel);

    Opts.AddLongOption("mon-address")
        .RequiredArgument("STR")
        .StoreResult(&MonitoringAddress);

    Opts.AddLongOption("mon-port")
        .RequiredArgument("NUM")
        .StoreResult(&MonitoringPort);

    Opts.AddLongOption("mon-threads")
        .RequiredArgument("NUM")
        .StoreResult(&MonitoringThreads);

    Opts.AddLongOption("server-address")
        .RequiredArgument("STR")
        .StoreResult(&ServerAddress);

    Opts.AddLongOption("server-port")
        .RequiredArgument("NUM")
        .StoreResult(&ServerPort);

    Opts.AddLongOption("secure-port", "connect secure port (overrides --server-port)")
        .RequiredArgument("NUM")
        .StoreResult(&SecurePort);

    Opts.AddLongOption("skip-cert-verification", "skip server certificate verification")
        .StoreTrue(&SkipCertVerification);

    Opts.AddLongOption("iam-token-file", "path to iam token")
        .RequiredArgument("STR")
        .StoreResult(&IamTokenFile);

    Opts.AddLongOption("config")
        .Help(TStringBuilder()
            << "config file name. Default is "
            << DefaultConfigFile)
        .RequiredArgument("STR")
        .DefaultValue(DefaultConfigFile)
        .StoreResult(&ConfigFile);

    Opts.AddLongOption("json")
        .StoreTrue(&JsonOutput);
}

int TCommand::Run(int argc, char** argv)
{
    OptsParseResult.ConstructInPlace(&Opts, argc, argv);

    Init();
    Start();

    if (!Execute()) {
        // wait until operation completed
        with_lock (WaitMutex) {
            while (ProgramShouldContinue.PollState() == TProgramShouldContinue::Continue) {
                WaitCondVar.WaitT(WaitMutex, WaitTimeout);
            }
        }
    }

    Stop();

    return ProgramShouldContinue.GetReturnCode();
}

void TCommand::Stop(int exitCode)
{
    ProgramShouldContinue.ShouldStop(exitCode);
    WaitCondVar.Signal();
}

bool TCommand::WaitForI(const TFuture<void>& future)
{
    while (ProgramShouldContinue.PollState() == TProgramShouldContinue::Continue) {
        if (future.Wait(WaitTimeout)) {
            return true;
        }
    }
    return false;
}

void TCommand::Init()
{
    LogSettings.UseLocalTimestamps = true;
    if (!VerboseLevel.empty()) {
        auto level = GetLogLevel(VerboseLevel);
        Y_ENSURE(level, "unknown log level: " << VerboseLevel.Quote());

        LogSettings.FiltrationLevel = *level;
    }

    Logging = CreateLoggingService("console", LogSettings);
    Log = Logging->CreateLog("NFS_CLIENT");

    if (MonitoringPort) {
        Monitoring = CreateMonitoringService(
            MonitoringPort,
            MonitoringAddress,
            MonitoringThreads);
    } else {
        Monitoring = CreateMonitoringServiceStub();
    }

    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(FILESTORE_CLIENT_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(FILESTORE_VFS_PROVIDER));

    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();

    NProto::TClientAppConfig appConfig;
    if (NFs::Exists(ConfigFile)) {
        ParseFromTextFormat(ConfigFile, appConfig);
    }

    auto& config = *appConfig.MutableClientConfig();
    if (ServerAddress) {
        config.SetHost(ServerAddress);
    }
    if (ServerPort) {
        config.SetPort(ServerPort);
    }
    if (SecurePort) {
        config.SetSecurePort(SecurePort);
    }
    if (config.GetHost() == "localhost" &&
        config.GetSecurePort() != 0)
    {
        // With TLS on transform localhost into fully qualified domain name.
        config.SetHost(FQDNHostName());
    }
    if (SkipCertVerification) {
        config.SetSkipCertVerification(SkipCertVerification);
    }

    if (!IamTokenFile) {
        IamTokenFile = DefaultIamTokenFile;
    }

    // Do not send token via insecure channel.
    if (config.GetSecurePort() != 0) {
        auto iamToken = GetEnv("IAM_TOKEN");
        if (!iamToken) {
            iamToken = GetIamTokenFromFile(IamTokenFile);
        }
        config.SetAuthToken(std::move(iamToken));
    }

    ClientConfig = std::make_shared<TClientConfig>(config);
}

void TCommand::Start()
{
    if (Scheduler) {
        Scheduler->Start();
    }

    if (Logging) {
        Logging->Start();
    }

    if (Monitoring) {
        Monitoring->Start();
    }
}

void TCommand::Stop()
{
    if (Monitoring) {
        Monitoring->Stop();
    }

    if (Logging) {
        Logging->Stop();
    }

    if (Scheduler) {
        Scheduler->Stop();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TFileStoreServiceCommand::Init()
{
    TCommand::Init();

    Client = CreateDurableClient(
        Logging,
        Timer,
        Scheduler,
        CreateRetryPolicy(ClientConfig),
        CreateFileStoreClient(ClientConfig, Logging));
}

void TFileStoreServiceCommand::Start()
{
    TCommand::Start();

    if (Client) {
        Client->Start();
    }
}

void TFileStoreServiceCommand::Stop()
{
    if (Client) {
        Client->Stop();
    }

    TCommand::Stop();
}

TFileStoreCommand::TFileStoreCommand()
{
    ClientId = CreateGuidAsString();

    Opts.AddLongOption("filesystem")
        .Required()
        .RequiredArgument("STR")
        .StoreResult(&FileSystemId);

    Opts.AddLongOption("disable-multitablet-forwarding")
        .NoArgument()
        .SetFlag(&DisableMultiTabletForwarding);
}

////////////////////////////////////////////////////////////////////////////////

void TFileStoreCommand::Start()
{
    TFileStoreServiceCommand::Start();
}

void TFileStoreCommand::Stop()
{
    TFileStoreServiceCommand::Stop();
}

TFileStoreCommand::TSessionGuard TFileStoreCommand::CreateSession()
{
    // TODO use ISession instead
    auto request = std::make_shared<NProto::TCreateSessionRequest>();
    request->SetFileSystemId(FileSystemId);
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetRestoreClientSession(true);

    TCallContextPtr ctx = MakeIntrusive<TCallContext>();
    auto response = WaitFor(Client->CreateSession(ctx, std::move(request)));
    CheckResponse(response);

    auto sessionId = response.GetSession().GetSessionId();

    Headers.SetClientId(ClientId);
    Headers.SetSessionId(sessionId);
    Headers.SetDisableMultiTabletForwarding(DisableMultiTabletForwarding);

    return TSessionGuard(*this);
}

void TFileStoreCommand::DestroySession()
{
    if (Headers.GetSessionId().Empty()) {
        return;
    }

    auto request = std::make_shared<NProto::TDestroySessionRequest>();
    request->SetFileSystemId(FileSystemId);
    request->MutableHeaders()->SetSessionId(Headers.GetSessionId());
    request->MutableHeaders()->SetClientId(Headers.GetClientId());

    TCallContextPtr ctx = MakeIntrusive<TCallContext>();
    auto response = WaitFor(Client->DestroySession(ctx, std::move(request)));
    CheckResponse(response);
}

////////////////////////////////////////////////////////////////////////////////

NProto::TNodeAttr TFileStoreCommand::ResolveNode(
    ui64 parentNodeId,
    TString name,
    bool ignoreMissing)
{
    const auto invalidNodeId = Max<ui64>();

    auto makeInvalidNode = [&] () {
        NProto::TNodeAttr node;
        node.SetType(NProto::E_INVALID_NODE);   // being explicit about the type
        node.SetId(invalidNodeId);
        return node;
    };

    if (parentNodeId == invalidNodeId) {
        return makeInvalidNode();
    }

    auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
    request->SetNodeId(parentNodeId);
    request->SetName(std::move(name));

    auto response = WaitFor(Client->GetNodeAttr(
        PrepareCallContext(),
        std::move(request)));

    const auto code = MAKE_FILESTORE_ERROR(NProto::E_FS_NOENT);
    if (ignoreMissing && response.GetError().GetCode() == code) {
        return makeInvalidNode();
    }

    CheckResponse(response);

    return response.GetNode();
}

TVector<TFileStoreCommand::TPathEntry> TFileStoreCommand::ResolvePath(
    TStringBuf path,
    bool ignoreMissing)
{
    TStringBuf tok;
    TStringBuf it(path);

    TVector<TPathEntry> result;
    result.emplace_back();
    result.back().Node.SetId(RootNodeId);
    result.back().Node.SetType(NProto::E_DIRECTORY_NODE);

    while (it.NextTok('/', tok)) {
        if (tok) {
            auto node = ResolveNode(
                result.back().Node.GetId(),
                ToString(tok),
                ignoreMissing
            );
            result.push_back({node, tok});
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEndpointCommand::TEndpointCommand()
{
}

void TEndpointCommand::Init()
{
    TCommand::Init();

    Client = CreateDurableClient(
        Logging,
        Timer,
        Scheduler,
        CreateRetryPolicy(ClientConfig),
        CreateEndpointManagerClient(ClientConfig, Logging));
}

void TEndpointCommand::Start()
{
    TCommand::Start();

    if (Client) {
        Client->Start();
    }
}

void TEndpointCommand::Stop()
{
    if (Client) {
        Client->Stop();
    }

    TCommand::Stop();
}

}   // namespace NCloud::NFileStore::NClient
