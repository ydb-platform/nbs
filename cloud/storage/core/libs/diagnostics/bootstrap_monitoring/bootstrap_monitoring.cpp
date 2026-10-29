#include "bootstrap_monitoring.h"

#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/executor_pool_basic.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/scheduler_basic.h>
#include <contrib/ydb/library/actors/http/http_proxy.h>
#include <contrib/ydb/library/actors/interconnect/poller/poller_actor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>
#include <library/cpp/monlib/service/monservice.h>

#include <util/stream/str.h>

#include <chrono>
#include <future>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBootstrapRequest final: public NMonitoring::IHttpRequest
{
private:
    const NHttp::THttpIncomingRequest& Request;
    const TString Uri;
    const TString Path;
    const TCgiParameters Params;
    const TCgiParameters EmptyParams;
    const THttpHeaders Headers;

public:
    explicit TBootstrapRequest(const NHttp::THttpIncomingRequest& request)
        : Request(request)
        , Uri(request.URL)
        , Path(request.URL.Before('?'))
        , Params(request.URL.After('?'))
        , Headers(ParseHeaders(request.Headers))
    {}

    static THttpHeaders ParseHeaders(TStringBuf headers)
    {
        const TString data(headers);
        TStringInput input(data);
        return THttpHeaders(&input);
    }

    const char* GetURI() const override
    {
        return Uri.c_str();
    }

    const char* GetPath() const override
    {
        return Path.c_str();
    }

    const TCgiParameters& GetParams() const override
    {
        return Params;
    }

    const TCgiParameters& GetPostParams() const override
    {
        return EmptyParams;
    }

    TStringBuf GetPostContent() const override
    {
        return {};
    }

    HTTP_METHOD GetMethod() const override
    {
        return HTTP_METHOD_GET;
    }

    const THttpHeaders& GetHeaders() const override
    {
        return Headers;
    }

    TString GetRemoteAddr() const override
    {
        return Request.Address ? Request.Address->ToString() : TString();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBootstrapMonitoringActor final
    : public NActors::TActorBootstrapped<TBootstrapMonitoringActor>
{
private:
    const NKikimrConfig::TMonitoringConfig Config;
    NMonitoring::TMonService2& Pages;
    std::promise<void> Ready;

public:
    TBootstrapMonitoringActor(
        const NKikimrConfig::TMonitoringConfig& config,
        NMonitoring::TMonService2& pages, std::promise<void> ready)
        : Config(config)
        , Pages(pages)
        , Ready(std::move(ready))
    {}

    void Bootstrap()
    {
        const auto proxy = Register(NHttp::CreateHttpProxy());
        Send(proxy, new NHttp::TEvHttpProxy::TEvRegisterHandler("/", SelfId()));

        auto port = std::make_unique<NHttp::TEvHttpProxy::TEvAddListeningPort>(
            Config.GetMonitoringPort());
        port->Address = Config.GetMonitoringAddress();
        port->SslCertificatePem = Config.GetMonitoringCertificate();
        port->CertificateFile = Config.GetMonitoringCertificateFile();
        port->PrivateKeyFile = Config.GetMonitoringPrivateKeyFile();
        port->Secure =
            !port->SslCertificatePem.empty() || !port->CertificateFile.empty();
        port->MaxRequestsPerSecond = Config.GetMaxRequestsPerSecond();
        port->InactivityTimeout =
            TDuration::Parse(Config.GetInactivityTimeout());
        Send(proxy, port.release());
        Become(&TBootstrapMonitoringActor::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvConfirmListen::TPtr&)
    {
        Ready.set_value();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
    {
        const auto& request = ev->Get()->Request;
        NHttp::THttpOutgoingResponsePtr response;
        if (request->Method != "GET") {
            response = request->CreateResponse("405", "Method Not Allowed");
        } else {
            try {
                TBootstrapRequest input(*request);
                TStringStream output;
                Pages.ServeRequest(output, input);
                response = request->CreateResponseString(output.Str());
            } catch (...) {
                response =
                    request->CreateResponse("500", "Internal Server Error");
            }
        }
        Send(
            ev->Sender,
            new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STRICT_STFUNC(StateWork,
                  hFunc(NHttp::TEvHttpProxy::TEvConfirmListen, Handle);
                  hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);)
};

////////////////////////////////////////////////////////////////////////////////

class TBootstrapMonitoring final: public IMonitoringService
{
private:
    const NKikimrConfig::TMonitoringConfig Config;
    const NMonitoring::TDynamicCountersPtr Counters =
        MakeIntrusive<NMonitoring::TDynamicCounters>();
    const NMonitoring::TDynamicCounters::TCounterPtr Initializing;
    NMonitoring::TMonService2 Pages{0, "Service initialization"};
    std::unique_ptr<NActors::TActorSystem> ActorSystem;

public:
    TBootstrapMonitoring(
        const NKikimrConfig::TMonitoringConfig& config,
        const TString& serviceName)
        : Config(config)
        , Initializing(Counters->GetSubgroup("counters", serviceName)
                           ->GetSubgroup("component", "bootstrap")
                           ->GetCounter("Initializing", false))
    {
        *Initializing = 1;
        Pages.Register(new NMonitoring::TDynamicCountersPage(
            "counters", "Counters", Counters));
    }

    ~TBootstrapMonitoring() override
    {
        Stop();
    }

    void Start() override
    {
        if (!Config.GetMonitoringPort()) {
            return;
        }

        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[1]);
        setup->Executors[0] =
            new NActors::TBasicExecutorPool(0, 1, 0, "BootstrapMon");
        setup->Scheduler =
            NActors::CreateSchedulerThread(NActors::TSchedulerConfig());
        setup->LocalServices.emplace_back(
            NActors::MakePollerActorId(),
            NActors::TActorSetupCmd(
                NActors::CreatePollerActor(),
                NActors::TMailboxType::ReadAsFilled, 0));
        ActorSystem = std::make_unique<NActors::TActorSystem>(setup);
        ActorSystem->Start();

        std::promise<void> ready;
        auto future = ready.get_future();
        ActorSystem->Register(
            new TBootstrapMonitoringActor(Config, Pages, std::move(ready)));
        if (future.wait_for(std::chrono::seconds(5)) !=
            std::future_status::ready)
        {
            Stop();
            ythrow yexception() << "Cannot start bootstrap monitoring on "
                                << Config.GetMonitoringAddress() << ":"
                                << Config.GetMonitoringPort();
        }
        future.get();
    }

    void Stop() override
    {
        if (ActorSystem) {
            ActorSystem->Stop();
            ActorSystem->Cleanup();
            ActorSystem.reset();
        }
        *Initializing = 0;
    }

    NMonitoring::TDynamicCountersPtr GetCounters() override
    {
        return Counters;
    }

    NMonitoring::IMonPagePtr RegisterIndexPage(
        const TString& path, const TString& title) override
    {
        return Pages.RegisterIndexPage(path, title);
    }

    void RegisterMonPage(NMonitoring::IMonPagePtr page) override
    {
        Pages.Register(std::move(page));
    }

    NMonitoring::IMonPagePtr GetMonPage(const TString& path) override
    {
        return Pages.FindPage(path);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMonitoringServicePtr CreateBootstrapMonitoring(
    const NKikimrConfig::TMonitoringConfig& config, const TString& serviceName)
{
    return std::make_shared<TBootstrapMonitoring>(config, serviceName);
}

}   // namespace NCloud::NStorage
