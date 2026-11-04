#include "monitoring.h"

#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/executor_pool_basic.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log_iface.h>
#include <contrib/ydb/library/actors/core/log_settings.h>
#include <contrib/ydb/library/actors/core/scheduler_basic.h>
#include <contrib/ydb/library/actors/http/http_proxy.h>
#include <contrib/ydb/library/actors/http/http_proxy_sock64.h>
#include <contrib/ydb/library/actors/interconnect/poller/poller_actor.h>
#include <contrib/ydb/library/actors/protos/services_common.pb.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>
#include <library/cpp/monlib/service/monservice.h>

#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/network/address.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

#include <chrono>
#include <future>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString DescribeEndpoint(const NKikimrConfig::TMonitoringConfig& config)
{
    TStringBuilder endpoint;
    endpoint << config.GetMonitoringAddress() << ":"
             << config.GetMonitoringPort();
    if (config.GetMonitoringCertificate()) {
        endpoint << " (inline certificate)";
    } else if (config.GetMonitoringCertificateFile()) {
        endpoint << " (certificate file: "
                 << config.GetMonitoringCertificateFile()
                 << ", private key file: "
                 << (config.GetMonitoringPrivateKeyFile()
                         ? config.GetMonitoringPrivateKeyFile()
                         : config.GetMonitoringCertificateFile())
                 << ")";
    }
    return endpoint;
}

////////////////////////////////////////////////////////////////////////////////

// The HTTP acceptor reports bind and certificate errors through actor logging.
// Forward those warnings to bootstrap's logger instead of silently dropping
// them in this temporary runtime, which has no normal YDB logging services.
class TMonitoringLoggerActor final
    : public NActors::TActorBootstrapped<TMonitoringLoggerActor>
{
private:
    struct TWarningState
    {
        NActors::TMonotonic NextReport;
        ui64 Suppressed = 0;
    };

    const TLog Log;
    const TString Endpoint;
    THashMap<TString, TWarningState> Warnings;

public:
    TMonitoringLoggerActor(TLog log, TString endpoint)
        : Log(std::move(log))
        , Endpoint(std::move(endpoint))
    {}

    void Bootstrap()
    {
        Become(&TMonitoringLoggerActor::StateWork);
    }

    void Handle(NActors::NLog::TEvLog::TPtr& ev)
    {
        if (!Log.IsOpen()) {
            return;
        }

        const auto& entry = *ev->Get();
        const auto priority = static_cast<ELogPriority>(entry.Level.ToPrio());
        ui64 suppressed = 0;
        if (priority == TLOG_WARNING &&
            entry.Component == NActorsServices::HTTP)
        {
            // The acceptor alternates the specific failure and a generic retry
            // warning every second. Track each message independently so both
            // are bounded, while a new failure cause is reported immediately.
            // Keep storage bounded even if unrelated warnings contain varying
            // request details during an indefinitely stalled initialization.
            if (Warnings.size() == 16 && !Warnings.contains(entry.Line)) {
                Warnings.clear();
            }
            auto [it, first] = Warnings.emplace(entry.Line, TWarningState{});
            auto& warning = it->second;
            const auto now = NActors::TActivationContext::Monotonic();
            if (!first && now < warning.NextReport) {
                ++warning.Suppressed;
                return;
            }
            suppressed = warning.Suppressed;
            warning.NextReport = now + TDuration::Minutes(1);
            warning.Suppressed = 0;
        }

        TStringBuilder message;
        message << "Initialization monitoring on " << Endpoint << ": "
                << entry.Line;
        if (suppressed) {
            message << " (suppressed " << suppressed << " repeats)";
        }
        Log.Write(priority, message);
    }

    STRICT_STFUNC(StateWork, hFunc(NActors::NLog::TEvLog, Handle);)
};

////////////////////////////////////////////////////////////////////////////////

// Adapt the HTTP backend's request to monlib so HTTPS and plain HTTP use the
// same counter-page renderer, including JSON, SPACK and Prometheus formats.
class TCountersRequest final: public NMonitoring::IHttpRequest
{
private:
    const NHttp::THttpIncomingRequest& Request;
    const TString Uri;
    const TString Path;
    const TCgiParameters Params;
    const TCgiParameters EmptyParams;
    const THttpHeaders Headers;

public:
    explicit TCountersRequest(const NHttp::THttpIncomingRequest& request)
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

class TCountersActor final: public NActors::TActorBootstrapped<TCountersActor>
{
private:
    const NKikimrConfig::TMonitoringConfig Config;
    const TDuration InactivityTimeout;
    NMonitoring::TMonService2& Pages;
    std::promise<void> Ready;

public:
    TCountersActor(
        const NKikimrConfig::TMonitoringConfig& config,
        NMonitoring::TMonService2& pages, std::promise<void> ready)
        : Config(config)
        // Validate before actor execution, so configuration errors reach the
        // caller's best-effort startup path instead of escaping an actor
        // handler.
        , InactivityTimeout(TDuration::Parse(config.GetInactivityTimeout()))
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
        port->Secure = Config.GetMonitoringCertificate() ||
                       Config.GetMonitoringCertificateFile();
        port->SslCertificatePem = Config.GetMonitoringCertificate();
        port->CertificateFile = Config.GetMonitoringCertificateFile();
        port->PrivateKeyFile = Config.GetMonitoringPrivateKeyFile();
        port->MaxRequestsPerSecond = Config.GetMaxRequestsPerSecond();
        port->InactivityTimeout = InactivityTimeout;
        Send(proxy, port.release());
        Become(&TCountersActor::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvConfirmListen::TPtr&)
    {
        Ready.set_value();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
    {
        const auto& request = ev->Get()->Request;
        NHttp::THttpOutgoingResponsePtr response;
        const auto path = request->URL.Before('?');
        if (request->Method != "GET") {
            const NHttp::THeadersBuilder headers{{"Allow", "GET"}};
            response =
                request->CreateResponse("405", "Method Not Allowed", headers);
        } else if (path != "/counters" && !path.StartsWith("/counters/")) {
            // No actor pages, tracing or authorization services exist in this
            // runtime. Expose only the pages needed for metric scrapes.
            response = request->CreateResponseNotFound();
        } else {
            try {
                TCountersRequest input(*request);
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TKikimrMonitoring::TImpl
{
    const NKikimrConfig::TMonitoringConfig Config;
    const TLog Log;
    const TString Endpoint;
    NMonitoring::TMonService2 Pages{0, "Service initialization"};
    // Destroy the runtime first so its actors cannot outlive Pages.
    std::unique_ptr<NActors::TActorSystem> ActorSystem;

    TImpl(
        const NKikimrConfig::TMonitoringConfig& config,
        NMonitoring::TDynamicCountersPtr counters, TLog log)
        : Config(config)
        , Log(std::move(log))
        , Endpoint(DescribeEndpoint(config))
    {
        // Pages only renders requests; its own plain HTTP server is never run.
        auto* page = new NMonitoring::TDynamicCountersPage(
            "counters", "Counters", std::move(counters));
        // Normal TMon returns 204 for groups that do not exist yet. Preserve
        // that contract while most service groups are absent during bootstrap.
        page->SetUnknownGroupPolicy(NMonitoring::EUnknownGroupPolicy::Ignore);
        Pages.Register(page);
    }

    void Start()
    {
        if (ActorSystem || !Config.GetMonitoringPort()) {
            return;
        }

        try {
            auto config = Config;
            if (const auto& address = config.GetMonitoringAddress(); address) {
                // Resolve on the caller's thread: resolution failures must
                // reach this catch block, not escape NHttp's acceptor actor.
                if (NHttp::IsIPv6(address)) {
                    // NHttp's IPv6 heuristic does not validate the literal.
                    in6_addr parsedAddress;
                    Y_ENSURE(
                        inet_pton(AF_INET6, address.data(), &parsedAddress) ==
                            1, "Invalid monitoring IPv6 address " << address);
                }
                TInet64StreamSocket socket(
                    TInet64StreamSocket::GuessAddressFamily(address));
                const auto bindAddress =
                    socket.MakeAddress(address, config.GetMonitoringPort());
                config.SetMonitoringAddress(NAddr::PrintHost(
                    NAddr::TOpaqueAddr(bindAddress->SockAddr())));
            }
            std::promise<void> ready;
            auto future = ready.get_future();
            // Construct before starting the runtime so invalid configuration is
            // caught here instead of escaping an actor's Bootstrap().
            auto actor = std::make_unique<TCountersActor>(
                config, Pages, std::move(ready));

            // NHttp provides the same HTTP/TLS transport as normal monitoring.
            // A temporary runtime lets it serve counters while registration and
            // CMS fetch block, without depending on the main actor system.
            auto setup = MakeHolder<NActors::TActorSystemSetup>();
            setup->ExecutorsCount = 1;
            setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[1]);
            setup->Executors[0] = new NActors::TBasicExecutorPool(
                0,
                Config.GetMonitoringThreads() ? Config.GetMonitoringThreads()
                                              : 1, 0, "BootstrapMon");
            setup->Scheduler =
                NActors::CreateSchedulerThread(NActors::TSchedulerConfig());
            const NActors::TActorId loggerId(0, "bootmonlog");
            auto logSettings = MakeIntrusive<NActors::NLog::TSettings>(
                loggerId, NActorsServices::LOGGER, NActors::NLog::PRI_WARN);
            logSettings->Append(
                NActorsServices::EServiceCommon_MIN,
                NActorsServices::EServiceCommon_MAX,
                NActorsServices::EServiceCommon_Name);
            setup->LocalServices.emplace_back(
                loggerId,
                NActors::TActorSetupCmd(
                    new TMonitoringLoggerActor(Log, Endpoint),
                    NActors::TMailboxType::ReadAsFilled, 0));
            setup->LocalServices.emplace_back(
                NActors::MakePollerActorId(),
                NActors::TActorSetupCmd(
                    NActors::CreatePollerActor(),
                    NActors::TMailboxType::ReadAsFilled, 0));
            ActorSystem = std::make_unique<NActors::TActorSystem>(
                setup, nullptr, std::move(logSettings));
            // Like other threads created during bootstrap.Init(), these
            // workers inherit the caller's signal mask. ConfigureSignals() has
            // already installed the process handlers in DoMain(). This early
            // runtime does not apply KikimrStart()'s additional worker masking;
            // process signals can therefore be delivered to these workers too.
            ActorSystem->Start();
            ActorSystem->Register(actor.release());

            // Wait for a healthy listener to bind before registration can
            // block. An unhealthy listener delays registration by at most five
            // seconds here, then lets initialization proceed while NHttp keeps
            // retrying independently of bootstrap and its scheduler.
            if (future.wait_for(std::chrono::seconds(5)) !=
                std::future_status::ready)
            {
                if (Log.IsOpen()) {
                    Log << TLOG_WARNING
                        << "Initialization monitoring listener is not ready on "
                        << Endpoint
                        << "; continuing daemon initialization while the "
                           "listener retries";
                }
                return;
            }
            future.get();
        } catch (...) {
            // Runtime setup failures are best effort too. Unlike the backend's
            // bind/TLS retries, these failures do not leave a runtime to retry.
            Stop();
            if (Log.IsOpen()) {
                Log << TLOG_WARNING
                    << "Cannot start initialization monitoring on " << Endpoint
                    << ": " << CurrentExceptionMessage()
                    << "; continuing daemon initialization";
            }
        }
    }

    void Stop()
    {
        // Destruction joins the temporary runtime and closes all sockets before
        // normal monitoring rebinds. No requests cross that transition.
        ActorSystem.reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TKikimrMonitoring::TKikimrMonitoring(
    const NKikimrConfig::TMonitoringConfig& config,
    const TString& serviceName, TLog log)
    : Counters(MakeIntrusive<NMonitoring::TDynamicCounters>())
    , InitializationCounters(Counters->GetSubgroup("counters", serviceName)
                                 ->GetSubgroup("component", "bootstrap"))
    , Impl(std::make_unique<TImpl>(config, Counters, std::move(log)))
{
    *InitializationCounters->GetCounter("Initializing", false) = 1;
}

TKikimrMonitoring::~TKikimrMonitoring()
{
    // Initialization may fail before a main actor system exists to shut down.
    Stop();
}

void TKikimrMonitoring::Start()
{
    Impl->Start();
}

void TKikimrMonitoring::Stop()
{
    Impl->Stop();
}

NMonitoring::TDynamicCountersPtr TKikimrMonitoring::GetCounters() const
{
    return Counters;
}

void TKikimrMonitoring::CompleteInitialization()
{
    // This gauge covers registration, CMS fetch and starting the main actor
    // system. Zero does not mean daemon readiness: other components and
    // endpoint restoration may still start later. Preserve it across the
    // rebind.
    *InitializationCounters->GetCounter("Initializing", false) = 0;
}

TKikimrMonitoringPtr CreateKikimrMonitoring(
    const NKikimrConfig::TMonitoringConfig& config,
    const TString& serviceName, TLog log)
{
    return std::make_shared<TKikimrMonitoring>(
        config, serviceName, std::move(log));
}

}   // namespace NCloud::NStorage
