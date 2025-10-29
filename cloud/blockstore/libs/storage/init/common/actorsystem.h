#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/kikimr/public.h>

#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/driver_lib/run/run.h>
#include <ydb/core/driver_lib/run/service_initializer.h>

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/util/should_continue.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>

#include <functional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TActorSystemArgs
{
    std::shared_ptr<NKikimr::TModuleFactories> ModuleFactories;
    ui32 NodeId;
    NActors::TScopeId ScopeId;
    NKikimrConfig::TAppConfigPtr AppConfig;
    IAsyncLoggerPtr AsyncLogger;
    std::function<void(
        NKikimr::TKikimrRunConfig&,
        NKikimr::TServiceInitializersList&)> OnInitialize;
    std::function<void(
        NKikimr::TKikimrRunConfig&)> PrepareKikimrRunConfig;
    NKikimr::TBasicKikimrServicesMask ServicesMask;
    std::function<void(IActorSystem&)> OnStart;
};

////////////////////////////////////////////////////////////////////////////////

class TActorSystem final
    : public NKikimr::TKikimrRunner
    , public IActorSystem
{
private:
    const TActorSystemArgs Args;

    bool Running;

    // in case KiKiMR monitoring not configured
    TIntrusivePtr<NMonitoring::TIndexMonPage> IndexMonPage =
        new NMonitoring::TIndexMonPage("", "");

public:
    TActorSystem(const TActorSystemArgs& args);

    void Init();

    void Start() override;
    void Stop() override;

    NActors::TActorId Register(
        NActors::IActorPtr actor,
        TStringBuf executorName) override;
    bool Send(
        const NActors::TActorId& recipient,
        NActors::IEventBasePtr event) override;

    TLog CreateLog(const TString& component) override;

    NMonitoring::IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override;
    void RegisterMonPage(NMonitoring::IMonPagePtr page) override;
    NMonitoring::IMonPagePtr GetMonPage(const TString& path) override;
    NMonitoring::TDynamicCountersPtr GetCounters() override;

    TProgramShouldContinue& GetProgramShouldContinue() override;
};

}   // namespace NCloud::NBlockStore::NStorage
