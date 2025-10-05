#include "bootstrap.h"

#include "logging.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NFileStore::NFsdev {

TBootstrap::TBootstrap()
{}

TBootstrap::~TBootstrap()
{}

void TBootstrap::Init()
{
    NCloud::TLogSettings logSettings;

    std::shared_ptr<TLogBackend> logBackend = CreateSpdkLogBackend();

    Logging = CreateLoggingService(std::move(logBackend), logSettings);
    Log = Logging->CreateLog("FSDEV");

    STORAGE_INFO("Init");
}

void TBootstrap::Start()
{
    STORAGE_INFO("Start");
}

void TBootstrap::Stop()
{
    STORAGE_INFO("Stop");
}

void TBootstrap::RpcFilestoreCreate(TString name)
{
    STORAGE_INFO("RpcFilestoreCreate: name=" << name);
}

void TBootstrap::RpcFilestoreDelete()
{
    STORAGE_INFO("RpcFilestoreDelete");
}

}   // namespace NCloud::NFileStore::NFsdev
