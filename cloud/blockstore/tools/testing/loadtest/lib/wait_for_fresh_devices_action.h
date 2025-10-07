#pragma once

#include "app_context.h"
#include "aliased_volumes.h"
#include "volume_infos.h"

#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TWaitForFreshDevicesActionRunner
{
private:
    TLog& Log;
    const TAliasedVolumes& AliasedVolumes;
    ILoggingServicePtr Logging;
    IClientFactory& ClientFactory;
    TTestContext& TestContext;

public:
    TWaitForFreshDevicesActionRunner(
        TLog& log,
        const TAliasedVolumes& aliasedVolumes,
        ILoggingServicePtr logging,
        IClientFactory& clientFactory,
        TTestContext& testContext);

    int Run(const NProto::TActionGraph::TWaitForFreshDevicesAction& action);
};

}   // namespace NCloud::NBlockStore::NLoadTest
