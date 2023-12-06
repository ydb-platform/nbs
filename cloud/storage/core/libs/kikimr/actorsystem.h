#pragma once

#include "public.h"

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/util/should_continue.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IActorSystem
    : public virtual TThrRefBase
    , public virtual IStartable
    , public ILoggingService
    , public IMonitoringService
{
    virtual NActors::TActorId Register(
        NActors::IActorPtr actor,
        TStringBuf executorName = {}) = 0;

    virtual bool Send(
        const NActors::TActorId& recipient,
        NActors::IEventBasePtr event) = 0;

    virtual TProgramShouldContinue& GetProgramShouldContinue() = 0;
};

}   // namespace NCloud
