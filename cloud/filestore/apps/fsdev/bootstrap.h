#pragma once

#include "private.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFsdev {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    ILoggingServicePtr Logging;
    TLog Log;

public:
    TBootstrap();
    ~TBootstrap();

    void Init();

    void Start();
    void Stop();

    void RpcFilestoreCreate(TString name);
    void RpcFilestoreDelete();
};

}   // namespace NCloud::NFileStore::NFsdev
