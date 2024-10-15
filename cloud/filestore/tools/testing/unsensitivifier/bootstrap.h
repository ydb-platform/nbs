#pragma once

#include "options.h"

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/tools/testing/loadtest/lib/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/logger/log.h>

namespace NCloud::NFileStore::NUnsensitivifier {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    TOptionsPtr Options;

public:
    TBootstrap(TOptionsPtr options);
    ~TBootstrap();

    [[nodiscard]] TOptionsPtr GetOptions() const
    {
        return Options;
    }

private:
    void InitDbgConfig();
    void InitClientConfig();
};

}   // namespace NCloud::NFileStore::NUnsensitivifier
