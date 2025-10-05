#pragma once

#include "private.h"
#include "spdk/rpc.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFsdev {

////////////////////////////////////////////////////////////////////////////////

struct TRpcFilestoreCreate
{
    char* Name = nullptr;

    TRpcFilestoreCreate() = default;
    ~TRpcFilestoreCreate();
    bool Decode(const struct spdk_json_val* params);
};

struct TRpcFilestoreDelete
{
    char* Name = nullptr;

    TRpcFilestoreDelete() = default;
    ~TRpcFilestoreDelete();
    bool Decode(const struct spdk_json_val* params);
};

}   // namespace NCloud::NFileStore::NFsdev
