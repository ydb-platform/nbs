#pragma once

#include "private.h"

#include <library/cpp/logger/priority.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    ELogPriority FiltrationLevel = TLOG_INFO;

    ui32 BlockSize = 4 * 1024;
    ui64 BlocksCount = 1024 * 1024;

    TString ListenUnixSocketPath;
    TString ListenAddress;
    ui32 ListenPort = 12345;

    bool StructuredReply = false;
    bool LimiterEnabled = true;
    ui32 MaxInFlightBytes = 128 * 1024 * 1024;
};

}   // namespace NCloud::NBlockStore::NBD
