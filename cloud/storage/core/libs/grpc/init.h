#pragma once

#include "public.h"

class TLog;

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// TGrpcInitializer is needed to call grpc_shutdown_blocking instead of grpc_shutdown
// see NBS-1032#5ea296701af0482eab4d6815
class TGrpcInitializer
{
public:
    TGrpcInitializer();
    ~TGrpcInitializer();
};

////////////////////////////////////////////////////////////////////////////////

// TLog is saved to the global variable, which is used by grpc logging callback.
// This means it also has to be stored somewhere else in case grpc lifetime is
// bound to some other global
bool GrpcLoggerInit(TLog log, bool enableTracing);

}   // namespace NCloud
