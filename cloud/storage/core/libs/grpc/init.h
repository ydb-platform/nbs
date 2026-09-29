#pragma once

#include "public.h"

#include <util/datetime/base.h>

class TLog;

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// TGrpcInitializer is needed to call grpc_shutdown_blocking instead of
// grpc_shutdown see NBS-1032#5ea296701af0482eab4d6815
class TGrpcInitializer
{
public:
    TGrpcInitializer();
    ~TGrpcInitializer();
};

////////////////////////////////////////////////////////////////////////////////

// Waits for global gRPC shutdown without releasing any initialization
// references. Call after destroying all gRPC objects and TGrpcInitializer
// instances, with no concurrent initialization. Returns false if gRPC remains
// initialized at timeout. The timeout bounds polling, but cannot interrupt a
// gRPC internal mutex wait.
[[nodiscard]] bool WaitForGrpcShutdown(TDuration timeout);

////////////////////////////////////////////////////////////////////////////////

// TLog is saved to the global variable, which is used by grpc logging callback.
// This means it also has to be stored somewhere else in case grpc lifetime is
// bound to some other global.
// Logger should only be initialized once. Some tests call `GrpcLoggerInit`
// several times but the logger will be initialized only once, other calls will
// be ignored.
void GrpcLoggerInit(TLog log, bool enableTracing);

}   // namespace NCloud
