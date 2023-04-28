#pragma once

#include "public.h"

class TLog;

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// TLog is saved to the global variable, which is used by grpc logging callback.
// This means it also has to be stored somewhere else in case grpc lifetime is
// bound to some other global
void GrpcLoggerInit(const TLog& log, bool enableTracing = false);

}   // namespace NCloud
