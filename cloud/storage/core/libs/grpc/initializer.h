#pragma once

#include "public.h"

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

}   // namespace NCloud
