#include "initializer.h"

#include <contrib/libs/grpc/include/grpc/grpc.h>

#include <util/generic/singleton.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TGrpcState
{
    TAtomic Counter = 0;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TGrpcInitializer::TGrpcInitializer()
{
    auto state = Singleton<TGrpcState>();

    if (AtomicGetAndIncrement(state->Counter) == 0) {
        grpc_init();
    }
}

TGrpcInitializer::~TGrpcInitializer()
{
    auto state = Singleton<TGrpcState>();

    if (AtomicDecrement(state->Counter) == 0) {
        grpc_shutdown_blocking();
    }
}

}   // namespace NCloud
