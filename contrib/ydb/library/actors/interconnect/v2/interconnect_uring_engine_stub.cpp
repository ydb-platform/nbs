#include "interconnect_uring_engine.h"

#include <contrib/ydb/library/actors/interconnect/interconnect_common.h> // TIntrusivePtr<TInterconnectProxyCommon> must be complete to be destroyed

namespace NActors {
    TUringEnginePtr CreateUringEngine(TIntrusivePtr<TInterconnectProxyCommon>) {
        return nullptr;
    }
}
