#pragma once

#include <contrib/ydb/core/kqp/common/events/events.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NKikimr::NPersQueueTests {

class TKqpProxyServiceMock : public TActorBootstrapped<TKqpProxyServiceMock> {
public:
    void Bootstrap();

    STFUNC(StateWork);

    void Handle(NKqp::TEvKqp::TEvQueryRequest::TPtr& ev, const TActorContext& ctx);

    ui64 NextWriteId = 1;
};

}
