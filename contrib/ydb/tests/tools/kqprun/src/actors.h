#include <contrib/ydb/core/kqp/common/events/events.h>
#include <contrib/ydb/core/kqp/executer_actor/kqp_executer.h>


namespace NKqpRun {

NActors::IActor* CreateRunScriptActorMock(THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> request, NThreading::TPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr> promise, ui64 resultSizeLimit);

}  // namespace NKqpRun
