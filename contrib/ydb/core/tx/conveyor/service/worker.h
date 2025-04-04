#pragma once
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/accessor/accessor.h>
#include <contrib/ydb/core/tx/conveyor/usage/abstract.h>
#include <contrib/ydb/library/services/services.pb.h>
#include <contrib/ydb/library/conclusion/result.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NKikimr::NConveyor {

class TWorkerTask {
private:
    YDB_READONLY_DEF(ITask::TPtr, Task);
    YDB_READONLY_DEF(NActors::TActorId, OwnerId);
    YDB_READONLY(TMonotonic, CreateInstant, TMonotonic::Now());
    YDB_READONLY_DEF(std::shared_ptr<TTaskSignals>, TaskSignals);
    std::optional<TMonotonic> StartInstant;
public:
    void OnBeforeStart() {
        StartInstant = TMonotonic::Now();
    }

    TMonotonic GetStartInstant() const {
        Y_ABORT_UNLESS(!!StartInstant);
        return *StartInstant;
    }

    TWorkerTask(ITask::TPtr task, const NActors::TActorId& ownerId, std::shared_ptr<TTaskSignals> taskSignals)
        : Task(task)
        , OwnerId(ownerId)
        , TaskSignals(taskSignals)
    {
        Y_ABORT_UNLESS(task);
    }

    bool operator<(const TWorkerTask& wTask) const {
        return Task->GetPriority() < wTask.Task->GetPriority();
    }
};

struct TEvInternal {
    enum EEv {
        EvNewTask = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvTaskProcessedResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        TWorkerTask Task;
    public:
        TEvNewTask() = default;

        const TWorkerTask& GetTask() const {
            return Task;
        }

        explicit TEvNewTask(const TWorkerTask& task)
            : Task(task) {
        }
    };

    class TEvTaskProcessedResult:
        public NActors::TEventLocal<TEvTaskProcessedResult, EvTaskProcessedResult>,
        public TConclusion<ITask::TPtr> {
    private:
        using TBase = TConclusion<ITask::TPtr>;
        YDB_READONLY_DEF(TMonotonic, StartInstant);
        YDB_READONLY_DEF(NActors::TActorId, OwnerId);
    public:
        TEvTaskProcessedResult(const TWorkerTask& originalTask, const TString& errorMessage)
            : TBase(TConclusionStatus::Fail(errorMessage))
            , StartInstant(originalTask.GetStartInstant())
            , OwnerId(originalTask.GetOwnerId()) {

        }
        TEvTaskProcessedResult(const TWorkerTask& originalTask, ITask::TPtr result)
            : TBase(result)
            , StartInstant(originalTask.GetStartInstant())
            , OwnerId(originalTask.GetOwnerId()) {

        }
    };
};

class TWorker: public NActors::TActorBootstrapped<TWorker> {
private:
    using TBase = NActors::TActorBootstrapped<TWorker>;
public:
    void HandleMain(TEvInternal::TEvNewTask::TPtr& ev);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInternal::TEvNewTask, HandleMain);
            default:
                ALS_ERROR(NKikimrServices::TX_CONVEYOR) << "unexpected event for task executor: " << ev->GetTypeRewrite();
                break;
        }
    }

    void Bootstrap() {
        Become(&TWorker::StateMain);
    }

    TWorker(const TString& conveyorName)
        : TBase("CONVEYOR::" + conveyorName + "::WORKER")
    {

    }
};

}
