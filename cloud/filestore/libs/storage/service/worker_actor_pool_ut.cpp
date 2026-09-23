#include "worker_actor_pool.h"

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>

#include <memory>
#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestWorkerActor final: public TActor<TTestWorkerActor>
{
private:
    THashSet<TActorId>& CreatedActors;
    THashSet<TActorId>& PoisonedActors;

public:
    TTestWorkerActor(
        THashSet<TActorId>& createdActors,
        THashSet<TActorId>& poisonedActors)
        : TActor(&TThis::StateWork)
        , CreatedActors(createdActors)
        , PoisonedActors(poisonedActors)
    {}

    void Registered(TActorSystem* sys, const TActorId& owner) override
    {
        TActor::Registered(sys, owner);
        UNIT_ASSERT(CreatedActors.insert(SelfId()).second);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr&,
        const TActorContext& ctx)
    {
        UNIT_ASSERT(PoisonedActors.insert(ctx.SelfID).second);
        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    static constexpr size_t InitialSize = 100;

    THashSet<TActorId> CreatedActors;
    THashSet<TActorId> PoisonedActors;
    TTestActorRuntimeBase Runtime;
    std::unique_ptr<TWorkerActorPool> Pool;

    TFixture()
    {
        Runtime.Initialize();
        Pool = std::make_unique<TWorkerActorPool>(
            [&](TActorId) {
                return new TTestWorkerActor(CreatedActors, PoisonedActors);
            });
        RunInActorContext([&](const TActorContext& ctx) {
            Pool->Initialize(ctx, InitialSize);
        });
    }

    void RunInActorContext(std::function<void(const TActorContext&)> action)
    {
        // Provides an actor context for the pool operations.
        class TTestActionActor final: public TActorBootstrapped<TTestActionActor>
        {
        private:
            const std::function<void(const TActorContext&)> Action;

        public:
            explicit TTestActionActor(std::function<void(const TActorContext&)> action)
                : Action(std::move(action))
            {}

            void Bootstrap(const TActorContext& ctx)
            {
                Action(ctx);
                Die(ctx);
            }
        };

        bool done = false;
        Runtime.Register(new TTestActionActor([&](const TActorContext& ctx) {
            action(ctx);
            done = true;
        }));

        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return done; };
        Runtime.DispatchEvents(options);
        UNIT_ASSERT(done);
    }

    TVector<TActorId> AcquireActors(size_t count)
    {
        TVector<TActorId> actors;
        RunInActorContext([&](const TActorContext& ctx) {
            for (size_t i = 0; i < count; ++i) {
                actors.push_back(Pool->GetOrCreateActor(ctx));
            }
        });
        return actors;
    }

    void ReleaseActor(const TActorId& id)
    {
        RunInActorContext([&](const TActorContext&) {
            Pool->ReleaseActor(id);
        });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWorkerActorPoolTest)
{
    Y_UNIT_TEST_F(ShouldRegisterInitialActors, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(InitialSize, Pool->GetSize());
        UNIT_ASSERT_VALUES_EQUAL(InitialSize, CreatedActors.size());
        for (const auto& id: CreatedActors) {
            UNIT_ASSERT(Runtime.FindActor(id));
        }
    }

    Y_UNIT_TEST_F(ShouldAcquireDistinctActorsWithoutCreatingMore, TFixture)
    {
        const auto actors = AcquireActors(InitialSize);
        THashSet<TActorId> acquiredActors;
        for (const auto& id: actors) {
            UNIT_ASSERT(CreatedActors.contains(id));
            UNIT_ASSERT(acquiredActors.insert(id).second);
        }
        UNIT_ASSERT_VALUES_EQUAL(InitialSize, CreatedActors.size());
        UNIT_ASSERT_VALUES_EQUAL(InitialSize, Pool->GetSize());
    }

    Y_UNIT_TEST_F(ShouldCreateActorWhenPoolIsExhausted, TFixture)
    {
        const auto actors = AcquireActors(InitialSize);
        const auto extraActor = AcquireActors(1).front();

        for (const auto& id: actors) {
            UNIT_ASSERT_VALUES_UNEQUAL(id, extraActor);
        }
        UNIT_ASSERT(CreatedActors.contains(extraActor));
        UNIT_ASSERT(Runtime.FindActor(extraActor));
        UNIT_ASSERT_VALUES_EQUAL(InitialSize + 1, CreatedActors.size());
        UNIT_ASSERT_VALUES_EQUAL(InitialSize + 1, Pool->GetSize());
    }

    Y_UNIT_TEST_F(ShouldReuseReleasedActor, TFixture)
    {
        const auto actors = AcquireActors(InitialSize);
        const auto releasedActor = actors.front();

        ReleaseActor(releasedActor);
        const auto reusedActor = AcquireActors(1).front();

        UNIT_ASSERT_VALUES_EQUAL(releasedActor, reusedActor);
        UNIT_ASSERT_VALUES_EQUAL(InitialSize, CreatedActors.size());
        UNIT_ASSERT_VALUES_EQUAL(InitialSize, Pool->GetSize());
    }

    Y_UNIT_TEST_F(ShouldPoisonAllActorsOnDestruction, TFixture)
    {
        const auto actors = AcquireActors(InitialSize + 1);
        // Leave one actor free and the other 100 checked out.
        ReleaseActor(actors.back());
        UNIT_ASSERT_VALUES_EQUAL(InitialSize + 1, CreatedActors.size());
        Pool.reset();

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return PoisonedActors.size() == InitialSize + 1;
        };
        Runtime.DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(InitialSize + 1, PoisonedActors.size());
        for (const auto& id: CreatedActors) {
            UNIT_ASSERT(PoisonedActors.contains(id));
            UNIT_ASSERT(!Runtime.FindActor(id));
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
