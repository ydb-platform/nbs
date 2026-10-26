#include "filesystem_event_subscriptions.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration SealPeriod = TDuration::Minutes(30);

const TInstant T0 = TInstant::Hours(1);

TActorId MakePipe(ui32 i)
{
    return TActorId(1, 1, i + 1, 1);
}

const TActorId Pipe1 = MakePipe(1);
const TActorId Pipe2 = MakePipe(2);

TVector<TActorId> Sorted(TVector<TActorId> ids)
{
    Sort(ids);
    return ids;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileSystemEventSubscriptionsTest)
{
    Y_UNIT_TEST(ShouldExtractNodeSubscribers)
    {
        TFileSystemEventSubscriptions subscriptions(SealPeriod);
        subscriptions.AddNodeSubscriber(10, Pipe1, T0);
        subscriptions.AddNodeSubscriber(10, Pipe2, T0);
        subscriptions.AddNodeSubscriber(10, Pipe1, T0);
        subscriptions.AddNodeSubscriber(20, Pipe2, T0);
        auto stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(2, stats.NodeCount);
        UNIT_ASSERT_VALUES_EQUAL(2, stats.ClientCount);

        UNIT_ASSERT_VALUES_EQUAL(
            Sorted({Pipe1, Pipe2}),
            Sorted(subscriptions.ExtractNodeSubscribers(10, T0)));
        stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1, stats.NodeCount);

        //
        // Extraction removes the Node.
        //

        UNIT_ASSERT(subscriptions.ExtractNodeSubscribers(10, T0).empty());
        UNIT_ASSERT(subscriptions.ExtractNodeSubscribers(30, T0).empty());

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TActorId>{Pipe2},
            subscriptions.ExtractNodeSubscribers(20, T0));
        stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.NodeCount);
    }

    Y_UNIT_TEST(ShouldExtractNodeRefSubscribers)
    {
        TFileSystemEventSubscriptions subscriptions(SealPeriod);
        subscriptions.AddNodeRefSubscriber(1, "a", Pipe1, T0);
        subscriptions.AddNodeRefSubscriber(1, "a", Pipe2, T0);
        subscriptions.AddNodeRefSubscriber(1, "b", Pipe1, T0);
        subscriptions.AddNodeRefSubscriber(2, "a", Pipe1, T0);
        auto stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(3, stats.NodeRefCount);

        UNIT_ASSERT_VALUES_EQUAL(
            Sorted({Pipe1, Pipe2}),
            Sorted(subscriptions.ExtractNodeRefSubscribers(1, "a", T0)));
        UNIT_ASSERT(
            subscriptions.ExtractNodeRefSubscribers(1, "a", T0).empty());
        UNIT_ASSERT(
            subscriptions.ExtractNodeRefSubscribers(1, "c", T0).empty());

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TActorId>{Pipe1},
            subscriptions.ExtractNodeRefSubscribers(1, "b", T0));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TActorId>{Pipe1},
            subscriptions.ExtractNodeRefSubscribers(2, "a", T0));
        stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.NodeRefCount);
    }

    Y_UNIT_TEST(ShouldExtractNodeXAttrSubscribers)
    {
        TFileSystemEventSubscriptions subscriptions(SealPeriod);
        subscriptions.AddNodeXAttrSubscriber(1, "user.a", Pipe1, T0);
        subscriptions.AddNodeXAttrSubscriber(1, "user.a", Pipe2, T0);
        subscriptions.AddNodeXAttrSubscriber(1, "user.b", Pipe1, T0);
        subscriptions.AddNodeXAttrSubscriber(2, "user.a", Pipe1, T0);
        UNIT_ASSERT_VALUES_EQUAL(3, subscriptions.GetNodeXAttrCount());

        //
        // NodeXAttrs, Nodes and NodeRefs are tracked independently.
        //

        UNIT_ASSERT_VALUES_EQUAL(0, subscriptions.GetNodeCount());
        UNIT_ASSERT_VALUES_EQUAL(0, subscriptions.GetNodeRefCount());
        UNIT_ASSERT(subscriptions.ExtractNodeSubscribers(1, T0).empty());
        UNIT_ASSERT(
            subscriptions.ExtractNodeRefSubscribers(1, "user.a", T0).empty());

        UNIT_ASSERT_VALUES_EQUAL(
            Sorted({Pipe1, Pipe2}),
            Sorted(subscriptions.ExtractNodeXAttrSubscribers(1, "user.a", T0)));
        UNIT_ASSERT(
            subscriptions.ExtractNodeXAttrSubscribers(1, "user.a", T0).empty());
        UNIT_ASSERT(
            subscriptions.ExtractNodeXAttrSubscribers(1, "user.c", T0).empty());

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TActorId>{Pipe1},
            subscriptions.ExtractNodeXAttrSubscribers(1, "user.b", T0));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TActorId>{Pipe1},
            subscriptions.ExtractNodeXAttrSubscribers(2, "user.a", T0));
        UNIT_ASSERT_VALUES_EQUAL(0, subscriptions.GetNodeXAttrCount());
    }

    Y_UNIT_TEST(ShouldStoreClientsInFirstPage)
    {
        TFileSystemEventSubscriptions subscriptions(SealPeriod);

        //
        // 64 clients - indices 0..63, the whole first page - read Node 1, the
        // even ones also read Node 2.
        //

        TVector<TActorId> all;
        TVector<TActorId> even;
        for (ui32 i = 0; i < 64; ++i) {
            const auto pipe = MakePipe(i);
            subscriptions.AddNodeSubscriber(1, pipe, T0);
            all.push_back(pipe);
            if (i % 2 == 0) {
                subscriptions.AddNodeSubscriber(2, pipe, T0);
                even.push_back(pipe);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(
            Sorted(even),
            Sorted(subscriptions.ExtractNodeSubscribers(2, T0)));
        UNIT_ASSERT_VALUES_EQUAL(
            Sorted(all),
            Sorted(subscriptions.ExtractNodeSubscribers(1, T0)));
    }

    Y_UNIT_TEST(ShouldStoreClientsBeyondFirstPage)
    {
        TFileSystemEventSubscriptions subscriptions(SealPeriod);

        //
        // Clients 0..199 read distinct Nodes, so the generation knows 200
        // clients - 4 pages of the bitmap. Node 1000 is read by clients from
        // the first, the second and the fourth pages only: no page is created
        // for the third one, and only these clients are notified.
        //

        for (ui32 i = 0; i < 200; ++i) {
            subscriptions.AddNodeSubscriber(i, MakePipe(i), T0);
        }

        const TVector<ui32> readers = {0, 63, 64, 127, 192, 199};
        TVector<TActorId> expected;
        for (const ui32 i: readers) {
            subscriptions.AddNodeSubscriber(1000, MakePipe(i), T0);
            subscriptions.AddNodeRefSubscriber(1, "x", MakePipe(i), T0);
            expected.push_back(MakePipe(i));
        }

        auto stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(201, stats.NodeCount);
        UNIT_ASSERT_VALUES_EQUAL(1, stats.NodeRefCount);
        UNIT_ASSERT_VALUES_EQUAL(200, stats.ClientCount);
        // (200 - 64) + 2 * 2 from readers
        UNIT_ASSERT_VALUES_EQUAL(140, stats.ExtraPageCount);

        UNIT_ASSERT_VALUES_EQUAL(
            Sorted(expected),
            Sorted(subscriptions.ExtractNodeSubscribers(1000, T0)));
        UNIT_ASSERT_VALUES_EQUAL(
            Sorted(expected),
            Sorted(subscriptions.ExtractNodeRefSubscribers(1, "x", T0)));

        //
        // The other Nodes are not affected.
        //

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TActorId>{MakePipe(150)},
            subscriptions.ExtractNodeSubscribers(150, T0));

        stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(199, stats.NodeCount);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.NodeRefCount);
        UNIT_ASSERT_VALUES_EQUAL(200, stats.ClientCount);
        UNIT_ASSERT_VALUES_EQUAL(135, stats.ExtraPageCount);
    }

    Y_UNIT_TEST(ShouldSealAndDropGenerations)
    {
        TFileSystemEventSubscriptions subscriptions(SealPeriod);

        subscriptions.AddNodeSubscriber(1, Pipe1, T0);
        subscriptions.AddNodeRefSubscriber(1, "a", Pipe1, T0);

        //
        // The first registration after the seal period seals the generation
        // with Node 1 and NodeRef (1, "a").
        //

        const auto t1 = T0 + SealPeriod;
        subscriptions.AddNodeSubscriber(2, Pipe2, t1);
        auto stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(2, stats.NodeCount);
        UNIT_ASSERT_VALUES_EQUAL(1, stats.NodeRefCount);

        //
        // Both generations are consulted.
        //

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TActorId>{Pipe1},
            subscriptions.ExtractNodeSubscribers(1, t1));

        //
        // One more seal period: the generation with NodeRef (1, "a") is
        // dropped, the generation with Node 2 is sealed.
        //

        const auto t2 = t1 + SealPeriod;
        UNIT_ASSERT(
            subscriptions.ExtractNodeRefSubscribers(1, "a", t2).empty());
        stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1, stats.NodeCount);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.NodeRefCount);

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TActorId>{Pipe2},
            subscriptions.ExtractNodeSubscribers(2, t2));
    }

    Y_UNIT_TEST(ShouldMergeSubscribersOfBothGenerations)
    {
        TFileSystemEventSubscriptions subscriptions(SealPeriod);

        subscriptions.AddNodeSubscriber(1, Pipe1, T0);
        subscriptions.AddNodeSubscriber(1, Pipe2, T0);

        const auto t1 = T0 + SealPeriod;
        subscriptions.AddNodeSubscriber(1, Pipe1, t1);
        auto stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(2, stats.NodeCount);

        UNIT_ASSERT_VALUES_EQUAL(
            Sorted({Pipe1, Pipe2}),
            Sorted(subscriptions.ExtractNodeSubscribers(1, t1)));
        stats = subscriptions.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.NodeCount);
    }
}

}   // namespace NCloud::NFileStore::NStorage
