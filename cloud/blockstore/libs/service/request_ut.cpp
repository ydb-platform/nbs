#include "request.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

void SetupRequest(TWriteBlocksLocalRequest& request, TString& data)
{
    TSgList sgList = {{data.data(), data.size()}};
    request.Sglist = TGuardedSgList(std::move(sgList));
    request.BlocksCount = 1;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(WriteBlocksLocalRequestTest)
{
    Y_UNIT_TEST(DestructorDoesNotCloseSglistWhenDependent)
    {
        TString data = "hello";
        TGuardedSgList sharedSglist;
        {
            TWriteBlocksLocalRequest request;
            SetupRequest(request, data);
            sharedSglist = request.Sglist;
        }
        UNIT_ASSERT(sharedSglist.Acquire());
    }

    Y_UNIT_TEST(DestructorClosesSglistWhenOwner)
    {
        TGuardedSgList sglistCopy;
        {
            TString data = "hello";
            TWriteBlocksLocalRequest request;
            SetupRequest(request, data);
            request.TakeDataOwnership();
            sglistCopy = request.Sglist;
        }
        UNIT_ASSERT(!sglistCopy.Acquire());
    }

    Y_UNIT_TEST(CreateDependentRequestSharesSglist)
    {
        TString data = "hello";
        TGuardedSgList depSglist;
        {
            TWriteBlocksLocalRequest owner;
            SetupRequest(owner, data);
            owner.TakeDataOwnership();
            TWriteBlocksLocalRequest dep(
                owner,
                TWriteBlocksLocalRequest::TDependentTag{});
            depSglist = dep.Sglist;
            UNIT_ASSERT(!dep.IsOwner());
        }
        UNIT_ASSERT(!depSglist.Acquire());
    }

    Y_UNIT_TEST(CreateDependentRequestDoesNotCopyBlocks)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest owner;
        SetupRequest(owner, data);
        owner.TakeDataOwnership();
        UNIT_ASSERT_VALUES_EQUAL(1, owner.GetBlocks().BuffersSize());

        TWriteBlocksLocalRequest dep(
            owner,
            TWriteBlocksLocalRequest::TDependentTag{});

        UNIT_ASSERT_VALUES_EQUAL(0, dep.GetBlocks().BuffersSize());
    }

    Y_UNIT_TEST(TakeOwnershipCopiesData)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest request;
        SetupRequest(request, data);
        UNIT_ASSERT(!request.IsOwner());

        request.TakeDataOwnership();
        data = "world";

        UNIT_ASSERT(request.IsOwner());
        auto guard = request.Sglist.Acquire();
        UNIT_ASSERT(guard);
        UNIT_ASSERT_VALUES_EQUAL(1u, guard.Get().size());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("hello"),
            guard.Get()[0].AsStringBuf());
    }

    Y_UNIT_TEST(TakeOwnershipDataIsIndependent)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest request;
        SetupRequest(request, data);
        request.TakeDataOwnership();

        data = "world";

        auto guard = request.Sglist.Acquire();
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("hello"),
            guard.Get()[0].AsStringBuf());
    }

    Y_UNIT_TEST(TakeOwnershipIsIdempotent)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest request;
        SetupRequest(request, data);
        request.TakeDataOwnership();

        request.TakeDataOwnership();

        UNIT_ASSERT(request.IsOwner());
        auto guard = request.Sglist.Acquire();
        UNIT_ASSERT_VALUES_EQUAL(1u, guard.Get().size());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("hello"),
            guard.Get()[0].AsStringBuf());
    }

    Y_UNIT_TEST(DependentRemainsValidAfterOwnerMove)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest owner;
        SetupRequest(owner, data);
        owner.TakeDataOwnership();

        TWriteBlocksLocalRequest dep(
            owner,
            TWriteBlocksLocalRequest::TDependentTag{});
        TGuardedSgList depSglist = dep.Sglist;

        TWriteBlocksLocalRequest moved = std::move(owner);

        auto guard = depSglist.Acquire();
        UNIT_ASSERT(guard);
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("hello"),
            guard.Get()[0].AsStringBuf());
    }

    Y_UNIT_TEST(DependentInvalidatedAfterMovedOwnerDestroyed)
    {
        TString data = "hello";
        TGuardedSgList depSglist;
        {
            TWriteBlocksLocalRequest owner;
            SetupRequest(owner, data);
            owner.TakeDataOwnership();

            TWriteBlocksLocalRequest dep(
                owner,
                TWriteBlocksLocalRequest::TDependentTag{});
            depSglist = dep.Sglist;

            TWriteBlocksLocalRequest moved = std::move(owner);
        }
        UNIT_ASSERT(!depSglist.Acquire());
    }

    Y_UNIT_TEST(MoveOwnerSglistAddressIsStable)
    {
        // RepeatedPtrField<string> move transfers the pointer array, not the
        // string objects, so buffer.data() addresses are stable after a move.
        TString data = "hello";
        TWriteBlocksLocalRequest src;
        SetupRequest(src, data);
        src.TakeDataOwnership();

        const char* addrBefore;
        {
            auto guard = src.Sglist.Acquire();
            UNIT_ASSERT(guard);
            addrBefore = guard.Get()[0].Data();
        }

        TWriteBlocksLocalRequest dst = std::move(src);

        auto guard = dst.Sglist.Acquire();
        UNIT_ASSERT(guard);
        UNIT_ASSERT_VALUES_EQUAL(addrBefore, guard.Get()[0].Data());
    }

    Y_UNIT_TEST(MoveAssignmentTransfersOwnershipAndClosesOldSglist)
    {
        TString data1 = "hello";
        TWriteBlocksLocalRequest dst;
        SetupRequest(dst, data1);
        dst.TakeDataOwnership();
        TGuardedSgList oldSglist = dst.Sglist;

        TString data2 = "world";
        TWriteBlocksLocalRequest src;
        SetupRequest(src, data2);
        src.TakeDataOwnership();

        dst = std::move(src);

        UNIT_ASSERT(!oldSglist.Acquire());

        UNIT_ASSERT(dst.IsOwner());
        auto guard = dst.Sglist.Acquire();
        UNIT_ASSERT(guard);
        UNIT_ASSERT_VALUES_EQUAL(1u, guard.Get().size());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("world"),
            guard.Get()[0].AsStringBuf());
    }
}

}   // namespace NCloud::NBlockStore
