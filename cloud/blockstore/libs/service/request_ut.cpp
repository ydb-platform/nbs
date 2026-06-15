#include "request.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

TWriteBlocksLocalRequest MakeDependentRequest(TString& data)
{
    TSgList sgList = {{data.data(), data.size()}};
    TWriteBlocksLocalRequest request;
    request.Sglist = TGuardedSgList(std::move(sgList));
    request.BlocksCount = 1;
    return request;
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
            TWriteBlocksLocalRequest request = MakeDependentRequest(data);
            sharedSglist = request.Sglist;
            UNIT_ASSERT(sharedSglist.Acquire());
        }
        UNIT_ASSERT(sharedSglist.Acquire());
    }

    Y_UNIT_TEST(DestructorClosesSglistWhenOwner)
    {
        TGuardedSgList sglistCopy;
        {
            TString data = "hello";
            TWriteBlocksLocalRequest request = MakeDependentRequest(data);
            request.TakeOwnershipOfData();
            UNIT_ASSERT(request.OwnsSglist);
            sglistCopy = request.Sglist;
        }
        UNIT_ASSERT(!sglistCopy.Acquire());
    }
    Y_UNIT_TEST(MoveConstructorTransfersOwnership)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest src = MakeDependentRequest(data);
        src.TakeOwnershipOfData();
        UNIT_ASSERT(src.OwnsSglist);

        TWriteBlocksLocalRequest dst = std::move(src);
        UNIT_ASSERT(dst.OwnsSglist);
        UNIT_ASSERT(dst.Sglist.Acquire());

        UNIT_ASSERT(src.Sglist.Empty());
    }

    Y_UNIT_TEST(MoveAssignmentClosesOldSglist)
    {
        TString data1 = "abc";
        TString data2 = "def";

        TWriteBlocksLocalRequest dst = MakeDependentRequest(data1);
        dst.TakeOwnershipOfData();

        TGuardedSgList oldSglist = dst.Sglist;

        TWriteBlocksLocalRequest src = MakeDependentRequest(data2);
        src.TakeOwnershipOfData();

        dst = std::move(src);

        UNIT_ASSERT(!oldSglist.Acquire());
        UNIT_ASSERT(dst.Sglist.Acquire());
    }

    Y_UNIT_TEST(MoveAssignmentSelfAssignment)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest request = MakeDependentRequest(data);
        request.TakeOwnershipOfData();

        auto* ptr = &request;
        *ptr = std::move(request);

        UNIT_ASSERT(request.OwnsSglist);
        UNIT_ASSERT(request.Sglist.Acquire());
    }

    Y_UNIT_TEST(CloneDependentSharesSglist)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest original = MakeDependentRequest(data);
        UNIT_ASSERT(!original.OwnsSglist);

        TWriteBlocksLocalRequest clone = original.Clone();
        UNIT_ASSERT(!clone.OwnsSglist);

        UNIT_ASSERT(original.Sglist.Acquire());
        UNIT_ASSERT(clone.Sglist.Acquire());
    }

    Y_UNIT_TEST(CloneOwnerIsIndependent)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest original = MakeDependentRequest(data);
        original.TakeOwnershipOfData();

        TWriteBlocksLocalRequest clone = original.Clone();
        UNIT_ASSERT(clone.OwnsSglist);

        original.Sglist.Close();
        UNIT_ASSERT(!original.Sglist.Acquire());
        UNIT_ASSERT(clone.Sglist.Acquire());
    }

    Y_UNIT_TEST(TakeOwnershipCopiesData)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest request = MakeDependentRequest(data);
        UNIT_ASSERT(!request.OwnsSglist);

        request.TakeOwnershipOfData();

        UNIT_ASSERT(request.OwnsSglist);

        auto guard = request.Sglist.Acquire();
        UNIT_ASSERT(guard);
        UNIT_ASSERT_VALUES_EQUAL(1u, guard.Get().size());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("hello"),
            TStringBuf(guard.Get()[0].Data(), guard.Get()[0].Size()));
    }

    Y_UNIT_TEST(TakeOwnershipOnMovedFromIsNoop)
    {
        TString data = "hello";
        TWriteBlocksLocalRequest src = MakeDependentRequest(data);
        TWriteBlocksLocalRequest dst = std::move(src);

        src.TakeOwnershipOfData();
        UNIT_ASSERT(!src.OwnsSglist);
    }
}

}   // namespace NCloud::NBlockStore
