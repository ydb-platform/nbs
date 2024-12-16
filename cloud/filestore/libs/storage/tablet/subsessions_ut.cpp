#include "subsessions.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSubSessions)
{
    Y_UNIT_TEST(ShouldAddSubSessions)
    {
        TSubSessions subsessions(0, 0);

        subsessions.UpdateSubSession(1, true, TActorId(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
    }

    Y_UNIT_TEST(ShouldUpdateSubSessions)
    {
        TSubSessions subsessions(0, 0);

        subsessions.UpdateSubSession(1, true, TActorId(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetMaxRoSeqNo());
        UNIT_ASSERT_VALUES_EQUAL(0, subsessions.GetMaxRwSeqNo());

        subsessions.UpdateSubSession(1, false, TActorId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(0, subsessions.GetMaxRoSeqNo());
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetMaxRwSeqNo());

        {
            auto subsession = subsessions.GetSubSessionBySeqNo(1);
            UNIT_ASSERT_VALUES_EQUAL(true, subsession.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, subsession->SeqNo);
            UNIT_ASSERT_VALUES_EQUAL(false, subsession->ReadOnly);
            UNIT_ASSERT_VALUES_EQUAL(TActorId(1, 1), subsession->Owner);
        }

        {
            subsessions.UpdateSubSession(2, true, TActorId(2, 1));
            UNIT_ASSERT_VALUES_EQUAL(2, subsessions.GetSize());
            auto subsession = subsessions.GetSubSessionBySeqNo(2);
            UNIT_ASSERT_VALUES_EQUAL(true, subsession.has_value());
            UNIT_ASSERT_VALUES_EQUAL(2, subsession->SeqNo);
            UNIT_ASSERT_VALUES_EQUAL(true, subsession->ReadOnly);
            UNIT_ASSERT_VALUES_EQUAL(2, subsessions.GetMaxRoSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetMaxRwSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(TActorId(2, 1), subsession->Owner);
        }
    }

    Y_UNIT_TEST(ShouldRemoveSubSessionWithLowestSeqNo)
    {
        TSubSessions subsessions(0, 0);
        TActorId ans;

        ans = subsessions.UpdateSubSession(1, true, TActorId(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(), ans);

        ans = subsessions.UpdateSubSession(2, false, TActorId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(2, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(), ans);

        ans = subsessions.UpdateSubSession(3, true, TActorId(2, 1));
        UNIT_ASSERT_VALUES_EQUAL(2, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(0, 1), ans);

        UNIT_ASSERT_VALUES_EQUAL(3, subsessions.GetMaxRoSeqNo());
        UNIT_ASSERT_VALUES_EQUAL(2, subsessions.GetMaxRwSeqNo());
    }

    Y_UNIT_TEST(ShouldRemoveSubSession)
    {
        TSubSessions subsessions(0, 0);
        TActorId ans;
        ui32 size = 0;

        ans = subsessions.UpdateSubSession(1, true, TActorId(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(), ans);

        ans = subsessions.UpdateSubSession(2, true, TActorId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(2, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(), ans);

        size = subsessions.DeleteSubSession(TActorId(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(1, size);

        size = subsessions.DeleteSubSession(TActorId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(0, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(0, size);
        UNIT_ASSERT_VALUES_EQUAL(false, subsessions.IsValid());
    }

    Y_UNIT_TEST(ShouldNotRemoveSessionIfWriterIsStillAlive)
    {
        TSubSessions subsessions(0, 0);
        TActorId ans;
        ui32 size = 0;

        ans = subsessions.UpdateSubSession(1, false, TActorId(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(), ans);

        ans = subsessions.UpdateSubSession(2, true, TActorId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(2, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(), ans);

        size = subsessions.DeleteSubSession(TActorId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(1, size);
    }

    Y_UNIT_TEST(ShouldNotRemoveSessionSeqNoIsLower)
    {
        TSubSessions subsessions(0, 0);
        TActorId ans;
        ui32 size = 0;

        ans = subsessions.UpdateSubSession(1, false, TActorId(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(), ans);

        ans = subsessions.UpdateSubSession(2, true, TActorId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(2, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(TActorId(), ans);

        size = subsessions.DeleteSubSession(TActorId(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, subsessions.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(1, size);
    }
}

}   // namespace NCloud::NFileStore::NStorage
