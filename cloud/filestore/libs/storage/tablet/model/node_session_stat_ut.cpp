#include "node_session_stat.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeToSessionStat)
{
    Y_UNIT_TEST(ShouldStat)
    {
        TNodeToSessionStat nodeToSessionStat;
        const ui64 nodeId1 = 1;

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.GetKind(nodeId1),
            TNodeToSessionStat::EKind::None);

        TString sessionId1 = "1";
        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.AddRead(nodeId1, sessionId1),
            TNodeToSessionStat::EKind::NodesOpenForReadingBySingleSession);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.AddRead(nodeId1, sessionId1),
            TNodeToSessionStat::EKind::NodesOpenForReadingBySingleSession);

        TString sessionId2 = "2";
        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.AddRead(nodeId1, sessionId2),
            TNodeToSessionStat::EKind::NodesOpenForReadingByMultipleSessions);

        TString sessionId3 = "3";
        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.AddWrite(nodeId1, sessionId3),
            TNodeToSessionStat::EKind::NodesOpenForWritingBySingleSession);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.AddWrite(nodeId1, sessionId2),
            TNodeToSessionStat::EKind::NodesOpenForWritingByMultipleSessions);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveRead(nodeId1, sessionId1),
            TNodeToSessionStat::EKind::NodesOpenForWritingByMultipleSessions);

        const ui64 nodeId2 = 2;
        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.AddWrite(nodeId2, sessionId2),
            TNodeToSessionStat::EKind::NodesOpenForWritingBySingleSession);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.GetKind(nodeId1),
            TNodeToSessionStat::EKind::NodesOpenForWritingByMultipleSessions);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveWrite(nodeId1, sessionId1),
            TNodeToSessionStat::EKind::NodesOpenForWritingByMultipleSessions);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveWrite(nodeId1, sessionId1),
            TNodeToSessionStat::EKind::NodesOpenForWritingByMultipleSessions);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveWrite(nodeId1, sessionId1),
            TNodeToSessionStat::EKind::NodesOpenForWritingByMultipleSessions);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveWrite(nodeId2, sessionId2),
            TNodeToSessionStat::EKind::None);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.GetKind(nodeId1),
            TNodeToSessionStat::EKind::NodesOpenForWritingByMultipleSessions);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveWrite(nodeId1, sessionId2),
            TNodeToSessionStat::EKind::NodesOpenForWritingBySingleSession);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveWrite(nodeId1, sessionId3),
            TNodeToSessionStat::EKind::NodesOpenForReadingByMultipleSessions);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveRead(nodeId1, sessionId1),
            TNodeToSessionStat::EKind::NodesOpenForReadingBySingleSession);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveRead(nodeId1, sessionId2),
            TNodeToSessionStat::EKind::None);

        UNIT_ASSERT_EQUAL(
            nodeToSessionStat.RemoveRead(nodeId1, sessionId1),
            TNodeToSessionStat::EKind::None);
    }
}

}   // namespace NCloud::NFileStore::NStorage
