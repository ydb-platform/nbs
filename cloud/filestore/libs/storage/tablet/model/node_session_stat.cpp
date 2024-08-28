#include "node_session_stat.h"

namespace NCloud::NFileStore::NStorage {

TNodeToSessionStat::EKind TNodeToSessionStat::AddRead(
    ui64 nodeId,
    const TString& sessionId)
{
    ++Stat[nodeId].ReadSessions[sessionId];
    return GetKind(nodeId);
}

TNodeToSessionStat::EKind TNodeToSessionStat::AddWrite(
    ui64 nodeId,
    const TString& sessionId)
{
    ++Stat[nodeId].WriteSessions[sessionId];
    return GetKind(nodeId);
}

void TNodeToSessionStat::Clean(
    const TStat::iterator& nodeStatIterator,
    const TString& sessionId)
{
    auto& nodeStat = nodeStatIterator->second;

    {
        const auto it = nodeStat.WriteSessions.find(sessionId);
        if (it != nodeStat.WriteSessions.end() && it->second <= 0) {
            nodeStat.WriteSessions.erase(it);
        }
    }

    {
        const auto it = nodeStat.ReadSessions.find(sessionId);
        if (it != nodeStat.ReadSessions.end() && it->second <= 0) {
            nodeStat.ReadSessions.erase(it);
        }
    }

    if (nodeStat.WriteSessions.empty() && nodeStat.ReadSessions.empty()) {
        Stat.erase(nodeStatIterator);
    }
}

TNodeToSessionStat::EKind TNodeToSessionStat::RemoveRead(
    ui64 nodeId,
    const TString& sessionId)
{
    const auto& nodeStatIterator = Stat.find(nodeId);
    if (nodeStatIterator != Stat.end()) {
        --nodeStatIterator->second.ReadSessions[sessionId];
        Clean(nodeStatIterator, sessionId);
    }
    return GetKind(nodeStatIterator);
}

TNodeToSessionStat::EKind TNodeToSessionStat::RemoveWrite(
    ui64 nodeId,
    const TString& sessionId)
{
    const auto& nodeStatIterator = Stat.find(nodeId);
    if (nodeStatIterator != Stat.end()) {
        --nodeStatIterator->second.WriteSessions[sessionId];
        Clean(nodeStatIterator, sessionId);
    }
    return GetKind(nodeStatIterator);
}

TNodeToSessionStat::EKind TNodeToSessionStat::GetKind(ui64 nodeId) const
{
    const auto& nodeStatIterator = Stat.find(nodeId);
    return GetKind(nodeStatIterator);
}

TNodeToSessionStat::EKind TNodeToSessionStat::GetKind(
    const TStat::const_iterator& nodeStatIterator) const
{
    if (nodeStatIterator == Stat.end()) {
        return EKind::None;
    }
    const auto& nodeStat = nodeStatIterator->second;
    if (nodeStat.WriteSessions.size() > 1) {
        return EKind::NodesOpenForWritingByMultipleSessions;
    }
    if (nodeStat.WriteSessions.size() == 1) {
        return EKind::NodesOpenForWritingBySingleSession;
    }
    if (nodeStat.ReadSessions.size() > 1) {
        return EKind::NodesOpenForReadingByMultipleSessions;
    }
    if (nodeStat.ReadSessions.size() == 1) {
        return EKind::NodesOpenForReadingBySingleSession;
    }
    return EKind::None;
}

}   // namespace NCloud::NFileStore::NStorage
