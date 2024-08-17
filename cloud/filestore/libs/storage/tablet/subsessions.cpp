#include "subsessions.h"

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MaxSubSessions = 2;

}

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId TSubSessions::AddSubSession(
    ui64 seqNo,
    bool readOnly,
    const NActors::TActorId& owner)
{
    MaxSeenSeqNo = std::max(MaxSeenSeqNo, seqNo);
    if (!readOnly) {
        MaxSeenRwSeqNo = std::max(MaxSeenRwSeqNo, seqNo);
    }
    SubSessions.push_back({seqNo, readOnly, owner});
    if (SubSessions.size() > MaxSubSessions) {
        auto loSeqNo = std::min_element(
            SubSessions.begin(),
            SubSessions.end(),
            [] (const auto& a, const auto& b) {
                return a.SeqNo < b.SeqNo;
            });
        auto ans = loSeqNo->Owner;
        SubSessions.erase(loSeqNo);
        return ans;
    }
    return {};
}

NActors::TActorId TSubSessions::UpdateSubSession(
    ui64 seqNo,
    bool readOnly,
    const NActors::TActorId& owner)
{
    MaxSeenSeqNo = std::max(MaxSeenSeqNo, seqNo);
    if (!readOnly) {
        MaxSeenRwSeqNo = std::max(MaxSeenRwSeqNo, seqNo);
    }
    auto* subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.SeqNo == seqNo;
        });
    if (subsession != SubSessions.end()) {
        subsession->ReadOnly = readOnly;
        if (subsession->Owner != owner) {
            auto toKill = subsession->Owner;
            subsession->Owner = owner;
            return toKill;
        }
        return {};
    }
    return AddSubSession(seqNo, readOnly, owner);
}

ui32 TSubSessions::DeleteSubSession(const NActors::TActorId& owner)
{
    auto subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.Owner == owner;
        });
    if (subsession == SubSessions.end()) {
        return true;
    }

    auto sessionSeqNo = subsession->SeqNo;
    SubSessions.erase(subsession);

    auto alive = !ReadyToDestroy(sessionSeqNo);
    if (!alive) {
        return false;
    }

    if (sessionSeqNo == MaxSeenRwSeqNo) {
        MaxSeenRwSeqNo = 0;
    }
    if (sessionSeqNo == MaxSeenSeqNo) {
        MaxSeenSeqNo = MaxSeenRwSeqNo;
    }

    return true;
}

ui32 TSubSessions::DeleteSubSession(ui64 sessionSeqNo)
{
    auto subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.SeqNo == sessionSeqNo;
        });

    if (subsession == SubSessions.end()) {
        return !ReadyToDestroy(sessionSeqNo);
    }

    SubSessions.erase(subsession);

    auto alive = !ReadyToDestroy(sessionSeqNo);
    if (!alive) {
        return false;
    }

    if (sessionSeqNo == MaxSeenRwSeqNo) {
        MaxSeenRwSeqNo = 0;
    }
    if (sessionSeqNo == MaxSeenSeqNo) {
        MaxSeenSeqNo = MaxSeenRwSeqNo;
    }

    return true;
}

TVector<NActors::TActorId> TSubSessions::GetSubSessions() const
{
    TVector<NActors::TActorId> ans;
    for (const auto& s: SubSessions) {
        ans.push_back(s.Owner);
    }
    return ans;
}

TVector<TSubSession> TSubSessions::GetAllSubSessions() const
{
    return SubSessions;
}

bool TSubSessions::HasSeqNo(ui64 seqNo) const
{
    auto subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.SeqNo == seqNo;
        });
    if (subsession != SubSessions.end()) {
        return true;
    }
    return false;
}

bool TSubSessions::IsValid() const
{
    if (SubSessions.empty()) {
        return false;
    }
    return AllOf(
        SubSessions,
        [&] (const auto& subsession) {
            return !!subsession.Owner;
        });
}

std::optional<TSubSession> TSubSessions::GetSubSessionBySeqNo(ui64 seqNo) const
{
    auto subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.SeqNo == seqNo;
        });
    if (subsession != SubSessions.end()) {
        return *subsession;
    }
    return std::nullopt;
}

bool TSubSessions::ReadyToDestroy(ui64 seqNo) const
{
    bool isHighestSeqNo = !MaxSeenSeqNo || (seqNo >= MaxSeenSeqNo);
    bool isWriter = !MaxSeenRwSeqNo || (MaxSeenRwSeqNo == seqNo);
    return isHighestSeqNo && isWriter;
}

}   // namespace NCloud::NFileStore::NStorage
