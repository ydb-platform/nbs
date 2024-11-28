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
    if (readOnly && seqNo < MaxRoSeqNo) {
        return owner;
    }
    if (!readOnly && seqNo < MaxRwSeqNo) {
        return owner;
    }

    MaxRoSeqNo = std::max(MaxRoSeqNo, seqNo);
    if (!readOnly) {
        MaxRwSeqNo = std::max(MaxRwSeqNo, seqNo);
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
    auto* subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.SeqNo == seqNo;
        });
    if (subsession != SubSessions.end()) {
        TActorId toKill;
        subsession->ReadOnly = readOnly;
        if (subsession->Owner != owner) {
            toKill = subsession->Owner;
            subsession->Owner = owner;
        }

        MaxRoSeqNo = 0;
        MaxRwSeqNo = 0;
        for (auto subsession: SubSessions) {
            if (subsession.ReadOnly) {
                MaxRoSeqNo = std::max(MaxRoSeqNo, subsession.SeqNo);
            } else {
                MaxRwSeqNo = std::max(MaxRwSeqNo, subsession.SeqNo);
            }
        }

        return toKill;
    }
    return AddSubSession(seqNo, readOnly, owner);
}

ui32 TSubSessions::DeleteSubSession(const NActors::TActorId& owner)
{
    auto* subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.Owner == owner;
        });
    if (subsession == SubSessions.end()) {
        return MaxRoSeqNo != 0 || MaxRwSeqNo != 0;
    }

    auto toDelete = subsession->SeqNo;
    bool writerAlive = toDelete < MaxRwSeqNo;
    bool migrationInProgress = MaxRwSeqNo && (MaxRoSeqNo > MaxRwSeqNo);

    UpdateSeqNoAfterDelete(toDelete);
    SubSessions.erase(subsession);

    return SubSessions.size() || writerAlive || migrationInProgress;
}

ui32 TSubSessions::DeleteSubSession(ui64 sessionSeqNo)
{
    auto* subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.SeqNo == sessionSeqNo;
        });

    bool writerAlive = sessionSeqNo < MaxRwSeqNo;
    bool migrationInProgress = MaxRwSeqNo && (MaxRoSeqNo > MaxRwSeqNo);

    UpdateSeqNoAfterDelete(sessionSeqNo);
    if (subsession != SubSessions.end()) {
        SubSessions.erase(subsession);
    }

    return SubSessions.size() || writerAlive || migrationInProgress;
}

void TSubSessions::UpdateSeqNoAfterDelete(ui64 seqNo)
{
    if (seqNo == MaxRoSeqNo) {
        MaxRoSeqNo = 0;
        return;
    }
    if (seqNo == MaxRwSeqNo) {
        MaxRwSeqNo = 0;
    }
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
    auto* subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.SeqNo == seqNo;
        });
    return subsession != SubSessions.end();
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
    auto* subsession = FindIf(
        SubSessions,
        [&] (const auto& subsession) {
            return subsession.SeqNo == seqNo;
        });
    if (subsession != SubSessions.end()) {
        return *subsession;
    }
    return std::nullopt;
}

}   // namespace NCloud::NFileStore::NStorage
