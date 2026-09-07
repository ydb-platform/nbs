#include "subsessions.h"

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MaxSubSessions = 2;
constexpr ui64 SubSessionOwnerGenerationBits = 32;
constexpr ui64 SubSessionOwnerGenerationMask =
    (1ULL << SubSessionOwnerGenerationBits) - 1;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ui64 MakeSubSessionOwnerGeneration(ui32 tabletGeneration, ui32 ownerGeneration)
{
    return ((1ULL * tabletGeneration) << SubSessionOwnerGenerationBits) |
           ownerGeneration;
}

ui32 ExtractTabletGeneration(ui64 ownerGeneration)
{
    return static_cast<ui32>(ownerGeneration >> SubSessionOwnerGenerationBits);
}

ui32 ExtractSubSessionOwnerGeneration(ui64 ownerGeneration)
{
    return static_cast<ui32>(ownerGeneration & SubSessionOwnerGenerationMask);
}

////////////////////////////////////////////////////////////////////////////////

TSubSessionUpdateResult TSubSessions::AddSubSession(
    ui64 seqNo,
    bool readOnly,
    const NActors::TActorId& owner,
    const NActors::TActorId& pipeServer,
    ui32 tabletGeneration)
{
    MaxSeenSeqNo = std::max(MaxSeenSeqNo, seqNo);
    if (!readOnly) {
        MaxSeenRwSeqNo = std::max(MaxSeenRwSeqNo, seqNo);
    }
    SubSessions.push_back(TSubSession{
        .SeqNo = seqNo,
        .ReadOnly = readOnly,
        .PipeInfo = TSessionPipeInfo{
            .Owner = owner,
            .PipeServer = pipeServer,
        },
        .OwnerGeneration = MakeSubSessionOwnerGeneration(
            tabletGeneration,
            1 /* ownerGeneration */),
    });
    if (SubSessions.size() > MaxSubSessions) {
        auto loSeqNo = std::min_element(
            SubSessions.begin(),
            SubSessions.end(),
            [] (const auto& a, const auto& b) {
                return a.SeqNo < b.SeqNo;
            });
        // The evicted subsession is a fully separate, older mount: both its
        // pipe server binding and its owner actor are stale.
        TSubSessionUpdateResult result;
        result.StalePipeServer = loSeqNo->PipeInfo.PipeServer;
        result.StaleOwner = loSeqNo->PipeInfo.Owner;
        SubSessions.erase(loSeqNo);
        return result;
    }
    return {};
}

TSubSessionUpdateResult TSubSessions::UpdateSubSession(
    ui64 seqNo,
    bool readOnly,
    const NActors::TActorId& owner,
    const NActors::TActorId& pipeServer,
    ui32 tabletGeneration)
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

        // Owner and PipeServer can change independently,
        // track each fact on its own.
        const bool ownerChanged = subsession->PipeInfo.Owner != owner;
        const bool pipeServerChanged =
            subsession->PipeInfo.PipeServer != pipeServer;
        if (!ownerChanged && !pipeServerChanged) {
            return {};
        }

        TSubSessionUpdateResult result;
        if (pipeServerChanged) {
            result.StalePipeServer = subsession->PipeInfo.PipeServer;
        }
        if (ownerChanged) {
            result.StaleOwner = subsession->PipeInfo.Owner;
        }

        subsession->PipeInfo.Owner = owner;
        subsession->PipeInfo.PipeServer = pipeServer;
        if (ownerChanged) {
            subsession->OwnerGeneration = MakeSubSessionOwnerGeneration(
                tabletGeneration,
                ExtractSubSessionOwnerGeneration(
                    subsession->OwnerGeneration) + 1);
        }
        return result;
    }
    return AddSubSession(seqNo, readOnly, owner, pipeServer, tabletGeneration);
}

TDeleteSubSessionResult TSubSessions::DeleteSubSessionIf(
    const std::function<bool(const TSubSession&)>& predicate)
{
    auto subsession = FindIf(SubSessions, predicate);
    if (subsession == SubSessions.end()) {
        return {};
    }

    auto sessionSeqNo = subsession->SeqNo;
    auto removed = *subsession;
    SubSessions.erase(subsession);

    if (ReadyToDestroy(sessionSeqNo)) {
        return {.Removed = removed, .SessionCanBeDestroyed = true};
    }

    // MaxSeenRwSeqNo only tracks a single seqNo (the highest one ever
    // passed with readOnly=false). If the removed one's seqNo equals it,
    // it is reset to 0.
    if (sessionSeqNo == MaxSeenRwSeqNo) {
        MaxSeenRwSeqNo = 0;
    }
    // With at most two subsessions, if the removed one's seqNo equals
    // MaxSeenSeqNo, the only other seqNo we can fall back to is MaxSeenRwSeqNo.
    if (sessionSeqNo == MaxSeenSeqNo) {
        MaxSeenSeqNo = MaxSeenRwSeqNo;
    }

    return {.Removed = removed, .SessionCanBeDestroyed = false};
}

TDeleteSubSessionResult TSubSessions::DeleteSubSessionByPipeServer(
    const NActors::TActorId& pipeServer)
{
    return DeleteSubSessionIf(
        [&] (const TSubSession& subsession) {
            return subsession.PipeInfo.PipeServer == pipeServer;
        });
}

TDeleteSubSessionResult TSubSessions::DeleteSubSession(ui64 sessionSeqNo)
{
    return DeleteSubSessionIf(
        [&] (const TSubSession& subsession) {
            return subsession.SeqNo == sessionSeqNo;
        });
}

TVector<NActors::TActorId> TSubSessions::GetSubSessionOwnerIds() const
{
    TVector<NActors::TActorId> ans;
    for (const auto& s: SubSessions) {
        ans.push_back(s.PipeInfo.Owner);
    }
    return ans;
}

TVector<NActors::TActorId> TSubSessions::GetSubSessionPipeServerIds() const
{
    TVector<NActors::TActorId> ans;
    for (const auto& s: SubSessions) {
        ans.push_back(s.PipeInfo.PipeServer);
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
            return !!subsession.PipeInfo.Owner;
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
