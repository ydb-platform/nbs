#pragma once

#include "public.h"

#include "index.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/system/file.h>
#include <util/system/rwlock.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TSession
{
public:
    const TFsPath Root;
    const TString ClientId;
    const TString SessionId;
    const TLocalIndexPtr Index;

    TString State;

private:
    THashMap<TString, TString> Attrs;
    THashMap<ui32, TFileHandle> Handles;
    TRWMutex Lock;

    THashMap<ui64, std::pair<bool, TInstant>> SubSessions;
public:
    TSession(
            const TFsPath& root,
            TString clientId,
            TString sessionId,
            TLocalIndexPtr index)
        : Root(root.RealPath())
        , ClientId(std::move(clientId))
        , SessionId(std::move(sessionId))
        , Index(std::move(index))
    {}

    void InsertHandle(TFileHandle handle)
    {
        TWriteGuard guard(Lock);

        const auto fhandle = static_cast<FHANDLE>(handle);
        auto [_, inserted] = Handles.emplace(fhandle, std::move(handle));
        Y_ABORT_UNLESS(inserted, "dup file handle for: %d", fhandle);
    }

    TFileHandle* LookupHandle(ui64 handle)
    {
        TReadGuard guard(Lock);

        auto it = Handles.find(handle);
        if (it == Handles.end()) {
            return nullptr;
        }

        return &it->second;
    }

    void DeleteHandle(ui64 handle)
    {
        TWriteGuard guard(Lock);

        Handles.erase(handle);
    }

    TIndexNodePtr LookupNode(ui64 nodeId)
    {
        return Index->LookupNode(nodeId);
    }

    bool TryInsertNode(TIndexNodePtr node)
    {
        return Index->TryInsertNode(std::move(node));
    }

    void ForgetNode(ui64 nodeId)
    {
        Index->ForgetNode(nodeId);
    }

    void Ping(ui64 sessionSeqNo)
    {
        auto it = SubSessions.find(sessionSeqNo);
        Y_ABORT_UNLESS(
            it != SubSessions.end(),
            "seq no: %lu not found",
            sessionSeqNo);
        it->second.second = TInstant::Now();
    }

    void GetInfo(NProto::TSessionInfo& info, ui64 seqNo) const
    {
        info.SetSessionId(SessionId);
        info.SetSessionState(State);
        info.SetSessionSeqNo(seqNo);
        auto it = SubSessions.find(seqNo);
        if (it != SubSessions.end()) {
            info.SetReadOnly(it->second.first);
        }
    }

    void ResetState(TString state)
    {
        State = std::move(state);
    }

    void AddSubSession(ui64 seqNo, bool readOnly)
    {
        SubSessions[seqNo] = std::make_pair(readOnly, TInstant::Now());
    }

    bool RemoveSubSession(ui64 seqNo)
    {
        SubSessions.erase(seqNo);
        return !SubSessions.empty();
    }

    bool HasSubSession(ui64 seqNo)
    {
        return SubSessions.count(seqNo);
    }

    bool RemoveStaleSubSessions(TInstant deadline)
    {
        EraseNodesIf(
            SubSessions,
            [=] (const auto& val) {
                const auto& [_, value] = val;
                const auto [ro, ts] = value;
                return ts < deadline;
            });
        return SubSessions.empty();
    }
};

}   // namespace NCloud::NFileStore
