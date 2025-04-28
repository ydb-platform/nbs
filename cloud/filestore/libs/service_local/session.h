#pragma once

#include "public.h"

#include "index.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/persistent_table.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/rwlock.h>
#include <util/system/tempfile.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TSession
{
public:
    const TFsPath RootPath;
    const TFsPath StatePath;
    const TString ClientId;
    TString SessionId;

private:
    struct THandle
    {
        TFileHandle FileHandle;
        ui64 RecordIndex = -1;
    };

    struct THandleTableHeader
    {
    };

    struct THandleTableRecord
    {
        ui64 HandleId = 0;
        ui64 NodeId = 0;
        int Flags = 0;
    };

    using THandleTable =
        TPersistentTable<THandleTableHeader, THandleTableRecord>;

    THashMap<TString, TString> Attrs;
    THashMap<ui64, THandle> Handles;
    std::unique_ptr<THandleTable> HandleTable;
    std::atomic<ui64> NextHandleId = 0;
    TString FuseState;
    TRWMutex Lock;

    const ILoggingServicePtr Logging;
    TLog Log;

    const ui32 MaxNodeCount;
    const ui32 MaxHandleCount;
    const bool OpenNodeByHandleEnabled;
    const ui32 NodeCleanupBatchSize;

    TLocalIndex Index;
    THashMap<ui64, std::pair<bool, TInstant>> SubSessions;
public:
    TSession(
             const TString& fileSystemId,
             const TFsPath& root,
             const TFsPath& statePath,
             TString clientId,
             ui32 maxNodeCount,
             ui32 maxHandleCount,
             bool openNodeByHandleEnabled,
             ui32 nodeCleanupBatchSize,
             ILoggingServicePtr logging)
        : RootPath(root.RealPath())
        , StatePath(statePath.RealPath())
        , ClientId(std::move(clientId))
        , Logging(std::move(logging))
        , Log(Logging->CreateLog(fileSystemId + "." + ClientId))
        , MaxNodeCount(maxNodeCount)
        , MaxHandleCount(maxHandleCount)
        , OpenNodeByHandleEnabled(openNodeByHandleEnabled)
        , NodeCleanupBatchSize(nodeCleanupBatchSize)
        , Index(
              RootPath,
              StatePath,
              MaxNodeCount,
              OpenNodeByHandleEnabled,
              NodeCleanupBatchSize,
              Log)
    {}

    void Init(bool restoreClientSession)
    {
        auto handlesPath = StatePath / "handles";

        bool isNewSession = !restoreClientSession || !HasStateFile("session") ||
                            !HasStateFile("fuse_state");

        if (isNewSession) {
            DeleteStateFile("session");
            DeleteStateFile("fuse_state");
            handlesPath.DeleteIfExists();

            SessionId = CreateGuidAsString();

            WriteStateFile("session", SessionId);
            WriteStateFile("fuse_state", "");
        } else {
            SessionId = ReadStateFile("session");
            FuseState = ReadStateFile("fuse_state");
        }

        STORAGE_INFO(
            (isNewSession ? "Create" : "Restore") << " session" <<
            ", StatePath=" << StatePath <<
            ", SessionId=" << SessionId <<
            ", MaxNodeCount=" << MaxNodeCount <<
            ", MaxHandleCount=" << MaxHandleCount <<
            ", OpenNodeByHandleEnabled=" << OpenNodeByHandleEnabled
        );


        HandleTable = std::make_unique<THandleTable>(
            handlesPath.GetPath(),
            MaxHandleCount);

        ui64 maxHandleId = 0;
        for (auto it = HandleTable->begin(); it != HandleTable->end(); it++) {
            maxHandleId = std::max(maxHandleId, it->HandleId);

            STORAGE_TRACE(
                "Resolving, HandleId=" << it->HandleId <<
                ", NodeId=" << it->NodeId <<
                ", Flags=" << it->Flags);
            auto node = LookupNode(it->NodeId);
            if (!node) {
                STORAGE_ERROR(
                    "Handle with missing node, HandleId=" << it->HandleId <<
                    ", NodeId" << it->NodeId);
                ReportLocalFsMissingHandleNode();
                HandleTable->DeleteRecord(it.GetIndex());
                continue;
            }

            try {
                auto handle = node->OpenHandle(it->Flags);
                auto [_, inserted] = Handles.emplace(
                    it->HandleId,
                    THandle{std::move(handle), it.GetIndex()});
                Y_ABORT_UNLESS(
                    inserted,
                    "dup file handle for: %lu",
                    it->HandleId);
            } catch (...) {
                STORAGE_ERROR(
                    "Failed to open Handle, HandleId=" << it->HandleId <<
                    ", NodeId" << it->NodeId <<
                    ", Exception=" << CurrentExceptionMessage());
                HandleTable->DeleteRecord(it.GetIndex());
                continue;
            }
        }

        NextHandleId = maxHandleId + 1;
    }

    [[nodiscard]] TResultOrError<ui64>
    InsertHandle(TFileHandle handle, ui64 nodeId, int flags)
    {
        TWriteGuard guard(Lock);

        const auto handleId = NextHandleId++;

        const auto recordIndex = HandleTable->AllocRecord();
        if (recordIndex == THandleTable::InvalidIndex) {
            return ErrorNoSpaceLeft();
        }

        auto* state = HandleTable->RecordData(recordIndex);
        state->HandleId = handleId;
        state->NodeId = nodeId;
        state->Flags = flags;

        if (Handles.find(handleId) != Handles.end()) {
            ReportLocalFsDuplicateFileHandle(TStringBuilder() <<
                "HandleId=" << handleId <<
                ", HandlesCount=" << Handles.size());
            return ErrorInvalidHandle(handleId);
        }

        Handles.emplace(handleId, THandle{std::move(handle), recordIndex});
        HandleTable->CommitRecord(recordIndex);

        return handleId;
    }

    TFileHandle* LookupHandle(ui64 handleId)
    {
        TReadGuard guard(Lock);

        auto it = Handles.find(handleId);
        if (it == Handles.end()) {
            return nullptr;
        }

        return &it->second.FileHandle;
    }

    void DeleteHandle(ui64 handleId)
    {
        TWriteGuard guard(Lock);

        auto it = Handles.find(handleId);
        if (it != Handles.end()) {
            HandleTable->DeleteRecord(it->second.RecordIndex);
            Handles.erase(it);
        }
    }

    TIndexNodePtr LookupNode(ui64 nodeId)
    {
        return Index.LookupNode(nodeId);
    }

    [[nodiscard]] bool TryInsertNode(
        TIndexNodePtr node,
        ui64 parentNodeId,
        const TString& name)
    {
        return Index.TryInsertNode(std::move(node), parentNodeId, name);
    }

    void ForgetNode(ui64 nodeId)
    {
        Index.ForgetNode(nodeId);
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
        info.SetSessionState(FuseState);
        info.SetSessionSeqNo(seqNo);
        auto it = SubSessions.find(seqNo);
        if (it != SubSessions.end()) {
            info.SetReadOnly(it->second.first);
        }
    }

    void ResetState(TString state)
    {
        TWriteGuard guard(Lock);

        HandleTable->Clear();
        Handles.clear();

        Index.Clear();

        FuseState = std::move(state);
        WriteStateFile("fuse_state", FuseState);
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

    bool RemoveStaleSubSessions(TInstant inactivityDeadline)
    {
        EraseNodesIf(
            SubSessions,
            [=] (const auto& val) {
                const auto& [_, value] = val;
                const auto [ro, ts] = value;
                return ts < inactivityDeadline;
            });
        return SubSessions.empty();
    }

private:
    TString ReadStateFile(const TString &fileName)
    {
        TFile file(
            StatePath / fileName,
            EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
        return TFileInput(file).ReadAll();
    }

    void WriteStateFile(const TString &fileName, const TString& value)
    {
        TFsPath tmpFilePath(
            MakeTempName(StatePath.GetPath().c_str(), fileName.c_str()));
        TFileOutput(tmpFilePath).Write(value);
        tmpFilePath.ForceRenameTo(StatePath / fileName);
    }

    bool HasStateFile(const TString &fileName)
    {
        return (StatePath / fileName).Exists();
    }

    void DeleteStateFile(const TString &fileName)
    {
        return (StatePath / fileName).DeleteIfExists();
    }
};

}   // namespace NCloud::NFileStore
