#include "util/generic/hash.h"
#include "util/system/types.h"

namespace NCloud::NFileStore::NStorage {

class TNodeToSessionStat
{
    struct TSessionStat
    {
        THashMap<TString, i32> ReadSessions;
        THashMap<TString, i32> WriteSessions;
    };
    using TStat = THashMap<ui64, TSessionStat>;
    TStat Stat;

    void Clean(
        const TStat::iterator& nodeStatIterator,
        const TString& sessionId);

public:
    enum class EKind
    {
        None,
        NodesOpenForWritingBySingleSession,
        NodesOpenForWritingByMultipleSessions,
        NodesOpenForReadingBySingleSession,
        NodesOpenForReadingByMultipleSessions,
    };

    EKind AddRead(ui64 nodeId, const TString& sessionId);
    EKind AddWrite(ui64 nodeId, const TString& sessionId);
    EKind RemoveRead(ui64 nodeId, const TString& sessionId);
    EKind RemoveWrite(ui64 nodeId, const TString& sessionId);
    [[nodiscard]] EKind GetKind(ui64 nodeId) const;

private:
    [[nodiscard]] EKind GetKind(
        const TStat::const_iterator& nodeStatIterator) const;
};

}   // namespace NCloud::NFileStore::NStorage
