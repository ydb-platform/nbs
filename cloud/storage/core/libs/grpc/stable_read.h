#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/stream/output.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class EStableReadDecision
{
    // Content equals the current one.
    Unchanged,
    // Content has been read for the first time, differs from the content
    // read previously or has not been stable for long enough yet.
    Wait,
    // Content has stayed unchanged for the hold time and can be applied.
    Apply,
};

// Files such as certificates are rewritten by external tools, not necessarily
// atomically, and a partially written file may be syntactically valid, e.g. a
// certificate chain without its intermediate certificate. TStableRead lets new
// content be applied only after it has stayed unchanged for the hold time
// since it was first read. A read is counted by the time it was made, so reads
// that happen to follow each other closely do not shorten the hold. This is a
// heuristic that reduces the chance of picking up an intermediate state of a
// rewrite, not a guarantee: a writer that stalls for longer than the hold time
// is indistinguishable from a finished one.
template <typename T>
class TStableRead
{
private:
    TMaybe<T> Pending;
    TInstant PendingSince;

public:
    EStableReadDecision Observe(
        const T& current,
        const T& content,
        TInstant now,
        TDuration holdTime)
    {
        if (content == current) {
            Pending.Clear();
            return EStableReadDecision::Unchanged;
        }

        if (!Pending.Defined() || *Pending != content) {
            Pending = content;
            PendingSince = now;
            return EStableReadDecision::Wait;
        }

        return now - PendingSince >= holdTime
            ? EStableReadDecision::Apply
            : EStableReadDecision::Wait;
    }

    // Forgets the pending content, e.g. after a read error: the hold starts
    // over when the content is read again.
    void Reset()
    {
        Pending.Clear();
    }

    [[nodiscard]] bool IsPending() const
    {
        return Pending.Defined();
    }
};

}   // namespace NCloud

////////////////////////////////////////////////////////////////////////////////

Y_DECLARE_OUT_SPEC(inline, NCloud::EStableReadDecision, out, decision)
{
    switch (decision) {
        case NCloud::EStableReadDecision::Unchanged:
            out << "Unchanged";
            break;
        case NCloud::EStableReadDecision::Wait:
            out << "Wait";
            break;
        case NCloud::EStableReadDecision::Apply:
            out << "Apply";
            break;
    }
}
