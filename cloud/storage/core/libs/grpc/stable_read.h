#pragma once

#include <util/generic/maybe.h>
#include <util/stream/output.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class EStableReadDecision
{
    // Content equals the current one.
    Unchanged,
    // Content has been read for the first time or differs from the content
    // read previously.
    Wait,
    // Content has been read unchanged twice in a row and can be applied.
    Apply,
};

// Files such as certificates are rewritten by external tools, not necessarily
// atomically, and a partially written file may be syntactically valid, e.g. a
// certificate chain without its intermediate certificate. TStableRead lets new
// content be applied only after it has been read unchanged twice in a row. The
// caller is responsible for spacing the reads apart, e.g. by feeding only
// periodic reads. This is a heuristic that reduces the chance of picking up an
// intermediate state of a rewrite, not a guarantee: a writer that stalls for
// longer than the interval between reads is indistinguishable from a finished
// one.
template <typename T>
class TStableRead
{
private:
    TMaybe<T> Pending;

public:
    EStableReadDecision Observe(const T& current, const T& content)
    {
        if (content == current) {
            Pending.Clear();
            return EStableReadDecision::Unchanged;
        }

        if (!Pending.Defined() || *Pending != content) {
            Pending = content;
            return EStableReadDecision::Wait;
        }

        return EStableReadDecision::Apply;
    }

    // Forgets the pending content, e.g. after a read error: the count starts
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
