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

// Guards against picking up a file that is still being rewritten: new content
// is applied only after two consecutive reads return it unchanged. The caller
// must space the reads apart. A best effort only: a writer that stalls longer
// than the interval between reads leaves a partial file that gets applied.
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
