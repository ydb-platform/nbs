#include "request_timing_journal.h"

#include "request_timing_graph_builder.h"

namespace NCloud {

bool ReplayRequestTimingJournal(
    TConstArrayRef<ui64> words, TRequestTimingGraphBuilder& builder)
{
    if (words.size() > MaxRequestTimingJournalWords) {
        return false;
    }
    size_t pos = 0;
    const auto fits32 = [](ui64 value)
    {
        return value <= std::numeric_limits<ui32>::max();
    };
    while (pos < words.size()) {
        const auto op = static_cast<ERequestTimingOp>(words[pos++]);
        const size_t remaining = words.size() - pos;
        switch (op) {
            case ERequestTimingOp::Fork:
            case ERequestTimingOp::Start:
            case ERequestTimingOp::Finish:
            case ERequestTimingOp::Cancel: {
                if (remaining < 2 || !fits32(words[pos])) {
                    return false;
                }
                const ui32 id = words[pos++];
                const ui64 now = words[pos++];
                switch (op) {
                    case ERequestTimingOp::Fork:
                        builder.Fork(id, now);
                        break;
                    case ERequestTimingOp::Start:
                        builder.Start(id, now);
                        break;
                    case ERequestTimingOp::Finish:
                        builder.Finish(id, now);
                        break;
                    case ERequestTimingOp::Cancel:
                        builder.Cancel(id, now);
                        break;
                    default:
                        Y_ABORT("unreachable journal operation");
                }
                break;
            }
            case ERequestTimingOp::Join: {
                if (remaining < 3 || !fits32(words[pos]) ||
                    words[pos + 2] > remaining - 3)
                {
                    return false;
                }
                const ui32 span = words[pos++];
                const ui64 now = words[pos++];
                const size_t count = words[pos++];
                TVector<ui32> children;
                children.reserve(count);
                for (size_t i = 0; i < count; ++i) {
                    if (!fits32(words[pos])) {
                        return false;
                    }
                    children.push_back(words[pos++]);
                }
                builder.Join(span, children, now);
                break;
            }
            case ERequestTimingOp::Wait: {
                if (remaining < 4 || !fits32(words[pos]) ||
                    !fits32(words[pos + 1]))
                {
                    return false;
                }
                builder.Wait(
                    words[pos], words[pos + 1], words[pos + 2], words[pos + 3]);
                pos += 4;
                break;
            }
            case ERequestTimingOp::Missing: {
                if (remaining < 3 || !fits32(words[pos]) ||
                    !fits32(words[pos + 1]))
                {
                    return false;
                }
                builder.Missing(words[pos], words[pos + 1], words[pos + 2]);
                pos += 3;
                break;
            }
            case ERequestTimingOp::Incomplete: {
                if (!remaining) {
                    return false;
                }
                const ui64 length = words[pos++];
                // Compare before rounding, avoiding length+7 overflow.
                if (length > (words.size() - pos) * sizeof(ui64)) {
                    return false;
                }
                const size_t count = (length + 7) / 8;
                if (length % 8 &&
                    (words[pos + count - 1] >> (8 * (length % 8))))
                {
                    return false;
                }
                TString reason;
                reason.resize(length);
                for (size_t i = 0; i < length; ++i) {
                    reason[i] =
                        static_cast<char>(words[pos + i / 8] >> (8 * (i % 8)));
                }
                pos += count;
                builder.Incomplete(std::move(reason));
                break;
            }
            default:
                return false;
        }
    }
    return true;
}

}   // namespace NCloud
