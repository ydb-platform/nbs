#pragma once

#include <util/generic/array_ref.h>
#include <util/system/types.h>

namespace NCloud {

class TRequestTimingGraphBuilder;

// Version-2 trace is an ordered stream of these opcodes and uint64 arguments.
// IDs are the original stage/part ordinals. Durations are already rounded
// microseconds: replay never uses the reader's clock or CPU frequency.
enum class ERequestTimingOp: ui64
{
    Fork = 1,     // span, now
    Start,        // fork, now
    Finish,       // span, now
    Join,         // span, now, child count, child IDs...
    Cancel,       // span, now
    Wait,         // span, categories, begin, end
    Missing,      // span, categories, duration
    Incomplete,   // byte length, ceil(length/8) little-endian words
};

// This bounds the compact representation, not the recorded observations.
// Overflow replays losslessly into the existing bounded graph representation.
constexpr size_t MaxRequestTimingJournalWords = 2048;

bool ReplayRequestTimingJournal(
    TConstArrayRef<ui64> words, TRequestTimingGraphBuilder& builder);

}   // namespace NCloud
