#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/system/backtrace.h>

#include <cstdlib>
#include <exception>

namespace {
// Avoid recursion if std::terminate is triggered inside TerminateHandler
thread_local int TerminateCount = 0;

void TerminateHandler() noexcept
{
    if (++TerminateCount != 1) {
        Cerr << "TerminateHandler is called recursively" << Endl;
        std::abort();
    }

    auto report = TStringStream();
    if (std::current_exception()) {
        report << "\n========== Uncaught exception ===============\n";
        FormatCurrentExceptionTo(report);
    } else {
        report << "Terminate for unknown reason (no current exception)\n";
    }
    report << "\n========== Terminate handler stack ==========\n";
    FormatBackTrace(&report);

    Cerr << report.Str() << '\n' << Flush;
    std::abort();
}

[[maybe_unused]] const auto _ = std::set_terminate(&TerminateHandler);
}   // namespace
