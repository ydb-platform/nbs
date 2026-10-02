#pragma once

#include <library/cpp/timezone_conversion/convert.h>

#include <util/datetime/base.h>
#include <util/generic/strbuf.h>

namespace NSystemdTime {

// Parses the timestamp syntax from systemd.time(7), including relative times.
// Also accepts epoch, @ followed by a span, and a timestamp +/- a span.
// Omitted dates and relative times use now; omitted zones use timeZone.
// Returns false for invalid input, overflow, or instants before the Unix epoch,
// leaving result unchanged. Calendar expressions are not supported.
bool TryParseTimestamp(
    TStringBuf input,
    TInstant& result,
    TInstant now = TInstant::Now(),
    const NDatetime::TTimeZone& timeZone = NDatetime::GetLocalTimeZone());

}   // namespace NSystemdTime
