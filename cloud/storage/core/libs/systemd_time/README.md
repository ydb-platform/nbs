# Timestamp parsing

`NSystemdTime::TryParseTimestamp` parses the timestamp syntax described in
[systemd.time(7)](https://www.freedesktop.org/software/systemd/man/latest/systemd.time.html)
into a microsecond-resolution `TInstant`.

```cpp
#include <cloud/storage/core/libs/systemd_time/timestamp.h>

TInstant timestamp;
if (NSystemdTime::TryParseTimestamp("30min ago", timestamp)) {
    // Use timestamp.
}
```

Supported inputs include dates with two-digit or four-digit years, optional
times, time-only inputs, optional English weekdays (validated against the date),
`now`, `today`, `yesterday`, `tomorrow`, relative spans (`+2h30min`, `5s ago`,
`1day left`), and Unix epoch seconds (`@1395716396`). Calendar timestamps, epoch
seconds, and relative spans all truncate fractions below microsecond precision.

Timezones may be `UTC`, `Z`, IANA names such as `Asia/Tokyo` and `CET`, or numeric
offsets (`+05`, `-0530`, `+05:30`). Directly attached offsets must use `Z` or
`±HH:MM`. The repository's timezone database supplies named zones without
changing the process timezone.

An omitted timezone defaults to the machine's local timezone. An omitted date
uses today's date in the selected timezone; an omitted time means midnight.
Pass the optional `now` and `timeZone` arguments to use a fixed reference time
and default timezone. Two-digit years use the usual 1969–2068 window.

Relative spans accept the units in systemd.time, including months of 30.4375 days
(2,629,800 seconds) and years of 365.25 days, so `12month` equals `1y`.
Unitless components mean seconds. Repeated times at a
DST transition select the earlier occurrence. Skipped times shift forward by
the size of the gap, matching `library/cpp/timezone_conversion`. Day keywords
use calendar days, so they handle days shorter or longer than 24 hours.

The parser also supports some extensions:

- `epoch` means 1970-01-01 00:00:00 UTC, regardless of the default timezone.
- A timestamp followed by a signed span applies that offset to the parsed
  instant: `today +1s`, `now -2h`, `2018-08-09 07:06:05 +1s2m`, or
  `2018-08-09T07:06:05+05:30 -1h`. Whitespace is required before the modifier's
  sign; whitespace after the sign is optional. Only one modifier is accepted.
- `@` may be followed by a span relative to the Unix epoch: `@1s 2m` or
  `@ 1 s`. An empty `@` means the epoch.

Offsets are elapsed durations applied after resolving the base timestamp's
timezone, including across DST transitions. A suffix that is already a valid
numeric timezone keeps that interpretation: `today +05` means midnight in
UTC+5, whereas `today +5` adds five seconds to midnight in the default zone.

Invalid input, arithmetic overflow, and instants before the Unix epoch return
`false` and leave the output unchanged. This includes `epoch -1s`.
Calendar expressions and standalone spans are outside the timestamp interface.
