#include "timestamp.h"

#include <util/string/ascii.h>

#include <chrono>
#include <limits>

namespace NSystemdTime {
namespace {

constexpr ui64 Second = 1000000;
constexpr ui64 Minute = 60 * Second;
constexpr ui64 Hour = 60 * Minute;
constexpr ui64 Day = 24 * Hour;
constexpr ui64 MaxTimestamp = std::numeric_limits<ui64>::max();

void SkipSpace(TStringBuf& input)
{
    while (!input.empty() && IsAsciiSpace(input.front())) {
        input.Skip(1);
    }
}

TStringBuf Trim(TStringBuf input)
{
    SkipSpace(input);
    while (!input.empty() && IsAsciiSpace(input.back())) {
        input.Chop(1);
    }
    return input;
}

bool ReadNumber(TStringBuf& input, ui64& value, size_t& digits)
{
    value = 0;
    digits = 0;
    while (!input.empty() && IsAsciiDigit(input.front())) {
        const unsigned digit = input.front() - '0';
        if (value > (MaxTimestamp - digit) / 10) {
            return false;
        }
        value = value * 10 + digit;
        input.Skip(1);
        ++digits;
    }
    return digits != 0;
}

bool ReadField(TStringBuf& input, int& value)
{
    ui64 number;
    size_t digits;
    if (!ReadNumber(input, number, digits) || digits > 2) {
        return false;
    }
    value = number;
    return true;
}

// Evaluates a decimal fraction exactly, truncating below microsecond precision.
// Working backwards avoids both floating point rounding and integer overflow.
ui64 ScaleFraction(TStringBuf digits, ui64 unit)
{
    ui64 value = 0;
    for (size_t i = digits.size(); i != 0; --i) {
        value = (value + (digits[i - 1] - '0') * unit) / 10;
    }
    return value;
}

bool ReadDecimal(TStringBuf& input, ui64& whole, TStringBuf& fraction)
{
    size_t digits;
    const bool hasWhole = ReadNumber(input, whole, digits);
    if (!hasWhole && digits != 0) {
        return false;
    }
    fraction = {};
    if (input.SkipPrefix(".")) {
        size_t length = 0;
        while (length < input.size() && IsAsciiDigit(input[length])) {
            ++length;
        }
        if (!length) {
            return false;
        }
        fraction = input.SubStr(0, length);
        input.Skip(length);
    }
    return hasWhole || !fraction.empty();
}

bool AddScaled(ui64 whole, TStringBuf fraction, ui64 unit, ui64& value)
{
    if (whole > (MaxTimestamp - value) / unit) {
        return false;
    }
    value += whole * unit;
    const ui64 subsecond = ScaleFraction(fraction, unit);
    if (subsecond > MaxTimestamp - value) {
        return false;
    }
    value += subsecond;
    return true;
}

bool ParseSpan(TStringBuf input, ui64& value)
{
    static constexpr struct
    {
        TStringBuf Name;
        ui64 Unit;
    } units[] = {
        {"usec", 1},
        {"us", 1},
        {"μs", 1},
        {"µs", 1},
        {"msec", 1000},
        {"ms", 1000},
        {"seconds", Second},
        {"second", Second},
        {"sec", Second},
        {"s", Second},
        {"minutes", Minute},
        {"minute", Minute},
        {"min", Minute},
        {"months", 36525 * Day / 1200},
        {"month", 36525 * Day / 1200},
        {"M", 36525 * Day / 1200},
        {"m", Minute},
        {"hours", Hour},
        {"hour", Hour},
        {"hr", Hour},
        {"h", Hour},
        {"days", Day},
        {"day", Day},
        {"d", Day},
        {"weeks", 7 * Day},
        {"week", 7 * Day},
        {"w", 7 * Day},
        {"years", 36525 * Day / 100},
        {"year", 36525 * Day / 100},
        {"y", 36525 * Day / 100},
    };

    value = 0;
    input = Trim(input);
    if (input.empty()) {
        return false;
    }
    while (!input.empty()) {
        ui64 whole;
        TStringBuf fraction;
        if (!ReadDecimal(input, whole, fraction)) {
            return false;
        }
        const bool separated = !input.empty() && IsAsciiSpace(input.front());
        SkipSpace(input);
        ui64 unit = Second;
        bool hasUnit = false;
        for (const auto& entry: units) {
            if (input.SkipPrefix(entry.Name)) {
                unit = entry.Unit;
                hasUnit = true;
                break;
            }
        }
        if (!hasUnit && !separated && !input.empty()) {
            return false;
        }
        if (!AddScaled(whole, fraction, unit, value)) {
            return false;
        }
        // A new component must start with a digit (or a decimal point).
        // Unknown units and signs inside the span fail on the next iteration.
        SkipSpace(input);
    }
    return true;
}

bool ParseZone(TStringBuf input, NDatetime::TTimeZone& zone)
{
    if (input == "UTC" || input == "Z") {
        zone = NDatetime::GetUtcTimeZone();
        return true;
    }
    if (input.StartsWith("+") || input.StartsWith("-")) {
        if (input.size() != 3 && input.size() != 5 &&
            !(input.size() == 6 && input[3] == ':'))
        {
            return false;
        }
        for (size_t i = 1; i < input.size(); ++i) {
            if (input.size() == 6 && i == 3) {
                continue;
            }
            if (!IsAsciiDigit(input[i])) {
                return false;
            }
        }
        int offset;
        if (!NDatetime::TryParseOffset(input, offset)) {
            return false;
        }
        zone = NDatetime::GetFixedTimeZone(offset);
        return true;
    }
    // Load from the repository's timezone database without changing process TZ.
    return cctz::load_time_zone(static_cast<std::string>(input), &zone);
}

int ReadWeekday(TStringBuf& input)
{
    static constexpr TStringBuf names[] = {
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    };
    const auto end = input.find_first_of(" \t\r\n\f\v");
    if (end == TStringBuf::npos) {
        return -1;
    }
    const auto word = input.SubStr(0, end);
    for (size_t i = 0; i < std::size(names); ++i) {
        if (AsciiEqualsIgnoreCase(word, names[i]) ||
            AsciiEqualsIgnoreCase(word, names[i].SubStr(0, 3)))
        {
            input.Skip(end);
            SkipSpace(input);
            return i;
        }
    }
    return -1;
}

bool ParseAbsolute(
    TStringBuf input,
    TInstant& result,
    TInstant now,
    NDatetime::TTimeZone zone)
{
    int dayOffset = 0;
    bool midnight = false;
    for (const auto keyword:
         {TStringBuf("today"), TStringBuf("yesterday"), TStringBuf("tomorrow")})
    {
        if (input.StartsWith(keyword) && (input.size() == keyword.size() ||
                                          IsAsciiSpace(input[keyword.size()])))
        {
            midnight = true;
            dayOffset = keyword == "yesterday"  ? -1
                        : keyword == "tomorrow" ? 1
                                                : 0;
            input.Skip(keyword.size());
            break;
        }
    }

    const int weekday = midnight ? -1 : ReadWeekday(input);
    int year = 0, month = 0, day = 0;
    int hour = 0, minute = 0, second = 0;
    ui64 microseconds = 0;
    if (!midnight) {
        const auto separator = input.find_first_of("-:");
        if (separator == TStringBuf::npos) {
            return false;
        }
        if (input[separator] == '-') {
            ui64 number;
            size_t digits;
            if (!ReadNumber(input, number, digits) ||
                (digits != 2 && digits != 4) || (digits == 4 && number == 0))
            {
                return false;
            }
            year = digits == 2 ? number + (number < 69 ? 2000 : 1900) : number;
            if (!input.SkipPrefix("-") || !ReadField(input, month) ||
                !input.SkipPrefix("-") || !ReadField(input, day))
            {
                return false;
            }
            // cctz normalizes its arguments; reject invalid calendar dates
            // first.
            const cctz::civil_day date(year, month, day);
            if (date.year() != year || date.month() != month ||
                date.day() != day)
            {
                return false;
            }
            if (input.SkipPrefix("T")) {
                if (input.empty() || !IsAsciiDigit(input.front())) {
                    return false;
                }
            } else {
                auto time = input;
                SkipSpace(time);
                if (!time.empty() && IsAsciiDigit(time.front())) {
                    input = time;
                }
            }
        }
        if (!input.empty() && IsAsciiDigit(input.front())) {
            if (!ReadField(input, hour) || !input.SkipPrefix(":") ||
                !ReadField(input, minute))
            {
                return false;
            }
            if (input.SkipPrefix(":")) {
                if (!ReadField(input, second)) {
                    return false;
                }
                if (input.SkipPrefix(".")) {
                    size_t digits = 0;
                    while (digits < input.size() && IsAsciiDigit(input[digits]))
                    {
                        ++digits;
                    }
                    if (!digits) {
                        return false;
                    }
                    microseconds = ScaleFraction(input.Head(digits), Second);
                    input.Skip(digits);
                }
            }
            if (hour > 23 || minute > 59 || second > 59) {
                return false;
            }
        } else if (!year) {
            return false;
        }
    }

    if (!input.empty()) {
        const bool spaced = IsAsciiSpace(input.front());
        input = Trim(input);
        if (!input.empty()) {
            if (!spaced && input != "Z" &&
                !(input.size() == 6 && input[3] == ':' &&
                  (input.front() == '+' || input.front() == '-')))
            {
                return false;
            }
            if (!ParseZone(input, zone)) {
                return false;
            }
        }
    }

    // Use seconds-based time_points to cover dates beyond the nanosecond
    // clock's range (e.g. the year 9999), and determine today in the selected
    // zone.
    const cctz::time_point<cctz::seconds> reference{
        std::chrono::seconds(now.Seconds())};
    const auto today = cctz::civil_day(zone.lookup(reference).cs) + dayOffset;
    if (!year) {
        year = today.year();
        month = today.month();
        day = today.day();
    }
    const cctz::civil_second civil(year, month, day, hour, minute, second);
    if (weekday >= 0 && static_cast<int>(cctz::get_weekday(civil)) != weekday) {
        return false;
    }
    // Choose the earlier occurrence of repeated times and shift skipped times
    // forward across the gap, as in the existing timezone_conversion library.
    const auto seconds = zone.lookup(civil).pre.time_since_epoch().count();
    if (seconds < 0 ||
        static_cast<ui64>(seconds) > (MaxTimestamp - microseconds) / Second)
    {
        return false;
    }
    result = TInstant::MicroSeconds(seconds * Second + microseconds);
    return true;
}

bool ApplyOffset(TInstant base, ui64 span, bool subtract, TInstant& result)
{
    const ui64 reference = base.MicroSeconds();
    if (subtract ? span > reference : span > MaxTimestamp - reference) {
        return false;
    }
    result =
        TInstant::MicroSeconds(subtract ? reference - span : reference + span);
    return true;
}

bool ParseBase(
    TStringBuf input,
    TInstant& result,
    TInstant now,
    const NDatetime::TTimeZone& timeZone)
{
    input = Trim(input);
    if (input == "now") {
        result = now;
        return true;
    }
    if (input == "epoch") {
        result = TInstant::Zero();
        return true;
    }
    if (input.SkipPrefix("@")) {
        input = Trim(input);
        ui64 value = 0;
        if (!input.empty() && !ParseSpan(input, value)) {
            return false;
        }
        result = TInstant::MicroSeconds(value);
        return true;
    }
    return ParseAbsolute(input, result, now, timeZone);
}

}   // namespace

bool TryParseTimestamp(
    TStringBuf input,
    TInstant& result,
    TInstant now,
    const NDatetime::TTimeZone& timeZone)
{
    input = Trim(input);
    if (input.empty() || input.find('\0') != TStringBuf::npos) {
        return false;
    }
    bool relative = true;
    bool subtract = false;
    if (input.SkipPrefix("+")) {
    } else if (input.SkipPrefix("-")) {
        subtract = true;
    } else if (input.ChopSuffix(" ago")) {
        subtract = true;
    } else if (!input.ChopSuffix(" left")) {
        relative = false;
    }
    if (relative) {
        ui64 span;
        if (!ParseSpan(input, span)) {
            return false;
        }
        return ApplyOffset(now, span, subtract, result);
    }

    // Prefer the complete timestamp so numeric timezone offsets keep their
    // meaning, e.g. "today +05" and "2018-08-09 07:06 +05:30".
    if (ParseBase(input, result, now, timeZone)) {
        return true;
    }

    // A span modifier needs whitespace before its sign. Date separators and
    // directly attached RFC3339 timezone offsets are therefore not split.
    for (size_t i = 1; i < input.size(); ++i) {
        if ((input[i] != '+' && input[i] != '-') || !IsAsciiSpace(input[i - 1]))
        {
            continue;
        }
        ui64 span;
        TInstant base;
        if (ParseSpan(input.SubStr(i + 1), span) &&
            ParseBase(input.SubStr(0, i), base, now, timeZone))
        {
            return ApplyOffset(base, span, input[i] == '-', result);
        }
    }
    return false;
}

}   // namespace NSystemdTime
