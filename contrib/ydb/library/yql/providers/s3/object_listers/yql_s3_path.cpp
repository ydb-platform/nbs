#include "yql_s3_path.h"

#include <contrib/ydb/library/yql/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYql::NS3 {

TString NormalizePath(const TString& path, char slash) {
    if (path.empty()) {
        throw yexception() << "Path should not be empty";
    }
    TString result;
    bool start = true;
    for (char c : path) {
        if (start && c == slash) {
            continue;
        }
        start = false;
        if (c != slash || result.back() != slash) {
            result.push_back(c);
        }
    }

    if (result.empty()) {
        YQL_ENSURE(start);
        result.push_back(slash);
    }

    return result;
}

size_t GetFirstWildcardPos(const TString& path) {
    return path.find_first_of("*?{");
}

TString EscapeRegex(const std::string_view& str) {
    return RE2::QuoteMeta(re2::StringPiece(str));

}

TString EscapeRegex(const TString& str) {
    return EscapeRegex(static_cast<std::string_view>(str));
}

TString RegexFromWildcards(const std::string_view& pattern) {
    const auto& escaped = EscapeRegex(pattern);
    TStringBuilder result;
    bool slash = false;
    bool group = false;

    for (const char& c : escaped) {
        switch (c) {
            case '{':
                result << "(?:";
                group = true;
                slash = false;
                break;
            case '}':
                result << ')';
                group = false;
                slash = false;
                break;
            case ',':
                if (group)
                    result << '|';
                else
                    result << "\\,";
                slash = false;
                break;
            case '\\':
                if (slash)
                    result << "\\\\";
                slash = !slash;
                break;
            case '*':
                result << ".*";
                slash = false;
                break;
            case '?':
                result << ".";
                slash = false;
                break;
            default:
                if (slash)
                    result << '\\';
                result << c;
                slash = false;
                break;
        }
    }
    return result;
}

}
