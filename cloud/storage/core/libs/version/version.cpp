#include "version.h"

#include <library/cpp/svnversion/svnversion.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <cctype>
#include <cmath>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TStringBuf None = "None";
const TStringBuf ArcPart = "/arc/";
const TStringBuf ArcadiaSuffix = "/arcadia";
const TStringBuf StablePrefix = "stable-";

} // namespace

////////////////////////////////////////////////////////////////////////////////

TString GetSvnUrl()
{
    auto url = GetArcadiaSourceUrl();
    if (url) {
        return url;
    }

    return GetSvnUrlFromInfo(GetProgramSvnVersion());
}

TString GetSvnUrlFromInfo(const TString& info)
{
    auto start = info.find("svn+ssh");
    if (start == TString::npos) {
        return {};
    }

    auto end = info.find("\n", start);
    if (end == TString::npos) {
        return {};
    }

    auto url = info.substr(start, end - start);

    int index = (int)url.size();
    --index;
    while (index >= 0) {
        if (std::isspace(url[index])) {
            --index;
        } else {
            break;
        }
    }

    return url.substr(0, index + 1);
}

TString GetSvnBranch()
{
    TStringBuf branch(GetBranch());
    if (branch) {
        branch.RSplitOff('/');
        return ToString(branch);
    }

    return GetSvnBranchFromUrl(GetSvnUrl());
}

TString GetSvnBranchFromUrl(const TString& url)
{
    if (!url) {
        return {};
    }

    auto arc = url.find(ArcPart);
    if (arc == TString::npos) {
        return {};
    }

    TStringBuf branch(url, arc + ArcPart.length());

    branch.ChopSuffix(ArcadiaSuffix);
    branch.RSplitOff('/');

    return ToString(branch);
}

int GetSvnRevision()
{
    auto res = GetProgramSvnRevision();
    if (res != -1) {
        return res;
    }

    res = GetRevisionFromBranch(GetSvnBranch());
    if (res != -1) {
        return res;
    }

    return GetProgramBuildTimestamp();
}

int GetRevisionFromBranch(const TString& branch)
{
    if (!branch.StartsWith(StablePrefix)) {
        return -1;
    }

    TVector<TStringBuf> splitted;
    StringSplitter(branch).Split('-').SkipEmpty().AddTo(&splitted);

    if (splitted.size() < 4) {
        return -1;
    }

    size_t n = 1;
    auto major = FromString<ui32>(splitted[n++]);
    auto minor = FromString<ui32>(splitted[n++]);
    auto tag = FromString<ui32>(splitted[n++]);
    auto hotfix = n < splitted.size() ? FromString<ui32>(splitted[n++]) : 0;

    if (splitted.size() != n) {
        return -1;
    }

#define ADD_VERSION_COMPONENT(component, symbolCount)                          \
    if (component > pow(10, symbolCount)) {                                    \
        return -1;                                                             \
    }                                                                          \
    version = version * pow(10, symbolCount) + component;

    int version = major;
    ADD_VERSION_COMPONENT(minor, 2);
    ADD_VERSION_COMPONENT(tag, 3);
    ADD_VERSION_COMPONENT(hotfix, 1);

#undef ADD_VERSION_COMPONENT
    return version;
}

const TString& GetFullVersionString()
{
    static const TString custom = GetCustomVersion();
    if (custom && custom != None) {
        return custom;
    }

    static const TString fullVersion = TStringBuilder()
        << GetSvnRevision()
        << '.'
        << GetSvnBranch();

    return fullVersion;
}

}   // namespace NCloud
