#pragma once

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TString GetSvnUrl();

TString GetSvnUrlFromInfo(const TString& info);

TString GetSvnBranch();

TString GetSvnBranchFromUrl(const TString& url);

int GetSvnRevision();

int GetRevisionFromBranch(const TString& branch);

const TString& GetFullVersionString();

}   // namespace NCloud
