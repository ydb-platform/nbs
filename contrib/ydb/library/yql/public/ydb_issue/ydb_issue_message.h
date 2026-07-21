#pragma once
#include <contrib/ydb/library/yql/public/issue/yql_issue_message.h>

namespace NYql {

TString IssueToBinaryMessage(const TIssue& issue);
TIssue IssueFromBinaryMessage(const TString& binaryMessage);

}
