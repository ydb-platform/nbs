#pragma once

#include "external_source.h"

#include <library/cpp/regex/pcre/regexp.h>

#include <contrib/ydb/public/api/protos/draft/fq.pb.h>

namespace NKikimr::NExternalSource {

IExternalSource::TPtr CreateObjectStorageExternalSource(const std::vector<TRegExMatch>& hostnamePatterns);

NYql::TIssues Validate(const FederatedQuery::Schema& schema, const FederatedQuery::ObjectStorageBinding::Subset& objectStorage, size_t pathsLimit);

NYql::TIssues ValidateDateFormatSetting(const google::protobuf::Map<TString, TString>& formatSetting, bool matchAllSettings = false);

}
