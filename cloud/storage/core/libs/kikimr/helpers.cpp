#include "helpers.h"

#include <contrib/ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NCloud {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeKikimrError(
    NKikimrProto::EReplyStatus status,
    TString errorReason)
{
    NProto::TError error;

    if (status != NKikimrProto::OK) {
        error.SetCode(MAKE_KIKIMR_ERROR(status));

        if (errorReason) {
            error.SetMessage(std::move(errorReason));
        } else {
            error.SetMessage(NKikimrProto::EReplyStatus_Name(status));
        }
    }

    return error;
}

NProto::TError MakeSchemeShardError(
    NKikimrScheme::EStatus status,
    TString errorReason)
{
    NProto::TError error;

    if (status != NKikimrScheme::StatusSuccess) {
        error.SetCode(MAKE_SCHEMESHARD_ERROR(status));

        if (errorReason) {
            error.SetMessage(std::move(errorReason));
        } else {
            error.SetMessage(NKikimrScheme::EStatus_Name(status));
        }
    }

    return error;
}

NProto::TError MakeDescribeSchemeError(
    const NKikimrScheme::TEvDescribeSchemeResult& result)
{
    auto error = MakeSchemeShardError(result.GetStatus(), result.GetReason());
    if (HasError(error)) {
        // Enrich the error message with the path to improve diagnostics
        error.SetMessage(
            TStringBuilder()
                << error.GetMessage()
                << ", path="
                << result.GetPath().Quote());
    }
    return error;
}

}   // namespace NCloud
