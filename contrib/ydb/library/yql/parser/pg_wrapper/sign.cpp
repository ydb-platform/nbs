#include "pg_include.h"

#include <contrib/ydb/library/yql/parser/pg_wrapper/interface/sign.h>

#include <contrib/ydb/library/yql/parser/pg_wrapper/pg_ops.h>

#include <contrib/ydb/library/yql/minikql/mkql_alloc.h>

namespace NYql {

std::expected<std::strong_ordering, TString> PgSign(TStringBuf value, ui32 typeId) {
    try {
        NKikimr::NMiniKQL::TOnlyThrowingBindTerminator bind;
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NKikimr::NMiniKQL::TPAllocScope scope;

        TPgConst pgConst(typeId, value);
        auto pgValue = pgConst.ExtractConst();

        auto signFunc = TPgSign::Create(typeId);
        auto sign = signFunc->MakeCallState()->GetSign(pgValue);
        if (!sign) {
            return std::unexpected(TString("Value is NULL"));
        }

        if (*sign < 0) {
            return std::strong_ordering::less;
        } else if (*sign > 0) {
            return std::strong_ordering::greater;
        } else {
            return std::strong_ordering::equal;
        }
    } catch (const NYql::NPg::TProcNotFoundException& e) {
        return std::unexpected(TString(e.what()));
    } catch (const NYql::NPg::TOperatorNotFoundException& e) {
        return std::unexpected(TString(e.what()));
    }
}

} // namespace NYql
