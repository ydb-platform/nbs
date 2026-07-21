#pragma once
#include <contrib/ydb/library/yql/minikql/mkql_node.h>
#include <contrib/ydb/library/yql/public/udf/udf_value.h>
namespace std {
template<>
class formatter<TString>: public formatter<std::string_view> {
public:
    auto format(const TString& str, auto& ctx) const {
        return formatter<std::string_view>::format(str.ConstRef(), ctx);
    }
};
}

namespace NKikimr::NMiniKQL {

struct IPrint: public NYql::NUdf::IRefCounted {
    virtual TString Stringify(NYql::NUdf::TUnboxedValuePod value) = 0;
    using TPtr = NYql::NUdf::TRefCountedPtr<IPrint>;
};

IPrint::TPtr MakePrinter(const TType* type);
}