#include "mkql_computation_node_pack.h"
#include "mkql_computation_node_pack_impl.h"
#include "mkql_computation_node_holders.h"
#include "presort.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/pack.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <library/cpp/resource/resource.h>
#include <ydb/library/yql/utils/fp_bits.h>

#include <util/system/yassert.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

using namespace NDetails;

template<bool Fast, typename T, typename TBuf>
void PackData(T value, TBuf& buffer) {
    static_assert(std::is_arithmetic_v<T>);
    if constexpr (Fast || sizeof(T) == 1 || std::is_floating_point_v<T>) {
        PutRawData(value, buffer);
    } else if constexpr (std::is_same_v<T, i16>) {
        PackInt16(value, buffer);
    } else if constexpr (std::is_same_v<T, ui16>) {
        PackUInt16(value, buffer);
    } else if constexpr (std::is_same_v<T, i32>) {
        PackInt32(value, buffer);
    } else if constexpr (std::is_same_v<T, ui32>) {
        PackUInt32(value, buffer);
    } else if constexpr (std::is_same_v<T, i64>) {
        PackInt64(value, buffer);
    } else {
        static_assert(std::is_same_v<T, ui64>);
        PackUInt64(value, buffer);
    }
}

template<typename TBuf>
void PackBlob(const char* data, size_t size, TBuf& buf) {
    buf.Append(data, size);
}

template <bool Fast, typename T>
T UnpackData(TChunkedInputBuffer& buf) {
    static_assert(std::is_arithmetic_v<T>);
    T res;
    if constexpr (Fast || sizeof(T) == 1 || std::is_floating_point_v<T>) {
        res = GetRawData<T>(buf);
    } else if constexpr (std::is_same_v<T, i16>) {
        res = UnpackInt16(buf);
    } else if constexpr (std::is_same_v<T, ui16>) {
        res = UnpackUInt16(buf);
    } else if constexpr (std::is_same_v<T, i32>) {
        res = UnpackInt32(buf);
    } else if constexpr (std::is_same_v<T, ui32>) {
        res = UnpackUInt32(buf);
    } else if constexpr (std::is_same_v<T, i64>) {
        res = UnpackInt64(buf);
    } else {
        static_assert(std::is_same_v<T, ui64>);
        res = UnpackUInt64(buf);
    }
    return res;
}

NUdf::TUnboxedValuePod UnpackString(TChunkedInputBuffer& buf, ui32 size) {
    auto res = MakeStringNotFilled(size, 0);
    NYql::NUdf::TMutableStringRef ref = res.AsStringRef();
    Y_VERIFY_DEBUG(size == ref.Size());
    buf.CopyTo(ref.Data(), size);
    return res;
}

template<typename TBuf>
void SerializeMeta(TBuf& buf, bool useMask, const NDetails::TOptionalUsageMask& mask, ui32 fullLen, bool singleOptional) {
    if (fullLen > 7) {
        NDetails::PutRawData(fullLen, buf);
        // Long length always singnals non-empty optional. So, don't check
        // EProps::SingleOptional here
    } else {
        ui8 length = 1 | (fullLen << 1);
        // Empty root optional always has short length. Embed empty flag
        // into the length
        if (singleOptional && !mask.IsEmptyMask()) {
            length |= 0x10;
        }
        NDetails::PutRawData(length, buf);
    }
    if (useMask) {
        // Prepend optional mask before data
        mask.Serialize(buf);
    }
}

class TFixedSizeBuffer {
public:
    TFixedSizeBuffer(char* buf, size_t size)
        : Data_(buf)
        , Capacity_(size)
    {
    }

    inline char* Pos() {
        return Data_ + Size_;
    }

    inline size_t Size() const {
        return Size_;
    }

    inline void Advance(size_t len) {
        Size_ += len;
    }

    inline void EraseBack(size_t len) {
        Y_VERIFY_DEBUG(Size_ >= len);
        Size_ -= len;
    }

    inline void Append(const char* data, size_t len) {
        Y_VERIFY_DEBUG(Size_ + len <= Capacity_);
        std::memcpy(Data_ + Size_, data, len);
        Size_ += len;
    }

    inline void Append(char c) {
        Y_VERIFY_DEBUG(Size_ + 1 <= Capacity_);
        *(Pos()) = c;
        ++Size_;
    }
private:
    char* const Data_;
    size_t Size_ = 0;
    const size_t Capacity_;
};

template<bool Fast>
std::pair<ui32, bool> SkipEmbeddedLength(TChunkedInputBuffer& buf, size_t totalBufSize) {
    if constexpr (Fast) {
        Y_FAIL("Should not be called");
    }
    ui32 length = 0;
    bool emptySingleOptional = false;
    if (totalBufSize > 8) {
        length = GetRawData<ui32>(buf);
        MKQL_ENSURE(length + 4 == totalBufSize, "Bad packed data. Invalid embedded size");
    } else {
        length = GetRawData<ui8>(buf);
        MKQL_ENSURE(length & 1, "Bad packed data. Invalid embedded size");
        emptySingleOptional = 0 != (length & 0x10);
        length = (length & 0x0f) >> 1;
        MKQL_ENSURE(length + 1 == totalBufSize, "Bad packed data. Invalid embedded size");
    }
    return {length, emptySingleOptional};
}

bool HasOptionalFields(const TType* type) {
    switch (type->GetKind()) {
    case TType::EKind::Void:
    case TType::EKind::Null:
    case TType::EKind::EmptyList:
    case TType::EKind::EmptyDict:
    case TType::EKind::Data:
        return false;

    case TType::EKind::Optional:
        return true;

    case TType::EKind::Pg:
        return true;

    case TType::EKind::List:
        return HasOptionalFields(static_cast<const TListType*>(type)->GetItemType());

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            if (HasOptionalFields(structType->GetMemberType(index))) {
                return true;
            }
        }
        return false;
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            if (HasOptionalFields(tupleType->GetElementType(index))) {
                return true;
            }
        }
        return false;
    }

    case TType::EKind::Dict:  {
        auto dictType = static_cast<const TDictType*>(type);
        return HasOptionalFields(dictType->GetKeyType()) || HasOptionalFields(dictType->GetPayloadType());
    }

    case TType::EKind::Variant:  {
        auto variantType = static_cast<const TVariantType*>(type);
        return HasOptionalFields(variantType->GetUnderlyingType());
    }

    case TType::EKind::Tagged:  {
        auto taggedType = static_cast<const TTaggedType*>(type);
        return HasOptionalFields(taggedType->GetBaseType());
    }

    case TType::EKind::Multi: {
        auto multiType = static_cast<const TMultiType*>(type);
        for (ui32 index = 0; index < multiType->GetElementsCount(); ++index) {
            if (HasOptionalFields(multiType->GetElementType(index))) {
                return true;
            }
        }
        return false;
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}

TPackProperties ScanTypeProperties(const TType* type, bool assumeList) {
    TPackProperties props;
    if (HasOptionalFields(type)) {
        props.Set(EPackProps::UseOptionalMask);
    }
    if (assumeList) {
        return props;
    }
    if (type->GetKind() == TType::EKind::Optional) {
        type = static_cast<const TOptionalType*>(type)->GetItemType();
        if (!HasOptionalFields(type)) {
            props.Set(EPackProps::SingleOptional);
            props.Reset(EPackProps::UseOptionalMask);
        }
    }
    // Here and after the type is unwrapped!!

    if (type->GetKind() == TType::EKind::Data) {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::JsonDocument:
            // Reuse entire packed value length for strings
            props.Set(EPackProps::UseTopLength);
            break;
        default:
            break;
        }
    }
    return props;
}

template<bool Fast>
NUdf::TUnboxedValue UnpackFromChunkedBuffer(const TType* type, TChunkedInputBuffer& buf, ui32 topLength,
    const THolderFactory& holderFactory, TPackerState& s)
{
    switch (type->GetKind()) {
    case TType::EKind::Void:
        return NUdf::TUnboxedValuePod::Void();
    case TType::EKind::Null:
        return NUdf::TUnboxedValuePod();
    case TType::EKind::EmptyList:
        return holderFactory.GetEmptyContainer();
    case TType::EKind::EmptyDict:
        return holderFactory.GetEmptyContainer();

    case TType::EKind::Data: {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Bool:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, bool>(buf));
        case NUdf::EDataSlot::Int8:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, i8>(buf));
        case NUdf::EDataSlot::Uint8:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, ui8>(buf));
        case NUdf::EDataSlot::Int16:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, i16>(buf));
        case NUdf::EDataSlot::Uint16:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, ui16>(buf));
        case NUdf::EDataSlot::Int32:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, i32>(buf));
        case NUdf::EDataSlot::Uint32:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, ui32>(buf));
        case NUdf::EDataSlot::Int64:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, i64>(buf));
        case NUdf::EDataSlot::Uint64:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, ui64>(buf));
        case NUdf::EDataSlot::Float:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, float>(buf));
        case NUdf::EDataSlot::Double:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, double>(buf));
        case NUdf::EDataSlot::Date:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, ui16>(buf));
        case NUdf::EDataSlot::Datetime:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, ui32>(buf));
        case NUdf::EDataSlot::Timestamp:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, ui64>(buf));
        case NUdf::EDataSlot::Interval:
            return NUdf::TUnboxedValuePod(UnpackData<Fast, i64>(buf));
        case NUdf::EDataSlot::TzDate: {
            auto ret = NUdf::TUnboxedValuePod(UnpackData<Fast, ui16>(buf));
            ret.SetTimezoneId(UnpackData<Fast, ui16>(buf));
            return ret;
        }
        case NUdf::EDataSlot::TzDatetime: {
            auto ret = NUdf::TUnboxedValuePod(UnpackData<Fast, ui32>(buf));
            ret.SetTimezoneId(UnpackData<Fast, ui16>(buf));
            return ret;
        }
        case NUdf::EDataSlot::TzTimestamp: {
            auto ret = NUdf::TUnboxedValuePod(UnpackData<Fast, ui64>(buf));
            ret.SetTimezoneId(UnpackData<Fast, ui16>(buf));
            return ret;
        }
        case NUdf::EDataSlot::Uuid: {
            return UnpackString(buf, 16);
        }
        case NUdf::EDataSlot::Decimal: {
            const auto des = NYql::NDecimal::Deserialize(buf.data(), buf.size());
            MKQL_ENSURE(!NYql::NDecimal::IsError(des.first), "Bad packed data: invalid decimal.");
            buf.Skip(des.second);
            return NUdf::TUnboxedValuePod(des.first);
        }
        default:
            ui32 size = 0;
            if constexpr (Fast) {
                size = NDetails::GetRawData<ui32>(buf);
            } else {
                if (s.Properties.Test(EPackProps::UseTopLength)) {
                    size = topLength;
                } else {
                    size = NDetails::UnpackUInt32(buf);
                }
            }
            return UnpackString(buf, size);
        }
        break;
    }

    case TType::EKind::Optional: {
        auto optionalType = static_cast<const TOptionalType*>(type);
        bool present;
        if constexpr (Fast) {
            present = NDetails::GetRawData<ui8>(buf);
        } else {
            present = !s.OptionalUsageMask.IsNextEmptyOptional();
        }

        if (present) {
            return UnpackFromChunkedBuffer<Fast>(optionalType->GetItemType(), buf, topLength, holderFactory, s).Release().MakeOptional();
        } else {
            return NUdf::TUnboxedValuePod();
        }
    }

    case TType::EKind::Pg: {
        auto pgType = static_cast<const TPgType*>(type);
        bool present;
        if constexpr (Fast) {
            present = NDetails::GetRawData<ui8>(buf);
        } else {
            present = !s.OptionalUsageMask.IsNextEmptyOptional();
        }
        if (present) {
            return PGUnpackImpl(pgType, buf);
        } else {
            return NUdf::TUnboxedValuePod();
        }
    }

    case TType::EKind::List: {
        auto listType = static_cast<const TListType*>(type);
        auto itemType = listType->GetItemType();

        ui64 len;
        if constexpr (Fast) {
            len = NDetails::GetRawData<ui64>(buf);
        } else {
            len = NDetails::UnpackUInt64(buf);
        }

        if (!len) {
            return holderFactory.GetEmptyContainer();
        }

        TTemporaryUnboxedValueVector tmp;
        for (ui64 i = 0; i < len; ++i) {
            tmp.emplace_back(UnpackFromChunkedBuffer<Fast>(itemType, buf, topLength, holderFactory, s));
        }

        NUdf::TUnboxedValue *items = nullptr;
        auto list = holderFactory.CreateDirectArrayHolder(len, items);
        for (ui64 i = 0; i < len; ++i) {
            items[i] = std::move(tmp[i]);
        }

        return std::move(list);
    }

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto res = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), itemsPtr);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            itemsPtr[index] = UnpackFromChunkedBuffer<Fast>(memberType, buf, topLength, holderFactory, s);
        }
        return std::move(res);
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto res = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), itemsPtr);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            auto elementType = tupleType->GetElementType(index);
            itemsPtr[index] = UnpackFromChunkedBuffer<Fast>(elementType, buf, topLength, holderFactory, s);
        }
        return std::move(res);
    }

    case TType::EKind::Dict: {
        auto dictType = static_cast<const TDictType*>(type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        auto dictBuilder = holderFactory.NewDict(dictType, NUdf::TDictFlags::EDictKind::Hashed);

        ui64 len;
        if constexpr (Fast) {
            len = NDetails::GetRawData<ui64>(buf);
        } else {
            len = NDetails::UnpackUInt64(buf);
        }

        for (ui64 i = 0; i < len; ++i) {
            auto key = UnpackFromChunkedBuffer<Fast>(keyType, buf, topLength, holderFactory, s);
            auto payload = UnpackFromChunkedBuffer<Fast>(payloadType, buf, topLength, holderFactory, s);
            dictBuilder->Add(std::move(key), std::move(payload));
        }
        return dictBuilder->Build();
    }

    case TType::EKind::Variant: {
        auto variantType = static_cast<const TVariantType*>(type);
        ui32 variantIndex;
        if constexpr (Fast) {
            variantIndex = NDetails::GetRawData<ui32>(buf);
        } else {
            variantIndex = NDetails::UnpackUInt32(buf);
        }

        TType* innerType = variantType->GetUnderlyingType();
        if (innerType->IsStruct()) {
            MKQL_ENSURE(variantIndex < static_cast<TStructType*>(innerType)->GetMembersCount(), "Bad variant index: " << variantIndex);
            innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
        } else {
            MKQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
            MKQL_ENSURE(variantIndex < static_cast<TTupleType*>(innerType)->GetElementsCount(), "Bad variant index: " << variantIndex);
            innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
        }
        return holderFactory.CreateVariantHolder(UnpackFromChunkedBuffer<Fast>(innerType, buf, topLength, holderFactory, s).Release(), variantIndex);
    }

    case TType::EKind::Tagged: {
        auto taggedType = static_cast<const TTaggedType*>(type);
        return UnpackFromChunkedBuffer<Fast>(taggedType->GetBaseType(), buf, topLength, holderFactory, s);
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}

template<bool Fast>
NUdf::TUnboxedValue DoUnpack(const TType* type, TChunkedInputBuffer& buf, size_t totalBufSize, const THolderFactory& holderFactory, TPackerState& s) {
    if constexpr (Fast) {
        NUdf::TUnboxedValue res;
        res = UnpackFromChunkedBuffer<Fast>(type, buf, 0, holderFactory, s);
        MKQL_ENSURE(buf.IsEmpty(), "Bad packed data - partial data read");
        return res;
    }

    auto pair = SkipEmbeddedLength<Fast>(buf, totalBufSize);
    ui32 length = pair.first;
    bool emptySingleOptional = pair.second;

    if (s.Properties.Test(EPackProps::UseOptionalMask)) {
        s.OptionalUsageMask.Reset(buf);
    }
    NUdf::TUnboxedValue res;
    if (s.Properties.Test(EPackProps::SingleOptional) && emptySingleOptional) {
        res = NUdf::TUnboxedValuePod();
    } else if (type->IsStruct()) {
        auto structType = static_cast<const TStructType*>(type);
        NUdf::TUnboxedValue* items = nullptr;
        res = s.TopStruct.NewArray(holderFactory, structType->GetMembersCount(), items);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            *items++ = UnpackFromChunkedBuffer<Fast>(memberType, buf, length, holderFactory, s);
        }
    } else {
        res = UnpackFromChunkedBuffer<Fast>(type, buf, length, holderFactory, s);
    }

    MKQL_ENSURE(buf.IsEmpty(), "Bad packed data - partial data read");
    return res;
}

template<bool Fast>
void DoUnpackBatch(const TType* type, TChunkedInputBuffer& buf, size_t totalSize, const THolderFactory& holderFactory, TPackerState& s, TUnboxedValueBatch& result) {
    ui64 len;
    ui32 topLength;
    const TType* itemType = type;
    if constexpr (!Fast) {
        auto pair = SkipEmbeddedLength<Fast>(buf, totalSize);
        topLength = pair.first;
        bool emptySingleOptional = pair.second;

        if (s.Properties.Test(EPackProps::UseOptionalMask)) {
            s.OptionalUsageMask.Reset(buf);
        }

        MKQL_ENSURE(!s.Properties.Test(EPackProps::SingleOptional) || !emptySingleOptional, "Unexpected header settings");
        len = NDetails::UnpackUInt64(buf);
    } else {
        topLength = 0;
        len = NDetails::GetRawData<ui64>(buf);
    }

    if (type->IsMulti()) {
        auto multiType = static_cast<const TMultiType*>(type);
        const ui32 width = multiType->GetElementsCount();
        Y_VERIFY_DEBUG(result.IsWide());
        Y_VERIFY_DEBUG(result.Width() == width);
        for (ui64 i = 0; i < len; ++i) {
            result.PushRow([&](ui32 j) {
                return UnpackFromChunkedBuffer<Fast>(multiType->GetElementType(j), buf, topLength, holderFactory, s);
            });
        }
    } else {
        Y_VERIFY_DEBUG(!result.IsWide());
        for (ui64 i = 0; i < len; ++i) {
            result.emplace_back(UnpackFromChunkedBuffer<Fast>(itemType, buf, topLength, holderFactory, s));
        }
    }
    MKQL_ENSURE(buf.IsEmpty(), "Bad packed data - partial data read");
}

template<bool Fast, bool Stable, typename TBuf>
void PackImpl(const TType* type, TBuf& buffer, const NUdf::TUnboxedValuePod& value, TPackerState& s) {
    switch (type->GetKind()) {
    case TType::EKind::Void:
        break;
    case TType::EKind::Null:
        break;
    case TType::EKind::EmptyList:
        break;
    case TType::EKind::EmptyDict:
        break;

    case TType::EKind::Data: {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Bool:
            PackData<Fast>(value.Get<bool>(), buffer);
            break;
        case NUdf::EDataSlot::Int8:
            PackData<Fast>(value.Get<i8>(), buffer);
            break;
        case NUdf::EDataSlot::Uint8:
            PackData<Fast>(value.Get<ui8>(), buffer);
            break;
        case NUdf::EDataSlot::Int16:
            PackData<Fast>(value.Get<i16>(), buffer);
            break;
        case NUdf::EDataSlot::Uint16:
            PackData<Fast>(value.Get<ui16>(), buffer);
            break;
        case NUdf::EDataSlot::Int32:
            PackData<Fast>(value.Get<i32>(), buffer);
            break;
        case NUdf::EDataSlot::Uint32:
            PackData<Fast>(value.Get<ui32>(), buffer);
            break;
        case NUdf::EDataSlot::Int64:
            PackData<Fast>(value.Get<i64>(), buffer);
            break;
        case NUdf::EDataSlot::Uint64:
            PackData<Fast>(value.Get<ui64>(), buffer);
            break;
        case NUdf::EDataSlot::Float: {
            float x = value.Get<float>();
            if constexpr (Stable) {
                NYql::CanonizeFpBits<float>(&x);
            }

            PackData<Fast>(x, buffer);
            break;
        }
        case NUdf::EDataSlot::Double: {
            double x = value.Get<double>();
            if constexpr (Stable) {
                NYql::CanonizeFpBits<double>(&x);
            }

            PackData<Fast>(x, buffer);
            break;
        }
        case NUdf::EDataSlot::Date:
            PackData<Fast>(value.Get<ui16>(), buffer);
            break;
        case NUdf::EDataSlot::Datetime:
            PackData<Fast>(value.Get<ui32>(), buffer);
            break;
        case NUdf::EDataSlot::Timestamp:
            PackData<Fast>(value.Get<ui64>(), buffer);
            break;
        case NUdf::EDataSlot::Interval:
            PackData<Fast>(value.Get<i64>(), buffer);
            break;
        case NUdf::EDataSlot::Uuid: {
            auto ref = value.AsStringRef();
            PackBlob(ref.Data(), ref.Size(), buffer);
            break;
        }
        case NUdf::EDataSlot::TzDate: {
            PackData<Fast>(value.Get<ui16>(), buffer);
            PackData<Fast>(value.GetTimezoneId(), buffer);
            break;
        }
        case NUdf::EDataSlot::TzDatetime: {
            PackData<Fast>(value.Get<ui32>(), buffer);
            PackData<Fast>(value.GetTimezoneId(), buffer);
            break;
        }
        case NUdf::EDataSlot::TzTimestamp: {
            PackData<Fast>(value.Get<ui64>(), buffer);
            PackData<Fast>(value.GetTimezoneId(), buffer);
            break;
        }
        case NUdf::EDataSlot::Decimal: {
            char buff[0x10U];
            PackBlob(buff, NYql::NDecimal::Serialize(value.GetInt128(), buff), buffer);
            break;
        }
        default: {
            auto stringRef = value.AsStringRef();
            if constexpr (Fast) {
                static_assert(std::is_same_v<decltype(stringRef.Size()), ui32>);
                PackData<Fast>(stringRef.Size(), buffer);
            } else {
                if (!s.Properties.Test(EPackProps::UseTopLength)) {
                    PackData<Fast>(stringRef.Size(), buffer);
                }
            }
            PackBlob(stringRef.Data(), stringRef.Size(), buffer);
        }
        }
        break;
    }

    case TType::EKind::Optional: {
        auto optionalType = static_cast<const TOptionalType*>(type);
        if constexpr (Fast) {
            PackData<Fast>(ui8(bool(value)), buffer);
        } else {
            s.OptionalUsageMask.SetNextEmptyOptional(!value);
        }
        if (value) {
            PackImpl<Fast, Stable>(optionalType->GetItemType(), buffer, value.GetOptionalValue(), s);
        }
        break;
    }

    case TType::EKind::Pg: {
        auto pgType = static_cast<const TPgType*>(type);
        if constexpr (Fast) {
            PackData<Fast>(ui8(bool(value)), buffer);
        } else {
            s.OptionalUsageMask.SetNextEmptyOptional(!value);
        }
        if (value) {
            PGPackImpl(Stable, pgType, value, buffer);
        }
        break;
    }

    case TType::EKind::List: {
        auto listType = static_cast<const TListType*>(type);
        auto itemType = listType->GetItemType();
        if (value.HasFastListLength()) {
            ui64 len = value.GetListLength();
            PackData<Fast>(len, buffer);
            TThresher<false>::DoForEachItem(value,
                [&](const NYql::NUdf::TUnboxedValuePod& item) { PackImpl<Fast, Stable>(itemType, buffer, item, s); });
        } else {
            const auto iter = value.GetListIterator();
            if constexpr (Fast) {
                ui64 count = 0;
                buffer.Advance(sizeof(count));
                char* dst = buffer.Pos() - sizeof(count);
                for (NUdf::TUnboxedValue item; iter.Next(item);) {
                    PackImpl<Fast, Stable>(itemType, buffer, item, s);
                    ++count;
                }
                std::memcpy(dst, &count, sizeof(count));
            } else {
                TUnboxedValueVector items;
                for (NUdf::TUnboxedValue item; iter.Next(item);) {
                    items.emplace_back(std::move(item));
                }
                PackData<Fast>(ui64(items.size()), buffer);
                for (const auto& item : items) {
                    PackImpl<Fast, Stable>(itemType, buffer, item, s);
                }
            }
        }
        break;
    }

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            PackImpl<Fast, Stable>(memberType, buffer, value.GetElement(index), s);
        }
        break;
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            auto elementType = tupleType->GetElementType(index);
            PackImpl<Fast, Stable>(elementType, buffer, value.GetElement(index), s);
        }
        break;
    }

    case TType::EKind::Dict:  {
        auto dictType = static_cast<const TDictType*>(type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();

        ui64 length = value.GetDictLength();
        PackData<Fast>(length, buffer);
        const auto iter = value.GetDictIterator();
        if constexpr (Fast) {
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                PackImpl<Fast, Stable>(keyType, buffer, key, s);
                PackImpl<Fast, Stable>(payloadType, buffer, payload, s);
            }
        } else {
            if (Stable && !value.IsSortedDict()) {
                // no key duplicates here
                TKeyTypes types;
                bool isTuple;
                bool encoded;
                bool useIHash;
                GetDictionaryKeyTypes(keyType, types, isTuple, encoded, useIHash);
                if (encoded) {
                    TGenericPresortEncoder packer(keyType);
                    typename decltype(s.EncodedDictBuffers)::value_type dictBuffer;
                    if (!s.EncodedDictBuffers.empty()) {
                        dictBuffer = std::move(s.EncodedDictBuffers.back());
                        s.EncodedDictBuffers.pop_back();
                        dictBuffer.clear();
                    }
                    dictBuffer.reserve(length);
                    for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                        NUdf::TUnboxedValue encodedKey = MakeString(packer.Encode(key, false));
                        dictBuffer.emplace_back(std::move(encodedKey), std::move(key), std::move(payload));
                    }

                    Sort(dictBuffer.begin(), dictBuffer.end(),
                        [&](const auto &left, const auto &right) {
                            return CompareKeys(std::get<0>(left), std::get<0>(right), types, isTuple) < 0;
                        });

                    for (const auto& x : dictBuffer) {
                        PackImpl<Fast, Stable>(keyType, buffer, std::get<1>(x), s);
                        PackImpl<Fast, Stable>(payloadType, buffer, std::get<2>(x), s);
                    }
                    dictBuffer.clear();
                    s.EncodedDictBuffers.push_back(std::move(dictBuffer));
                } else {
                    typename decltype(s.DictBuffers)::value_type dictBuffer;
                    if (!s.DictBuffers.empty()) {
                        dictBuffer = std::move(s.DictBuffers.back());
                        s.DictBuffers.pop_back();
                        dictBuffer.clear();
                    }
                    dictBuffer.reserve(length);
                    for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                        dictBuffer.emplace_back(std::move(key), std::move(payload));
                    }

                    NUdf::ICompare::TPtr cmp = useIHash ? MakeCompareImpl(keyType) : nullptr;
                    Sort(dictBuffer.begin(), dictBuffer.end(), TKeyPayloadPairLess(types, isTuple, cmp.Get()));

                    for (const auto& p: dictBuffer) {
                        PackImpl<Fast, Stable>(keyType, buffer, p.first, s);
                        PackImpl<Fast, Stable>(payloadType, buffer, p.second, s);
                    }
                    dictBuffer.clear();
                    s.DictBuffers.push_back(std::move(dictBuffer));
                }
            } else {
                for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                    PackImpl<Fast, Stable>(keyType, buffer, key, s);
                    PackImpl<Fast, Stable>(payloadType, buffer, payload, s);
                }
            }
        }
        break;
    }

    case TType::EKind::Variant: {
        auto variantType = static_cast<const TVariantType*>(type);
        ui32 variantIndex = value.GetVariantIndex();
        TType* innerType = variantType->GetUnderlyingType();
        if (innerType->IsStruct()) {
            innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
        } else {
            MKQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
            innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
        }
        PackData<Fast>(variantIndex, buffer);
        PackImpl<Fast, Stable>(innerType, buffer, value.GetVariantItem(), s);
        break;
    }

    case TType::EKind::Tagged: {
        auto taggedType = static_cast<const TTaggedType*>(type);
        PackImpl<Fast, Stable>(taggedType->GetBaseType(), buffer, value, s);
        break;
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}


} // namespace

template<bool Fast>
TValuePackerGeneric<Fast>::TValuePackerGeneric(bool stable, const TType* type)
    : Stable_(stable)
    , Type_(type)
    , State_(ScanTypeProperties(Type_, false))
{
    MKQL_ENSURE(!Fast || !Stable_, "Stable mode is not supported");
}

template<bool Fast>
NUdf::TUnboxedValue TValuePackerGeneric<Fast>::Unpack(TStringBuf buf, const THolderFactory& holderFactory) const {
    TChunkedInputBuffer chunked(buf);
    return DoUnpack<Fast>(Type_, chunked, buf.size(), holderFactory, State_);
}

template<bool Fast>
TStringBuf TValuePackerGeneric<Fast>::Pack(const NUdf::TUnboxedValuePod& value) const {
    auto& s = State_;
    if constexpr (Fast) {
        Buffer_.Proceed(0);
        if (Stable_) {
            PackImpl<Fast, true>(Type_, Buffer_, value, s);
        } else {
            PackImpl<Fast, false>(Type_, Buffer_, value, s);
        }
        return TStringBuf(Buffer_.data(), Buffer_.size());
    }

    s.OptionalUsageMask.Reset();
    const size_t lengthReserve = sizeof(ui32);
    Buffer_.Proceed(lengthReserve + s.OptionalMaskReserve);

    if (Stable_) {
        PackImpl<Fast, true>(Type_, Buffer_, value, s);
    } else {
        PackImpl<Fast, false>(Type_, Buffer_, value, s);
    }

    size_t delta = 0;
    size_t len = Buffer_.Size();

    if (s.Properties.Test(EPackProps::UseOptionalMask)) {
        // Prepend optional mask
        const size_t actualOptionalMaskSize = s.OptionalUsageMask.CalcSerializedSize();

        if (actualOptionalMaskSize > s.OptionalMaskReserve) {
            TBuffer buf(Buffer_.Size() + actualOptionalMaskSize - s.OptionalMaskReserve);
            buf.Proceed(actualOptionalMaskSize - s.OptionalMaskReserve);
            buf.Append(Buffer_.Data(), Buffer_.Size());
            Buffer_.Swap(buf);
            s.OptionalMaskReserve = actualOptionalMaskSize;
            len = Buffer_.Size();
        }

        delta = s.OptionalMaskReserve - actualOptionalMaskSize;
        Buffer_.Proceed(lengthReserve + delta);
        s.OptionalUsageMask.Serialize(Buffer_);
    }

    // Prepend length
    if (len - delta - lengthReserve > 7) {
        const ui32 length = len - delta - lengthReserve;
        Buffer_.Proceed(delta);
        Buffer_.Append((const char*)&length, sizeof(length));
        // Long length always singnals non-empty optional. So, don't check EProps::SingleOptional here
    } else {
        ui8 length = 1 | ((len - delta - lengthReserve) << 1);
        // Empty root optional always has short length. Embed empty flag into the length
        if (s.Properties.Test(EPackProps::SingleOptional) && !s.OptionalUsageMask.IsEmptyMask()) {
            length |= 0x10;
        }
        delta += 3;
        Buffer_.Proceed(delta);
        Buffer_.Append((const char*)&length, sizeof(length));
    }
    return TStringBuf(Buffer_.Data() + delta, len - delta);
}


// Transport packer
template<bool Fast>
TValuePackerTransport<Fast>::TValuePackerTransport(bool stable, const TType* type)
    : Type_(type)
    , State_(ScanTypeProperties(Type_, false))
    , IncrementalState_(ScanTypeProperties(Type_, true))
{
    MKQL_ENSURE(!stable, "Stable packing is not supported");
}

template<bool Fast>
TValuePackerTransport<Fast>::TValuePackerTransport(const TType* type)
    : Type_(type)
    , State_(ScanTypeProperties(Type_, false))
    , IncrementalState_(ScanTypeProperties(Type_, true))
{
}

template<bool Fast>
NUdf::TUnboxedValue TValuePackerTransport<Fast>::Unpack(TStringBuf buf, const THolderFactory& holderFactory) const {
    TChunkedInputBuffer chunked(buf);
    return DoUnpack<Fast>(Type_, chunked, buf.size(), holderFactory, State_);
}

template<bool Fast>
NUdf::TUnboxedValue TValuePackerTransport<Fast>::Unpack(TRope&& buf, const THolderFactory& holderFactory) const {
    const size_t totalSize = buf.GetSize();
    TChunkedInputBuffer chunked(std::move(buf));
    return DoUnpack<Fast>(Type_, chunked, totalSize, holderFactory, State_);
}

template<bool Fast>
void TValuePackerTransport<Fast>::UnpackBatch(TStringBuf buf, const THolderFactory& holderFactory, TUnboxedValueBatch& result) const {
    TChunkedInputBuffer chunked(buf);
    DoUnpackBatch<Fast>(Type_, chunked, buf.size(), holderFactory, IncrementalState_, result);
}

template<bool Fast>
void TValuePackerTransport<Fast>::UnpackBatch(TRope&& buf, const THolderFactory& holderFactory, TUnboxedValueBatch& result) const {
    const size_t totalSize = buf.GetSize();
    TChunkedInputBuffer chunked(std::move(buf));
    DoUnpackBatch<Fast>(Type_, chunked, totalSize, holderFactory, IncrementalState_, result);
}

template<bool Fast>
TPagedBuffer::TPtr TValuePackerTransport<Fast>::Pack(const NUdf::TUnboxedValuePod& value) const {
    MKQL_ENSURE(ItemCount_ == 0, "Can not mix Pack() and AddItem() calls");
    TPagedBuffer::TPtr result = std::make_shared<TPagedBuffer>();
    if constexpr (Fast) {
        PackImpl<Fast, false>(Type_, *result, value, State_);
    } else {
        State_.OptionalUsageMask.Reset();
        result->ReserveHeader(sizeof(ui32) + State_.OptionalMaskReserve);
        PackImpl<Fast, false>(Type_, *result, value, State_);
        BuildMeta(result, false);
    }
    return result;
}

template<bool Fast>
void TValuePackerTransport<Fast>::StartPack() {
    Buffer_ = std::make_shared<TPagedBuffer>();
    if constexpr (Fast) {
        // reserve place for list item count
        Buffer_->ReserveHeader(sizeof(ItemCount_));
    } else {
        IncrementalState_.OptionalUsageMask.Reset();
        Buffer_->ReserveHeader(sizeof(ui32) + State_.OptionalMaskReserve + MAX_PACKED64_SIZE);
    }
}

template<bool Fast>
TValuePackerTransport<Fast>& TValuePackerTransport<Fast>::AddItem(const NUdf::TUnboxedValuePod& value) {
    Y_VERIFY_DEBUG(!Type_->IsMulti());
    const TType* itemType = Type_;
    if (!ItemCount_) {
        StartPack();
    }

    PackImpl<Fast, false>(itemType, *Buffer_, value, IncrementalState_);
    ++ItemCount_;
    return *this;
}

template<bool Fast>
TValuePackerTransport<Fast>& TValuePackerTransport<Fast>::AddWideItem(const NUdf::TUnboxedValuePod* values, ui32 width) {
    Y_VERIFY_DEBUG(Type_->IsMulti());
    Y_VERIFY_DEBUG(static_cast<const TMultiType*>(Type_)->GetElementsCount() == width);
    const TMultiType* itemType = static_cast<const TMultiType*>(Type_);
    if (!ItemCount_) {
        StartPack();
    }

    for (ui32 i = 0; i < width; ++i) {
        PackImpl<Fast, false>(itemType->GetElementType(i), *Buffer_, values[i], IncrementalState_);
    }
    ++ItemCount_;
    return *this;
}

template<bool Fast>
void TValuePackerTransport<Fast>::Clear() {
    Buffer_.reset();
    ItemCount_ = 0;
}

template<bool Fast>
TPagedBuffer::TPtr TValuePackerTransport<Fast>::Finish() {
    if (!ItemCount_) {
        StartPack();
    }
    if constexpr (Fast) {
        char* dst = Buffer_->Header(sizeof(ItemCount_));
        Y_VERIFY_DEBUG(dst);
        std::memcpy(dst, &ItemCount_, sizeof(ItemCount_));
    } else {
        BuildMeta(Buffer_, true);
    }
    TPagedBuffer::TPtr result = std::move(Buffer_);
    Clear();
    return result;
}

template<bool Fast>
void TValuePackerTransport<Fast>::BuildMeta(TPagedBuffer::TPtr& buffer, bool addItemCount) const {
    const size_t itemCountSize = addItemCount ? GetPack64Length(ItemCount_) : 0;
    const size_t packedSize = buffer->Size() + itemCountSize;

    auto& s = addItemCount ? IncrementalState_ : State_;

    const bool useMask = s.Properties.Test(EPackProps::UseOptionalMask);
    const size_t maskSize = useMask ? s.OptionalUsageMask.CalcSerializedSize() : 0;

    const size_t fullLen = maskSize + packedSize;
    MKQL_ENSURE(fullLen <= Max<ui32>(), "Packed obbject size exceeds 4G");

    size_t metaSize = (fullLen > 7 ? sizeof(ui32) : sizeof(ui8)) + maskSize;

    if (char* header = buffer->Header(metaSize + itemCountSize)) {
        TFixedSizeBuffer buf(header, metaSize + itemCountSize);
        SerializeMeta(buf, useMask, s.OptionalUsageMask, fullLen, s.Properties.Test(EPackProps::SingleOptional));
        if (addItemCount) {
            if constexpr (Fast) {
                PackData<Fast>(ItemCount_, buf);
            } else {
                // PackData() can not be used here - it may overwrite some bytes past the end of header
                char tmp[MAX_PACKED64_SIZE];
                size_t actualItemCountSize = Pack64(ItemCount_, tmp);
                std::memcpy(buf.Pos(), tmp, actualItemCountSize);
                buf.Advance(actualItemCountSize);
            }
        }
        MKQL_ENSURE(buf.Size() == metaSize + itemCountSize, "Partial header write");
    } else {
        s.OptionalMaskReserve = maskSize;

        TPagedBuffer::TPtr resultBuffer = std::make_shared<TPagedBuffer>();
        SerializeMeta(*resultBuffer, useMask, s.OptionalUsageMask, fullLen, s.Properties.Test(EPackProps::SingleOptional));
        if (addItemCount) {
            PackData<Fast>(ItemCount_, *resultBuffer);
        }

        buffer->ForEachPage([&resultBuffer](const char* data, size_t len) {
            resultBuffer->Append(data, len);
        });

        buffer = std::move(resultBuffer);
    }
}

template class TValuePackerGeneric<true>;
template class TValuePackerGeneric<false>;
template class TValuePackerTransport<true>;
template class TValuePackerTransport<false>;

TValuePackerBoxed::TValuePackerBoxed(TMemoryUsageInfo* memInfo, bool stable, const TType* type)
    : TBase(memInfo)
    , TValuePacker(stable, type)
{}

} // NMiniKQL
} // NKikimr