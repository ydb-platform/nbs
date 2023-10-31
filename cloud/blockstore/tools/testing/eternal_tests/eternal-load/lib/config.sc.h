#pragma once

#pragma once

#if !defined(runtime_h_included_siaudtusdyf)
#define runtime_h_included_siaudtusdyf

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/typetraits.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <array>
#include <functional>
#include <initializer_list>
#include <type_traits>

class IOutputStream;

namespace NDomSchemeRuntime {
    struct TValidateInfo {
        enum class ESeverity : ui8 {
            Error,
            Warning
        };
        ESeverity Severity;

        TValidateInfo()
            : Severity(ESeverity::Error)
        {
        }
        TValidateInfo(ESeverity severity)
            : Severity(severity)
        {
        }
    };
    using TErrorCallback = std::function<void(TString, TString)>;
    using TErrorCallbackEx = std::function<void(TString, TString, TValidateInfo)>;

    template <typename T>
    struct TPrimitiveTypeTraits {
        static TStringBuf TypeName() {
            return "unknown type"sv;
        }
        // for non-primive types
        template<typename TTraits> using TWrappedType = T;
        template<typename TTraits> using TConstWrappedType = T;
    };

    template <typename TValueRef, typename TTraits>
    struct TValueCommon {
    private:
        TValueCommon& operator=(const TValueCommon&) = delete;
    public:
        TValueCommon(TValueRef value)
            : Value__(value)
        {
        }

        TValueCommon(const TValueCommon&) = default;

        TValueRef Value__;

        bool IsNull() const {
            return TTraits::IsNull(Value__);
        }

        TValueRef GetRawValue() const {
            return Value__;
        }

        TString ToJson() const {
            return TTraits::ToJson(Value__);
        }
    };

    template<typename TTraits, typename TValueRef, typename T>
    struct TTraitsWrapper {
        static bool IsValidPrimitive(const T& t, TValueRef v) {
            return TTraits::IsValidPrimitive(t, v);
        }

        static void Get(TValueRef value, const T& def, T& ret) {
            TTraits::Get(value, def, ret);
        }

        static void Get(TValueRef value, T& ret) {
            TTraits::Get(value, ret);
        }

        static void Set(TValueRef value, const T& t) {
            TTraits::Set(value, t);
        }
    };

    template<typename TTraits, typename TValueRef>
    struct TTraitsWrapper<TTraits, TValueRef, TDuration> {
        static bool IsValidPrimitive(const TDuration&, TValueRef v) {
            typename TTraits::TStringType str;
            if (!TTraits::IsNull(v) && TTraits::IsValidPrimitive(str, v)) {
                TTraits::Get(v, str);
                TDuration d;
                return TDuration::TryParse(str, d);
            }
            double doubleValue;
            return TTraits::Get(v, 0., doubleValue);
        }

        static void Get(TValueRef value, const TDuration& def, TDuration& ret) {
            typename TTraits::TStringType stringValue;
            if (!TTraits::IsNull(value) && TTraits::IsValidPrimitive(stringValue, value)) {
                TTraits::Get(value, stringValue);
                ret = TDuration::Parse(stringValue);
                return;
            }
            double doubleValue;
            // we pass default value but that doesn't matter: we don't use it if function returns false
            if (!TTraits::IsNull(value) && TTraits::Get(value, 0., doubleValue)) {
                ret = TDuration::Parse(ToString(doubleValue));
                return;
            }
            ret = def;
        }

        static void Get(TValueRef value, TDuration& ret) {
            typename TTraits::TStringType stringValue;
            if (TTraits::IsValidPrimitive(stringValue, value)) {
                TTraits::Get(value, stringValue);
                ret = TDuration::Parse(stringValue);
                return;
            }
            double doubleValue;
            TTraits::Get(value, doubleValue);
            ret = TDuration::Parse(ToString(doubleValue));
        }

        static void Set(TValueRef value, const TDuration& t) {
            TTraits::Set(value, t.ToString());
        }
    };

    template<typename T>
    inline bool TryFromStringWrapper(const TStringBuf& src, T& dst) {
        return ::TryFromString<T>(src, dst);
    }

    template<>
    inline bool TryFromStringWrapper<TStringBuf>(const TStringBuf& src, TStringBuf& dst) {
        dst = src;
        return true;
    }

    template <typename TValueRef, typename TTraits, typename T>
    struct TConstPrimitiveImpl : public TValueCommon<TValueRef, TTraits> {
        using TBase = TValueCommon<TValueRef, TTraits>;
        bool WithDefault;
        T Default;

        explicit TConstPrimitiveImpl(TValueRef value)
            : TBase(value)
            , WithDefault(false)
        {
        }

        explicit TConstPrimitiveImpl(TValueRef value, T def)
            : TBase(value)
            , WithDefault(true)
            , Default(def)
        {
        }

        inline T Get() const {
            T ret;
            if (WithDefault) {
                TTraitsWrapper<TTraits, TValueRef, T>::Get(this->Value__, Default, ret);
            } else {
                TTraitsWrapper<TTraits, TValueRef, T>::Get(this->Value__, ret);
            }
            return ret;
        }

        template <class U>
        inline T Get(U&& def) const {
            T ret;
            TTraitsWrapper<TTraits, TValueRef, T>::Get(this->Value__, std::forward<U>(def), ret);
            return ret;
        }

        struct TRef {
            T Value;
            T* operator->() {
                return &Value;
            }
        };

        inline TRef operator->() const {
            return TRef { this->Get() };
        }

        inline T operator*() const {
            return this->Get();
        }

        inline operator T() const {
            return this->Get();
        }

        template<typename T2>
        bool operator==(const T2& rhs) const {
            return this->Get() == rhs;
        }

        bool operator==(const TConstPrimitiveImpl& rhs) const {
            return this->Get() == rhs.Get();
        }

        #ifndef __cpp_impl_three_way_comparison
        template<typename T2>
        bool operator!=(const T2& rhs) const {
            return this->Get() != rhs;
        }
        #endif

        template <typename THelper = std::nullptr_t>
        bool Validate(const TString& path, bool /*strict*/, const TErrorCallbackEx& onError, THelper /*helper*/ = THelper()) const {
            T tmp{};
            bool ok = true;
            if (!TTraits::IsNull(this->Value__)) {
                if (!TTraitsWrapper<TTraits, TValueRef, T>::IsValidPrimitive(tmp, this->Value__)) {
                    if (onError) {
                        onError(path, TString("is not ") + TPrimitiveTypeTraits<T>::TypeName(), TValidateInfo());
                    }
                    ok = false;
                }
            }
            return ok;
        }

        bool Validate(const TString& path, bool strict, const TErrorCallback& onError) const {
            return Validate(path, strict, [&onError](TString p, TString e, TValidateInfo) { if (onError) { onError(p, e); } });
        }
    };

    template <typename TTraits, typename T>
    struct TConstPrimitive : public TConstPrimitiveImpl<typename TTraits::TConstValueRef, TTraits, T> {
        using TBase = TConstPrimitiveImpl<typename TTraits::TConstValueRef, TTraits, T>;
        using TConstValueRef = const typename TTraits::TConstValueRef;
        using TConst = TConstPrimitive<TTraits, T>;

        explicit TConstPrimitive(TConstValueRef value)
            : TBase(value)
        {
        }

        explicit TConstPrimitive(TConstValueRef value, T def)
            : TBase(value, def)
        {
        }

        friend inline IOutputStream& operator<< (IOutputStream& out, const TConstPrimitive& v) {
            return (out << v.Get());
        }
    };


    template <typename TTraits, typename T>
    struct TPrimitive : public TConstPrimitiveImpl<typename TTraits::TValueRef, TTraits, T> {
        using TBase = TConstPrimitiveImpl<typename TTraits::TValueRef, TTraits, T>;
        using TValueRef = typename TTraits::TValueRef;
        using TConst = TConstPrimitive<TTraits, T>;

        explicit TPrimitive(TValueRef value)
            : TBase(value)
        {
        }

        explicit TPrimitive(TValueRef value, T def)
            : TBase(value, def)
        {
        }

        TPrimitive(const TPrimitive& t) = default;

        inline void Set(const T& t) {
            TTraitsWrapper<TTraits, TValueRef, T>::Set(this->Value__, t);
        }

        using TBase::operator==;

        template <typename T2>
        inline TPrimitive& operator=(const T2& t) {
            this->Set(t);
            return *this;
        }

        inline TPrimitive& operator=(const TPrimitive& t) {
            this->Set(t);
            return *this;
        }

        inline TPrimitive& operator+=(const T& t) {
            this->Set(this->Get() + t);
            return *this;
        }

        friend inline IOutputStream& operator<< (IOutputStream& out, const TPrimitive& v) {
            return (out << v.Get());
        }
    };

#define REGISTER_PRIMITIVE_TYPE(type, name)     \
    template <>                                 \
    struct TPrimitiveTypeTraits<type> {         \
        static constexpr TStringBuf TypeName() { \
            return name;                        \
        }                                       \
        template <typename TTraits> using TWrappedType = TPrimitive<TTraits, type>; \
        template <typename TTraits> using TConstWrappedType = TConstPrimitive<TTraits, type>; \
    };

    REGISTER_PRIMITIVE_TYPE(bool, "bool")
    REGISTER_PRIMITIVE_TYPE(i8, "i8")
    REGISTER_PRIMITIVE_TYPE(i16, "i16")
    REGISTER_PRIMITIVE_TYPE(i32, "i32")
    REGISTER_PRIMITIVE_TYPE(i64, "i64")
    REGISTER_PRIMITIVE_TYPE(ui8, "ui8")
    REGISTER_PRIMITIVE_TYPE(ui16, "ui16")
    REGISTER_PRIMITIVE_TYPE(ui32, "ui32")
    REGISTER_PRIMITIVE_TYPE(ui64, "ui64")
    REGISTER_PRIMITIVE_TYPE(double, "double")
    REGISTER_PRIMITIVE_TYPE(TStringBuf, "string")
    REGISTER_PRIMITIVE_TYPE(TString, "string")
    REGISTER_PRIMITIVE_TYPE(TDuration, "duration")

#undef REGISTER_PRIMITIVE_TYPE

    template <typename TTraits, typename TEnable = void>
    struct TDefaultValue {
        using TConstValueRef = typename TTraits::TConstValueRef;

        static TConstValueRef Or(TConstValueRef value) {
            return value;
        }
    };

    Y_HAS_SUBTYPE(TValue, ValueType);

    template <typename TTraits>
    struct TDefaultValue<TTraits, std::enable_if_t<THasValueType<TTraits>::value>> {
        typename TTraits::TValue Value;

        TDefaultValue() = default;

        template <typename T>
        TDefaultValue(T&& def):
            Value(TTraits::Value(std::forward<T>(def)))
        {
        }

        using TConstValueRef = typename TTraits::TConstValueRef;

        TConstValueRef Or(TConstValueRef value) const {
            return TTraits::IsNull(value) ? TTraits::Ref(Value) : value;
        }
    };

    template <typename TValueRef, typename TTraits, typename T>
    struct TConstArrayImpl : public TValueCommon<TValueRef, TTraits> {
        using TBase = TValueCommon<TValueRef, TTraits>;
        bool WithDefault;
        TDefaultValue<TTraits> Default;

        explicit TConstArrayImpl(TValueRef value)
            : TBase(value)
            , WithDefault(false)
        {
        }

        template <typename U>
        TConstArrayImpl(TValueRef value, std::initializer_list<U> def)
            : TBase(value)
            , WithDefault(true)
            , Default(def)
        {
        }

        typename TTraits::TConstValueRef Get() const {
            if (WithDefault) {
                return Default.Or(this->Value__);
            } else {
                return this->Value__;
            }
        }

        struct TConstIterator {
            using iterator_category = std::forward_iterator_tag;
            using value_type = typename T::TConst;
            using difference_type = ptrdiff_t;
            using pointer = typename T::TConst*;
            using reference = typename T::TConst&;

            const TConstArrayImpl* A;
            typename TTraits::TArrayIterator It;

            inline typename T::TConst operator*() const {
                return typename T::TConst { TTraits::ArrayElement(A->Get(), It) };
            }

            inline TConstIterator& operator++() {
                ++It;
                return *this;
            }

            inline TConstIterator operator++(int) {
                TConstIterator it(*this);
                ++(*this);
                return it;
            }

            friend inline bool operator==(const TConstIterator& l, const TConstIterator& r) noexcept {
                return l.It == r.It;
            }

            friend inline bool operator!=(const TConstIterator& l, const TConstIterator& r) noexcept {
                return l.It != r.It;
            }
        };

        inline size_t Size() const noexcept {
            return TTraits::ArraySize(this->Get());
        }

        inline bool Empty() const noexcept {
            return !this->Size();
        }

        inline typename TTraits::TConstValueRef ByIndex(size_t n) const {
            return TTraits::ArrayElement(this->Get(), n);
        }

        inline typename T::TConst operator[](size_t n) const {
            return typename T::TConst {this->ByIndex(n)};
        }

        inline TConstIterator begin() const {
            return {this, TTraits::ArrayBegin(this->Get())};
        }

        inline TConstIterator end() const {
            return {this, TTraits::ArrayEnd(this->Get())};
        }

        template <typename THelper = std::nullptr_t>
        bool Validate(const TString& path, bool strict, const TErrorCallbackEx& onError, THelper helper = THelper()) const {
            if (!TTraits::IsNull(this->Value__) && !TTraits::IsArray(this->Value__)) {
                if (onError) {
                    onError(path, TString("is not an array"), TValidateInfo());
                }
                return false;
            }
            bool ok = true;
            for (size_t i = 0; i < Size(); ++i) {
                if (!(*this)[i].Validate(path + "/" + ToString(i), strict, onError, helper)) {
                    ok = false;
                };
            }
            return ok;
        }

        bool Validate(const TString& path, bool strict, const TErrorCallback& onError) {
            return Validate(path, strict, [&onError](TString p, TString e, TValidateInfo) { if (onError) { onError(p, e); } });
        }
    };

    template <typename TTraits, typename T>
    struct TConstArray : public TConstArrayImpl<typename TTraits::TConstValueRef, TTraits, T> {
        using TBase = TConstArrayImpl<typename TTraits::TConstValueRef, TTraits, T>;
        using TConst = TConstArray<TTraits, T>;

        explicit TConstArray(typename TTraits::TConstValueRef value)
            : TBase(value)
        {
        }

        template <typename U>
        TConstArray(typename TTraits::TConstValueRef value, std::initializer_list<U> def)
            : TBase(value, def)
        {
        }
    };

    template <typename TTraits, typename T>
    struct TArray : public TConstArrayImpl<typename TTraits::TValueRef, TTraits, T> {
        using TBase = TConstArrayImpl<typename TTraits::TValueRef, TTraits, T>;
        using TConst = TConstArray<TTraits, T>;

        explicit TArray(typename TTraits::TValueRef value)
            : TBase(value)
        {
        }

        template <typename U>
        TArray(typename TTraits::TValueRef value, std::initializer_list<U> def)
            : TBase(value, def)
        {
        }

        TArray(const TArray& rhs) = default;

        using TBase::Size;
        using TBase::Empty;
        using TBase::ByIndex;
        using TBase::operator[];

        typename TTraits::TValueRef GetMutable() {
            return this->Value__;
        }

        inline size_t Size() noexcept {
            return TTraits::ArraySize(this->GetMutable());
        }

        inline bool Empty() noexcept {
            return !this->Size();
        }

        inline typename TTraits::TValueRef ByIndex(size_t n) {
            return TTraits::ArrayElement(this->GetMutable(), n);
        }

        inline T operator[](size_t n) {
            return T {this->ByIndex(n)};
        }

        T Add() {
            return (*this)[this->Size()];
        }

        void Clear() {
            TTraits::ArrayClear(this->GetMutable());
        }

        template <typename TOtherArray>
        TArray& Assign(const TOtherArray& rhs) {
            Clear();
            for (auto val : rhs) {
                Add() = val;
            }
            return *this;
        }

        template <typename U>
        TArray& operator= (std::initializer_list<U> rhs) {
            return Assign(rhs);
        }

        template <typename TOtherArray>
        TArray& operator= (const TOtherArray& rhs) {
            return Assign(rhs);
        }

        TArray& operator= (const TArray& rhs) {
            return Assign(rhs);
        }
    };

    template<typename TKey, typename TStringType>
    struct TKeyToString {
        TString Value;

        TKeyToString(TKey key) {
            Value = ToString(key);
        }
    };

    template<>
    struct TKeyToString<TStringBuf, TStringBuf> {
        TStringBuf Value;
    };

    template<>
    struct TKeyToString<TString, TStringBuf> {
        TStringBuf Value;
    };

    template<>
    struct TKeyToString<TString, TString> {
        TStringBuf Value;
    };

    template <typename TKey, typename TValueRef, typename TTraits, typename T>
    struct TConstDictImpl : public TValueCommon<TValueRef, TTraits> {
        using TBase = TValueCommon<TValueRef, TTraits>;

        explicit TConstDictImpl(TValueRef value)
            : TBase(value)
        {
        }

        struct TConstIterator {
            const TConstDictImpl* A;
            typename TTraits::TDictIterator It;

            inline TConstIterator& operator++() {
                ++It;
                return *this;
            }

            inline TConstIterator operator++(int) {
                TConstIterator it(*this);
                ++(*this);
                return it;
            }

            friend inline bool operator==(const TConstIterator& l, const TConstIterator& r) noexcept {
                return l.It == r.It;
            }

            friend inline bool operator!=(const TConstIterator& l, const TConstIterator& r) noexcept {
                return l.It != r.It;
            }

            inline const TConstIterator& operator*() {
                return *this;
            }

            inline TKey Key() const {
                return FromString<TKey>(TTraits::DictIteratorKey(A->Value__, It));
            }

            inline typename T::TConst Value() const {
                return typename T::TConst { TTraits::DictIteratorValue(A->Value__, It) };
            }
        };

        inline size_t Size() const noexcept {
            return TTraits::DictSize(this->Value__);
        }

        inline bool Empty() const noexcept {
            return !this->Size();
        }

        inline typename T::TConst operator[](TKey key) const {
            TKeyToString<TKey, TTraits> keyToString {key};
            return typename T::TConst { TTraits::DictElement(this->Value__, keyToString.Value) };
        }

        inline TConstIterator begin() const {
            return {this, TTraits::DictBegin(this->Value__)};
        }

        inline TConstIterator end() const {
            return {this, TTraits::DictEnd(this->Value__)};
        }

        template <typename THelper = std::nullptr_t>
        bool Validate(const TString& path, bool strict, const TErrorCallbackEx& onError, THelper helper = THelper()) const {
            if (!TTraits::IsNull(this->Value__) && !TTraits::IsDict(this->Value__)) {
                if (onError) {
                    onError(path, TString("is not an array"), TValidateInfo());
                }
                return false;
            }
            bool ok = true;
            typename TTraits::TDictIterator it = TTraits::DictBegin(this->Value__);
            typename TTraits::TDictIterator end = TTraits::DictEnd(this->Value__);
            for (; it != end; ++it) {
                TKey key;
                if (!TryFromStringWrapper<TKey>(TTraits::DictIteratorKey(this->Value__, it), key)) {
                    ok = false;
                    if (onError) {
                        onError(path + "/" + TTraits::DictIteratorKey(this->Value__, it), TString("has invalid key type. ") + TPrimitiveTypeTraits<TKey>::TypeName() + " expected", TValidateInfo());
                    }
                }
                typename T::TConst val {TTraits::DictIteratorValue(this->Value__, it)};
                if (!val.Validate(path + "/" + TTraits::DictIteratorKey(this->Value__, it), strict, onError, helper)) {
                    ok = false;
                }
            }
            return ok;
        }

        bool Validate(const TString& path, bool strict, const TErrorCallback& onError) const {
            return Validate(path, strict, [&onError](TString p, TString e, TValidateInfo) { if (onError) { onError(p, e); } });
        }
    };

    template <typename TTraits, typename TKey, typename T>
    struct TConstDict : public TConstDictImpl<TKey, typename TTraits::TConstValueRef, TTraits, T> {
        using TBase = TConstDictImpl<TKey, typename TTraits::TConstValueRef, TTraits, T>;
        using TConst = TConstDict<TTraits, TKey, T>;

        explicit TConstDict(typename TTraits::TConstValueRef value)
            : TBase(value)
        {
        }
    };

    template <typename TTraits, typename TKey, typename T>
    struct TDict : public TConstDictImpl<TKey, typename TTraits::TValueRef, TTraits, T> {
        using TBase = TConstDictImpl<TKey, typename TTraits::TValueRef, TTraits, T>;
        using TConst = TConstDict<TTraits, TKey, T>;

        explicit TDict(typename TTraits::TValueRef value)
            : TBase(value)
        {
        }

        TDict(const TDict& rhs) = default;

        void Clear() {
            TTraits::DictClear(this->Value__);
        }

        template <typename TOtherDict>
        TDict& Assign(const TOtherDict& rhs) {
            Clear();
            for (auto val : rhs) {
                (*this)[val.Key()] = val.Value();
            }
            return *this;
        }

        template <typename TOtherDict>
        TDict& operator= (const TOtherDict& rhs) {
            return Assign(rhs);
        }

        TDict& operator= (const TDict& rhs) {
            return Assign(rhs);
        }

        inline T operator[](TKey key) {
            TKeyToString<TKey, TTraits> keyToString {key};
            return T {TTraits::DictElement(this->Value__, keyToString.Value)};
        }
    };

    template <typename TValueRef, typename TTraits>
    struct TAnyValueImpl : TValueCommon<TValueRef, TTraits> {
        using TBase = TValueCommon<TValueRef, TTraits>;
        bool WithDefault;
        TDefaultValue<TTraits> Default;

        TAnyValueImpl(TValueRef value)
            : TBase(value)
            , WithDefault(false)
        {
        }

        template <typename T>
        TAnyValueImpl(TValueRef value, T&& def)
            : TBase(value)
            , WithDefault(true)
            , Default(std::forward<T>(def))
        {
        }

        typename TTraits::TConstValueRef Get() const {
            if (WithDefault) {
                return Default.Or(this->Value__);
            } else {
                return this->Value__;
            }
        }

        operator typename TTraits::TConstValueRef() const {
            return this->Get();
        }

        typename TTraits::TConstValueRef operator->() const {
            return this->Get();
        }

        template <typename THelper = std::nullptr_t>
        bool Validate(const TString& /*path*/, bool /*strict*/, const TErrorCallbackEx& /*onError*/, THelper /*helper*/ = THelper()) const {
            return true;
        }

        bool Validate(const TString& /*path*/, bool /*strict*/, const TErrorCallback& /*onError*/) const {
            return true;
        }

        bool IsArray() const {
            return TTraits::IsArray(this->Get());
        }

        bool IsDict() const {
            return TTraits::IsDict(this->Get());
        }

        bool IsString() const {
            return this->IsPrimitive<typename TTraits::TStringType>();
        }

        template <typename T>
        bool IsPrimitive() const {
            T type{};
            return TTraitsWrapper<TTraits, TValueRef, T>::IsValidPrimitive(type, this->Get());
        }

        template <typename T>
        TConstArray<TTraits, typename TPrimitiveTypeTraits<T>::template TWrappedType<TTraits>> AsArray() const {
            return TConstArray<TTraits, typename TPrimitiveTypeTraits<T>::template TWrappedType<TTraits>>(this->Get());
        }

        template <typename TKey, typename TValue>
        TConstDict<TTraits, TKey, typename TPrimitiveTypeTraits<TValue>::template TWrappedType<TTraits>> AsDict() const {
            return TConstDict<TTraits, TKey, typename TPrimitiveTypeTraits<TValue>::template TWrappedType<TTraits>>(this->Get());
        }

        TConstPrimitive<TTraits, typename TTraits::TStringType> AsString() const {
            return this->AsPrimitive<typename TTraits::TStringType>();
        }

        template <typename T>
        TConstPrimitive<TTraits, T> AsPrimitive() const {
            if (WithDefault) {
                T defaultValue;
                TTraitsWrapper<TTraits, typename TTraits::TConstValueRef, T>::Get(Default.Or(this->Value__), defaultValue);
                return TConstPrimitive<TTraits, T>(this->Value__, std::move(defaultValue));
            } else {
                return TConstPrimitive<TTraits, T>(this->Value__);
            }
        }
    };

    template <typename TTraits>
    struct TConstAnyValue : public TAnyValueImpl<typename TTraits::TConstValueRef, TTraits> {
        using TBase = TAnyValueImpl<typename TTraits::TConstValueRef, TTraits>;
        using TConst = TConstAnyValue<TTraits>;

        TConstAnyValue(typename TTraits::TConstValueRef value)
            : TBase(value)
        {
        }

        template <typename T>
        TConstAnyValue(typename TTraits::TConstValueRef value, T&& def)
            : TBase(value, std::forward<T>(def))
        {
        }
    };

    template <typename TTraits>
    struct TAnyValue : public TAnyValueImpl<typename TTraits::TValueRef, TTraits> {
        using TBase = TAnyValueImpl<typename TTraits::TValueRef, TTraits>;
        using TConst = TConstAnyValue<TTraits>;

        TAnyValue(typename TTraits::TValueRef value)
            : TBase(value)
        {
        }

        template <typename T>
        TAnyValue(typename TTraits::TValueRef value, T&& def)
            : TBase(value, std::forward<T>(def))
        {
        }

        TAnyValue(const TAnyValue& rhs) = default;

        typename TTraits::TValueRef GetMutable() {
            return this->Value__;
        }

        operator typename TTraits::TValueRef() {
            return this->GetMutable();
        }

        typename TTraits::TValueRef operator->() {
            return this->GetMutable();
        }

        template <typename T>
        TAnyValue& Assign(const T& rhs) {
            return AssignImpl(rhs, TDummy{});
        }

    private:
        struct TDummy{};

        template <typename T>
        TAnyValue& AssignImpl(const T& rhs, ...) {
            TTraitsWrapper<TTraits, typename TTraits::TValueRef, T>::Set(this->GetMutable(), rhs);
            return *this;
        }

        template <typename T, std::enable_if_t<std::is_convertible<T, typename TTraits::TStringType>::value>* = nullptr>
        TAnyValue& AssignImpl(const T& rhs, TDummy) {
            TTraits::Set(this->GetMutable(), static_cast<typename TTraits::TStringType>(rhs));
            return *this;
        }

        Y_HAS_MEMBER(Get);

        template <typename TOtherAnyValue, std::enable_if_t<THasGet<TOtherAnyValue>::value>* = nullptr>
        TAnyValue& AssignImpl(const TOtherAnyValue& rhs, TDummy) {
            *GetMutable() = *rhs.Get();
            return *this;
        }

    public:
        template <typename T>
        TAnyValue& operator= (const T& rhs) {
            return Assign(rhs);
        }

        TAnyValue& operator= (const TAnyValue& rhs) {
            return Assign(rhs);
        }
    };

    // checks that levenshtein distance is <= 1
    inline bool IsSimilar(TStringBuf s1, TStringBuf s2) {
        size_t minLen = Min(s1.size(), s2.size());
        size_t prefixLen = 0;
        while (prefixLen < minLen && s1[prefixLen] == s2[prefixLen]) {
            ++prefixLen;
        }
        return
            s1.SubStr(prefixLen+1) == s2.SubStr(prefixLen+1) ||
            s1.SubStr(prefixLen+0) == s2.SubStr(prefixLen+1) ||
            s1.SubStr(prefixLen+1) == s2.SubStr(prefixLen+0);
    }
}

#endif


namespace NCloud {

namespace NBlockStore {

    template <typename TTraits>
    struct TRangeConfigConst {
        using TValueRef = typename TTraits::TConstValueRef;

        using TConst = typename::NCloud::NBlockStore::TRangeConfigConst<TTraits>;

        TValueRef Value__;

        inline TRangeConfigConst(const TValueRef& value)
            : Value__(value)
        {
        }

        inline const TRangeConfigConst* operator->() const noexcept {
            return this;
        }

        inline TValueRef GetRawValue() const {
            return Value__;
        }

        inline bool HasStartOffset() const {
            return !this->StartOffset().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> StartOffset() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("StartOffset"))};
        }

        inline bool HasRequestBlockCount() const {
            return !this->RequestBlockCount().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> RequestBlockCount() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("RequestBlockCount"))};
        }

        inline bool HasRequestCount() const {
            return !this->RequestCount().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> RequestCount() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("RequestCount"))};
        }

        inline bool HasStep() const {
            return !this->Step().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> Step() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("Step"))};
        }

        inline bool HasStartBlockIdx() const {
            return !this->StartBlockIdx().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> StartBlockIdx() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("StartBlockIdx"))};
        }

        inline bool HasLastBlockIdx() const {
            return !this->LastBlockIdx().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> LastBlockIdx() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("LastBlockIdx"))};
        }

        inline bool HasNumberToWrite() const {
            return !this->NumberToWrite().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> NumberToWrite() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("NumberToWrite"))};
        }

        inline bool HasWriteParts() const {
            return !this->WriteParts().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> WriteParts() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("WriteParts"))};
        }

        inline bool IsNull() const {
            return TTraits::IsNull(Value__);
        }
        inline TString ToJson() const {
            return TTraits::ToJson(Value__);
        }
        inline bool Validate(const TString& path = TString(), bool strict = false, const NDomSchemeRuntime::TErrorCallback& onError = NDomSchemeRuntime::TErrorCallback()) const {
            return Validate(path, strict, [&onError](TString p, TString e, NDomSchemeRuntime::TValidateInfo) { if (onError) { onError(p, e); } });
        }
        template <typename THelper = std::nullptr_t>
        inline bool Validate(const TString& path = TString(), bool strict = false, const NDomSchemeRuntime::TErrorCallbackEx& onError = NDomSchemeRuntime::TErrorCallbackEx(), THelper helper = THelper()) const {
            (void)strict;
            (void)helper;
            if (TTraits::IsNull(Value__)) {
                return true;
            } else if (!TTraits::IsDict(Value__)) {
                if (onError) { onError(path, "is not a dict", NDomSchemeRuntime::TValidateInfo()); }
                return false;
            }
            bool ok = true;
            auto keys = TTraits::GetKeys(Value__);
            for (const auto& key : keys) {
                static constexpr std::array<TStringBuf, 8> knownNames {{
                    TStringBuf("StartOffset"),
                    TStringBuf("RequestBlockCount"),
                    TStringBuf("RequestCount"),
                    TStringBuf("Step"),
                    TStringBuf("StartBlockIdx"),
                    TStringBuf("LastBlockIdx"),
                    TStringBuf("NumberToWrite"),
                    TStringBuf("WriteParts"),
                }};
                const bool isKnown = (Find(knownNames, key) != knownNames.end());
                Y_UNUSED(isKnown);
                bool found = false;
                TString loweredKey = to_lower(key);
                if (key == TStringBuf("StartOffset") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("startoffset"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("StartOffset")) {
                        if (onError) { onError(subPath, "looks like misspelled \"StartOffset\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("RequestBlockCount") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("requestblockcount"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("RequestBlockCount")) {
                        if (onError) { onError(subPath, "looks like misspelled \"RequestBlockCount\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("RequestCount") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("requestcount"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("RequestCount")) {
                        if (onError) { onError(subPath, "looks like misspelled \"RequestCount\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("Step") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("step"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("Step")) {
                        if (onError) { onError(subPath, "looks like misspelled \"Step\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("StartBlockIdx") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("startblockidx"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("StartBlockIdx")) {
                        if (onError) { onError(subPath, "looks like misspelled \"StartBlockIdx\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("LastBlockIdx") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("lastblockidx"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("LastBlockIdx")) {
                        if (onError) { onError(subPath, "looks like misspelled \"LastBlockIdx\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("NumberToWrite") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("numbertowrite"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("NumberToWrite")) {
                        if (onError) { onError(subPath, "looks like misspelled \"NumberToWrite\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("WriteParts") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("writeparts"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("WriteParts")) {
                        if (onError) { onError(subPath, "looks like misspelled \"WriteParts\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (!found && strict) {
                    if (onError) { onError(path + "/" + key, "is an unknown key", NDomSchemeRuntime::TValidateInfo()); }
                    ok = false;
                }
            }
            return ok;
        }
    };

    template <typename TTraits>
    struct TRangeConfig {
        using TValueRef = typename TTraits::TValueRef;

        using TConst = typename::NCloud::NBlockStore::TRangeConfigConst<TTraits>;

        TValueRef Value__;

        inline TRangeConfig(const TValueRef& value)
            : Value__(value)
        {
        }

        inline operator TConst() const {
            return TConst(Value__);
        }

        inline const TRangeConfig* operator->() const noexcept {
            return this;
        }

        inline TRangeConfig* operator->() noexcept {
            return this;
        }

        inline TValueRef GetRawValue() const {
            return Value__;
        }

        inline bool HasStartOffset() const {
            return !this->StartOffset().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> StartOffset() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("StartOffset"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> StartOffset() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("StartOffset"))};
        }

        inline bool HasRequestBlockCount() const {
            return !this->RequestBlockCount().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> RequestBlockCount() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("RequestBlockCount"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> RequestBlockCount() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("RequestBlockCount"))};
        }

        inline bool HasRequestCount() const {
            return !this->RequestCount().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> RequestCount() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("RequestCount"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> RequestCount() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("RequestCount"))};
        }

        inline bool HasStep() const {
            return !this->Step().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> Step() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("Step"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> Step() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("Step"))};
        }

        inline bool HasStartBlockIdx() const {
            return !this->StartBlockIdx().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> StartBlockIdx() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("StartBlockIdx"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> StartBlockIdx() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("StartBlockIdx"))};
        }

        inline bool HasLastBlockIdx() const {
            return !this->LastBlockIdx().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> LastBlockIdx() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("LastBlockIdx"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> LastBlockIdx() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("LastBlockIdx"))};
        }

        inline bool HasNumberToWrite() const {
            return !this->NumberToWrite().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> NumberToWrite() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("NumberToWrite"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> NumberToWrite() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("NumberToWrite"))};
        }

        inline bool HasWriteParts() const {
            return !this->WriteParts().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> WriteParts() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("WriteParts"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> WriteParts() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("WriteParts"))};
        }

        inline bool IsNull() const {
            return TTraits::IsNull(Value__);
        }
        inline TString ToJson() const {
            return TTraits::ToJson(Value__);
        }
        inline bool Validate(const TString& path = TString(), bool strict = false, const NDomSchemeRuntime::TErrorCallback& onError = NDomSchemeRuntime::TErrorCallback()) const {
            return Validate(path, strict, [&onError](TString p, TString e, NDomSchemeRuntime::TValidateInfo) { if (onError) { onError(p, e); } });
        }
        template <typename THelper = std::nullptr_t>
        inline bool Validate(const TString& path = TString(), bool strict = false, const NDomSchemeRuntime::TErrorCallbackEx& onError = NDomSchemeRuntime::TErrorCallbackEx(), THelper helper = THelper()) const {
            return TConst(*this).Validate(path, strict, onError, helper);
        }
        inline void Clear() {
            return TTraits::DictClear(this->Value__);
        }
        template <typename TRhs>
        inline TRangeConfig& Assign(const TRhs& rhs) {
            Clear();
            if (!rhs.StartOffset().IsNull()) {
                StartOffset() = rhs.StartOffset();
            }
            if (!rhs.RequestBlockCount().IsNull()) {
                RequestBlockCount() = rhs.RequestBlockCount();
            }
            if (!rhs.RequestCount().IsNull()) {
                RequestCount() = rhs.RequestCount();
            }
            if (!rhs.Step().IsNull()) {
                Step() = rhs.Step();
            }
            if (!rhs.StartBlockIdx().IsNull()) {
                StartBlockIdx() = rhs.StartBlockIdx();
            }
            if (!rhs.LastBlockIdx().IsNull()) {
                LastBlockIdx() = rhs.LastBlockIdx();
            }
            if (!rhs.NumberToWrite().IsNull()) {
                NumberToWrite() = rhs.NumberToWrite();
            }
            if (!rhs.WriteParts().IsNull()) {
                WriteParts() = rhs.WriteParts();
            }
            return *this;
        }
        template <typename TRhs>
        inline TRangeConfig& operator=(const TRhs& rhs) {
            return Assign(rhs);
        }
        inline TRangeConfig& operator=(const TRangeConfig& rhs) {
            if (GetRawValue() == rhs.GetRawValue()) {
                return *this;
            }
            return Assign(rhs);
        }
        TRangeConfig() = default;
        TRangeConfig(const TRangeConfig& other) = default;
        inline TRangeConfig& SetDefault() {
            return *this;
        }
    };

    template <typename TTraits>
    struct TTestConfigDescConst {
        using TValueRef = typename TTraits::TConstValueRef;

        using TConst = typename::NCloud::NBlockStore::TTestConfigDescConst<TTraits>;

        TValueRef Value__;

        inline TTestConfigDescConst(const TValueRef& value)
            : Value__(value)
        {
        }

        inline const TTestConfigDescConst* operator->() const noexcept {
            return this;
        }

        inline TValueRef GetRawValue() const {
            return Value__;
        }

        inline bool HasTestId() const {
            return !this->TestId().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> TestId() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("TestId"))};
        }

        inline bool HasFilePath() const {
            return !this->FilePath().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType> FilePath() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("FilePath"))};
        }

        inline bool HasIoDepth() const {
            return !this->IoDepth().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16> IoDepth() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("IoDepth"))};
        }

        inline bool HasFileSize() const {
            return !this->FileSize().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> FileSize() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("FileSize"))};
        }

        inline bool HasBlockSize() const {
            return !this->BlockSize().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> BlockSize() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("BlockSize"))};
        }

        inline bool HasWriteRate() const {
            return !this->WriteRate().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16> WriteRate() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("WriteRate"))};
        }

        inline bool HasRangeBlockCount() const {
            return !this->RangeBlockCount().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> RangeBlockCount() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("RangeBlockCount"))};
        }

        inline bool HasRanges() const {
            return !this->Ranges().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstArray<TTraits, TRangeConfigConst<TTraits>> Ranges() const {
            return ::NDomSchemeRuntime::TConstArray<TTraits, TRangeConfigConst<TTraits>>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("Ranges"))};
        }

        inline TRangeConfigConst<TTraits> Ranges(size_t idx) const {
            return Ranges()[idx];
        }

        inline bool HasSlowRequestThreshold() const {
            return !this->SlowRequestThreshold().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType> SlowRequestThreshold() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("SlowRequestThreshold")), "10s"};
        }

        inline bool IsNull() const {
            return TTraits::IsNull(Value__);
        }
        inline TString ToJson() const {
            return TTraits::ToJson(Value__);
        }
        inline bool Validate(const TString& path = TString(), bool strict = false, const NDomSchemeRuntime::TErrorCallback& onError = NDomSchemeRuntime::TErrorCallback()) const {
            return Validate(path, strict, [&onError](TString p, TString e, NDomSchemeRuntime::TValidateInfo) { if (onError) { onError(p, e); } });
        }
        template <typename THelper = std::nullptr_t>
        inline bool Validate(const TString& path = TString(), bool strict = false, const NDomSchemeRuntime::TErrorCallbackEx& onError = NDomSchemeRuntime::TErrorCallbackEx(), THelper helper = THelper()) const {
            (void)strict;
            (void)helper;
            if (TTraits::IsNull(Value__)) {
                return true;
            } else if (!TTraits::IsDict(Value__)) {
                if (onError) { onError(path, "is not a dict", NDomSchemeRuntime::TValidateInfo()); }
                return false;
            }
            bool ok = true;
            bool found_TestId = false;
            bool found_FilePath = false;
            bool found_IoDepth = false;
            bool found_FileSize = false;
            bool found_BlockSize = false;
            bool found_WriteRate = false;
            auto keys = TTraits::GetKeys(Value__);
            for (const auto& key : keys) {
                static constexpr std::array<TStringBuf, 9> knownNames {{
                    TStringBuf("TestId"),
                    TStringBuf("FilePath"),
                    TStringBuf("IoDepth"),
                    TStringBuf("FileSize"),
                    TStringBuf("BlockSize"),
                    TStringBuf("WriteRate"),
                    TStringBuf("RangeBlockCount"),
                    TStringBuf("Ranges"),
                    TStringBuf("SlowRequestThreshold"),
                }};
                const bool isKnown = (Find(knownNames, key) != knownNames.end());
                Y_UNUSED(isKnown);
                bool found = false;
                TString loweredKey = to_lower(key);
                if (key == TStringBuf("TestId") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("testid"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("TestId")) {
                        if (onError) { onError(subPath, "looks like misspelled \"TestId\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                        found_TestId = true;
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("FilePath") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("filepath"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("FilePath")) {
                        if (onError) { onError(subPath, "looks like misspelled \"FilePath\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                        found_FilePath = true;
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("IoDepth") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("iodepth"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("IoDepth")) {
                        if (onError) { onError(subPath, "looks like misspelled \"IoDepth\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                        found_IoDepth = true;
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("FileSize") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("filesize"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("FileSize")) {
                        if (onError) { onError(subPath, "looks like misspelled \"FileSize\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                        found_FileSize = true;
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("BlockSize") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("blocksize"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("BlockSize")) {
                        if (onError) { onError(subPath, "looks like misspelled \"BlockSize\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                        found_BlockSize = true;
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("WriteRate") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("writerate"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("WriteRate")) {
                        if (onError) { onError(subPath, "looks like misspelled \"WriteRate\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                        found_WriteRate = true;
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("RangeBlockCount") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("rangeblockcount"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("RangeBlockCount")) {
                        if (onError) { onError(subPath, "looks like misspelled \"RangeBlockCount\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("Ranges") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("ranges"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("Ranges")) {
                        if (onError) { onError(subPath, "looks like misspelled \"Ranges\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstArray<TTraits, TRangeConfigConst<TTraits>> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (key == TStringBuf("SlowRequestThreshold") || strict && !isKnown && NDomSchemeRuntime::IsSimilar(loweredKey, TStringBuf("slowrequestthreshold"))) {
                    found = true;
                    const TString subPath = path + "/" + key;
                    if (key != TStringBuf("SlowRequestThreshold")) {
                        if (onError) { onError(subPath, "looks like misspelled \"SlowRequestThreshold\"", NDomSchemeRuntime::TValidateInfo{NDomSchemeRuntime::TValidateInfo::ESeverity::Warning}); }
                        ok = false;
                    } else {
                    }
                    ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType> f {TTraits::GetField(Value__, key)};
                    if (!f.Validate(subPath, strict, onError, helper)) {
                        ok = false;
                    }
                }
                if (!found && strict) {
                    if (onError) { onError(path + "/" + key, "is an unknown key", NDomSchemeRuntime::TValidateInfo()); }
                    ok = false;
                }
            }
            if (!found_TestId) {
                ok = false;
                if (onError) { onError(path + "/TestId", "is a required field and is not found", NDomSchemeRuntime::TValidateInfo()); }
            }
            if (!found_FilePath) {
                ok = false;
                if (onError) { onError(path + "/FilePath", "is a required field and is not found", NDomSchemeRuntime::TValidateInfo()); }
            }
            if (!found_IoDepth) {
                ok = false;
                if (onError) { onError(path + "/IoDepth", "is a required field and is not found", NDomSchemeRuntime::TValidateInfo()); }
            }
            if (!found_FileSize) {
                ok = false;
                if (onError) { onError(path + "/FileSize", "is a required field and is not found", NDomSchemeRuntime::TValidateInfo()); }
            }
            if (!found_BlockSize) {
                ok = false;
                if (onError) { onError(path + "/BlockSize", "is a required field and is not found", NDomSchemeRuntime::TValidateInfo()); }
            }
            if (!found_WriteRate) {
                ok = false;
                if (onError) { onError(path + "/WriteRate", "is a required field and is not found", NDomSchemeRuntime::TValidateInfo()); }
            }
            return ok;
        }
    };

    template <typename TTraits>
    struct TTestConfigDesc {
        using TValueRef = typename TTraits::TValueRef;

        using TConst = typename::NCloud::NBlockStore::TTestConfigDescConst<TTraits>;

        TValueRef Value__;

        inline TTestConfigDesc(const TValueRef& value)
            : Value__(value)
        {
        }

        inline operator TConst() const {
            return TConst(Value__);
        }

        inline const TTestConfigDesc* operator->() const noexcept {
            return this;
        }

        inline TTestConfigDesc* operator->() noexcept {
            return this;
        }

        inline TValueRef GetRawValue() const {
            return Value__;
        }

        inline bool HasTestId() const {
            return !this->TestId().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> TestId() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("TestId"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> TestId() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("TestId"))};
        }

        inline bool HasFilePath() const {
            return !this->FilePath().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType> FilePath() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("FilePath"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, typename TTraits::TStringType> FilePath() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, typename TTraits::TStringType>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("FilePath"))};
        }

        inline bool HasIoDepth() const {
            return !this->IoDepth().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16> IoDepth() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("IoDepth"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui16> IoDepth() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui16>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("IoDepth"))};
        }

        inline bool HasFileSize() const {
            return !this->FileSize().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> FileSize() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("FileSize"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> FileSize() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("FileSize"))};
        }

        inline bool HasBlockSize() const {
            return !this->BlockSize().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> BlockSize() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("BlockSize"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> BlockSize() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("BlockSize"))};
        }

        inline bool HasWriteRate() const {
            return !this->WriteRate().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16> WriteRate() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui16>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("WriteRate"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui16> WriteRate() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui16>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("WriteRate"))};
        }

        inline bool HasRangeBlockCount() const {
            return !this->RangeBlockCount().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64> RangeBlockCount() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("RangeBlockCount"))};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, ui64> RangeBlockCount() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, ui64>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("RangeBlockCount"))};
        }

        inline bool HasRanges() const {
            return !this->Ranges().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstArray<TTraits, TRangeConfigConst<TTraits>> Ranges() const {
            return ::NDomSchemeRuntime::TConstArray<TTraits, TRangeConfigConst<TTraits>>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("Ranges"))};
        }

        inline TRangeConfigConst<TTraits> Ranges(size_t idx) const {
            return Ranges()[idx];
        }

        inline ::NDomSchemeRuntime::TArray<TTraits, TRangeConfig<TTraits>> Ranges() {
            return ::NDomSchemeRuntime::TArray<TTraits, TRangeConfig<TTraits>>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("Ranges"))};
        }

        inline TRangeConfig<TTraits> Ranges(size_t idx) {
            return Ranges()[idx];
        }

        inline bool HasSlowRequestThreshold() const {
            return !this->SlowRequestThreshold().IsNull();
        }

        inline ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType> SlowRequestThreshold() const {
            return ::NDomSchemeRuntime::TConstPrimitive<TTraits, typename TTraits::TStringType>{TTraits::GetField(static_cast<typename TTraits::TConstValueRef>(Value__), TStringBuf("SlowRequestThreshold")), "10s"};
        }

        inline ::NDomSchemeRuntime::TPrimitive<TTraits, typename TTraits::TStringType> SlowRequestThreshold() {
            return ::NDomSchemeRuntime::TPrimitive<TTraits, typename TTraits::TStringType>{TTraits::GetField(static_cast<typename TTraits::TValueRef>(Value__), TStringBuf("SlowRequestThreshold")), "10s"};
        }

        inline bool IsNull() const {
            return TTraits::IsNull(Value__);
        }
        inline TString ToJson() const {
            return TTraits::ToJson(Value__);
        }
        inline bool Validate(const TString& path = TString(), bool strict = false, const NDomSchemeRuntime::TErrorCallback& onError = NDomSchemeRuntime::TErrorCallback()) const {
            return Validate(path, strict, [&onError](TString p, TString e, NDomSchemeRuntime::TValidateInfo) { if (onError) { onError(p, e); } });
        }
        template <typename THelper = std::nullptr_t>
        inline bool Validate(const TString& path = TString(), bool strict = false, const NDomSchemeRuntime::TErrorCallbackEx& onError = NDomSchemeRuntime::TErrorCallbackEx(), THelper helper = THelper()) const {
            return TConst(*this).Validate(path, strict, onError, helper);
        }
        inline void Clear() {
            return TTraits::DictClear(this->Value__);
        }
        template <typename TRhs>
        inline TTestConfigDesc& Assign(const TRhs& rhs) {
            Clear();
            if (!rhs.TestId().IsNull()) {
                TestId() = rhs.TestId();
            }
            if (!rhs.FilePath().IsNull()) {
                FilePath() = rhs.FilePath();
            }
            if (!rhs.IoDepth().IsNull()) {
                IoDepth() = rhs.IoDepth();
            }
            if (!rhs.FileSize().IsNull()) {
                FileSize() = rhs.FileSize();
            }
            if (!rhs.BlockSize().IsNull()) {
                BlockSize() = rhs.BlockSize();
            }
            if (!rhs.WriteRate().IsNull()) {
                WriteRate() = rhs.WriteRate();
            }
            if (!rhs.RangeBlockCount().IsNull()) {
                RangeBlockCount() = rhs.RangeBlockCount();
            }
            if (!rhs.Ranges().IsNull()) {
                Ranges() = rhs.Ranges();
            }
            if (!rhs.SlowRequestThreshold().IsNull()) {
                SlowRequestThreshold() = rhs.SlowRequestThreshold();
            }
            return *this;
        }
        template <typename TRhs>
        inline TTestConfigDesc& operator=(const TRhs& rhs) {
            return Assign(rhs);
        }
        inline TTestConfigDesc& operator=(const TTestConfigDesc& rhs) {
            if (GetRawValue() == rhs.GetRawValue()) {
                return *this;
            }
            return Assign(rhs);
        }
        TTestConfigDesc() = default;
        TTestConfigDesc(const TTestConfigDesc& other) = default;
        inline TTestConfigDesc& SetDefault() {
            if (!HasSlowRequestThreshold()) {
                SlowRequestThreshold() = "10s";
            }
            return *this;
        }
    };

}

}

