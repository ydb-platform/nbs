#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/typetraits.h>
#include <util/system/type_name.h>

namespace NCloud::NBlockStore {

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

Y_HAS_MEMBER(Name);

template <typename T, typename = void>
struct TNameSelector;

template <typename T>
struct TNameSelector<T, std::enable_if_t<THasName<T>::value>>
{
    static TString Get()
    {
        return T::Name();
    }
};

template <typename T>
struct TNameSelector<T, std::enable_if_t<!THasName<T>::value>>
{
    static TString Get()
    {
        return TypeName<T>();
    }
};

template <typename T>
using TName = TNameSelector<T>;

////////////////////////////////////////////////////////////////////////////////

Y_HAS_MEMBER(Describe);

template <typename T, typename = void>
struct TDescribeSelector;

template <typename T>
struct TDescribeSelector<T, std::enable_if_t<THasDescribe<T>::value>>
{
    static void Describe(IOutputStream& out, const T& obj)
    {
        obj.Describe(out, obj);
    }
};

template <typename T>
struct TDescribeSelector<T, std::enable_if_t<!THasDescribe<T>::value>>
{
    static void Describe(IOutputStream& out, const T& obj)
    {
        Y_UNUSED(out);
        Y_UNUSED(obj);
    }
};

template <typename T>
using TDescribe = TDescribeSelector<T>;

}   // namespace NImpl

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString GetName()
{
    return NImpl::TName<T>::Get();
}

template <typename T>
void Describe(IOutputStream& out, const T& obj)
{
    NImpl::TDescribe<T>::Describe(out, obj);
}

}   // namespace NCloud::NBlockStore
