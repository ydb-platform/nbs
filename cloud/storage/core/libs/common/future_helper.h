#pragma once

#include <library/cpp/threading/future/future.h>

namespace NCloud {

/*
The semantics of TFuture::ExtractValue() is broken because it requires that
there be no GetValue() before ExtractValue(). Since a chain of subscribers is
usually built from the creation of the promise to the point of invocation where
the feature is saved, it is impossible to guarantee that a new subscriber will
appear later in the chain who will read from the future, which means that the
code written a long time ago will break in an unobvious way, since
ExtractValue() will start throwing an exception, which can be even worse. not
for every call if getValue() occurs under a condition.

ExtractValue() should be called with care, only at the very end of the call
chain, in the furthest subscriber, the one that was bound at the point of the
initial creation call. Otherwise, subscribers that will be called later will
receive an empty value.
*/

template <typename T>
T UnsafeExtractValue(const NThreading::TFuture<T>& future)
{
    return std::move(const_cast<T&>(future.GetValue()));
}

}   // namespace NCloud
