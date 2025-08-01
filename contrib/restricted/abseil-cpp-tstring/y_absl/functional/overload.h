// Copyright 2023 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: overload.h
// -----------------------------------------------------------------------------
//
// `y_absl::Overload()` returns a functor that provides overloads based on the
// functors passed to it.
// Before using this function, consider whether named function overloads would
// be a better design.
// One use case for this is locally defining visitors for `std::visit` inside a
// function using lambdas.

// Example: Using  `y_absl::Overload` to define a visitor for `std::variant`.
//
// std::variant<int, TString, double> v(int{1});
//
// assert(std::visit(y_absl::Overload(
//                        [](int) -> y_absl::string_view { return "int"; },
//                        [](const TString&) -> y_absl::string_view {
//                          return "string";
//                        },
//                        [](double) -> y_absl::string_view { return "double"; }),
//                     v) == "int");
//
// Note: This requires C++17.

#ifndef Y_ABSL_FUNCTIONAL_OVERLOAD_H_
#define Y_ABSL_FUNCTIONAL_OVERLOAD_H_

#include "y_absl/base/config.h"
#include "y_absl/meta/type_traits.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN

#if defined(Y_ABSL_INTERNAL_CPLUSPLUS_LANG) && \
    Y_ABSL_INTERNAL_CPLUSPLUS_LANG >= 201703L

template <int&... ExplicitArgumentBarrier, typename... T>
auto Overload(T&&... ts) {
  struct OverloadImpl : y_absl::remove_cvref_t<T>... {
    using y_absl::remove_cvref_t<T>::operator()...;
  };
  return OverloadImpl{std::forward<T>(ts)...};
}
#else
namespace functional_internal {
template <typename T>
constexpr bool kDependentFalse = false;
}

template <typename Dependent = int, typename... T>
auto Overload(T&&...) {
  static_assert(functional_internal::kDependentFalse<Dependent>,
                "Overload is only usable with C++17 or above.");
}

#endif
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_FUNCTIONAL_OVERLOAD_H_
