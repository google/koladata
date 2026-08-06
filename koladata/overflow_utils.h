// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#ifndef KOLADATA_OVERFLOW_UTILS_H_
#define KOLADATA_OVERFLOW_UTILS_H_

#include <cstdint>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"

namespace koladata {

// Overflow-checked arithmetic helpers. These set boolean flags or return an
// error Status instead of silently wrapping on overflow. Use them e.g. in
// buffer size computations to prevent bogus sizes that can cause heap buffer
// overflows.
//
// The safe_* helpers are convenient to use in tight loops where it's faster to
// accumulate an overflow status instead of checking it after every arithmetic
// operation. They can also be used to chain multiple operations together in a
// fairly readable way. For example, a * x + b can be written as:
//
//   bool overflow = false;
//   int64_t result = safe_add(safe_mul(a, x, &overflow), b, &overflow);


// Returns x + y and sets overflow to true if the addition overflows.
template <typename Res = int64_t, typename Lhs, typename Rhs>
inline Res safe_add(Lhs x, Rhs y, bool* overflow) {
  Res result;
  *overflow = *overflow | __builtin_add_overflow(x, y, &result);
  return result;
}

// Returns x * y and sets overflow to true if the multiplication overflows.
template <typename Res = int64_t, typename Lhs, typename Rhs>
inline Res safe_mul(Lhs x, Rhs y, bool* overflow) {
  Res result;
  *overflow = *overflow | __builtin_mul_overflow(x, y, &result);
  return result;
}

// Returns |x| and sets overflow to true if the absolute value overflows.
template <typename Res = int64_t, typename T>
inline Res safe_abs(T x, bool* overflow) {
  Res result;
  bool is_overflow;
  if constexpr (std::is_signed_v<T>) {
    is_overflow = (x < 0) ? __builtin_sub_overflow(T(0), x, &result)
                          : __builtin_add_overflow(x, T(0), &result);
  } else {
    is_overflow = __builtin_add_overflow(x, T(0), &result);
  }
  *overflow = *overflow | is_overflow;
  return result;
}

// Returns x * y, or an error if the multiplication overflows.
template <typename Res = int64_t, typename Lhs, typename Rhs>
inline absl::StatusOr<Res> SafeMul(Lhs x, Rhs y) {
  bool overflow = false;
  Res result = safe_mul<Res>(x, y, &overflow);
  if (overflow) {
    return absl::InvalidArgumentError(
        absl::StrCat("integer overflow in multiplication: ", x, " * ", y));
  }
  return result;
}

// Returns x + y, or an error if the addition overflows.
template <typename Res = int64_t, typename Lhs, typename Rhs>
inline absl::StatusOr<Res> SafeAdd(Lhs x, Rhs y) {
  bool overflow = false;
  Res result = safe_add<Res>(x, y, &overflow);
  if (overflow) {
    return absl::InvalidArgumentError(
        absl::StrCat("integer overflow in addition: ", x, " + ", y));
  }
  return result;
}

// Returns |x|, or an error if x causes overflow (where std::abs is UB).
template <typename Res = int64_t, typename T>
inline absl::StatusOr<Res> SafeAbs(T x) {
  bool overflow = false;
  Res result = safe_abs<Res>(x, &overflow);
  if (overflow) {
    return absl::InvalidArgumentError(
        absl::StrCat("absolute value of ", x, " causes overflow"));
  }
  return result;
}

}  // namespace koladata

#endif  // KOLADATA_OVERFLOW_UTILS_H_
