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
#ifndef KOLADATA_OPERATORS_UTILS_H_
#define KOLADATA_OPERATORS_UTILS_H_

#include <cstdint>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/schema_utils.h"

namespace koladata::ops {

// Similar to RETURN_IF_ERROR, but doesn't add an extra source location
// to the status.
#define KD_RETURN_AS_IS_IF_ERROR(X)      \
  if (absl::Status st = (X); !st.ok()) { \
    return st;                           \
  }

// Constraint for UnaryOpEval and BinaryOpEval
struct NumericArgs {
  template <class T1, class T2 = int>
  constexpr static bool kIsInvocable =
      std::is_arithmetic_v<T1> && std::is_arithmetic_v<T2> &&
      !std::is_same_v<T1, bool> && !std::is_same_v<T2, bool>;

  explicit constexpr NumericArgs(absl::string_view name1,
                                 absl::string_view name2 = "")
      : name1(name1), name2(name2) {}

  // Note: this function is not guaranteed to be called. It is NOT called if
  // kIsInvocable is true, shapes are compatible, and output type is
  // deducible. It's goal is not to detect error, but to format a custom error
  // message. If it returns Ok, a default error handling will be applied.
  absl::Status CheckArgs(const DataSlice& ds1) const {
    return ExpectNumeric(name1, ds1);
  }
  absl::Status CheckArgs(const DataSlice& ds1, const DataSlice& ds2) const {
    DCHECK_NE(name2, "");
    KD_RETURN_AS_IS_IF_ERROR(ExpectNumeric(name1, ds1));
    return ExpectNumeric(name2, ds2);
  }

  const absl::string_view name1, name2;
};

// Constraint for UnaryOpEval and BinaryOpEval
struct IntegerArgs {
  template <class T1, class T2 = int>
  constexpr static bool kIsInvocable =
      (std::is_same_v<T1, int32_t> || std::is_same_v<T1, int64_t>) &&
      (std::is_same_v<T2, int32_t> || std::is_same_v<T2, int64_t>);

  explicit constexpr IntegerArgs(absl::string_view name1,
                                 absl::string_view name2 = "")
      : name1(name1), name2(name2) {}

  absl::Status CheckArgs(const DataSlice& ds1) const {
    return ExpectInteger(name1, ds1);
  }
  absl::Status CheckArgs(const DataSlice& ds1, const DataSlice& ds2) const {
    DCHECK_NE(name2, "");
    KD_RETURN_AS_IS_IF_ERROR(ExpectInteger(name1, ds1));
    return ExpectInteger(name2, ds2);
  }

  const absl::string_view name1, name2;
};

#undef KD_RETURN_AS_IS_IF_ERROR

// Verifies that DataItem is compatible with type T. I.e. contains T or missing.
template <class T>
absl::Status CheckType(const internal::DataItem& item) {
  if (item.has_value() && !item.holds_value<T>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("value doesn't match schema; expected %s, got %s",
                        arolla::GetQType<T>()->name(), item.dtype()->name()));
  }
  return absl::OkStatus();
}

// Verifies that DataSliceImpl doesn't contain anything other than T.
// I.e. either contains DenseArray<T> or is fully missing.
template <class T>
absl::Status CheckType(const internal::DataSliceImpl& slice) {
  if (!slice.is_empty_and_unknown() && slice.dtype() != arolla::GetQType<T>()) {
    if (slice.is_mixed_dtype()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "value doesn't match schema; expected %s, got mixed slice",
          arolla::GetQType<T>()->name()));
    }
    return absl::InvalidArgumentError(
        absl::StrFormat("value doesn't match schema; expected %s, got %s",
                        arolla::GetQType<T>()->name(), slice.dtype()->name()));
  }
  return absl::OkStatus();
}

// Verifies that DataSlice doesn't contain anything other than T.
template <class T>
absl::Status CheckType(const DataSlice& slice) {
  return slice.VisitImpl([](const auto& imp) { return CheckType<T>(imp); });
}

// Verifies that a qtype is named tuple and has only DataSlice values.
absl::Status VerifyNamedTuple(arolla::QTypePtr qtype);

// Returns true if the qtype is NonDeterministicToken QType.
absl::Status VerifyIsNonDeterministicToken(arolla::QTypePtr qtype);

// Returns true if the qtype is either DataSlice or Unspecified.
bool IsDataSliceOrUnspecified(arolla::QTypePtr type);

// Returns the names of fields associated with an already validated named tuple
// slot.
std::vector<absl::string_view> GetFieldNames(
    arolla::TypedSlot named_tuple_slot);

// Returns the values of an already validated named tuple slot which has all
// DataSlice values.
std::vector<DataSlice> GetValueDataSlices(
    arolla::TypedSlot named_tuple_slot,
    arolla::FramePtr frame);

// Returns the value of a bool argument that is expected to be a DataItem.
absl::StatusOr<bool> GetBoolArgument(const DataSlice& slice,
                                     absl::string_view arg_name);

// Returns the view to a value of a string argument that is expected to be a
// DataItem.
absl::StatusOr<absl::string_view> GetStringArgument(const DataSlice& slice,
                                                    absl::string_view arg_name);

// Returns the value of an expected scalar integer argument as int64_t.
absl::StatusOr<int64_t> GetIntegerArgument(const DataSlice& slice,
                                           absl::string_view arg_name);

// Returns a present DataItem if b is true, otherwise missing.
DataSlice AsMask(bool b);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_UTILS_H_
