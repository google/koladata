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
#ifndef KOLADATA_TESTING_MATCHERS_H_
#define KOLADATA_TESTING_MATCHERS_H_

#include <ostream>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "arolla/jagged_shape/testing/matchers.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/testing/traversing_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::testing {
namespace matchers_impl {

class DataSliceEquivalentMatcher {
 public:
  using is_gtest_matcher = void;

  explicit DataSliceEquivalentMatcher(DataSlice expected_slice)
      : expected_slice_(std::move(expected_slice)) {}

  bool MatchAndExplain(const DataSlice& slice,
                       ::testing::MatchResultListener* listener) const {
    bool is_equivalent = slice.IsEquivalentTo(expected_slice_);
    *listener << DataSliceRepr(slice)
              << (is_equivalent ? " which is equivalent"
                                : " which is not equivalent");
    return is_equivalent;
  }

  void DescribeTo(::std::ostream* os) const {
    *os << "is equivalent to " << DataSliceRepr(expected_slice_);
  }
  void DescribeNegationTo(::std::ostream* os) const {
    *os << "is not equivalent to " << DataSliceRepr(expected_slice_);
  }

 private:
  DataSlice expected_slice_;
};

class DataSliceDeepEquivalentMatcher {
 public:
  using is_gtest_matcher = void;

  explicit DataSliceDeepEquivalentMatcher(
      DataSlice expected_slice, DeepEquivalentParams comparison_params)
      : expected_slice_(std::move(expected_slice)),
        comparison_params_(comparison_params) {}

  bool MatchAndExplain(const DataSlice& slice,
                       ::testing::MatchResultListener* listener) const {
    auto match_result_or = TryMatch(slice);
    if (!match_result_or.ok()) {
      *listener << match_result_or.status();
      return false;
    }
    auto [is_equivalent, msg] = std::move(*match_result_or);
    *listener << msg;
    return is_equivalent;
  }

  void DescribeTo(::std::ostream* os) const {
    *os << "is deep equivalent to " << DataSliceRepr(expected_slice_);
  }
  void DescribeNegationTo(::std::ostream* os) const {
    *os << "is not deep equivalent to " << DataSliceRepr(expected_slice_);
  }

 private:
  absl::StatusOr<std::pair<bool, std::string>> TryMatch(
      const DataSlice& slice) const {
    ASSIGN_OR_RETURN(
        auto mismatches,
        DeepEquivalentMismatches(slice, expected_slice_,
                                 /*max_count=*/5, comparison_params_));
    if (mismatches.empty()) {
      return std::make_pair(
          true, absl::StrCat(DataSliceRepr(slice), " which is equivalent"));
    }
    ReprOption repr_option(
        {.depth = 1, .unbounded_type_max_len = 100, .show_databag_id = false});
    auto expected_repr = DataSliceRepr(expected_slice_, repr_option);
    auto actual_repr = DataSliceRepr(slice, repr_option);
    std::string msg = absl::StrFormat(
        "Expected: is equal to %s\nActual: %s, with difference:\n%s",
        expected_repr, actual_repr, absl::StrJoin(mismatches, "\n"));
    return std::make_pair(false, std::move(msg));
  }

  DataSlice expected_slice_;
  DeepEquivalentParams comparison_params_;
};

}  // namespace matchers_impl

// Returns GMock matcher for DataSlice equivalence.
//
// Usage:
//   EXPECT_THAT(my_data_slice, IsEquivalentTo(expected_slice));
//
inline auto IsEquivalentTo(const DataSlice& expected_slice) {
  return matchers_impl::DataSliceEquivalentMatcher(expected_slice);
}

// Returns GMock matcher for DataSlice deep equivalence.
//
// Usage:
//   EXPECT_THAT(my_data_slice, IsEquivalentTo(expected_slice));
//   EXPECT_THAT(
//       my_data_slice,
//       IsDeepEquivalentTo(expected_slice, DeepEquivalentParams(
//           {.partial = true, .ids_equality=true, .schema_equality=true})));
//
inline auto IsDeepEquivalentTo(const DataSlice& expected_slice,
                               const DeepEquivalentParams& comparison_params =
                                   DeepEquivalentParams()) {
  return matchers_impl::DataSliceDeepEquivalentMatcher(expected_slice,
                                                       comparison_params);
}

// Bring matchers for internal::DataSliceImpl and internal::DataItem into
// koladata::testing namespace.
using ::koladata::internal::testing::IsEquivalentTo;

// Bring matchers for arolla::JaggedShape into koladata::testing namespace.
using ::arolla::testing::IsEquivalentTo;

}  // namespace koladata::testing

namespace koladata {

// go/gunitadvanced#teaching-googletest-how-to-print-your-values.
inline void PrintTo(const DataSlice& slice, std::ostream* os) {
  *os << DataSliceRepr(slice);
}

}  // namespace koladata

#endif  // KOLADATA_TESTING_MATCHERS_H_
