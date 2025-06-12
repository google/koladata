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
#include <utility>

#include "gtest/gtest.h"
#include "arolla/jagged_shape/testing/matchers.h"
#include "arolla/util/repr.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/testing/matchers.h"

namespace koladata::testing {
namespace matchers_impl {

class DataSliceEquivalentMatcher {
 public:
  using is_gtest_matcher = void;

  explicit DataSliceEquivalentMatcher(const DataSlice& expected_slice)
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

}  // namespace matchers_impl

// Returns GMock matcher for DataSlice equivalence.
//
// Usage:
//   EXPECT_THAT(my_data_slice, IsEquivalentTo(expected_slice));
//
inline auto IsEquivalentTo(const DataSlice& expected_slice) {
  return matchers_impl::DataSliceEquivalentMatcher(expected_slice);
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
