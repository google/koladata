// Copyright 2024 Google LLC
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
#ifndef KOLADATA_INTERNAL_TESTING_MATCHERS_H_
#define KOLADATA_INTERNAL_TESTING_MATCHERS_H_

#include <ostream>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "arolla/util/demangle.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/triples.h"

namespace koladata::internal::testing {
namespace matchers_impl {

template <typename T>
class DataItemWithMatcher {
 public:
  using is_gtest_matcher = void;

  explicit DataItemWithMatcher(::testing::Matcher<T> value_matcher)
      : value_matcher_(value_matcher) {}

  bool MatchAndExplain(const DataItem& v,
                       ::testing::MatchResultListener* listener) const {
    if (!v.holds_value<T>()) {
      *listener << "stores a value with dtype " << v.dtype()->name()
                << " which does not match C++ type `" << arolla::TypeName<T>()
                << "`";
      return false;
    }
    bool matched = value_matcher_.MatchAndExplain(v.value<T>(), listener);
    if (!matched) {
      *listener << "the value is " << v.DebugString();
    }
    return matched;
  }

  void DescribeTo(::std::ostream* os) const {
    *os << "stores value of type `" << arolla::TypeName<T>() << "` that ";
    value_matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(::std::ostream* os) const {
    *os << "doesn't store a value of type `" << arolla::TypeName<T>()
        << "` or stores a value that ";
    value_matcher_.DescribeNegationTo(os);
  }

 private:
  ::testing::Matcher<T> value_matcher_;
};

// IsEquivalentTo matcher for DataSliceImpl and DataItem.
template <typename Impl>
class DataSliceImplEquivalentMatcher {
 public:
  using is_gtest_matcher = void;

  explicit DataSliceImplEquivalentMatcher(const Impl& expected)
      : expected_(std::move(expected)) {}

  bool MatchAndExplain(const Impl& slice,
                       ::testing::MatchResultListener* listener) const {
    bool is_equivalent = slice.IsEquivalentTo(expected_);
    *listener << (is_equivalent ? "which is equivalent"
                                : "which is not equivalent");
    return is_equivalent;
  }

  void DescribeTo(::std::ostream* os) const {
    *os << "is equivalent to " << absl::StrCat(expected_);
  }
  void DescribeNegationTo(::std::ostream* os) const {
    *os << "is not equivalent to " << absl::StrCat(expected_);
  }

 private:
  Impl expected_;
};

}  // namespace matchers_impl

// Returns GMock matcher for DataItem content.
//
// Usage:
//   EXPECT_THAT(my_data_item, DataItemWith<value_type>(value_matcher));
//
template <typename T, typename ValueMatcher>
::testing::Matcher<DataItem> DataItemWith(ValueMatcher value_matcher) {
  return matchers_impl::DataItemWithMatcher<T>(
      ::testing::MatcherCast<T>(value_matcher));
}

// Returns GMock matcher for DataSliceImpl equivalence.
//
// Usage:
//   EXPECT_THAT(my_data_slice, IsEquivalentTo(expected_slice));
//
inline auto IsEquivalentTo(const DataSliceImpl& expected_slice) {
  return matchers_impl::DataSliceImplEquivalentMatcher<DataSliceImpl>(
      expected_slice);
}
inline auto IsEquivalentTo(const DataItem& expected_item) {
  return matchers_impl::DataSliceImplEquivalentMatcher<DataItem>(expected_item);
}

// Returns GMock for missing DataItems.
//
// Usage:
//   EXPECT_THAT(my_data_item, MissingDataItem());
//
MATCHER(MissingDataItem, negation ? "is not missing" : "is missing") {
  if (arg.has_value()) {
    *result_listener << "is not missing, contains " << arg.DebugString();
    return false;
  }
  return true;
}

// Note: `DataBagEqual` is tested in `triples_test.cc` as it is just a wrapper
// around `operator==` on Triples.
class DataBagEqual {
 public:
  using is_gtest_matcher = void;

  // NOLINTNEXTLINE(google-explicit-constructor)
  DataBagEqual(const DataBagImpl& db) : expected_db_(db) {}
  // NOLINTNEXTLINE(google-explicit-constructor)
  DataBagEqual(const DataBagImplPtr& db) : expected_db_(*db) {}

  bool MatchAndExplain(const DataBagImplPtr& db, std::ostream* stream) const {
    return MatchAndExplain(*db, stream);
  }

  bool MatchAndExplain(const DataBagImpl& db, std::ostream* stream) const {
    using Triples = ::koladata::internal::debug::Triples;
    Triples triples(db.ExtractContent().value());
    Triples expected_triples(expected_db_.ExtractContent().value());
    if (triples == expected_triples) {
      return true;
    } else {
      if (stream) {
        *stream << "\nEXPECTED: " << expected_triples.DebugString()
                << "\nACTUAL: " << triples.DebugString();
        *stream << "\n\nPRESENT BUT NOT EXPECTED: "
                << triples.Subtract(expected_triples).DebugString();
        *stream << "\n\nEXPECTED BUT NOT PRESENT: "
                << expected_triples.Subtract(triples).DebugString();
      }
      return false;
    }
  }

  void DescribeTo(std::ostream* os) const { *os << "data bags are equal"; }
  void DescribeNegationTo(std::ostream* os) const {
    *os << "data bags are not equal";
  }

 private:
  const DataBagImpl& expected_db_;
};

}  // namespace koladata::internal::testing

#endif  // KOLADATA_INTERNAL_TESTING_MATCHERS_H_
