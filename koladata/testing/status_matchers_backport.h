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

#ifndef KOLADATA_TESTING_STATUS_MATCHERS_BACKPORT_H
#define KOLADATA_TESTING_STATUS_MATCHERS_BACKPORT_H

#include <ostream>  // NOLINT
#include <string>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/util/status_macros_backport.h"  // IWYU pragma: keep. Includes ASSERT_OK.

namespace koladata::testing {

using ::testing::_;
using ::testing::Matcher;
using ::testing::MatcherCast;
using ::testing::MatcherInterface;
using ::testing::MatchResultListener;
using ::testing::PrintToString;
using ::testing::SafeMatcherCast;
using ::testing::StringMatchResultListener;

namespace internal_status {

inline const absl::Status& GetStatus(const absl::Status& status) {
  return status;
}

template <typename T>
inline const absl::Status& GetStatus(const absl::StatusOr<T>& status) {
  return status.status();
}

////////////////////////////////////////////////////////////
// Implementation of IsOkAndHolds().

// Monomorphic implementation of matcher IsOkAndHolds(m).  StatusOrType can be
// either StatusOr<T> or a reference to it.
template <typename StatusOrType>
class IsOkAndHoldsMatcherImpl : public MatcherInterface<StatusOrType> {
 public:
  typedef
      typename std::remove_reference<StatusOrType>::type::value_type value_type;

  template <typename InnerMatcher>
  explicit IsOkAndHoldsMatcherImpl(InnerMatcher&& inner_matcher)
      : inner_matcher_(SafeMatcherCast<const value_type&>(
            std::forward<InnerMatcher>(inner_matcher))) {}

  void DescribeTo(std::ostream* os) const override {
    *os << "is OK and has a value that ";
    inner_matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const override {
    *os << "isn't OK or has a value that ";
    inner_matcher_.DescribeNegationTo(os);
  }

  bool MatchAndExplain(StatusOrType actual_value,
                       MatchResultListener* result_listener) const override {
    if (!actual_value.ok()) {
      *result_listener << "which has status " << actual_value.status();
      return false;
    }

    StringMatchResultListener inner_listener;
    const bool matches =
        inner_matcher_.MatchAndExplain(actual_value.value(), &inner_listener);
    const std::string inner_explanation = inner_listener.str();
    if (!inner_explanation.empty()) {
      *result_listener << "which contains value "
                       << PrintToString(actual_value.value()) << ", "
                       << inner_explanation;
    }
    return matches;
  }

 private:
  const Matcher<const value_type&> inner_matcher_;
};

// Implements IsOkAndHolds(m) as a polymorphic matcher.
template <typename InnerMatcher>
class IsOkAndHoldsMatcher {
 public:
  explicit IsOkAndHoldsMatcher(InnerMatcher inner_matcher)
      : inner_matcher_(std::move(inner_matcher)) {}

  // Converts this polymorphic matcher to a monomorphic matcher of the
  // given type.  StatusOrType can be either StatusOr<T> or a
  // reference to StatusOr<T>.
  template <typename StatusOrType>
  operator Matcher<StatusOrType>() const {  // NOLINT
    return Matcher<StatusOrType>(
        new IsOkAndHoldsMatcherImpl<StatusOrType>(inner_matcher_));
  }

 private:
  const InnerMatcher inner_matcher_;
};

////////////////////////////////////////////////////////////
// Implementation of StatusIs().

// `StatusCode` is implicitly convertible from `int`, `::absl::StatusCode`,
// and any enum that is associated with an error space, and explicitly
// convertible to these types as well.
//
// We need this class because ::absl::StatusCode (as a scoped enum) is not
// implicitly convertible to int. In order to handle use cases like
//   StatusIs(Anyof(::absl::StatusCode::kUnknown,
//   ::absl::StatusCode::kCancelled))
// which uses polymorphic matchers, we need to unify the interfaces into
// Matcher<StatusCode>.
class StatusCode {
 public:
  StatusCode(int code) : code_(code) {}  // NOLINT
  StatusCode(::absl::StatusCode code)    // NOLINT
      : code_(static_cast<int>(code)) {}
  explicit operator int() const { return code_; }
  explicit operator ::absl::StatusCode() const {
    return static_cast<::absl::StatusCode>(code_);
  }
  template <typename T>
  explicit operator T() const {
    return static_cast<T>(code_);
  }

  friend inline void PrintTo(const StatusCode& code, std::ostream* os) {
    *os << code.code_;
  }

 private:
  int code_;
};

// Relational operators to handle matchers like Eq, Lt, etc..
inline bool operator==(const StatusCode& lhs, const StatusCode& rhs) {
  return static_cast<int>(lhs) == static_cast<int>(rhs);
}
inline bool operator!=(const StatusCode& lhs, const StatusCode& rhs) {
  return static_cast<int>(lhs) != static_cast<int>(rhs);
}
inline bool operator<(const StatusCode& lhs, const StatusCode& rhs) {
  return static_cast<int>(lhs) < static_cast<int>(rhs);
}
inline bool operator<=(const StatusCode& lhs, const StatusCode& rhs) {
  return static_cast<int>(lhs) <= static_cast<int>(rhs);
}
inline bool operator>(const StatusCode& lhs, const StatusCode& rhs) {
  return static_cast<int>(lhs) > static_cast<int>(rhs);
}
inline bool operator>=(const StatusCode& lhs, const StatusCode& rhs) {
  return static_cast<int>(lhs) >= static_cast<int>(rhs);
}

// StatusIs() is a polymorphic matcher.  This class is the common
// implementation of it shared by all types T where StatusIs() can be
// used as a Matcher<T>.
class StatusIsMatcherCommonImpl {
 public:
  StatusIsMatcherCommonImpl(Matcher<StatusCode> code_matcher,
                            Matcher<const std::string&> message_matcher)
      : code_matcher_(std::move(code_matcher)),
        message_matcher_(std::move(message_matcher)) {}

  void DescribeTo(std::ostream* os) const;

  void DescribeNegationTo(std::ostream* os) const;

  bool MatchAndExplain(const absl::Status& status,
                       MatchResultListener* result_listener) const;

 private:
  const Matcher<StatusCode> code_matcher_;
  const Matcher<const std::string&> message_matcher_;
};

// Monomorphic implementation of matcher StatusIs() for a given type
// T.  T can be Status, StatusOr<>, or a reference to either of them.
template <typename T>
class MonoStatusIsMatcherImpl : public MatcherInterface<T> {
 public:
  explicit MonoStatusIsMatcherImpl(StatusIsMatcherCommonImpl common_impl)
      : common_impl_(std::move(common_impl)) {}

  void DescribeTo(std::ostream* os) const override {
    common_impl_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const override {
    common_impl_.DescribeNegationTo(os);
  }

  bool MatchAndExplain(T actual_value,
                       MatchResultListener* result_listener) const override {
    return common_impl_.MatchAndExplain(GetStatus(actual_value),
                                        result_listener);
  }

 private:
  StatusIsMatcherCommonImpl common_impl_;
};

// Implements StatusIs() as a polymorphic matcher.
class StatusIsMatcher {
 public:
  template <typename StatusCodeMatcher, typename StatusMessageMatcher>
  StatusIsMatcher(StatusCodeMatcher&& code_matcher,
                  StatusMessageMatcher&& message_matcher)
      : common_impl_(MatcherCast<StatusCode>(
                         std::forward<StatusCodeMatcher>(code_matcher)),
                     MatcherCast<const std::string&>(
                         std::forward<StatusMessageMatcher>(message_matcher))) {
  }

  // Converts this polymorphic matcher to a monomorphic matcher of the
  // given type.  T can be StatusOr<>, Status, or a reference to
  // either of them.
  template <typename T>
  operator Matcher<T>() const {  // NOLINT
    return Matcher<T>(new MonoStatusIsMatcherImpl<T>(common_impl_));
  }

 private:
  const StatusIsMatcherCommonImpl common_impl_;
};

// CanonicalStatusIs() is a polymorphic matcher.  This class is the common
// implementation of it shared by all types T where CanonicalStatusIs() can be
// used as a Matcher<T>.
class CanonicalStatusIsMatcherCommonImpl {
 public:
  CanonicalStatusIsMatcherCommonImpl(
      Matcher<StatusCode> code_matcher,
      Matcher<const std::string&> message_matcher)
      : code_matcher_(std::move(code_matcher)),
        message_matcher_(std::move(message_matcher)) {}

  void DescribeTo(std::ostream* os) const;

  void DescribeNegationTo(std::ostream* os) const;

  bool MatchAndExplain(const absl::Status& status,
                       MatchResultListener* result_listener) const;

 private:
  const Matcher<StatusCode> code_matcher_;
  const Matcher<const std::string&> message_matcher_;
};

// Monomorphic implementation of matcher CanonicalStatusIs() for a given type
// T.  T can be Status, StatusOr<>, or a reference to either of them.
template <typename T>
class MonoCanonicalStatusIsMatcherImpl : public MatcherInterface<T> {
 public:
  explicit MonoCanonicalStatusIsMatcherImpl(
      CanonicalStatusIsMatcherCommonImpl common_impl)
      : common_impl_(std::move(common_impl)) {}

  void DescribeTo(std::ostream* os) const override {
    common_impl_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const override {
    common_impl_.DescribeNegationTo(os);
  }

  bool MatchAndExplain(T actual_value,
                       MatchResultListener* result_listener) const override {
    return common_impl_.MatchAndExplain(GetStatus(actual_value),
                                        result_listener);
  }

 private:
  CanonicalStatusIsMatcherCommonImpl common_impl_;
};

// Implements CanonicalStatusIs() as a polymorphic matcher.
class CanonicalStatusIsMatcher {
 public:
  template <typename StatusCodeMatcher, typename StatusMessageMatcher>
  CanonicalStatusIsMatcher(StatusCodeMatcher&& code_matcher,
                           StatusMessageMatcher&& message_matcher)
      : common_impl_(MatcherCast<StatusCode>(
                         std::forward<StatusCodeMatcher>(code_matcher)),
                     MatcherCast<const std::string&>(
                         std::forward<StatusMessageMatcher>(message_matcher))) {
  }

  // Converts this polymorphic matcher to a monomorphic matcher of the given
  // type.  T can be StatusOr<>, Status, or a reference to either of them.
  template <typename T>
  operator Matcher<T>() const {  // NOLINT
    return Matcher<T>(new MonoCanonicalStatusIsMatcherImpl<T>(common_impl_));
  }

 private:
  const CanonicalStatusIsMatcherCommonImpl common_impl_;
};

// Monomorphic implementation of matcher IsOk() for a given type T.
// T can be Status, StatusOr<>, or a reference to either of them.
template <typename T>
class MonoIsOkMatcherImpl : public MatcherInterface<T> {
 public:
  void DescribeTo(std::ostream* os) const override { *os << "is OK"; }
  void DescribeNegationTo(std::ostream* os) const override {
    *os << "is not OK";
  }
  bool MatchAndExplain(T actual_value, MatchResultListener*) const override {
    return GetStatus(actual_value).ok();
  }
};

// Implements IsOk() as a polymorphic matcher.
class IsOkMatcher {
 public:
  template <typename T>
  operator Matcher<T>() const {  // NOLINT
    return Matcher<T>(new MonoIsOkMatcherImpl<T>());
  }
};

}  // namespace internal_status

// Returns a gMock matcher that matches a StatusOr<> whose status is
// OK and whose value matches the inner matcher.
template <typename InnerMatcher>
internal_status::IsOkAndHoldsMatcher<typename std::decay<InnerMatcher>::type>
IsOkAndHolds(InnerMatcher&& inner_matcher) {
  return internal_status::IsOkAndHoldsMatcher<
      typename std::decay<InnerMatcher>::type>(
      std::forward<InnerMatcher>(inner_matcher));
}

// The one and two-arg StatusIs methods may infer the expected ErrorSpace from
// the StatusCodeMatcher argument. If you call StatusIs(e) or StatusIs(e, msg)
// and the argument `e` is:
// - an enum type,
// - which is associated with a custom ErrorSpace `S`,
// - and is not "OK" (i.e. 0),
// then the matcher will match a Status or StatusOr<> whose error space is `S`.
//
// Otherwise, the expected error space is the canonical error space.

// Returns a gMock matcher that matches a Status or StatusOr<> whose error space
// is the inferred error space (see above), whose status code matches
// code_matcher, and whose error message matches message_matcher.
template <typename StatusCodeMatcher, typename StatusMessageMatcher>
internal_status::StatusIsMatcher StatusIs(
    StatusCodeMatcher&& code_matcher, StatusMessageMatcher&& message_matcher) {
  return internal_status::StatusIsMatcher(
      std::forward<StatusCodeMatcher>(code_matcher),
      std::forward<StatusMessageMatcher>(message_matcher));
}

// Returns a gMock matcher that matches a Status or StatusOr<> whose error space
// is the inferred error space (see above), and whose status code matches
// code_matcher.
template <typename StatusCodeMatcher>
internal_status::StatusIsMatcher StatusIs(StatusCodeMatcher&& code_matcher) {
  return StatusIs(std::forward<StatusCodeMatcher>(code_matcher), _);
}

// Returns a gMock matcher that matches a Status or StatusOr<> whose canonical
// status code (i.e., Status::CanonicalCode) matches code_matcher and whose
// error message matches message_matcher.
template <typename StatusCodeMatcher, typename StatusMessageMatcher>
internal_status::CanonicalStatusIsMatcher CanonicalStatusIs(
    StatusCodeMatcher&& code_matcher, StatusMessageMatcher&& message_matcher) {
  return internal_status::CanonicalStatusIsMatcher(
      std::forward<StatusCodeMatcher>(code_matcher),
      std::forward<StatusMessageMatcher>(message_matcher));
}

// Returns a gMock matcher that matches a Status or StatusOr<> whose canonical
// status code (i.e., Status::CanonicalCode) matches code_matcher.
template <typename StatusCodeMatcher>
internal_status::CanonicalStatusIsMatcher CanonicalStatusIs(
    StatusCodeMatcher&& code_matcher) {
  return CanonicalStatusIs(std::forward<StatusCodeMatcher>(code_matcher), _);
}

// Returns a gMock matcher that matches a Status or StatusOr<> which is OK.
inline internal_status::IsOkMatcher IsOk() {
  return internal_status::IsOkMatcher();
}

}  // namespace koladata::testing

#define EXPECT_OK_FROM_GUNIT_TEMPORARY_GUARD

#endif  // KOLADATA_TESTING_STATUS_MATCHERS_BACKPORT_H
