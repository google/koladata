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
#include "py/koladata/serving/embedded_slices_internal.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/riegeli.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/cpp_function_bridge.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/operators/math.h"
#include "koladata/signature.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::serving::embedded_slices_internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::QValueWith;
using ::koladata::testing::IsEquivalentTo;
using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

DataSlice CreateConstantFunctor(int64_t value) {
  auto returns_expr =
      test::DataItem(arolla::expr::ExprQuote((arolla::expr::Literal(value))));
  auto signature = functor::Signature::Create({}).value();
  auto koda_signature = functor::CppSignatureToKodaSignature(signature).value();
  return functor::CreateFunctor(returns_expr, koda_signature, {}, {}).value();
}

TEST(ParseEmbeddedSlicesTest, Trival) {
  auto functor_names = test::DataSlice<arolla::Text>({"f57", "f42"});
  ASSERT_OK_AND_ASSIGN(
      std::string data,
      arolla::serialization::EncodeAsRiegeliData(
          {arolla::TypedValue::FromValue(std::move(functor_names)),
           arolla::TypedValue::FromValue(CreateConstantFunctor(57)),
           arolla::TypedValue::FromValue(CreateConstantFunctor(42))},
          {}));
  ASSERT_OK_AND_ASSIGN(auto functors, ParseEmbeddedSlices(data));
  ASSERT_THAT(functors, UnorderedElementsAre(Pair("f57", _), Pair("f42", _)));
  EXPECT_THAT(
      functor::CallFunctorWithCompilationCache(functors.at("f57"), {}, {}),
      IsOkAndHolds(QValueWith<int64_t>(57)));
}

TEST(ParseEmbeddedSlicesTest, CorruptData) {
  EXPECT_THAT(ParseEmbeddedSlices("\x08\x96"),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("invalid embedded data")));
}

TEST(ParseEmbeddedSlicesTest, EmptyData) {
  EXPECT_THAT(ParseEmbeddedSlices(""),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("invalid embedded data")));
}

TEST(ParseEmbeddedSlicesTest, IncorrectNames) {
  {
    auto functor_names = test::DataItem(arolla::Text("f57"));
    ASSERT_OK_AND_ASSIGN(
        std::string data,
        arolla::serialization::EncodeAsRiegeliData(
            {
                arolla::TypedValue::FromValue(std::move(functor_names)),
                arolla::TypedValue::FromValue(CreateConstantFunctor(57)),
            },
            {}));
    EXPECT_THAT(
        ParseEmbeddedSlices(data),
        StatusIs(absl::StatusCode::kInternal,
                 HasSubstr("names must be a full data slice of rank 1")));
  }
  {
    auto functor_names = test::DataSlice<arolla::Text>({"f57", std::nullopt});
    ASSERT_OK_AND_ASSIGN(
        std::string data,
        arolla::serialization::EncodeAsRiegeliData(
            {
                arolla::TypedValue::FromValue(std::move(functor_names)),
                arolla::TypedValue::FromValue(CreateConstantFunctor(57)),
            },
            {}));
    EXPECT_THAT(ParseEmbeddedSlices(data),
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("names must be a full data slice")));
  }
  {
    auto functor_names = test::DataSlice<arolla::Bytes>({"f57", "f42"});
    ASSERT_OK_AND_ASSIGN(
        std::string data,
        arolla::serialization::EncodeAsRiegeliData(
            {
                arolla::TypedValue::FromValue(std::move(functor_names)),
                arolla::TypedValue::FromValue(CreateConstantFunctor(57)),
                arolla::TypedValue::FromValue(CreateConstantFunctor(42)),
            },
            {}));
    EXPECT_THAT(ParseEmbeddedSlices(data),
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("names must be a slice of strings")));
  }
  {
    auto functor_names = test::DataSlice<arolla::Text>({"f57", "f42"});
    ASSERT_OK_AND_ASSIGN(
        std::string data,
        arolla::serialization::EncodeAsRiegeliData(
            {
                arolla::TypedValue::FromValue(std::move(functor_names)),
                arolla::TypedValue::FromValue(CreateConstantFunctor(57)),
            },
            {}));
    EXPECT_THAT(
        ParseEmbeddedSlices(data),
        StatusIs(absl::StatusCode::kInternal,
                 HasSubstr("number of names must match number of slices")));
  }
}

TEST(GetEmbeddedSlice, Trival) {
  EmbeddedSlices functors;
  ASSERT_OK_AND_ASSIGN(functors["add"], functor::CreateFunctorFromFunction(
                                            &ops::Add, "add", "x, y"));
  ASSERT_OK_AND_ASSIGN(
      functors["multiply"],
      functor::CreateFunctorFromFunction(&ops::Multiply, "multiply", "x, y"));
  EXPECT_THAT(GetEmbeddedSlice(functors, "add"),
              IsOkAndHolds(IsEquivalentTo(functors.at("add"))));
  EXPECT_THAT(GetEmbeddedSlice(functors, "divide"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "embedded slice not found: divide"));
}

}  // namespace
}  // namespace koladata::serving::embedded_slices_internal
