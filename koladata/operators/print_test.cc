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
#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/cleanup/cleanup.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/text.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/test_utils.h"

namespace koladata::ops {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::Eq;

// The main test is located in
// py/koladata/operators/tests/core_with_print_test.py; here
// we only test the default printing to stdout, without Python-specific
// callback.
TEST(IoTest, CorePrintTest) {
  std::streambuf* old_cout_rdbuf = std::cout.rdbuf();
  std::stringstream captured_output;
  std::cout.rdbuf(captured_output.rdbuf());
  absl::Cleanup restore_cout = [&] { std::cout.rdbuf(old_cout_rdbuf); };

  EXPECT_THAT(
      arolla::testing::InvokeExprOperator<int32_t>(
          "kd.core.with_print", 57,
          arolla::MakeTupleFromFields(test::DataItem(arolla::Text("foo")),
                                      test::DataItem(std::string("bar"))),
          /*sep=*/test::DataItem(arolla::Text(" ")),
          /*end=*/test::DataItem(arolla::Text("\n")),
          internal::NonDeterministicToken()),
      IsOkAndHolds(Eq(57)));

  EXPECT_THAT(captured_output.str(), Eq("foo b'bar'\n"));
}

}  // namespace
}  // namespace koladata::ops
