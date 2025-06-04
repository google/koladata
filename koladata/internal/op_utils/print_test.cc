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
#include "koladata/internal/op_utils/print.h"

#include <iostream>
#include <sstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/cleanup/cleanup.h"
#include "koladata/data_slice_qtype.h"

namespace koladata::internal {
namespace {

using ::testing::Eq;

// The main test is located in
// py/koladata/operators/tests/core_with_print_test.py; here
// we only test the default printing to stdout, without Python-specific
// callback.
TEST(PrinterTest, WritesToStdoutByDefault) {
  std::streambuf* old_cout_rdbuf = std::cout.rdbuf();
  std::stringstream captured_output;
  std::cout.rdbuf(captured_output.rdbuf());
  absl::Cleanup restore_cout = [&] { std::cout.rdbuf(old_cout_rdbuf); };

  Printer& printer = GetSharedPrinter();
  printer.Print("foo\n");
  printer.Print("bar");

  EXPECT_THAT(captured_output.str(), Eq("foo\nbar"));
}

}  // namespace
}  // namespace koladata::internal
