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
#ifndef KOLADATA_INTERNAL_OP_UTILS_PRINT_H_
#define KOLADATA_INTERNAL_OP_UTILS_PRINT_H_

#include "absl/functional/any_invocable.h"
#include "absl/strings/string_view.h"

namespace koladata::internal {

// Interface for printing strings. By default it prints to stdout, but
// it is possible to override the callback.
class Printer {
 public:
  virtual ~Printer() = default;

  // Prints the given message.
  virtual void Print(absl::string_view message) const = 0;

  // Sets the callback to be used for printing. This overrides the default
  // printing to std::cout.
  virtual void SetPrintCallback(
      absl::AnyInvocable<void(absl::string_view) const> callback) = 0;
};

// Returns a shared instance of the printer. Thread safe.
Printer& GetSharedPrinter();

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_PRINT_H_
