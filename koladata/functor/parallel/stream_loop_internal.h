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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_LOOP_INTERNAL_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_LOOP_INTERNAL_H_

#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/stream.h"

// Utilities for implementing stream looping operations.
namespace koladata::functor::parallel::stream_loop_internal {

// A helper type representing the result of parsing a loop condition.
//
// Valid states:
//
//   {false, nullptr}: The condition is negative.
//   {true, nullptr}: The condition is positive.
//   {false, nonnull reader}: The condition is pending; parsing should be
//                            retried once the reader is ready.
struct ParsedLoopCondition {
  bool value;
  StreamReaderPtr absl_nullable reader;
};

// Parses a loop condition value, which must be either a `DATA_SLICE` storing
// a mask data-item or a single-item stream storing a mask data-item.
absl::StatusOr<ParsedLoopCondition> ParseLoopCondition(
    arolla::TypedRef condition);

// Parses the given loop condition given as a `DATA_SLICE` storing a mask
// data-item.
absl::StatusOr<ParsedLoopCondition> ParseLoopConditionDataSlice(
    const DataSlice& condition);

// Parses the given loop condition given as a stream storing a mask data-item.
absl::StatusOr<ParsedLoopCondition> ParseLoopConditionStream(
    StreamReaderPtr absl_nonnull condition);

// A helper class for managing loop variables.
//
// This class stores variable values in a way that allows passing them to
// a functor call without additional allocations or copying:
//
//   CallFunctorWithCompilationCache(functor, vars.values(), vars.kwnames())
//
// It also supports updating these variables from the functor's result.
//
// Disclaimer: This class is tailored around the needs of the stream loop
// routines, and it isn't complete and hermetic. If you intend to use it in
// other contexts, please consider improving it first.
//
class Vars {
 public:
  // Constructs an instances with the given `initial_values` and `kwnames`.
  //
  // `kwnames` provides names for the trailing variables and must not exceed the
  // size of `initial_values`. The variables at the beginning remain unnamed,
  // and can only be updated via `mutable_values()`.
  Vars(std::vector<arolla::TypedRef> initial_values,
       std::vector<std::string> kwnames);

  // Move-only.
  Vars(Vars&&) = default;
  Vars& operator=(Vars&&) = default;

  // Returns the current `values`; the order of values matches the order
  // in `init_values` provided to the constructor.
  absl::Span<const arolla::TypedRef> values() const { return values_; }

  // Returns `kwnames` passed in the constructor.
  absl::Span<const std::string> kwnames() const { return kwnames_; }

  // Provides mutable access to the current `values`.
  //
  // Important: The returned span allows direct mutation of the values,
  // including changing their types.
  //
  // This class does NOT guarantee the lifetime of overwritten values, and it
  // becomes the caller's responsibility. (While seemingly a quirk, this
  // behaviour is actually convenient for "foreach" loops where the input stream
  // owns the values.)
  absl::Span<arolla::TypedRef> mutable_values() {
    return absl::MakeSpan(values_);
  }

  // Updates the variables with values from the given `update` which must be
  // a namedtuple. New variable values must retain the same type as their
  // current ones. This class guarantees the lifetime of the new values.
  absl::Status Update(arolla::TypedValue update);

 private:
  arolla::TypedValue initial_values_holder_;
  std::vector<arolla::TypedRef> values_;
  std::vector<std::string> kwnames_;

  struct Index {
    arolla::TypedRef& mutable_ref;
    arolla::TypedValue holder;
  };
  absl::flat_hash_map<absl::string_view, Index> index_;
};

}  // namespace koladata::functor::parallel::stream_loop_internal

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_WHILE_LOOP_H_
