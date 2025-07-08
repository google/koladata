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
#ifndef KOLADATA_FUNCTOR_CALL_H_
#define KOLADATA_FUNCTOR_CALL_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// Calls the given functor with the provided arguments and keyword arguments.
// The functor would typically be created by the CreateFunctor method,
// and consists of a returns expression, a signature, and a set of variables.
//
// `args` must contain values for positional arguments followed by values for
// keyword arguments; `kwnames` must contain the names of the keyword arguments,
// so `kwnames` corresponds to a suffix of `args`. The passed arguments will be
// bound to the parameters of the signature to produce the values for the inputs
// (I.foo) in the provided expression and in the variable expressions. The
// expressions can also refer to the variables via V.foo, in which case the
// variable expression will be evaluated before evaluating the expression that
// refers to it. In case of a cycle in variables, an error will be returned.
//
// If the functor has __stack_trace_frame__ attribute set, all the errors
// returned from it will be wrapped with a child error with StackTraceFrame
// payload.
//
absl::StatusOr<arolla::TypedValue> CallFunctorWithCompilationCache(
    const DataSlice& functor, absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_CALL_H_
