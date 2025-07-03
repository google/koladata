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
#ifndef KOLADATA_FUNCTOR_SIGNATURE_UTILS_H_
#define KOLADATA_FUNCTOR_SIGNATURE_UTILS_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/signature.h"

namespace koladata::functor {

// Binds arguments in a given function call to a signature. This method
// mirrors inspect.Signature.bind() + apply_defaults() in Python 3.
// The return value will have length signature.parameters().size(), and
// correspond to the parameters in order. A variadic positional argument
// will receive an Arolla tuple, and a variadic keyword argument will receive
// an Arolla namedtuple. This returns TypedValues and not TypedRefs because
// we allocate a tuple/namedtuple for variadic parameters and the return value
// must own it.
// If `default_values_db` is specified, it will be attached to all default
// argument values. It is needed if `KodaSignatureToCppSignature` was used
// with detach_default_values_db=true.
absl::StatusOr<std::vector<arolla::TypedValue>> BindArguments(
    const Signature& signature, absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames,
    DataBagPtr default_values_db = nullptr);

// Return the constants used to store the parameter kinds in the Koda signature.
const DataSlice& PositionalOnlyParameterKind();
const DataSlice& PositionalOrKeywordParameterKind();
const DataSlice& VarPositionalParameterKind();
const DataSlice& KeywordOnlyParameterKind();
const DataSlice& VarKeywordParameterKind();

// Returns the constant used to indicate that a parameter has no default value.
const DataSlice& NoDefaultValueMarker();

// Returns functor signature for the *args and **kwargs parameters.
const DataSlice& KodaArgsKwargsSignature();


// Converts a C++ Signature object to a Koda DataItem storing the signature.
// The returned DataItem will have a new DataBag created to store the triples.
// KodaSignatureToCppSignature can be found in
// koladata/signature_storage.h.
absl::StatusOr<DataSlice> CppSignatureToKodaSignature(
  const Signature& signature);

#endif  // KOLADATA_FUNCTOR_SIGNATURE_UTILS_H_

}  // namespace koladata::functor
