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
#ifndef KOLADATA_FUNCTOR_SIGNATURE_H_
#define KOLADATA_FUNCTOR_SIGNATURE_H_

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace koladata::functor {

// Describes the signature of a Koda functor. This class mirrors the
// inspect.Signature class in Python 3.
// This class is similar to ExprOperatorSignature in Arolla, but is more
// rich since it supports all five Python 3 parameter kinds.
// On the other hand, given that functors are stored as DataItems, the default
// values, when present, must be DataItems too.
// Note that this class is meant for C++ use only, and is not meant to be
// exposed to Python or used as an Arolla QValue.
class Signature {
 public:
  // Describes a single parameter in a signature. This class mirrors
  // the inspect.Parameter class in Python 3.
  struct Parameter {
    // Describes the kind of a parameter in a signature.
    // These values correspond to the same Python 3 parameter kinds.
    // The order of definition here is important, the parameter kinds must
    // appear in this order in any signature.
    enum class Kind {
      kPositionalOnly,
      kPositionalOrKeyword,
      kVarPositional,
      kKeywordOnly,
      kVarKeyword,
    };

    std::string name;
    Kind kind;
    // This DataSlice must always be a data item (0-dim).
    std::optional<DataSlice> default_value = std::nullopt;
  };

  // Creates a signature from a list of parameters.
  // This method validates the signature and returns an error if it is invalid.
  // Since all signatures are validated on creation, further uses of the
  // Signature object can assume it is valid.
  static absl::StatusOr<Signature> Create(
      absl::Span<const Parameter> parameters);

  const std::vector<Parameter>& parameters() const { return parameters_; }
  const absl::flat_hash_map<std::string, size_t>& keyword_parameter_index()
      const {
    return keyword_parameter_index_;
  }

  Signature(const Signature& other) = default;
  Signature& operator=(const Signature& other) = default;
  Signature(Signature&& other) = default;
  Signature& operator=(Signature&& other) = default;

 private:
  Signature(absl::Span<const Parameter> parameters);

  std::vector<Parameter> parameters_;
  absl::flat_hash_map<std::string, size_t> keyword_parameter_index_;
};

// Binds arguments in a given function call to a signature. This method
// mirrors inspect.Signature.bind() + apply_defaults() in Python 3.
// The return value will have length signature.parameters().size(), and
// correspond to the parameters in order. A variadic positional argument
// will receive an Arolla tuple, and a variadic keyword argument will receive
// an Arolla namedtuple. This returns TypedValues and not TypedRefs because
// we allocate a tuple/namedtuple for variadic parameters and the return value
// must own it.
absl::StatusOr<std::vector<arolla::TypedValue>> BindArguments(
    const Signature& signature, absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames);

// Makes it possible to use absl::StrCat/absl::StrFormat (with %v) with kinds.
template <typename Sink>
void AbslStringify(Sink& sink, Signature::Parameter::Kind kind) {
  using enum Signature::Parameter::Kind;
  switch (kind) {
    case kPositionalOnly:
      sink.Append("positional only");
      return;
    case kPositionalOrKeyword:
      sink.Append("positional or keyword");
      return;
    case kVarPositional:
      sink.Append("variadic positional");
      return;
    case kKeywordOnly:
      sink.Append("keyword only");
      return;
    case kVarKeyword:
      sink.Append("variadic keyword");
      return;
      // We do not have a default case so that the compiler will complain if
      // we add a new enum value and do not handle it here.
  }
  sink.Append("unknown");
}

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_SIGNATURE_H_
