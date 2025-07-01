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
#include "koladata/functor/default_signature.h"

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/expr/constants.h"
#include "koladata/functor/signature.h"
#include "koladata/functor/signature_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<DataSlice> DefaultKodaSignatureFromInputs(
    absl::Span<const std::string> inputs) {
  std::vector<Signature::Parameter> parameters;
  parameters.reserve(inputs.size() + 2);
  parameters.push_back({
      .name = "self",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
      .default_value = expr::UnspecifiedSelfInput(),
  });
  for (const auto& input : inputs) {
    if (input == "self") {
      continue;
    }
    parameters.push_back(Signature::Parameter{
        .name = input,
        .kind = Signature::Parameter::Kind::kKeywordOnly,
    });
  }
  parameters.push_back(Signature::Parameter{
      .name = "__extra_inputs__",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  });
  ASSIGN_OR_RETURN(auto signature, Signature::Create(parameters));
  return CppSignatureToKodaSignature(signature);
}

}  // namespace koladata::functor
