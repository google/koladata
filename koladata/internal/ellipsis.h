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
#ifndef KOLADATA_INTERNAL_ELLIPSIS_H_
#define KOLADATA_INTERNAL_ELLIPSIS_H_

#include "absl/strings/string_view.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata::internal {

struct Ellipsis {
  arolla::ReprToken ArollaReprToken() const { return {"..."}; }
  void ArollaFingerprint(arolla::FingerprintHasher* hasher) const {
    hasher->Combine(absl::string_view("::koladata::internal::EllipsisQValue"));
  }
};

}  // namespace koladata::internal

namespace arolla {

AROLLA_DECLARE_SIMPLE_QTYPE(ELLIPSIS, koladata::internal::Ellipsis);

}  // namespace arolla

#endif  // KOLADATA_INTERNAL_ELLIPSIS_H_
