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
#include "koladata/internal/pseudo_random.h"

#include <cstdint>

#include "absl/base/attributes.h"
#include "absl/numeric/int128.h"
#include "absl/random/random.h"
#include "arolla/util/fingerprint.h"

// Note: Implement the function as a weak symbol so that this implementation can
// be overridden by a build dependency or via LD_PRELOAD.
extern "C" ABSL_ATTRIBUTE_WEAK uint64_t KoladataInternalPseudoRandomUint64() {
  static thread_local absl::BitGen bitgen;
  return absl::Uniform<uint64_t>(bitgen);
}

namespace koladata::internal {

arolla::Fingerprint PseudoRandomFingerprint() {
  return arolla::Fingerprint(
      absl::MakeUint128(KoladataInternalPseudoRandomUint64(),
                        KoladataInternalPseudoRandomUint64()));
}

}  // namespace koladata::internal
