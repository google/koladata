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

#include <atomic>
#include <cstdint>
#include <random>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/numeric/int128.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "arolla/util/fingerprint.h"

extern "C" {

// Note: Implement these functions as weak symbols so that the implementations
// can be overridden by a build dependency or via LD_PRELOAD.

ABSL_ATTRIBUTE_WEAK uint64_t KoladataInternalPseudoRandomUint64() {
  static thread_local absl::BitGen bitgen;
  return absl::Uniform<uint64_t>(bitgen);
}

extern "C" ABSL_ATTRIBUTE_WEAK int KoladataInternalReseedPseudoRandom(
    std::seed_seq&& /*entropy*/) {
  return -1;
}

}  // extern "C"

namespace koladata::internal {
namespace {

constinit std::atomic_uint64_t pseudo_random_epoch_id = 0;

}  // namespace

// Reseeds the pseudo-random sequence using the given seed.
//
// This function has effect on the subsequent calls to
// KoladataInternalPseudoRandomUint64 and, consequently, on the pseudo-random
// instance ID and fingerprint.
absl::Status ReseedPseudoRandom(std::seed_seq&& entropy) {
  if (KoladataInternalReseedPseudoRandom(std::move(entropy)) < 0) {
    return absl::UnimplementedError(
        "KoladataInternalReseedPseudoRandom does not support reseeding.");
  }
  pseudo_random_epoch_id = KoladataInternalPseudoRandomUint64();
  return absl::OkStatus();
}

const std::atomic_uint64_t* absl_nonnull PseudoRandomEpochIdPtr() {
  while (pseudo_random_epoch_id.load(std::memory_order_relaxed) == 0)
      [[unlikely]] {
    uint64_t zero = 0;
    pseudo_random_epoch_id.compare_exchange_weak(
        zero, KoladataInternalPseudoRandomUint64());
  }
  return &pseudo_random_epoch_id;
}

arolla::Fingerprint PseudoRandomFingerprint() {
  return arolla::Fingerprint(
      absl::MakeUint128(KoladataInternalPseudoRandomUint64(),
                        KoladataInternalPseudoRandomUint64()));
}

}  // namespace koladata::internal
