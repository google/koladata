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
#include "koladata/internal/random.h"

#include <cstdint>
#include <cstdlib>
#include <random>
#include <string>

#include "absl/base/const_init.h"
#include "absl/base/no_destructor.h"
#include "absl/numeric/int128.h"
#include "absl/random/random.h"
#include "absl/random/uniform_int_distribution.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "arolla/util/fingerprint.h"

namespace koladata::internal {
namespace {

// Storing KOLADATA_DETERMINISTIC_SEED in a global variable to avoid overriding
// it in runtime.
absl::string_view GetDeterministicSeed() {
  static const absl::NoDestructor<std::string> seed{[] {
    const char* seed_cstr = std::getenv("KOLADATA_DETERMINISTIC_SEED");
    return seed_cstr == nullptr ? "" : seed_cstr;
  }()};
  return *seed;
}

bool IsDeterministicMode() {
  static const bool is_deterministic_mode = !GetDeterministicSeed().empty();
  return is_deterministic_mode;
}

uint64_t NonDeterministicRandomUint64() {
  thread_local absl::BitGen gen;
  return absl::uniform_int_distribution<uint64_t>()(gen);
}

}  // namespace

// Not in :: namespace for testing purposes.
uint64_t DeterministicRandomUint64() {
  // TODO: b/464002636 — Add a check that the function is only used from a
  // single thread?
  static absl::NoDestructor<std::mt19937_64> global_gen{[] {
    std::seed_seq seed(GetDeterministicSeed().begin(),
                       GetDeterministicSeed().end());
    return std::mt19937_64(seed);
  }()};
  static absl::NoDestructor<absl::Mutex> global_gen_mutex(absl::kConstInit);
  thread_local absl::NoDestructor<std::mt19937_64> gen{[&] {
    absl::MutexLock lock(*global_gen_mutex);
    return std::mt19937_64(
        std::uniform_int_distribution<uint64_t>()(*global_gen));
  }()};
  return std::uniform_int_distribution<uint64_t>()(*gen);
}

uint64_t MaybeDeterministicRandomUint64() {
  return IsDeterministicMode() ? DeterministicRandomUint64()
                               : NonDeterministicRandomUint64();
}

arolla::Fingerprint MaybeDeterministicRandomFingerprint() {
  return arolla::Fingerprint(absl::MakeUint128(
      MaybeDeterministicRandomUint64(), MaybeDeterministicRandomUint64()));
}

}  // namespace koladata::internal
