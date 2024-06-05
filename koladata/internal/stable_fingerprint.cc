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
#include "koladata/internal/stable_fingerprint.h"

#include <cstddef>

#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "cityhash/city.h"
#include "arolla/util/fingerprint.h"

namespace koladata::internal {

StableFingerprintHasher::StableFingerprintHasher(absl::string_view salt)
    : state_{3102879407, 2758948377}  // initial_seed
{
  Combine(salt);
}

StableFingerprintHasher::StableFingerprintHasher(
    const arolla::Fingerprint& salt)
    : state_{absl::Uint128Low64(salt.value), absl::Uint128High64(salt.value)} {}

arolla::Fingerprint StableFingerprintHasher::Finish() && {
  return arolla::Fingerprint{absl::MakeUint128(state_.second, state_.first)};
}

StableFingerprintHasher& StableFingerprintHasher::CombineRawBytes(
    const void* data, size_t size) {
  state_ = cityhash::CityHash128WithSeed(
      static_cast<const char*>(data), size, state_);
  return *this;
}

}  // namespace koladata::internal
