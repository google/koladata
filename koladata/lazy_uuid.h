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
#ifndef KOLADATA_LAZY_UUID_H_
#define KOLADATA_LAZY_UUID_H_

#include <optional>

#include "absl/base/attributes.h"
#include "absl/base/call_once.h"
#include "arolla/util/fingerprint.h"
#include "koladata/internal/pseudo_random.h"

namespace koladata {

// A unique identifier of an object. The identifier is either specified upon
// construction, or generated using internal::PseudoRandomFingerprint() upon
// the first access.
//
// This class is thread-safe.
class LazyUuid {
 public:
  // Constructs an instance with a pseudo-random identifier.
  LazyUuid() = default;

  // Constructs an instance with the given identifier; if the value is not set,
  // the behaviour is the same as in the default constructor.
  explicit LazyUuid(std::optional<arolla::Fingerprint> value) {
    if (value.has_value()) {
      value_ = *value;
      absl::call_once(once_, [] {});
    }
  }

  // Not copyable or movable.
  LazyUuid(const LazyUuid&) = delete;
  LazyUuid& operator=(const LazyUuid&) = delete;

  // Returns the identifier.
  const arolla::Fingerprint& get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    absl::call_once(once_,
                    [this] { value_ = internal::PseudoRandomFingerprint(); });
    return value_;
  }

 private:
  mutable arolla::Fingerprint value_;
  mutable absl::once_flag once_;
};

}  // namespace koladata

#endif  // KOLADATA_LAZY_UUID_H_
