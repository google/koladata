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
#ifndef KOLADATA_INTERNAL_STABLE_FINGERPRINT_H_
#define KOLADATA_INTERNAL_STABLE_FINGERPRINT_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/strings/string_view.h"
#include "arolla/expr/quote.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/expr_quote_utils.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {

// Version of FingerprintHasher that is used for stable fingerprinting.
//
// This version's requirements are that the results for the same inputs are
// stable and equal in all:
// * in the same runtime
// * across different runtimes
// * across different BUILD(s) (as long as the underlying CityHash does not
//                              change).
class StableFingerprintHasher {
 public:
  explicit StableFingerprintHasher(absl::string_view salt);
  explicit StableFingerprintHasher(const arolla::Fingerprint& salt);

  // Returns the resulting fingeprint.
  arolla::Fingerprint Finish() &&;

  // Combines a list of values to the fingerprint state.
  template <typename... Args>
  StableFingerprintHasher& Combine(const Args&... args) &;

  // Combines a list of values to the fingerprint state.
  template <typename... Args>
  StableFingerprintHasher&& Combine(const Args&... args) &&;

  // Combines a raw byte sequence to the fingerprint state.
  StableFingerprintHasher& CombineRawBytes(const void* data, size_t size);

 private:
  std::pair<uint64_t, uint64_t> state_;
};

namespace fingerprint_impl {

// Returns true if class has
// `void StableFingerprint(StableFingerprintHasher*) const` method.
template <typename T, class = void>
struct HasStableFingerprintMethod : std::false_type {};

template <class T>
struct HasStableFingerprintMethod<
    T, std::void_t<decltype(
        static_cast<void (T::*)(StableFingerprintHasher*) const>(
            &T::StableFingerprint))>> : std::true_type {};

}  // namespace fingerprint_impl

template <typename... Args>
StableFingerprintHasher& StableFingerprintHasher::Combine(
    const Args&... args) & {
  auto combine = [this](const auto& arg) {
    using Arg = std::decay_t<decltype(arg)>;
    if constexpr (fingerprint_impl::HasStableFingerprintMethod<Arg>::value) {
      arg.StableFingerprint(this);
    } else if constexpr (std::is_same_v<Arg, MissingValue>) {
      Combine(absl::string_view("s\0td::nu\0llop\0t_\0t"));
    } else if constexpr (std::is_same_v<Arg, arolla::Fingerprint> ||
                         std::is_same_v<Arg, ObjectId> ||
                         std::is_same_v<Arg, AllocationId>) {
      CombineRawBytes(&arg, sizeof(arg));
    } else if constexpr (std::is_same_v<Arg, arolla::Unit>) {
      Combine(absl::string_view("a\0roll\0a::U\0nit"));
    } else if constexpr (std::is_same_v<Arg, arolla::Text>) {
      Combine(absl::string_view("arolla::Text"), absl::string_view(arg));
    } else if constexpr (std::is_same_v<Arg, arolla::Bytes>) {
      Combine(absl::string_view("arolla::Bytes"), absl::string_view(arg));
    } else if constexpr (std::is_same_v<Arg, arolla::expr::ExprQuote>) {
      Combine(StableFingerprint(arg));
    } else if constexpr (std::is_same_v<Arg, std::string> ||
                         std::is_same_v<Arg, absl::string_view>) {
      Combine(arg.size()).CombineRawBytes(arg.data(), arg.size());
    } else if constexpr (std::is_arithmetic_v<Arg> || std::is_enum_v<Arg>) {
      auto type_name = absl::string_view(typeid(Arg).name());
      CombineRawBytes(type_name.data(), type_name.size())
          .CombineRawBytes(&arg, sizeof(arg));
    } else {
      static_assert(sizeof(Arg) == 0,
                    "Unsupported type Arg in databag::StableFingerprintHasher");
    }
  };
  (combine(args), ...);
  return *this;
}

template <typename... Args>
StableFingerprintHasher&& StableFingerprintHasher::Combine(
    const Args&... args) && {
  Combine(args...);
  return std::move(*this);
}

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_STABLE_FINGERPRINT_H_
