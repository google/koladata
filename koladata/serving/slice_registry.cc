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
#include "koladata/serving/slice_registry.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "koladata/data_slice.h"

namespace koladata::serving {
namespace {

struct SliceRegistry {
  absl::flat_hash_map<std::string, std::unique_ptr<DataSlice>> slices
      ABSL_GUARDED_BY(m);
  absl::Mutex m;

  static SliceRegistry& get() {
    static absl::NoDestructor<SliceRegistry> r;
    return *r;
  }
};

}  // namespace

const DataSlice* /*absl_nullable*/ GetRegisteredSlice(absl::string_view key) {
  SliceRegistry& r = SliceRegistry::get();
  absl::MutexLock lock(&r.m);
  auto it = r.slices.find(key);
  if (it == r.slices.end()) {
    return nullptr;
  } else {
    return it->second.get();
  }
}

absl::Status RegisterSlice(absl::string_view key, DataSlice slice) {
  SliceRegistry& r = SliceRegistry::get();
  absl::MutexLock lock(&r.m);

  auto [_, emplaced] =
      r.slices.try_emplace(key, std::make_unique<DataSlice>(std::move(slice)));
  if (!emplaced) {
    return absl::AlreadyExistsError(
        absl::StrCat("slice ", key, " is already registered"));
  }
  return absl::OkStatus();
}

}  // namespace koladata::serving
