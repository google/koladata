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
#include "koladata/iterables/iterable_qtype.h"

#include <memory>
#include <string>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/fast_dynamic_downcast_final.h"

namespace koladata::iterables {
namespace {

class IterableQType final : public arolla::BasicDerivedQType {
 public:
  explicit IterableQType(arolla::QTypePtr value_qtype)
      : BasicDerivedQType(ConstructorArgs{
            .name = "ITERABLE[" + std::string(value_qtype->name()) + "]",
            .base_qtype = arolla::GetSequenceQType(value_qtype),
            .value_qtype = value_qtype,
        }) {}
};

class IterableQTypeRegistry {
 public:
  arolla::QTypePtr GetIterableQType(arolla::QTypePtr value_qtype) {
    absl::WriterMutexLock l(&lock_);
    auto& result = registry_[value_qtype];
    if (!result) {
      result = std::make_unique<IterableQType>(value_qtype);
    }
    return result.get();
  }

 private:
  absl::Mutex lock_;
  absl::flat_hash_map<arolla::QTypePtr, std::unique_ptr<IterableQType>>
      registry_ ABSL_GUARDED_BY(lock_);
};

}  // namespace

bool IsIterableQType(const arolla::QType* qtype) {
  return fast_dynamic_downcast_final<const IterableQType*>(qtype) != nullptr;
}

arolla::QTypePtr GetIterableQType(arolla::QTypePtr value_qtype) {
  static absl::NoDestructor<IterableQTypeRegistry> registry;
  return registry->GetIterableQType(value_qtype);
}

}  // namespace koladata::iterables
