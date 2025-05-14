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
#include "koladata/functor/parallel/future_qtype.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/meta.h"
#include "koladata/functor/parallel/future.h"

namespace koladata::functor::parallel {

namespace {
class FutureQType final : public arolla::SimpleQType {
 public:
  explicit FutureQType(arolla::QTypePtr value_qtype)
      : SimpleQType(arolla::meta::type<FuturePtr>(),
                    "FUTURE[" + std::string(value_qtype->name()) + "]",
                    value_qtype) {}
};

class FutureQTypeRegistry {
 public:
  arolla::QTypePtr GetFutureQType(arolla::QTypePtr value_qtype) {
    absl::WriterMutexLock l(&lock_);
    auto& result = registry_[value_qtype];
    if (!result) {
      result = std::make_unique<FutureQType>(value_qtype);
    }
    return result.get();
  }

 private:
  absl::Mutex lock_;
  absl::flat_hash_map<arolla::QTypePtr, std::unique_ptr<FutureQType>> registry_
      ABSL_GUARDED_BY(lock_);
};

}  // namespace

bool IsFutureQType(arolla::QTypePtr qtype) {
  return fast_dynamic_downcast_final<const FutureQType*>(qtype) != nullptr;
}

arolla::QTypePtr GetFutureQType(arolla::QTypePtr value_qtype) {
  static absl::NoDestructor<FutureQTypeRegistry> registry;
  return registry->GetFutureQType(value_qtype);
}

arolla::TypedValue MakeFutureQValue(FuturePtr future) {
  DCHECK_NE(future, nullptr);
  auto result = arolla::TypedValue::FromValueWithQType(
      std::move(future), GetFutureQType(future->value_qtype()));
  DCHECK_OK(result.status());
  return *std::move(result);
}

}  // namespace koladata::functor::parallel
