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
#include "koladata/functor/parallel/stream_qtype.h"

#include <memory>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "koladata/functor/parallel/stream.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/meta.h"

namespace koladata::functor::parallel {
namespace {

using ::arolla::QTypePtr;
using ::arolla::SimpleQType;
using ::arolla::TypedValue;

class StreamQType final : public SimpleQType {
 public:
  explicit StreamQType(QTypePtr value_qtype)
      : SimpleQType(arolla::meta::type<StreamPtr>(),
                    absl::StrCat("STREAM[", value_qtype->name(), "]"),
                    value_qtype, "::koladata::functor::parallel::StreamQType") {
  }
};

class StreamQTypeRegistry {
 public:
  QTypePtr GetStreamQType(QTypePtr value_qtype) {
    absl::MutexLock lock(&lock_);
    auto& result = registry_[value_qtype];
    if (!result) {
      result = std::make_unique<StreamQType>(value_qtype);
    }
    return result.get();
  }

 private:
  absl::Mutex lock_;
  absl::flat_hash_map<QTypePtr, std::unique_ptr<StreamQType>> registry_
      ABSL_GUARDED_BY(lock_);
};

}  // namespace

bool IsStreamQType(QTypePtr qtype) {
  return fast_dynamic_downcast_final<const StreamQType*>(qtype) != nullptr;
}

QTypePtr GetStreamQType(QTypePtr value_qtype) {
  static absl::NoDestructor<StreamQTypeRegistry> registry;
  return registry->GetStreamQType(value_qtype);
}

TypedValue MakeStreamQValue(StreamPtr stream) {
  DCHECK_NE(stream, nullptr);
  auto result = TypedValue::FromValueWithQType(
      std::move(stream), GetStreamQType(stream->value_qtype()));
  DCHECK_OK(result.status());
  return *std::move(result);
}

}  // namespace koladata::functor::parallel
