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
#ifndef KOLADATA_FUNCTOR_PARALLEL_CONTEXT_GUARD_H_
#define KOLADATA_FUNCTOR_PARALLEL_CONTEXT_GUARD_H_

#include <cstddef>
#include <memory>

#include "absl/log/check.h"

namespace koladata::functor::parallel {

// Encapsulation of an abstract RAII-based scope guard.
class ContextGuard final {
 public:
  ContextGuard() = default;

  ~ContextGuard() {
    if (destructor_fn_ != nullptr) {
      destructor_fn_(buffer_);
    }
  }

  template <typename T, typename... Args>
  void init(Args&&... args) {
    DCHECK(destructor_fn_ == nullptr);
    if constexpr (sizeof(T) <= sizeof(buffer_)) {
      new (buffer_) T(std::forward<Args>(args)...);
      destructor_fn_ = [](void* buffer) noexcept {
        static_cast<T*>(buffer)->~T();
      };
    } else {
      static_assert(sizeof(std::unique_ptr<T>) <= sizeof(buffer_));
      init<std::unique_ptr<T>>(new T(std::forward<Args>(args)...));
    }
  }

  ContextGuard(const ContextGuard&) = delete;
  ContextGuard operator=(const ContextGuard&) = delete;

 private:
  using DestructorFn = void (*)(void*) noexcept;
  static constexpr int kBufferSize = 3 * sizeof(void*);

  DestructorFn destructor_fn_ = nullptr;
  alignas(std::max_align_t) char buffer_[kBufferSize];
};

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_CONTEXT_GUARD__H_
