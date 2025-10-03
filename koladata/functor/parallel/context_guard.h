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


namespace koladata::functor::parallel {

// Encapsulation of an abstract RAII-based scope guard.
class ContextGuard {
 public:
  static constexpr size_t kBufferSize = 128;
  static constexpr size_t kBufferAlign = alignof(std::max_align_t);

  template <typename T, typename... Args>
  void init(Args&&... args) {
    struct ImplT : Impl {
      T t;
      explicit ImplT(Args&&... args) : t(std::forward<Args>(args)...) {}
    };
    static_assert(sizeof(ImplT) <= kBufferSize,
                  "not enough space provided in the ContextGuard buffer.");
    placeholder_.reset(new (&buffer_) ImplT(std::forward<Args>(args)...));
  }

 private:
  struct Impl {
    virtual ~Impl() = default;
  };

  struct Deleter {
    void operator()(Impl* p) {
      p->~Impl();
    }
  };

  alignas(kBufferSize) char buffer_[kBufferSize];
  std::unique_ptr<Impl, Deleter> placeholder_;
};
}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_CONTEXT_GUARD__H_
