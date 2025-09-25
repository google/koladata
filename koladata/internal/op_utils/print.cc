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
#include "koladata/internal/op_utils/print.h"

#include <iostream>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "koladata/data_slice_qtype.h"

namespace koladata::internal {
namespace {

class PrinterImpl final : public Printer {
 public:
  static Printer& Get() {
    static absl::NoDestructor<PrinterImpl> registry;
    return *registry;
  }

  void SetPrintCallback(
      absl::AnyInvocable<void(absl::string_view) const> callback) override {
    absl::MutexLock lock(mutex_);
    print_callback_ = std::move(callback);
  }

  void Print(absl::string_view message) const override {
    absl::MutexLock lock(mutex_);
    print_callback_(message);
  }

 private:
  absl::AnyInvocable<void(absl::string_view) const> print_callback_
      ABSL_GUARDED_BY(mutex_) = [](absl::string_view message) {
        std::cout << message;
        std::cout.flush();
      };
  mutable absl::Mutex mutex_;
};

}  // namespace

Printer& GetSharedPrinter() { return PrinterImpl::Get(); }

}  // namespace koladata::internal
