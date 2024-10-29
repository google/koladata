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
#include "koladata/internal/op_utils/trampoline_executor.h"
#include <utility>

#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

void TrampolineExecutor::Enqueue(Callback callback) {
  buffer_.emplace_back(std::move(callback));
}

absl::Status TrampolineExecutor::Run() && {
  FlushBuffer();
  while (!stack_.empty()) {
    RETURN_IF_ERROR(std::move(stack_.back())());
    stack_.pop_back();
    FlushBuffer();
  }
  return absl::OkStatus();
}

void TrampolineExecutor::FlushBuffer() {
  for (auto iter = buffer_.rbegin(); iter != buffer_.rend(); ++iter) {
    stack_.emplace_back(std::move(*iter));
  }
  buffer_.clear();
}

}  // namespace koladata::internal
