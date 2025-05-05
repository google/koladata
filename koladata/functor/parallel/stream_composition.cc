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
#include "koladata/functor/parallel/stream_composition.h"

#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

class StreamInterleave::Scheduler {
 public:
  explicit Scheduler(StreamWriterPtr /*absl_nonnull*/ writer)
      : writer_(std::move(writer)) {}

  ~Scheduler() { writer_->TryClose(absl::OkStatus()); }

  static void ProcessInput(std::shared_ptr<Scheduler> self,
                           StreamReaderPtr reader) {
    auto try_read_result = reader->TryRead();
    while (auto* item = try_read_result.item()) {
      if (!self->writer_->TryWrite(*item)) {
        return;  // Closed or orphaned.
      }
      try_read_result = reader->TryRead();
    }
    if (auto* status = try_read_result.close_status()) {
      if (!status->ok()) {
        self->writer_->TryClose(std::move(*status));
      }
      return;
    }
    // Note: Consider adding a subscription method that automatically passes
    // the reader as the first argument to the callback.
    auto* reader_ptr = reader.get();
    reader_ptr->SubscribeOnce(
        [self = std::move(self), reader = std::move(reader)]() mutable {
          ProcessInput(std::move(self), std::move(reader));
        });
  }

 private:
  const StreamWriterPtr /*absl_nonnull*/ writer_;
};

StreamInterleave::StreamInterleave(StreamWriterPtr /*absl_nonnull*/ writer)
    : scheduler_(std::make_shared<Scheduler>(std::move(writer))) {}

void StreamInterleave::Add(const StreamPtr /*absl_nonnull*/& stream) {
  DCHECK(stream != nullptr);
  DCHECK(scheduler_ != nullptr);
  if (stream != nullptr && scheduler_ != nullptr) {
    Scheduler::ProcessInput(scheduler_, stream->MakeReader());
  }
}

}  // namespace koladata::functor::parallel
