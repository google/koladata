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
#include "koladata/functor/parallel/testing_stream_prime.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/iterables/iterable_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

absl::StatusOr<bool> IsPrime(int64_t n) {
  if (n < 0) {
    return absl::InvalidArgumentError("n must be positive");
  }
  for (int64_t i = 2; i < n; ++i) {
    if (n % i == 0) {
      return false;
    }
  }
  return true;
}

// TODO: Create a helper to read int32 or int64 from DataSlice.
absl::StatusOr<int64_t> ReadMaxValue(const DataSlice& value_slice) {
  if (value_slice.GetShape().rank() != 0) {
    return absl::InvalidArgumentError("max_value must be a single value");
  }
  const auto& value = value_slice.item();
  int64_t result;
  if (value.dtype() == arolla::GetQType<int64_t>()) {
    result = value.value<int64_t>();
  } else if (value.dtype() == arolla::GetQType<int32_t>()) {
    result = value.value<int32_t>();
  } else {
    return absl::InvalidArgumentError("max_value must be a INT64 or INT32");
  }
  if (result < 2) {
    return absl::InvalidArgumentError("max_value must be at least 2");
  }
  return result;
}

void ProducePrimes(int64_t max_value, StreamWriterPtr stream_writer) {
  for (int64_t i = max_value; i >= 2; --i) {
    absl::StatusOr<bool> is_prime = IsPrime(i);
    if (!is_prime.ok()) {
      std::move(*stream_writer).Close(is_prime.status());
      return;
    }
    if (*is_prime) {
      stream_writer->Write(
          arolla::TypedRef::FromValue(DataSlice::CreateFromScalar(i)));
    }
  }
  std::move(*stream_writer).Close();
}

absl::StatusOr<StreamPtr> FindPrimes(const ExecutorPtr& executor,
                                     const DataSlice& max_value_slice) {
  ASSIGN_OR_RETURN(int64_t max_value, ReadMaxValue(max_value_slice));
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  // TODO: Forward cancellation context.
  executor->Schedule([max_value, writer = std::move(writer)]() mutable {
    ProducePrimes(max_value, std::move(writer));
  });

  return std::move(stream);
}

absl::StatusOr<arolla::Sequence> ProducePrimesEager(int64_t max_value) {
  std::vector<int64_t> primes;
  for (int64_t i = max_value; i >= 2; --i) {
    ASSIGN_OR_RETURN(bool is_prime, IsPrime(i));
    if (is_prime) {
      primes.push_back(i);
    }
  }
  ASSIGN_OR_RETURN(auto seq, arolla::MutableSequence::Make(
                                 arolla::GetQType<DataSlice>(), primes.size()));
  for (size_t i = 0; i < primes.size(); ++i) {
    seq.UnsafeSetRef(
        i, arolla::TypedRef::FromValue(DataSlice::CreateFromScalar(primes[i])));
  }
  return std::move(seq).Finish();
}

absl::StatusOr<arolla::Sequence> FindPrimesEager(
    const DataSlice& max_value_slice) {
  ASSIGN_OR_RETURN(int64_t max_value, ReadMaxValue(max_value_slice));
  return ProducePrimesEager(max_value);
}

class StreamMakePrimeOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         input_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          ASSIGN_OR_RETURN(auto stream, FindPrimes(frame.Get(executor_slot),
                                                   frame.Get(input_slot)));
          frame.Set(output_slot, std::move(stream));
          return absl::OkStatus();
        });
  }
};

class IterableMakePrimeOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [input_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         output_slot = output_slot.UnsafeToSlot<arolla::Sequence>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          ASSIGN_OR_RETURN(auto seq, FindPrimesEager(frame.Get(input_slot)));
          frame.Set(output_slot, std::move(seq));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
TestingStreamMakePrimeOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("the first argument must be an executor");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "the second argument must be a DataSlice");
  }
  auto stream_qtype = GetStreamQType(arolla::GetQType<DataSlice>());
  if (output_type != stream_qtype) {
    return absl::InvalidArgumentError("output type must be a DataSlice stream");
  }
  return std::make_shared<StreamMakePrimeOp>(input_types, output_type);
}

absl::StatusOr<arolla::OperatorPtr>
TestingIterableMakePrimeOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "the second argument must be a DataSlice");
  }
  auto iter_qtype = iterables::GetIterableQType(arolla::GetQType<DataSlice>());
  if (output_type != iter_qtype) {
    return absl::InvalidArgumentError(
        absl::StrCat("output type must be a DataSlice iterable, found: ",
                     output_type->name()));
  }
  return std::make_shared<IterableMakePrimeOp>(input_types, output_type);
}

}  // namespace koladata::functor::parallel
