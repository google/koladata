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
#ifndef KOLADATA_OPERATORS_JSON_STREAM_H_
#define KOLADATA_OPERATORS_JSON_STREAM_H_

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::ops {

// kd.json_stream._salvage_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr> JsonStreamSalvageStream(
    koladata::functor::parallel::ExecutorPtr absl_nonnull executor,
    koladata::functor::parallel::StreamPtr absl_nonnull input_stream,
    const DataSlice& allow_nan, const DataSlice& ensure_ascii,
    const DataSlice& max_depth);

// kd.json_stream._prettify_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr> JsonStreamPrettifyStream(
    koladata::functor::parallel::ExecutorPtr absl_nonnull executor,
    koladata::functor::parallel::StreamPtr absl_nonnull input_stream,
    const DataSlice& indent_string);

// kd.json_stream._head_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr> JsonStreamHeadStream(
    koladata::functor::parallel::ExecutorPtr absl_nonnull executor,
    koladata::functor::parallel::StreamPtr absl_nonnull input_stream,
    const DataSlice& n);

// kd.json_stream._select_nonempty_objects_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr>
JsonStreamSelectNonemptyObjectsStream(koladata::functor::parallel::ExecutorPtr
                                      absl_nonnull executor,
                                      koladata::functor::parallel::StreamPtr
                                      absl_nonnull input_stream);

// kd.json_stream._select_nonempty_arrays_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr>
JsonStreamSelectNonemptyArraysStream(koladata::functor::parallel::ExecutorPtr
                                     absl_nonnull executor,
                                     koladata::functor::parallel::StreamPtr
                                     absl_nonnull input_stream);

// kd.json_stream._select_nonnull_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr>
JsonStreamSelectNonnullStream(koladata::functor::parallel::ExecutorPtr
                              absl_nonnull executor,
                              koladata::functor::parallel::StreamPtr
                              absl_nonnull input_stream);

// kd.json_stream._get_object_key_value_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr>
JsonStreamGetObjectKeyValueStream(koladata::functor::parallel::ExecutorPtr
                                  absl_nonnull executor,
                                  koladata::functor::parallel::StreamPtr
                                  absl_nonnull input_stream,
                                  const DataSlice& key);

// kd.json_stream._get_object_key_values_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr>
JsonStreamGetObjectKeyValuesStream(koladata::functor::parallel::ExecutorPtr
                                   absl_nonnull executor,
                                   koladata::functor::parallel::StreamPtr
                                   absl_nonnull input_stream,
                                   const DataSlice& key);

// kd.json_stream._implode_array_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr>
JsonStreamImplodeArrayStream(koladata::functor::parallel::ExecutorPtr
                             absl_nonnull executor,
                             koladata::functor::parallel::StreamPtr
                             absl_nonnull input_stream);

// kd.json_stream._explode_array_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr>
JsonStreamExplodeArrayStream(koladata::functor::parallel::ExecutorPtr
                             absl_nonnull executor,
                             koladata::functor::parallel::StreamPtr
                             absl_nonnull input_stream);

// kd.json_stream._get_array_nth_value_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr>
JsonStreamGetArrayNthValueStream(koladata::functor::parallel::ExecutorPtr
                                 absl_nonnull executor,
                                 koladata::functor::parallel::StreamPtr
                                 absl_nonnull input_stream,
                                 const DataSlice& n);

// kd.json_stream._unquote_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr> JsonStreamUnquoteStream(
    koladata::functor::parallel::ExecutorPtr absl_nonnull executor,
    koladata::functor::parallel::StreamPtr absl_nonnull input_stream);

// kd.json_stream._quote_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr> JsonStreamQuoteStream(
    koladata::functor::parallel::ExecutorPtr absl_nonnull executor,
    koladata::functor::parallel::StreamPtr absl_nonnull input_stream);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_JSON_STREAM_H_
