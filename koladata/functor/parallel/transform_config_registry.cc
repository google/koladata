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
#include "koladata/functor/parallel/transform_config_registry.h"
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "koladata/functor/parallel/create_transform_config.h"
#include "koladata/functor/parallel/transform_config.h"
#include "koladata/functor/parallel/transform_config.pb.h"
#include "google/protobuf/text_format.h"

namespace koladata::functor::parallel {

namespace {

// The default parallel transform config as textproto.
// You can edit this config directly, or, to extend it dynamically, for example,
// for transformations of non-third-party operators, you can use
// AROLLA_INITIALIZER.
// Example:
// AROLLA_INITIALIZER(
//         .reverse_deps = {arolla::initializer_dep::kOperators},
//         .init_fn = [] {
//           return ExtendDefaultParallelTransformConfig(R"pb(
//             operator_replacements {
//               from_op: "test.op"
//               to_op: "test.op"
//             }
//           )pb");
//         });
// Note: The reverse dependency to kOperators makes it so that a user can depend
// on the transformation registration in another Arolla Initializer by
// depending on kOperators.
constexpr char kDefaultConfigTextProto[] = R"pb(
  operator_replacements { from_op: "core.make_tuple" to_op: "core.make_tuple" }
  operator_replacements { from_op: "kd.tuple" to_op: "kd.tuple" }
  operator_replacements {
    from_op: "core.get_nth"
    to_op: "core.get_nth"
    argument_transformation { keep_literal_argument_indices: 1 }
  }
  operator_replacements {
    from_op: "kd.tuples.get_nth"
    to_op: "kd.tuples.get_nth"
    argument_transformation { keep_literal_argument_indices: 1 }
  }
  operator_replacements {
    from_op: "koda_internal.view.get_item"
    to_op: "koda_internal.parallel._parallel_get_item"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      keep_literal_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "namedtuple.make"
    to_op: "namedtuple.make"
    argument_transformation { keep_literal_argument_indices: 0 }
  }
  operator_replacements { from_op: "kd.namedtuple" to_op: "kd.namedtuple" }
  operator_replacements {
    from_op: "namedtuple.get_field"
    to_op: "namedtuple.get_field"
    argument_transformation { keep_literal_argument_indices: 1 }
  }
  operator_replacements {
    from_op: "kd.tuples.get_namedtuple_field"
    to_op: "kd.tuples.get_namedtuple_field"
    argument_transformation { keep_literal_argument_indices: 1 }
  }
  operator_replacements {
    from_op: "koda_internal.non_deterministic"
    to_op: "koda_internal.non_deterministic"
    argument_transformation { keep_literal_argument_indices: 1 }
  }
  operator_replacements {
    from_op: "kd.functor.call"
    to_op: "koda_internal.parallel._parallel_call"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 0
    }
  }
  operator_replacements {
    from_op: "kd.functor.call_fn_returning_stream_when_parallel"
    to_op: "koda_internal.parallel.parallel_call_fn_returning_stream"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.if_"
    to_op: "koda_internal.parallel._parallel_if"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 1
      functor_argument_indices: 2
    }
  }
  operator_replacements {
    from_op: "kd.functor.switch"
    to_op: "koda_internal.parallel._parallel_switch"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 2
    }
  }
  operator_replacements {
    from_op: "kd.iterables.make"
    to_op: "koda_internal.parallel._parallel_stream_make"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      arguments: NON_DETERMINISTIC_TOKEN
    }
  }
  operator_replacements {
    from_op: "kd.iterables.make_unordered"
    to_op: "koda_internal.parallel._parallel_stream_make_unordered"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.iterables.chain"
    to_op: "koda_internal.parallel._parallel_stream_chain"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      arguments: NON_DETERMINISTIC_TOKEN
    }
  }
  operator_replacements {
    from_op: "kd.iterables.interleave"
    to_op: "koda_internal.parallel._parallel_stream_interleave"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.flat_map_chain"
    to_op: "koda_internal.parallel._parallel_stream_flat_map_chain"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "kd.functor.flat_map_interleaved"
    to_op: "koda_internal.parallel._parallel_stream_flat_map_interleaved"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "kd.functor.reduce"
    to_op: "koda_internal.parallel._parallel_stream_reduce"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 0
    }
  }
  operator_replacements {
    from_op: "kd.iterables.reduce_concat"
    to_op: "koda_internal.parallel._parallel_stream_reduce_concat"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.iterables.reduce_updated_bag"
    to_op: "koda_internal.parallel._parallel_stream_reduce_updated_bag"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.while_"
    to_op: "koda_internal.parallel._parallel_stream_while"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 0
      functor_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "kd.functor.for_"
    to_op: "koda_internal.parallel._parallel_stream_for"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 1
      functor_argument_indices: 2
      functor_argument_indices: 3
    }
  }
  operator_replacements {
    from_op: "kd.functor.map"
    to_op: "koda_internal.parallel._parallel_stream_map"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      functor_argument_indices: 0
    }
  }
  operator_replacements {
    from_op: "kd.assertion.with_assertion"
    to_op: "koda_internal.parallel._parallel_with_assertion"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      arguments: NON_DETERMINISTIC_TOKEN
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.chunk_values"
    to_op: "kd.json_stream._chunk_values_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.explode_array"
    to_op: "kd.json_stream._explode_array_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.filter_json"
    to_op: "kd.json_stream._filter_json_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.get_array_nth_value"
    to_op: "kd.json_stream._get_array_nth_value_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.get_object_key_value"
    to_op: "kd.json_stream._get_object_key_value_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.get_object_key_values"
    to_op: "kd.json_stream._get_object_key_values_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.head"
    to_op: "kd.json_stream._head_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.implode_array"
    to_op: "kd.json_stream._implode_array_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.prettify"
    to_op: "kd.json_stream._prettify_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.quote"
    to_op: "kd.json_stream._quote_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.salvage"
    to_op: "kd.json_stream._salvage_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.select_nonempty_arrays"
    to_op: "kd.json_stream._select_nonempty_arrays_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.select_nonempty_objects"
    to_op: "kd.json_stream._select_nonempty_objects_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.select_nonnull"
    to_op: "kd.json_stream._select_nonnull_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.stream_string_value"
    to_op: "kd.json_stream._stream_string_value_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.json_stream.unquote"
    to_op: "kd.json_stream._unquote_parallel"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
)pb";

class TransformConfigRegistry {
 public:
  TransformConfigRegistry() {
    google::protobuf::TextFormat::ParseFromString(kDefaultConfigTextProto,
                                        &config_proto_);
  }

  absl::StatusOr<ParallelTransformConfigPtr> GetParallelTransformConfig(
      bool allow_runtime_transforms) {
    absl::MutexLock lock(mutex_);
    config_proto_.set_allow_runtime_transforms(allow_runtime_transforms);
    return CreateParallelTransformConfigFromProto(config_proto_);
  }

  absl::Status Extend(absl::string_view text_proto) {
    ParallelTransformConfigProto::OperatorReplacement extension;
    if (!google::protobuf::TextFormat::ParseFromString(text_proto, &extension)) {
      return absl::InvalidArgumentError(
          absl::StrCat("failed to parse text proto: ", text_proto));
    }
    absl::MutexLock lock(mutex_);
    *config_proto_.add_operator_replacements() = std::move(extension);
    return absl::OkStatus();
  }

 private:
  absl::Mutex mutex_;
  ParallelTransformConfigProto config_proto_ ABSL_GUARDED_BY(mutex_);
};

TransformConfigRegistry& GetRegistry() {
  static absl::NoDestructor<TransformConfigRegistry> registry;
  return *registry;
}

}  // namespace

absl::StatusOr<ParallelTransformConfigPtr> GetDefaultParallelTransformConfig(
  bool allow_runtime_transforms
) {
  return GetRegistry().GetParallelTransformConfig(allow_runtime_transforms);
}


absl::Status ExtendDefaultParallelTransformConfig(
    absl::string_view text_proto) {
  return GetRegistry().Extend(text_proto);
}

}  // namespace koladata::functor::parallel
