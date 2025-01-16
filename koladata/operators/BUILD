# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Arolla Operators on Koda abstractions.
#
# Note: tests for code here are located under
# py/koladata/operators/tests.
#
# Operators that are available in public namespace, should be stored in `kde`
# namespace, while internal operators used to implement various functionality
# should be under `koda_internal` namespace
# (e.g. `koda_internal.to_arolla_boolean`).

load(
    "@com_google_arolla//arolla/codegen/qexpr:register_operator.bzl",
    "make_optional_type",
    "operator_libraries",
    "operator_overload_list",
)
load(
    "@com_google_arolla//arolla/qexpr/operators/dense_array:lifter.bzl",
    "make_dense_array_type",
)

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

operator_list = [
    # go/keep-sorted start
    ":operator_to_data_slice",
    # go/keep-sorted end
]

cc_library(
    name = "operators",
    srcs = ["operators.cc"],
    deps = operator_list + [
        ":json",
        ":lib",
        "//koladata:arolla_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal/op_utils:error",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/util",
    ],
    alwayslink = 1,
)

cc_library(
    name = "lib",
    srcs = [
        "arolla_bridge.cc",
        "bags.cc",
        "comparison.cc",
        "core.cc",
        "dicts.cc",
        "entities.cc",
        "ids.cc",
        "lists.cc",
        "math.cc",
        "non_deterministic_op.cc",
        "objs.cc",
        "predicates.cc",
        "schema.cc",
        "shapes.cc",
        "slices.cc",
        "strings.cc",
        "utils.cc",
    ],
    hdrs = [
        "allocation.h",
        "arolla_bridge.h",
        "assertion.h",
        "bags.h",
        "comparison.h",
        "core.h",
        "dicts.h",
        "entities.h",
        "ids.h",
        "lists.h",
        "masking.h",
        "math.h",
        "non_deterministic_op.h",
        "objs.h",
        "predicates.h",
        "schema.h",
        "shapes.h",
        "slices.h",
        "strings.h",
        "utils.h",
    ],
    deps = [
        "//koladata:alloc_utils",
        "//koladata:arolla_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata:pointwise_utils",
        "//koladata:uuid_utils",
        "//koladata/internal:casting",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:ellipsis",
        "//koladata/internal:error_cc_proto",
        "//koladata/internal:error_utils",
        "//koladata/internal:non_deterministic_token",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal/op_utils:agg_common_schema",
        "//koladata/internal/op_utils:agg_uuid",
        "//koladata/internal/op_utils:at",
        "//koladata/internal/op_utils:collapse",
        "//koladata/internal/op_utils:deep_clone",
        "//koladata/internal/op_utils:deep_uuid",
        "//koladata/internal/op_utils:equal",
        "//koladata/internal/op_utils:error",
        "//koladata/internal/op_utils:extract",
        "//koladata/internal/op_utils:has",
        "//koladata/internal/op_utils:inverse_select",
        "//koladata/internal/op_utils:itemid",
        "//koladata/internal/op_utils:new_ids_like",
        "//koladata/internal/op_utils:presence_and",
        "//koladata/internal/op_utils:presence_or",
        "//koladata/internal/op_utils:reverse",
        "//koladata/internal/op_utils:select",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/functional:overload",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/jagged_shape/dense_array/util",
        "@com_google_arolla//arolla/jagged_shape/util:concat",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/standard_type_properties",
        "@com_google_arolla//arolla/serving",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "json",
    srcs = ["json.cc"],
    hdrs = ["json.h"],
    deps = [
        ":lib",
        "//koladata:data_slice",
        "//koladata/internal:casting",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "@com_github_nlohmann_json//:json",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "bags",
    srcs = ["bags.cc"],
    hdrs = ["bags.h"],
    deps = [
        ":lib",
        "//koladata:data_bag",
        "//koladata:data_slice_qtype",
        "//koladata/internal:non_deterministic_token",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/jagged_shape/dense_array/util",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
    ],
)

to_data_slice_types = [
    "int32_t",
    "int64_t",
    "float",
    "double",
    "bool",
    "::arolla::Unit",
    "::arolla::Text",
    "::arolla::Bytes",
]

operator_libraries(
    name = "operator_to_data_slice",
    operator_name = "koda_internal._to_data_slice",
    overloads = operator_overload_list(
        hdrs = ["arolla_bridge.h"],
        arg_lists = [[t] for t in to_data_slice_types] +
                    [[make_optional_type(t)] for t in to_data_slice_types] +
                    [[make_dense_array_type(t)] for t in to_data_slice_types],
        op_class = "::koladata::ops::ToDataSliceOp",
        deps = [":lib"],
    ),
)

cc_test(
    name = "arolla_bridge_test",
    srcs = ["arolla_bridge_test.cc"],
    deps = [
        ":lib",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:error_cc_proto",
        "//koladata/internal:error_utils",
        "//koladata/internal:object_id",
        "//koladata/internal/testing:matchers",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)
