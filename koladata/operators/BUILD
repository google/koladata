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
# //py/koladata/operator_tests.
#
# Operators that are available in public namespace, should be stored in `kde`
# namespace, while internal operators used to implement various functionality
# should be under `koda_internal` namespace
# (e.g. `koda_internal.convert_and_eval`).

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
        ":lib",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "@com_google_arolla//arolla/qexpr",
    ],
    alwayslink = 1,
)

cc_library(
    name = "lib",
    srcs = [
        "align.cc",
        "comparison.cc",
        "convert_and_eval.cc",
        "core.cc",
        "explode.cc",
        "get_attr.cc",
        "group_by.cc",
        "jagged_shape.cc",
        "math.cc",
        "reverse.cc",
        "schema.cc",
        "slice.cc",
        "utils.cc",
        "uu_schema.cc",
        "uuid.cc",
        "uuobj.cc",
    ],
    hdrs = [
        "align.h",
        "allocation.h",
        "assertion.h",
        "clone.h",
        "collapse.h",
        "comparison.h",
        "convert_and_eval.h",
        "core.h",
        "dict.h",
        "equal.h",
        "explode.h",
        "extract.h",
        "get_attr.h",
        "group_by.h",
        "itemid.h",
        "jagged_shape.h",
        "list.h",
        "logical.h",
        "math.h",
        "nofollow.h",
        "reverse.h",
        "schema.h",
        "select.h",
        "shallow_clone.h",
        "slice.h",
        "translate.h",
        "utils.h",
        "uu_schema.h",
        "uuid.h",
        "uuobj.h",
    ],
    deps = [
        "//koladata:arolla_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:ellipsis",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal/op_utils:at",
        "//koladata/internal/op_utils:collapse",
        "//koladata/internal/op_utils:equal",
        "//koladata/internal/op_utils:extract",
        "//koladata/internal/op_utils:has",
        "//koladata/internal/op_utils:itemid",
        "//koladata/internal/op_utils:presence_and",
        "//koladata/internal/op_utils:presence_or",
        "//koladata/internal/op_utils:reverse",
        "//koladata/internal/op_utils:reverse_select",
        "//koladata/internal/op_utils:select",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serving",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "compile_expr_operators",
    srcs = ["compile_expr_operators.cc"],
    deps = [
        "//koladata/expr:expr_operators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/eval",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
    alwayslink = 1,
)

cc_library(
    name = "translate",
    hdrs = ["translate.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
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
        hdrs = ["convert_and_eval.h"],
        arg_lists = [[t] for t in to_data_slice_types] +
                    [[make_optional_type(t)] for t in to_data_slice_types] +
                    [[make_dense_array_type(t)] for t in to_data_slice_types],
        op_class = "::koladata::ops::ToDataSliceOp",
        deps = [":lib"],
    ),
)

cc_test(
    name = "convert_and_eval_test",
    srcs = ["convert_and_eval_test.cc"],
    deps = [
        ":lib",
        "//koladata:data_slice",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)
