# Copyright 2025 Google LLC
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

# Utilities that are used in Koda operators.

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "equal",
    srcs = ["equal.cc"],
    hdrs = ["equal.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "equal_test",
    srcs = ["equal_test.cc"],
    deps = [
        ":equal",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "equal_benchmarks",
    srcs = ["equal_benchmarks.cc"],
    deps = [
        ":benchmark_util",
        ":equal",
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/bool:lib",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "group_by_utils",
    hdrs = ["group_by_utils.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/aggregation:lib",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "group_by_utils_test",
    srcs = ["group_by_utils_test.cc"],
    deps = [
        ":group_by_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "extract",
    srcs = ["extract.cc"],
    hdrs = ["extract.h"],
    deps = [
        ":equal",
        ":group_by_utils",
        ":has",
        ":presence_and",
        ":presence_or",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:types_buffer",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//arolla/util:traceme",
    ],
)

cc_test(
    name = "extract_test",
    srcs = ["extract_test.cc"],
    deps = [
        ":extract",
        ":new_ids_like",
        "//koladata:test_utils",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:schema_utils",
        "//koladata/internal:uuid_object",
        "//koladata/internal/testing:deep_op_utils",
        "//koladata/internal/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "object_finder",
    srcs = [],
    hdrs = ["object_finder.h"],
    deps = [
        ":traverse_helper",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "object_finder_test",
    srcs = ["object_finder_test.cc"],
    deps = [
        ":object_finder",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/testing:deep_op_utils",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "deep_comparator",
    srcs = [],
    hdrs = ["deep_comparator.h"],
    deps = [
        ":traverse_helper",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "deep_comparator_test",
    srcs = ["deep_comparator_test.cc"],
    deps = [
        ":deep_clone",
        ":deep_comparator",
        ":traverse_helper",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/testing:deep_op_utils",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "deep_diff",
    srcs = ["deep_diff.cc"],
    hdrs = ["deep_diff.h"],
    deps = [
        ":traverse_helper",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "deep_diff_test",
    srcs = ["deep_diff_test.cc"],
    deps = [
        ":deep_diff",
        ":traverse_helper",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/testing:deep_op_utils",
        "//koladata/internal/testing:matchers",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "traverser",
    srcs = [],
    hdrs = ["traverser.h"],
    deps = [
        ":traverse_helper",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "traverser_test",
    srcs = ["traverser_test.cc"],
    deps = [
        ":traverser",
        "//koladata:test_utils",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:uuid_object",
        "//koladata/internal/testing:deep_op_utils",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "deep_clone",
    srcs = ["deep_clone.cc"],
    hdrs = ["deep_clone.h"],
    deps = [
        ":extract",
        ":traverser",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "deep_clone_test",
    srcs = ["deep_clone_test.cc"],
    deps = [
        ":deep_clone",
        "//koladata:test_utils",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:uuid_object",
        "//koladata/internal/testing:deep_op_utils",
        "//koladata/internal/testing:matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "deep_uuid",
    srcs = ["deep_uuid.cc"],
    hdrs = ["deep_uuid.h"],
    deps = [
        ":traverser",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "deep_uuid_test",
    srcs = ["deep_uuid_test.cc"],
    deps = [
        ":deep_uuid",
        "//koladata:test_utils",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:uuid_object",
        "//koladata/internal/testing:deep_op_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "deep_equivalent",
    srcs = ["deep_equivalent.cc"],
    hdrs = ["deep_equivalent.h"],
    deps = [
        ":deep_comparator",
        ":deep_diff",
        ":object_finder",
        ":traverse_helper",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "deep_equivalent_test",
    srcs = ["deep_equivalent_test.cc"],
    deps = [
        ":deep_clone",
        ":deep_equivalent",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/testing:deep_op_utils",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "expand",
    hdrs = ["expand.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "//koladata/internal:types",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "expand_test",
    srcs = ["expand_test.cc"],
    deps = [
        ":expand",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "presence_and",
    hdrs = ["presence_and.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "presence_and_test",
    srcs = ["presence_and_test.cc"],
    deps = [
        ":presence_and",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "select",
    srcs = ["select.cc"],
    hdrs = ["select.h"],
    deps = [
        ":error",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/aggregation:lib",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "select_test",
    srcs = ["select_test.cc"],
    deps = [
        ":select",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "presence_or",
    hdrs = ["presence_or.h"],
    deps = [
        ":has",
        ":presence_and",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "presence_or_test",
    srcs = ["presence_or_test.cc"],
    deps = [
        ":presence_or",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "has",
    srcs = ["has.cc"],
    hdrs = ["has.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "has_test",
    srcs = ["has_test.cc"],
    deps = [
        ":has",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "coalesce_with_filtered",
    hdrs = ["coalesce_with_filtered.h"],
    deps = [
        ":has",
        ":presence_and",
        ":presence_or",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "coalesce_with_filtered_test",
    srcs = ["coalesce_with_filtered_test.cc"],
    deps = [
        ":coalesce_with_filtered",
        "//koladata/internal:data_slice",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "benchmark_util",
    testonly = 1,
    hdrs = ["benchmark_util.h"],
    deps = [
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_arolla//arolla/dense_array/testing",
    ],
)

cc_test(
    name = "extract_benchmarks",
    srcs = ["extract_benchmarks.cc"],
    deps = [
        ":extract",
        ":presence_and",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "presence_or_benchmarks",
    srcs = ["presence_or_benchmarks.cc"],
    deps = [
        ":benchmark_util",
        ":presence_or",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "presence_and_benchmarks",
    srcs = ["presence_and_benchmarks.cc"],
    deps = [
        ":benchmark_util",
        ":presence_and",
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/random",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "collapse",
    hdrs = ["collapse.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr/operators/aggregation:lib",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "new_ids_like",
    srcs = ["new_ids_like.cc"],
    hdrs = ["new_ids_like.h"],
    deps = [
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_arolla//arolla/memory",
    ],
)

cc_test(
    name = "new_ids_like_test",
    srcs = ["new_ids_like_test.cc"],
    deps = [
        ":new_ids_like",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "at",
    srcs = ["at.cc"],
    hdrs = ["at.h"],
    deps = [
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "inverse_select",
    hdrs = ["inverse_select.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/aggregation:lib",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "reverse",
    srcs = ["reverse.cc"],
    hdrs = ["reverse.h"],
    deps = [
        "//koladata/internal:data_slice",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "reverse_test",
    srcs = ["reverse_test.cc"],
    deps = [
        ":reverse",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "collapse_test",
    srcs = ["collapse_test.cc"],
    deps = [
        ":collapse",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "at_test",
    srcs = ["at_test.cc"],
    deps = [
        ":at",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "base62",
    srcs = ["base62.cc"],
    hdrs = ["base62.h"],
    deps = [
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "base62_benchmark",
    srcs = ["base62_benchmark.cc"],
    deps = [
        ":base62",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "base62_test",
    srcs = ["base62_test.cc"],
    deps = [
        ":base62",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "itemid",
    srcs = ["itemid.cc"],
    hdrs = ["itemid.h"],
    deps = [
        ":base62",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "itemid_test",
    srcs = ["itemid_test.cc"],
    deps = [
        ":base62",
        ":itemid",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "inverse_select_test",
    srcs = ["inverse_select_test.cc"],
    deps = [
        ":inverse_select",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "agg_uuid",
    srcs = ["agg_uuid.cc"],
    hdrs = ["agg_uuid.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "//koladata/internal:stable_fingerprint",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "agg_common_schema",
    srcs = ["agg_common_schema.cc"],
    hdrs = ["agg_common_schema.h"],
    deps = [
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:schema_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "agg_common_schema_test",
    srcs = ["agg_common_schema_test.cc"],
    deps = [
        ":agg_common_schema",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "trampoline_executor",
    srcs = ["trampoline_executor.cc"],
    hdrs = ["trampoline_executor.h"],
    deps = [
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "trampoline_executor_test",
    srcs = ["trampoline_executor_test.cc"],
    deps = [
        ":trampoline_executor",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "error",
    srcs = ["error.cc"],
    hdrs = ["error.h"],
    deps = [
        "//koladata/internal:error_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "error_test",
    srcs = ["error_test.cc"],
    deps = [
        ":error",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "qexpr",
    hdrs = ["qexpr.h"],
    deps = [
        ":error",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:traceme",
    ],
)

cc_test(
    name = "qexpr_test",
    srcs = ["qexpr_test.cc"],
    deps = [
        ":qexpr",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "traverse_helper",
    hdrs = ["traverse_helper.h"],
    deps = [
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "traverse_helper_test",
    srcs = ["traverse_helper_test.cc"],
    deps = [
        ":traverse_helper",
        "//koladata:test_utils",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/testing:deep_op_utils",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "print",
    srcs = ["print.cc"],
    hdrs = ["print.h"],
    deps = [
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "print_test",
    srcs = ["print_test.cc"],
    deps = [
        ":print",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/cleanup",
        "@com_google_googletest//:gtest_main",
    ],
)
