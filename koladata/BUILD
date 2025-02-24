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

# Koda, a library for advanced data manipulation.

# Implementation of Koda infrastructure and C++ API. Libraries in this
# module should not depend on Python C library.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

exports_files(["LICENSE"])

package_group(
    name = "internal",
    packages = [
        # TODO: b/379873449 - consider placing all internal code in a `_src` subdirectory and only
        # including that in the internal package (i.e. allow-list instead of block-list).
        "//koladata/...",
        "//py/koladata/...",
        "-//py/koladata/ext/...",
    ],
)

cc_test(
    name = "repr_utils_test",
    srcs = ["repr_utils_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":object_factories",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:error_cc_proto",
        "//koladata/internal:error_utils",
        "//koladata/internal:object_id",
        "//koladata/s11n",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "data_bag",
    srcs = [
        "data_bag.cc",
        "data_bag_repr.cc",
    ],
    hdrs = [
        "data_bag.h",
        "data_bag_repr.h",
    ],
    deps = [
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:error_utils",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal:triples",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "data_bag_test",
    srcs = ["data_bag_test.cc"],
    deps = [
        ":data_bag",
        ":object_factories",
        ":test_utils",
        "//koladata/internal:data_bag",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_bag_repr_test",
    srcs = ["data_bag_repr_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":object_factories",
        ":test_utils",
        ":uuid_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:schema_utils",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_slice_op_test",
    srcs = ["data_slice_op_test.cc"],
    deps = [
        ":data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "data_slice",
    srcs = [
        "adoption_utils.cc",
        "casting.cc",
        "data_slice.cc",
        "data_slice_repr.cc",
        "extract_utils.cc",
        "repr_utils.cc",
        "schema_utils.cc",
        "shape_utils.cc",
    ],
    hdrs = [
        "adoption_utils.h",
        "casting.h",
        "data_slice.h",
        "data_slice_op.h",
        "data_slice_repr.h",
        "extract_utils.h",
        "repr_utils.h",
        "schema_utils.h",
        "shape_utils.h",
    ],
    deps = [
        ":data_bag",
        "//koladata/internal:casting",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:error_cc_proto",
        "//koladata/internal:error_utils",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal/op_utils:expand",
        "//koladata/internal/op_utils:extract",
        "//koladata/internal/op_utils:group_by_utils",
        "//koladata/internal/op_utils:has",
        "//koladata/internal/op_utils:presence_and",
        "//koladata/internal/op_utils:presence_or",
        "//koladata/s11n:codec_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:overload",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "data_slice_qtype",
    srcs = ["data_slice_qtype.cc"],
    hdrs = ["data_slice_qtype.h"],
    deps = [
        ":data_bag",
        ":data_slice",
        "//koladata/internal:data_item",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "data_slice_test",
    srcs = ["data_slice_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":data_slice_qtype",
        ":object_factories",
        ":test_utils",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:error_cc_proto",
        "//koladata/internal:error_utils",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal/testing:matchers",
        "//koladata/s11n",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/jagged_shape/testing",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_slice_repr_test",
    srcs = ["data_slice_repr_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":object_factories",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":object_factories",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal/op_utils:base62",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "adoption_utils_test",
    srcs = ["adoption_utils_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":object_factories",
        ":test_utils",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "arolla_utils",
    srcs = ["arolla_utils.cc"],
    hdrs = ["arolla_utils.h"],
    deps = [
        ":data_slice",
        ":data_slice_qtype",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/array",
        "@com_google_arolla//arolla/array/qtype",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "arolla_utils_test",
    srcs = ["arolla_utils_test.cc"],
    deps = [
        ":arolla_utils",
        ":data_slice",
        ":data_slice_qtype",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/array",
        "@com_google_arolla//arolla/array/qtype",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "schema_constants",
    srcs = ["schema_constants.cc"],
    hdrs = ["schema_constants.h"],
    deps = [
        ":data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "schema_constants_test",
    srcs = ["schema_constants_test.cc"],
    deps = [
        ":schema_constants",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "alloc_utils",
    hdrs = ["alloc_utils.h"],
    deps = [
        ":data_bag",
        ":data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/functional:overload",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
    ],
)

cc_test(
    name = "alloc_utils_test",
    srcs = ["alloc_utils_test.cc"],
    deps = [
        ":alloc_utils",
        ":data_bag",
        ":data_slice",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "uuid_utils",
    srcs = ["uuid_utils.cc"],
    hdrs = ["uuid_utils.h"],
    deps = [
        ":data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "uuid_utils_test",
    srcs = ["uuid_utils_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":test_utils",
        ":uuid_utils",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:stable_fingerprint",
        "//koladata/internal:uuid_object",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "object_factories",
    srcs = ["object_factories.cc"],
    hdrs = ["object_factories.h"],
    deps = [
        ":alloc_utils",
        ":data_bag",
        ":data_slice",
        ":data_slice_qtype",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:error_cc_proto",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal:uuid_object",
        "//koladata/internal/op_utils:error",
        "//koladata/internal/op_utils:has",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "object_factories_test",
    srcs = ["object_factories_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":object_factories",
        ":test_utils",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal/testing:matchers",
        "//koladata/s11n",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/jagged_shape/testing",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "pointwise_utils",
    hdrs = ["pointwise_utils.h"],
    deps = [
        ":data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:missing_value",
        "@com_google_absl//absl/functional:overload",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "pointwise_utils_test",
    srcs = ["pointwise_utils_test.cc"],
    deps = [
        ":pointwise_utils",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "test_utils",
    testonly = 1,
    hdrs = ["test_utils.h"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_library(
    name = "data_bag_comparison",
    hdrs = ["data_bag_comparison.h"],
    deps = [
        ":data_bag",
        "//koladata/internal:triples",
    ],
)

cc_library(
    name = "subslice_utils",
    srcs = ["subslice_utils.cc"],
    hdrs = ["subslice_utils.h"],
    deps = [
        ":arolla_utils",
        ":data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:error_utils",
        "//koladata/internal/op_utils:at",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/dense_array:lib",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "subslice_utils_test",
    srcs = ["subslice_utils_test.cc"],
    deps = [
        ":data_slice",
        ":subslice_utils",
        ":test_utils",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "test_utils_test",
    srcs = ["test_utils_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/testing:matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_bag_comparison_test",
    srcs = ["data_bag_comparison_test.cc"],
    deps = [
        ":data_bag",
        ":data_bag_comparison",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_slice",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "casting_test",
    srcs = ["casting_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal/testing:matchers",
        "//koladata/s11n",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "shape_utils_test",
    srcs = ["shape_utils_test.cc"],
    deps = [
        ":data_slice",
        "//koladata/internal:data_slice",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "schema_utils_test",
    srcs = ["schema_utils_test.cc"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal/testing:matchers",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)
