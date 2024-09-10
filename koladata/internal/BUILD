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

# C++ libraries for the production quality databag implementation

load("@rules_python//python:proto.bzl", "py_proto_library")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

proto_library(
    name = "error_proto",
    srcs = ["error.proto"],
    deps = [
        "@com_google_arolla//arolla/serialization_base:base_proto",
    ],
)

exports_files(["error.proto"])

cc_proto_library(
    name = "error_cc_proto",
    deps = [":error_proto"],
)

py_proto_library(
    name = "error_py_proto",
    deps = [":error_proto"],
)

cc_library(
    name = "error_utils",
    srcs = ["error_utils.cc"],
    hdrs = ["error_utils.h"],
    deps = [
        ":data_item",
        ":error_cc_proto",
        "//koladata/s11n:codec_cc_proto",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serialization",
        "@com_google_arolla//arolla/serialization_base:base_cc_proto",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "error_utils_test",
    srcs = ["error_utils_test.cc"],
    deps = [
        ":data_item",
        ":dtype",
        ":error_cc_proto",
        ":error_utils",
        ":object_id",
        "//koladata/s11n",
        "//koladata/s11n:codec_cc_proto",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "object_id",
    srcs = ["object_id.cc"],
    hdrs = ["object_id.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_library(
    name = "missing_value",
    hdrs = ["missing_value.h"],
)

cc_library(
    name = "types",
    hdrs = ["types.h"],
    deps = [
        ":dtype",
        ":missing_value",
        ":object_id",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "types_test",
    srcs = ["types_test.cc"],
    deps = [
        ":missing_value",
        ":types",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "object_id_test",
    srcs = ["object_id_test.cc"],
    deps = [
        ":object_id",
        ":stable_fingerprint",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "object_id_benchmarks",
    srcs = ["object_id_benchmarks.cc"],
    deps = [
        ":benchmark_helpers",
        ":object_id",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "dense_source",
    srcs = [
        "dense_source.cc",
    ],
    hdrs = ["dense_source.h"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dtype",
        ":missing_value",
        ":object_id",
        ":types",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:dynamic_annotations",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "dense_source_test",
    srcs = ["dense_source_test.cc"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dense_source",
        ":dtype",
        ":object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/testing:test_operators",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "sparse_source",
    srcs = [
        "sparse_source.cc",
    ],
    hdrs = ["sparse_source.h"],
    deps = [
        ":data_item",
        ":data_slice",
        ":missing_value",
        ":object_id",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "sparse_source_test",
    srcs = ["sparse_source_test.cc"],
    deps = [
        ":data_item",
        ":data_slice",
        ":object_id",
        ":sparse_source",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dense_source_benchmarks",
    srcs = ["dense_source_benchmarks.cc"],
    deps = [
        ":benchmark_helpers",
        ":data_slice",
        ":dense_source",
        ":object_id",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "data_list",
    srcs = ["data_list.cc"],
    hdrs = ["data_list.h"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dtype",
        ":missing_value",
        ":object_id",
        "@com_google_absl//absl/log:check",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "data_list_test",
    srcs = ["data_list_test.cc"],
    deps = [
        ":data_item",
        ":data_list",
        ":data_slice",
        ":missing_value",
        ":object_id",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "dict",
    srcs = ["dict.cc"],
    hdrs = ["dict.h"],
    deps = [
        ":data_item",
        ":missing_value",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "dict_test",
    srcs = ["dict_test.cc"],
    deps = [
        ":data_item",
        ":dict",
        ":missing_value",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dict_benchmarks",
    srcs = ["dict_benchmarks.cc"],
    deps = [
        ":data_item",
        ":dict",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "data_slice",
    srcs = ["data_slice.cc"],
    hdrs = ["data_slice.h"],
    deps = [
        ":data_item",
        ":dtype",
        ":missing_value",
        ":object_id",
        ":types",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "data_slice_test",
    srcs = ["data_slice_test.cc"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dtype",
        ":object_id",
        ":types",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "data_slice_accessors",
    srcs = ["data_slice_accessors.cc"],
    hdrs = ["data_slice_accessors.h"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dense_source",
        ":object_id",
        ":sparse_source",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
    ],
)

cc_test(
    name = "data_slice_accessors_test",
    srcs = ["data_slice_accessors_test.cc"],
    deps = [
        ":data_item",
        ":data_slice",
        ":data_slice_accessors",
        ":dense_source",
        ":object_id",
        ":sparse_source",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_slice_benchmarks",
    srcs = ["data_slice_benchmarks.cc"],
    deps = [
        ":benchmark_helpers",
        ":data_slice",
        ":data_slice_accessors",
        ":dense_source",
        ":object_id",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "triples",
    srcs = ["triples.cc"],
    hdrs = ["triples.h"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":object_id",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "triples_test",
    srcs = ["triples_test.cc"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":object_id",
        ":triples",
        ":uuid_object",
        "//koladata/internal/testing:matchers",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "data_bag",
    srcs = ["data_bag.cc"],
    hdrs = ["data_bag.h"],
    deps = [
        ":data_item",
        ":data_list",
        ":data_slice",
        ":data_slice_accessors",
        ":dense_source",
        ":dict",
        ":dtype",
        ":error_cc_proto",
        ":error_utils",
        ":object_id",
        ":schema_utils",
        ":sparse_source",
        ":uuid_object",
        "//koladata/internal/op_utils:has",
        "//koladata/internal/op_utils:presence_or",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/functional:overload",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/memory",
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
        ":data_item",
        ":data_slice",
        ":object_id",
        ":uuid_object",
        "//koladata/internal/testing:matchers",
        "//koladata/s11n",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_bag_merge_test",
    srcs = ["data_bag_merge_test.cc"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dtype",
        ":object_id",
        ":schema_utils",
        ":uuid_object",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_bag_list_test",
    srcs = ["data_bag_list_test.cc"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_bag_dict_test",
    srcs = ["data_bag_dict_test.cc"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":object_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_bag_schema_test",
    srcs = ["data_bag_schema_test.cc"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dtype",
        ":object_id",
        ":schema_utils",
        ":uuid_object",
        "//koladata/internal/testing:matchers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_bag_benchmarks",
    timeout = "long",  # Times out in bazel.
    srcs = ["data_bag_benchmarks.cc"],
    deps = [
        ":benchmark_helpers",
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dtype",
        ":object_id",
        ":schema_utils",
        ":uuid_object",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "data_item",
    srcs = ["data_item.cc"],
    hdrs = ["data_item.h"],
    deps = [
        ":dtype",
        ":expr_quote_utils",
        ":missing_value",
        ":object_id",
        ":stable_fingerprint",
        ":types",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_double_conversion//:double-conversion",
    ],
)

cc_library(
    name = "benchmark_helpers",
    srcs = ["benchmark_helpers.cc"],
    hdrs = ["benchmark_helpers.h"],
    deps = [
        ":data_item",
        ":data_slice",
        ":object_id",
        "@com_google_arolla//arolla/dense_array",
    ],
)

cc_library(
    name = "expr_quote_utils",
    srcs = ["expr_quote_utils.cc"],
    hdrs = ["expr_quote_utils.h"],
    deps = [
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/serialization",
        "@com_google_arolla//arolla/serialization_codecs:all",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "expr_quote_utils_test",
    srcs = ["expr_quote_utils_test.cc"],
    deps = [
        ":expr_quote_utils",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/testing:test_operators",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stable_fingerprint",
    srcs = ["stable_fingerprint.cc"],
    hdrs = ["stable_fingerprint.h"],
    deps = [
        ":expr_quote_utils",
        ":missing_value",
        ":object_id",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
        "@com_google_cityhash//:cityhash",
    ],
)

cc_library(
    name = "uuid_object",
    srcs = ["uuid_object.cc"],
    hdrs = ["uuid_object.h"],
    deps = [
        ":data_item",
        ":data_slice",
        ":object_id",
        ":stable_fingerprint",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array/ops",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "uuid_object_test",
    srcs = ["uuid_object_test.cc"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dtype",
        ":object_id",
        ":uuid_object",
        "//koladata/internal/testing:matchers",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_item_test",
    srcs = ["data_item_test.cc"],
    deps = [
        ":data_item",
        ":dtype",
        ":object_id",
        ":schema_utils",
        ":uuid_object",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/testing:test_operators",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "stable_fingerprint_test",
    srcs = ["stable_fingerprint_test.cc"],
    deps = [
        ":object_id",
        ":stable_fingerprint",
        ":types",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_item_benchmarks",
    srcs = ["data_item_benchmarks.cc"],
    deps = [
        ":data_item",
        ":object_id",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

##### Building blocks for Schema #####

cc_library(
    name = "dtype",
    srcs = ["dtype.cc"],
    hdrs = ["dtype.h"],
    deps = [
        ":stable_fingerprint",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "dtype_test",
    srcs = ["dtype_test.cc"],
    deps = [
        ":dtype",
        ":stable_fingerprint",
        ":types",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "casting",
    srcs = ["casting.cc"],
    hdrs = ["casting.h"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dtype",
        ":object_id",
        ":schema_utils",
        "//koladata/internal/op_utils:equal",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qexpr/operators/core:lib",
        "@com_google_arolla//arolla/qexpr/operators/strings:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "casting_test",
    srcs = ["casting_test.cc"],
    deps = [
        ":casting",
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dtype",
        ":object_id",
        ":schema_utils",
        "//koladata/internal/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "schema_utils",
    srcs = ["schema_utils.cc"],
    hdrs = ["schema_utils.h"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dtype",
        ":error_cc_proto",
        ":error_utils",
        ":missing_value",
        ":object_id",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:overload",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "schema_utils_test",
    srcs = ["schema_utils_test.cc"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dtype",
        ":error_cc_proto",
        ":error_utils",
        ":object_id",
        ":schema_utils",
        "//koladata/s11n",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "schema_utils_benchmarks",
    srcs = ["schema_utils_benchmarks.cc"],
    deps = [
        ":data_item",
        ":data_slice",
        ":dtype",
        ":object_id",
        ":schema_utils",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "ellipsis",
    srcs = ["ellipsis.cc"],
    hdrs = ["ellipsis.h"],
    deps = [
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)
