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

# Serving tools for Koda.

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_library")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

# Standard dependencies for evaluating Koda functors from C++.
cc_library(
    name = "standard_deps",
    visibility = ["//visibility:public"],
    deps = [
        "//koladata/functor:operators",
        "//koladata/iterables:operators",
        "//koladata/operators",
        "//koladata/s11n",
        "//py/koladata/operators:base_cc_operators",
        "@com_google_arolla//arolla/expr/operators:bootstrap",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/all:operators_on_dense_arrays",
        "@com_google_arolla//arolla/serialization_codecs/dense_array/decoders",
        "@com_google_arolla//arolla/serialization_codecs/generic/decoders",
    ],
    alwayslink = 1,
)

bzl_library(
    name = "serving_bzl",
    srcs = ["serving.bzl"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_arolla//arolla/codegen:utils_bzl",
    ],
)

exports_files(
    [
        "embedded_slices.cc.jinja2",
        "embedded_slices.h.jinja2",
    ],
)

py_library(
    name = "serving_impl",
    srcs = ["serving_impl.py"],
    visibility = ["//visibility:public"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/type_checking",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "serialized_slices",
    srcs = ["serialized_slices.cc"],
    hdrs = ["serialized_slices.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//koladata:data_slice",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serialization",
        "@com_google_arolla//arolla/serialization:riegeli",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "serialized_slices_test",
    srcs = ["serialized_slices_test.cc"],
    deps = [
        ":serialized_slices",
        ":standard_deps",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:test_utils",
        "//koladata/functor",
        "//koladata/functor:call",
        "//koladata/functor:cpp_function_bridge",
        "//koladata/functor:signature_utils",
        "//koladata/operators:lib",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/serialization:riegeli",
        "@com_google_arolla//arolla/serialization_codecs:all",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)
