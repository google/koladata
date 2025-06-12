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

# Util for Koda operator tests.

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(
    default_testonly = 1,
    default_visibility = [
        "//koladata:internal",
    ],
)

licenses(["notice"])

py_library(
    name = "koda_test_eval",
    srcs = ["koda_test_eval.py"],
    deps = [
        ":data_conversion",
        "//py:python_path",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/operator_tests:backend_test_base_flags",
    ],
)

py_library(
    name = "data_conversion",
    srcs = ["data_conversion.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "qtypes",
    srcs = ["qtypes.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_test_eval_test",
    srcs = ["koda_test_eval_test.py"],
    deps = [
        ":data_conversion",
        ":koda_test_eval",
        "//py:python_path",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/flags",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:flagsaver",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "data_conversion_test",
    srcs = ["data_conversion_test.py"],
    deps = [
        ":data_conversion",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

bzl_library(
    name = "koda_operator_test_def_bzl",
    srcs = ["koda_operator_test_def.bzl"],
    deps = ["@com_google_arolla//py/arolla/operator_tests:arolla_operator_test_def_bzl"],
)
