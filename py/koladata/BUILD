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

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = ["//py/koladata:__subpackages__"])

licenses(["notice"])

exports_files(["LICENSE"])

py_library(
    name = "koladata",
    visibility = ["//visibility:public"],
    deps = [":kd"],
)

py_library(
    name = "kd",
    srcs = ["kd.py"],
    visibility = ["//visibility:public"],
    deps = [
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/functions",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/functor:kdf",
        "//py/koladata/functor:tracing_decorator",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:general_eager_ops",
        "//py/koladata/types:list_item",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:schema_constants",
        "//py/koladata/types:schema_item",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "kd_test",
    srcs = ["kd_test.py"],
    deps = [
        ":kd",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/functor:signature_utils",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "benchmarks",
    srcs = ["benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        ":kd",
        "//py/google_benchmark",
        "@com_google_arolla//py/arolla",
    ],
)

cc_test(
    name = "cc_benchmarks",
    srcs = ["cc_benchmarks.cc"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/functor",
        "//koladata/functor:call",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/operators",
        "//koladata/s11n",
        "//py/koladata/operators",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/io",
        "@com_google_arolla//arolla/jagged_shape/array/qexpr",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qexpr",
        "@com_google_arolla//arolla/jagged_shape/dense_array/serialization_codecs",
        "@com_google_arolla//arolla/jagged_shape/expr",
        "@com_google_arolla//arolla/jagged_shape/qexpr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serialization_codecs:all_decoders",
        "@com_google_arolla//arolla/serving",
        "@com_google_arolla//arolla/util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
