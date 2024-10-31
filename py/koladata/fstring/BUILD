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

# Utilities to support f-string like manipulations with Koda objects.

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "fstring_cc_lib",
    srcs = ["fstring.cc"],
    hdrs = ["fstring.h"],
    deps = [
        ":fstring_processor",
        "//koladata/s11n",
        "//py/koladata/exceptions:py_exception_utils",
        "//py/koladata/types:py_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_py_extension(
    name = "fstring_py_ext",
    srcs = ["fstring_module.cc"],
    deps = [":fstring_cc_lib"],
)

py_library(
    name = "fstring",
    srcs = ["fstring.py"],
    deps = [
        ":fstring_py_ext",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
    ],
)

cc_library(
    name = "fstring_processor",
    srcs = ["fstring_processor.cc"],
    hdrs = ["fstring_processor.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/operators",
        "//koladata/s11n",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/eval",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serialization",
        "@com_google_arolla//arolla/serialization_base:base_cc_proto",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "fstring_processor_test",
    srcs = ["fstring_processor_test.cc"],
    deps = [
        ":fstring_processor",
        "//koladata:data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "//py/koladata/operators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/expr/eval",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

py_test(
    name = "fstr_test",
    srcs = ["fstr_test.py"],
    deps = [
        ":fstring",
        "//py/koladata/exceptions",
        "//py/koladata/expr:input_container",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)
