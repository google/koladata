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

# Functor utilities.

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "expr_to_py",
    srcs = ["expr_to_py.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "expr_to_py_test",
    srcs = ["expr_to_py_test.py"],
    deps = [
        ":expr_to_py",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "fn_to_py",
    srcs = ["fn_to_py.py"],
    deps = [
        ":expr_to_py",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "fn_to_py_test",
    srcs = ["fn_to_py_test.py"],
    deps = [
        ":fn_to_py",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "functor",
    srcs = ["functor.py"],
    deps = [
        ":fn_to_py",
        "//py:python_path",
    ],
)
