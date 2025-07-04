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

# Testing utilities.

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = [
    "//py/koladata/testing:__subpackages__",
])

licenses(["notice"])

py_library(
    name = "testing",
    srcs = ["testing.py"],
    visibility = ["//koladata:internal"],
    deps = [
        ":test_utils",
        "//py:python_path",
        "//py/koladata/operators/tests/util:qtypes",
    ],
)

py_library(
    name = "test_utils",
    srcs = ["test_utils.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:ellipsis",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "signature_test_utils",
    srcs = ["signature_test_utils.py"],
    visibility = ["//koladata:internal"],
    deps = [
        ":testing",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "signature_test_utils_test",
    srcs = ["signature_test_utils_test.py"],
    deps = [
        ":signature_test_utils",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "test_utils_test",
    srcs = ["test_utils_test.py"],
    deps = [
        ":test_utils",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:ellipsis",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)
