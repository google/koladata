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

# Tests for kd_ext.ids operators.

load("@rules_python//python:defs.bzl", "py_test")

licenses(["notice"])

py_test(
    name = "ids_with_auto_attributes_test",
    srcs = ["ids_with_auto_attributes_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata:kd_ext",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "auto_id_update_test",
    srcs = ["auto_id_update_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata:kd_ext",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "auto_reference_update_test",
    srcs = ["auto_reference_update_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata:kd_ext",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "auto_id_pointwise_update_test",
    srcs = ["auto_id_pointwise_update_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata:kd_ext",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "auto_reference_pointwise_update_test",
    srcs = ["auto_reference_pointwise_update_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata:kd_ext",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
